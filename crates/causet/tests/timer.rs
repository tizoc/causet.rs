//! The timer Service: `TimerFired` through the bus, in deadline order, first
//! in first out at equal deadlines, and nothing left running once the Store
//! and its Services drop.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use causet::hosting::EventBus;
use causet::{
    Action, ActionMeta, ActionPort, Adapts, Component, ComponentContext, ComponentRegistry, Effect,
    EffectContext, EffectMeta, ManualClock, Mount, Never, Protocol, Registry, RequestId, Store,
    StoreOptions, Timer, TimerFired, root,
};

struct Alarms;

#[derive(Default)]
struct AlarmState {
    tags: BTreeMap<RequestId, String>,
    fired: Vec<String>,
}

#[derive(Debug, Action)]
#[causet(type = "alarms.action")]
enum AlarmAction {
    Set { tag: String, after_ms: u64 },
    Fired { request: RequestId },
}

#[derive(Debug, Effect)]
#[causet(type = "alarms.effect")]
enum AlarmEffect {
    Arm { request: RequestId, after_ms: u64 },
}

impl Component for Alarms {
    type State = AlarmState;
    type Service = Timer;
    type Action = AlarmAction;
    type Effect = AlarmEffect;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: AlarmAction, _: &ActionMeta) {
        match action {
            AlarmAction::Set { tag, after_ms } => {
                let request = ctx.new_request();
                ctx.state_mut().tags.insert(request, tag);
                ctx.into_emitter()
                    .emit_effect(AlarmEffect::Arm { request, after_ms });
            }
            AlarmAction::Fired { request } => {
                let tag = ctx
                    .state_mut()
                    .tags
                    .remove(&request)
                    .expect("an armed alarm");
                ctx.state_mut().fired.push(tag);
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: AlarmEffect, _: &EffectMeta) {
        let AlarmEffect::Arm { request, after_ms } = effect;
        let reply = ctx.reply::<TimerFired>();
        ctx.service()
            .after(Duration::from_millis(after_ms), reply, request);
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<TimerFired>();
    }
}

impl Adapts<TimerFired> for Alarms {
    fn adapt(fired: TimerFired, port: &mut ActionPort<'_, Self>) {
        port.emit_action(AlarmAction::Fired {
            request: fired.request,
        });
    }
}

fn protocol() -> Arc<Protocol<AlarmState, Timer>> {
    let mut registry = Registry::<AlarmState, Timer>::new();
    registry.root::<Alarms>();
    registry.freeze().unwrap()
}

fn store() -> (Store<AlarmState, Timer>, Mount<Alarms>) {
    let p = protocol();
    let (_sink, inbox) = EventBus::new(&p, 16);
    let store = Store::new(
        p.clone(),
        AlarmState::default(),
        Timer::new(),
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    (store, p.resolve(&root::<Alarms>()).unwrap())
}

fn set(store: &mut Store<AlarmState, Timer>, alarms: &Mount<Alarms>, tag: &str, after_ms: u64) {
    store
        .dispatch_to(
            alarms,
            AlarmAction::Set {
                tag: tag.into(),
                after_ms,
            },
        )
        .unwrap();
}

#[test]
fn timers_fire_in_deadline_order_and_equal_deadlines_in_the_order_set() {
    let (mut s, alarms) = store();
    // Deadlines are taken when each timer is set: "late" stays last only if
    // the next two are set within 230 ms, which leaves room for slow runs
    // (Miri takes tens of milliseconds per dispatch).
    set(&mut s, &alarms, "late", 250);
    set(&mut s, &alarms, "first", 20);
    set(&mut s, &alarms, "second", 20);

    let deadline = Instant::now() + Duration::from_secs(5);
    while s.state().fired.len() < 3 {
        assert!(
            Instant::now() < deadline,
            "timers did not fire: {:?}",
            s.state().fired
        );
        if s.drain_events(16).unwrap() == 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }
    assert_eq!(s.state().fired, ["first", "second", "late"]);
}

#[test]
fn dropping_the_store_stops_the_timer_with_alarms_still_pending() {
    let (mut s, alarms) = store();
    set(&mut s, &alarms, "never", 60_000);

    let started = Instant::now();
    drop(s);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the timer thread held up the drop"
    );
}

#[test]
fn a_simulated_timer_fires_only_when_its_control_fires_it_on_the_shared_clock() {
    const MS: u64 = 1_000_000;
    let clock = ManualClock::new(1_000 * MS);
    let (timer, control) = Timer::simulated(&clock);
    let p = protocol();
    let (_sink, inbox) = EventBus::new(&p, 16);
    let options = StoreOptions::with_clock(clock.clone());
    let mut s = Store::new(p.clone(), AlarmState::default(), timer, inbox, options).unwrap();
    let alarms = p.resolve(&root::<Alarms>()).unwrap();
    set(&mut s, &alarms, "late", 250);
    set(&mut s, &alarms, "first", 20);
    set(&mut s, &alarms, "second", 20);
    assert_eq!(
        (control.pending(), control.next_deadline()),
        (3, Some(1_020 * MS))
    );

    // No time passes on its own.
    assert_eq!(control.fire_due(), 0);
    clock.advance(20 * MS);
    assert_eq!(control.fire_due(), 2);
    assert_eq!(s.drain_events(16), Ok(2));
    assert_eq!(s.state().fired, ["first", "second"]);
    assert_eq!(control.next_deadline(), Some(1_250 * MS));

    clock.advance(230 * MS);
    assert_eq!(control.fire_due(), 1);
    assert_eq!(s.drain_events(16), Ok(1));
    assert_eq!(s.state().fired, ["first", "second", "late"]);
    assert_eq!((control.pending(), control.next_deadline()), (0, None));
}

#[test]
fn a_host_can_drive_timers_on_its_own_clock_without_a_thread() {
    use causet::hosting::EventAvailability;
    use causet::{Clock, Timestamp};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct HostClock(Arc<AtomicU64>);
    impl Clock for HostClock {
        fn now(&mut self) -> Timestamp {
            self.0.load(Ordering::Relaxed)
        }
    }

    let now = Arc::new(AtomicU64::new(1_000_000));
    let (timer, control) = Timer::driven(HostClock(now.clone()));
    let p = protocol();
    let alarms = p.resolve(&root::<Alarms>()).unwrap();
    let (_, inbox) = EventBus::new(&p, 1);
    let mut store = Store::new(
        p,
        AlarmState::default(),
        timer,
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    let ready = store.readiness();
    set(&mut store, &alarms, "later", 20);
    set(&mut store, &alarms, "first", 10);
    assert_eq!(control.next_deadline(), Some(11_000_000));
    assert_eq!(control.fire_due(), 0);
    now.store(21_000_000, Ordering::Relaxed);
    // Both are due, but capacity holds back the second without reordering.
    assert_eq!(control.fire_due(), 1);
    assert_eq!(control.next_deadline(), Some(21_000_000));
    assert_eq!(ready.current(), Some(EventAvailability::Events));
    assert!(
        store.state().fired.is_empty(),
        "only a drain runs the adapter"
    );
    store.drain_events(1).unwrap();
    assert_eq!(store.state().fired, ["first"]);
    assert_eq!(control.fire_due(), 1);
    store.drain_events(1).unwrap();
    assert_eq!(store.state().fired, ["first", "later"]);
    assert_eq!(control.next_deadline(), None);
    set(&mut store, &alarms, "cancelled on drop", 10);
    drop(store);
    assert_eq!(control.pending(), 0);
    assert_eq!(control.fire_due(), 0);
}
