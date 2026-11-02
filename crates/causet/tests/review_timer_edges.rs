//! Timer edges found in review: overdue timers keep their deadline order
//! after the worker lane was full.

mod common;

use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use causet::hosting::EventBus;
use causet::observers::{BusDepths, Observer};
use causet::*;

struct Alarms;

#[derive(Default)]
struct AlarmState {
    pending: BTreeMap<RequestId, String>,
    fired: Vec<String>,
}

#[derive(Debug, Action)]
#[causet(type = "review.alarm.action")]
enum AlarmAction {
    Arm { tag: String, after_ms: u64 },
    Fired { request: RequestId },
}

#[derive(Debug, Effect)]
#[causet(type = "review.alarm.effect")]
struct Arm {
    request: RequestId,
    after_ms: u64,
}

impl Component for Alarms {
    type State = AlarmState;
    type Service = Timer;
    type Action = AlarmAction;
    type Effect = Arm;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: AlarmAction, _: &ActionMeta) {
        match action {
            AlarmAction::Arm { tag, after_ms } => {
                let request = ctx.new_request();
                ctx.state_mut().pending.insert(request, tag);
                ctx.into_emitter().emit_effect(Arm { request, after_ms });
            }
            AlarmAction::Fired { request } => {
                let tag = ctx.state_mut().pending.remove(&request).unwrap();
                ctx.state_mut().fired.push(tag);
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, arm: Arm, _: &EffectMeta) {
        let reply = ctx.reply::<TimerFired>();
        ctx.service()
            .after(Duration::from_millis(arm.after_ms), reply, arm.request);
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<TimerFired>();
    }
}

impl Adapts<TimerFired> for Alarms {
    fn adapt(event: TimerFired, out: &mut ActionPort<'_, Self>) {
        out.emit_action(AlarmAction::Fired {
            request: event.request,
        });
    }
}

struct CountFull(Arc<AtomicU64>);

impl Observer for CountFull {
    fn bus(&mut self, depths: &BusDepths) {
        self.0.fetch_add(depths.rejected_full, Ordering::Relaxed);
    }
}

#[test]
fn overdue_timers_keep_deadline_order_after_the_worker_lane_was_full() {
    let mut registry = Registry::<AlarmState, Timer>::new();
    registry.root::<Alarms>();
    let p = registry.freeze().unwrap();
    let mount = p.resolve(&root::<Alarms>()).unwrap();
    let (sink, inbox) = EventBus::new(&p, 1);
    let mut store = Store::new(
        p,
        AlarmState::default(),
        Timer::new(),
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    let arm = |store: &mut Store<AlarmState, Timer>, tag: &str, after_ms| {
        store
            .dispatch_to(
                &mount,
                AlarmAction::Arm {
                    tag: tag.into(),
                    after_ms,
                },
            )
            .unwrap();
    };
    arm(&mut store, "filler", 60_000);
    let request = *store.state().pending.keys().next().unwrap();
    thread::spawn(move || sink.try_send_to(&mount, TimerFired { request }).unwrap())
        .join()
        .unwrap();
    arm(&mut store, "blocker", 0);

    // Observe a failed send, without removing any Event, so the timer worker
    // is known to be blocked delivering "blocker" before more timers arrive.
    let rejected = Arc::new(AtomicU64::new(0));
    store.add_observer(CountFull(Arc::clone(&rejected)));
    let bound = Instant::now() + Duration::from_secs(2);
    while rejected.load(Ordering::Relaxed) == 0 {
        store.drain_events(0).unwrap();
        assert!(
            Instant::now() < bound,
            "the timer did not wait on the full bus"
        );
        thread::sleep(Duration::from_millis(1));
    }

    arm(&mut store, "late", 100);
    arm(&mut store, "early", 0);
    thread::sleep(Duration::from_millis(150));

    let bound = Instant::now() + Duration::from_secs(2);
    while store.state().fired.len() < 4 {
        store.drain_events(1).unwrap();
        assert!(
            Instant::now() < bound,
            "timers did not finish: {:?}",
            store.state().fired
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(store.state().fired, ["filler", "blocker", "early", "late"]);
}

/// A deadline years off leaves the timer working: a wait that long must not
/// end the timer's thread (the OCaml port's `Unix.select` refused one over
/// 10^8 seconds on macOS, and no later timer fired).
#[test]
fn a_timer_years_off_leaves_the_timer_working() {
    let mut registry = Registry::<AlarmState, Timer>::new();
    registry.root::<Alarms>();
    let p = registry.freeze().unwrap();
    let mount = p.resolve(&root::<Alarms>()).unwrap();
    let (_sink, inbox) = EventBus::new(&p, 16);
    let mut store = Store::new(
        p,
        AlarmState::default(),
        Timer::new(),
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    for (tag, after_ms) in [("in five years", 5 * 365 * 86_400 * 1_000), ("soon", 10)] {
        store
            .dispatch_to(
                &mount,
                AlarmAction::Arm {
                    tag: tag.into(),
                    after_ms,
                },
            )
            .unwrap();
        thread::sleep(Duration::from_millis(20));
    }
    let bound = Instant::now() + Duration::from_secs(2);
    while store.state().fired.is_empty() {
        store.drain_events(16).unwrap();
        assert!(Instant::now() < bound, "the timer stopped firing");
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(store.state().fired, ["soon"]);
}
