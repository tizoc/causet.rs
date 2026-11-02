//! Execute the portable Store, real browser clocks, readiness and driven
//! timers as WebAssembly, rather than just cross-compiling their types.
#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use causet::SessionId;
use causet::hosting::{EventAvailability, EventBus};
use causet::observers::TraceLog;
use causet::{
    Action, ActionMeta, ActionPort, Adapts, Clock, Component, ComponentContext, ComponentRegistry,
    Effect, EffectContext, EffectMeta, Event, ManualClock, Never, Registry, RequestId, Store,
    StoreOptions, SystemClock, Timer, TimerFired, root,
};
use wasm_bindgen_test::wasm_bindgen_test;

struct App;

#[derive(Default)]
struct State {
    pending: Option<RequestId>,
    fired: usize,
    pushed: usize,
}

#[derive(Debug, Action)]
#[causet(type = "web.app.action")]
enum Input {
    Arm,
    Fired(RequestId),
    Pushed,
}

#[derive(Debug, Effect)]
#[causet(type = "web.app.arm")]
struct Arm(RequestId);

#[derive(Debug, Event)]
#[causet(type = "web.app.pushed")]
struct Pushed;

impl Component for App {
    type State = State;
    type Service = Timer;
    type Action = Input;
    type Effect = Arm;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, input: Input, _: &ActionMeta) {
        match input {
            Input::Arm => {
                let request = ctx.new_request();
                ctx.state_mut().pending = Some(request);
                ctx.into_emitter().emit_effect(Arm(request));
            }
            Input::Fired(request) => {
                assert_eq!(ctx.state_mut().pending.take(), Some(request));
                ctx.state_mut().fired += 1;
            }
            Input::Pushed => ctx.state_mut().pushed += 1,
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, arm: Arm, _: &EffectMeta) {
        let reply = ctx.reply::<TimerFired>();
        ctx.service().after(Duration::from_nanos(10), reply, arm.0);
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<TimerFired>();
        r.event::<Pushed>();
    }
}

impl Adapts<TimerFired> for App {
    fn adapt(event: TimerFired, port: &mut ActionPort<'_, Self>) {
        port.emit_action(Input::Fired(event.request));
    }
}

impl Adapts<Pushed> for App {
    fn adapt(_: Pushed, port: &mut ActionPort<'_, Self>) {
        port.emit_action(Input::Pushed);
    }
}

fn protocol() -> std::sync::Arc<causet::Protocol<State, Timer>> {
    let mut registry = Registry::new();
    registry.root::<App>();
    registry.freeze().unwrap()
}

#[wasm_bindgen_test]
fn system_clock_and_store_execute_on_webassembly() {
    let mut clock = SystemClock::new();
    let now = clock.now();
    assert!(
        now > 1_000_000_000_000_000_000,
        "real Unix epoch nanoseconds"
    );
    assert!(clock.now() >= now);

    let protocol = protocol();
    let app = protocol.resolve(&root::<App>()).unwrap();
    let (sink, inbox) = EventBus::new(&protocol, 1);
    let (timer, _) = Timer::driven(SystemClock::new());
    let mut store = Store::new(
        protocol,
        State::default(),
        timer,
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    store.add_observer(TraceLog::new());
    store.dispatch_to(&app, Input::Pushed).unwrap();
    // An event-loop callback shares the driver thread; explicitly select the
    // worker lane for its input and backpressure.
    sink.try_send_to_as_worker(&app, Pushed).unwrap();
    assert!(matches!(
        sink.try_send_to_as_worker(&app, Pushed),
        Err(causet::SendError::Full(_))
    ));
    assert_eq!(store.drain_events(1), Ok(1));
    assert_eq!(store.state().pushed, 2);
    assert!(
        store
            .observer::<TraceLog>()
            .unwrap()
            .entries()
            .iter()
            .all(|entry| entry.time >= now)
    );
}

#[wasm_bindgen_test]
fn readiness_notifies_the_host_and_observes_closure() {
    let protocol = protocol();
    let app = protocol.resolve(&root::<App>()).unwrap();
    let (sink, inbox) = EventBus::new(&protocol, 1);
    let ready = inbox.readiness();
    let wakes = Arc::new(AtomicUsize::new(0));
    let count = wakes.clone();
    let _subscription = ready
        .subscribe(move || {
            count.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    sink.try_send_to_as_worker(&app, Pushed).unwrap();
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert_eq!(ready.current(), Some(EventAvailability::Events));
    drop(inbox);
    assert_eq!(wakes.load(Ordering::SeqCst), 2);
    assert_eq!(ready.current(), Some(EventAvailability::Closed));
}

#[wasm_bindgen_test]
fn driven_timers_deliver_through_the_real_store_without_threads() {
    let clock = ManualClock::new(100);
    let (timer, control) = Timer::driven(clock.clone());
    let protocol = protocol();
    let app = protocol.resolve(&root::<App>()).unwrap();
    let (_, inbox) = EventBus::new(&protocol, 1);
    let mut store = Store::new(
        protocol,
        State::default(),
        timer,
        inbox,
        StoreOptions::with_clock(clock.clone()),
    )
    .unwrap();
    store.dispatch_to(&app, Input::Arm).unwrap();
    assert_eq!(control.next_deadline(), Some(110));
    assert_eq!(control.fire_due(), 0);
    clock.set(110);
    assert_eq!(control.fire_due(), 1);
    assert_eq!(store.readiness().current(), Some(EventAvailability::Events));
    assert_eq!(store.state().fired, 0);
    assert_eq!(store.drain_events(1), Ok(1));
    assert_eq!(store.state().fired, 1);
    assert_eq!(control.next_deadline(), None);
}

#[wasm_bindgen_test]
fn held_events_do_not_become_ready_until_released() {
    let protocol = protocol();
    let app = protocol.resolve(&root::<App>()).unwrap();
    let (sink, inbox, held) = EventBus::held(&protocol, 1, SessionId(1));
    let ready = inbox.readiness();
    let (timer, _) = Timer::simulated(&ManualClock::new(0));
    let mut store = Store::new(
        protocol,
        State::default(),
        timer,
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    sink.try_send_to_as_worker(&app, Pushed).unwrap();
    assert_eq!(ready.current(), None);
    held.release(held.list()[0].id).unwrap();
    assert_eq!(ready.current(), Some(EventAvailability::Events));
    assert_eq!(store.drain_events(1), Ok(1));
    assert_eq!(store.state().pushed, 1);
}
