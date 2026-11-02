//! Store transfer preserves local ownership and the Store lane, including
//! after the Store has already run on its constructing thread. The lane moves
//! with the Store at its first call on the new thread, not before.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::mpsc;
use std::thread;

use causet::SessionId;
use causet::hosting::EventBus;
use causet::{
    Action, ActionMeta, ActionPort, Adapts, Component, ComponentContext, ComponentRegistry, Effect,
    EffectContext, EffectMeta, Event, ManualClock, Never, Registry, SendError, Store, StoreOptions,
    root,
};
use static_assertions::{assert_impl_all, assert_not_impl_any};

assert_impl_all!(Store<Cell<u32>, Cell<u32>>: Send);
assert_not_impl_any!(Store<Rc<()>, ()>: Send);
assert_not_impl_any!(Store<(), Rc<()>>: Send);
assert_not_impl_any!(Store<(), ()>: Sync);

struct Counter;

#[derive(Debug, Action)]
#[causet(type = "send.counter.action")]
enum CounterAction {
    Request,
    Counted,
}

#[derive(Debug, Effect)]
#[causet(type = "send.counter.effect")]
struct Count;

#[derive(Debug, Event)]
#[causet(type = "send.counter.event")]
struct Counted;

impl Component for Counter {
    type State = Cell<u32>;
    type Service = Cell<u32>;
    type Action = CounterAction;
    type Effect = Count;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: CounterAction, _: &ActionMeta) {
        match action {
            CounterAction::Request => ctx.into_emitter().emit_effect(Count),
            CounterAction::Counted => *ctx.state_mut().get_mut() += 1,
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, _: Count, _: &EffectMeta) {
        let service = ctx.service();
        service.set(service.get() + 1);
        ctx.reply::<Counted>().try_send(Counted).unwrap();
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Counted>();
    }
}

impl Adapts<Counted> for Counter {
    fn adapt(_: Counted, port: &mut ActionPort<'_, Self>) {
        port.emit_action(CounterAction::Counted);
    }
}

#[test]
fn a_store_with_send_but_not_sync_values_can_move_before_and_after_execution() {
    let mut registry = Registry::new();
    registry.root::<Counter>();
    let protocol = registry.freeze().unwrap();
    let mount = protocol.resolve(&root::<Counter>()).unwrap();
    // With no worker capacity, a synchronous reply can only succeed in the
    // Store lane. Moving threads must update that lane's owner each time.
    let (_, inbox) = EventBus::with_session(&protocol, 0, SessionId(17));
    let mut store = Store::new(
        protocol,
        Cell::new(0),
        Cell::new(0),
        inbox,
        StoreOptions::with_clock(ManualClock::new(100)),
    )
    .unwrap();
    let mut store = thread::spawn(move || {
        store.dispatch_to(&mount, CounterAction::Request).unwrap();
        assert_eq!(store.drain_events(1), Ok(1));
        store
    })
    .join()
    .unwrap();
    assert_eq!(store.state().get(), 1);
    store.dispatch_to(&mount, CounterAction::Request).unwrap();
    assert_eq!(store.drain_events(1), Ok(1));
    assert_eq!(store.state().get(), 2);
    assert_eq!(store.services().get(), 2);
    assert_eq!(store.session(), SessionId(17));
}

#[test]
fn a_moved_store_keeps_its_old_thread_as_the_store_lane_until_it_runs_on_the_new_one() {
    let mut registry = Registry::new();
    registry.root::<Counter>();
    let protocol = registry.freeze().unwrap();
    let mount = protocol.resolve(&root::<Counter>()).unwrap();
    // With no worker capacity, a send is accepted only in the Store lane.
    let (sink, inbox) = EventBus::with_session(&protocol, 0, SessionId(18));
    let mut store = Store::new(
        protocol,
        Cell::new(0),
        Cell::new(0),
        inbox,
        StoreOptions::with_clock(ManualClock::new(100)),
    )
    .unwrap();
    // It runs here first, which makes this thread its Store lane.
    assert_eq!(store.drain_events(1), Ok(0));

    let (go, run) = mpsc::channel();
    let (ran, done) = mpsc::channel();
    let driver = thread::spawn(move || {
        run.recv().unwrap();
        assert_eq!(store.drain_events(8), Ok(1));
        ran.send(()).unwrap();
        store
    });

    // Moved, but not yet run on the driver: this thread is still the Store
    // lane, so its send bypasses the worker capacity.
    sink.try_send_to(&mount, Counted).unwrap();

    // The driver's first drain takes the lane: this thread is a worker now,
    // and the capacity applies.
    go.send(()).unwrap();
    done.recv().unwrap();
    assert!(matches!(
        sink.try_send_to(&mount, Counted),
        Err(SendError::Full(Counted))
    ));
    let store = driver.join().unwrap();
    assert_eq!(store.state().get(), 1);
}

#[test]
fn local_state_and_services_still_work_without_send_bounds() {
    struct Local;

    impl Component for Local {
        type State = Rc<Cell<u32>>;
        type Service = Rc<Cell<u32>>;
        type Action = CounterAction;
        type Effect = Never;
        type Output = Never;

        fn reduce(mut ctx: ComponentContext<'_, Self>, _: CounterAction, _: &ActionMeta) {
            let state = ctx.state_mut();
            state.set(state.get() + 1);
        }

        fn handle(_: EffectContext<'_, Self>, effect: Never, _: &EffectMeta) {
            match effect {}
        }
    }

    let mut registry = Registry::new();
    registry.root::<Local>();
    let protocol = registry.freeze().unwrap();
    let mount = protocol.resolve(&root::<Local>()).unwrap();
    let (_, inbox) = EventBus::new(&protocol, 0);
    let mut store = Store::new(
        protocol,
        Rc::new(Cell::new(0)),
        Rc::new(Cell::new(0)),
        inbox,
        StoreOptions::with_clock(ManualClock::new(0)),
    )
    .unwrap();
    store.dispatch_to(&mount, CounterAction::Counted).unwrap();
    assert_eq!(store.state().get(), 1);
}
