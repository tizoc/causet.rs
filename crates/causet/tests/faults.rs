//! A panic in the work an Effect's `dispatch` runs faults the Store as it
//! leaves that work, whether or not the handler catches it: the fault is the
//! panic's, the bus closes, and nothing else in the root input runs.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use causet::hosting::{EventBus, panic_is_ignored};
use causet::observers::{FaultInfo, Observer, Phase, TraceLog};
use causet::*;

#[derive(Debug, Action)]
#[causet(type = "catcher.action")]
enum CatcherAction {
    /// Queues the Effect `then`, then an Action and an Effect after it.
    Start(Then),
    /// Queues the Effect `Catch` from a nested level.
    Nested,
    Note(u32),
    Boom,
}

#[derive(Debug, Clone, Copy)]
enum Then {
    /// A handler that catches a dispatch's panic and carries on.
    Catch,
    /// A handler that dispatches `Nested`, whose Effect catches the panic,
    /// and catches what comes out of its own dispatch.
    CatchNested,
    /// A handler that catches the panic, then panics with its own message.
    CatchThenPanic,
}

#[derive(Debug, Effect)]
#[causet(type = "catcher.effect")]
enum CatcherEffect {
    Run(Then),
    After,
}

#[derive(Debug, Event)]
#[causet(type = "catcher.ping")]
struct Ping;

/// What the handlers saw.
#[derive(Default)]
struct Seen {
    caught: Vec<String>,
    after_catch: Vec<DispatchOutcome>,
    ignored_after_catch: Vec<bool>,
    reply: Option<bool>,
    after_ran: bool,
}

struct Catcher;

impl Component for Catcher {
    type State = Vec<u32>;
    type Service = Seen;
    type Action = CatcherAction;
    type Effect = CatcherEffect;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: CatcherAction, _: &ActionMeta) {
        match action {
            CatcherAction::Start(then) => {
                let mut out = ctx.into_emitter();
                out.emit_effect(CatcherEffect::Run(then));
                out.emit_action(CatcherAction::Note(99));
                out.emit_effect(CatcherEffect::After);
            }
            CatcherAction::Nested => ctx
                .into_emitter()
                .emit_effect(CatcherEffect::Run(Then::Catch)),
            CatcherAction::Note(n) => ctx.state_mut().push(n),
            CatcherAction::Boom => panic!("boom"),
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: CatcherEffect, _: &EffectMeta) {
        let then = match effect {
            CatcherEffect::After => {
                ctx.service().after_ran = true;
                return;
            }
            CatcherEffect::Run(then) => then,
        };
        assert_eq!(
            ctx.dispatch(CatcherAction::Note(1)),
            DispatchOutcome::Executed
        );
        let inner = match then {
            Then::CatchNested => CatcherAction::Nested,
            Then::Catch | Then::CatchThenPanic => CatcherAction::Boom,
        };
        let caught = catch_unwind(AssertUnwindSafe(|| ctx.dispatch(inner)));
        let message = match caught {
            Ok(outcome) => format!("returned {outcome:?}"),
            Err(payload) => payload
                .downcast_ref::<&str>()
                .map_or("another payload", |m| m)
                .to_owned(),
        };
        let after = ctx.dispatch(CatcherAction::Note(2));
        let reply = ctx.reply::<Ping>().try_send(Ping);
        let seen = ctx.service();
        seen.caught.push(message);
        seen.after_catch.push(after);
        seen.ignored_after_catch.push(panic_is_ignored());
        seen.reply = Some(matches!(reply, Err(SendError::Closed(_))));
        if let Then::CatchThenPanic = then {
            panic!("the handler's own panic");
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Ping>();
    }
}

impl Adapts<Ping> for Catcher {
    fn adapt(_: Ping, port: &mut ActionPort<'_, Self>) {
        port.emit_action(CatcherAction::Start(Then::Catch));
    }
}

/// Every fault, with its phase and execution.
type Faults = Arc<Mutex<Vec<(Phase, Option<ExecutionId>, String)>>>;

struct Watcher(Faults);

impl Observer for Watcher {
    fn fault(&mut self, fault: &FaultInfo<'_>) {
        let entry = (fault.phase, fault.at, fault.message.to_owned());
        self.0.lock().unwrap().push(entry);
    }
}

struct Fixture {
    store: Store<Vec<u32>, Seen>,
    sink: EventSink,
    mount: Mount<Catcher>,
    faults: Faults,
}

fn fixture() -> Fixture {
    let mut registry = Registry::<Vec<u32>, Seen>::new();
    registry.root::<Catcher>();
    let protocol = registry.freeze().unwrap();
    let (sink, inbox) = EventBus::new(&protocol, 16);
    let options = StoreOptions::with_clock(ManualClock::new(1_000));
    let mut store = Store::new(
        Arc::clone(&protocol),
        Vec::new(),
        Seen::default(),
        inbox,
        options,
    )
    .unwrap();
    let faults = Faults::default();
    store.add_observer(TraceLog::new());
    store.add_observer(Watcher(Arc::clone(&faults)));
    let mount = protocol.resolve(&root::<Catcher>()).unwrap();
    Fixture {
        store,
        sink,
        mount,
        faults,
    }
}

impl Fixture {
    /// Every execution started, as (kind, depth).
    fn started(&self) -> Vec<(&'static str, u32)> {
        let trace = self.store.observer::<TraceLog>().unwrap();
        trace.entries().iter().map(|e| (e.kind, e.depth)).collect()
    }

    /// The execution of the Action that panicked.
    fn boom(&self) -> ExecutionId {
        let trace = self.store.observer::<TraceLog>().unwrap();
        trace
            .entries()
            .iter()
            .find(|e| e.kind == "catcher.boom")
            .unwrap()
            .id
    }
}

#[test]
fn a_panic_a_handler_catches_from_its_dispatch_faults_the_store() {
    let mut f = fixture();
    let result = f
        .store
        .dispatch_to(&f.mount, CatcherAction::Start(Then::Catch));

    // The root input returns the fault: the panic's message, at its place.
    assert_eq!(result, Err(DispatchError::Faulted("boom".into())));
    assert_eq!(f.store.fault(), Some("boom"));
    let boom = f.boom();
    assert_eq!(
        *f.faults.lock().unwrap(),
        [(Phase::Reducer, Some(boom), "boom".to_owned())],
        "observers are told once"
    );

    // The handler caught the panic, then found the Store faulted: its
    // dispatch ran nothing, its reply found the bus closed, and its panics
    // counted as ignored.
    let seen = f.store.services();
    assert_eq!(seen.caught, ["boom"]);
    assert_eq!(seen.after_catch, [DispatchOutcome::Faulted]);
    assert_eq!(seen.ignored_after_catch, [true]);
    assert_eq!(seen.reply, Some(true));
    assert!(
        !panic_is_ignored(),
        "no longer, once the root input returned"
    );

    // Nothing ran after the panic: not the handler's dispatch, nor what was
    // queued after its Effect. What ran before has its own depth.
    assert_eq!(*f.store.state(), [1]);
    assert!(!f.store.services().after_ran);
    assert_eq!(
        f.started(),
        [
            ("catcher.start", 1),
            ("catcher.run", 2),
            ("catcher.note", 3),
            ("catcher.boom", 3),
        ]
    );

    // Later calls are refused as faulted, and the bus is closed.
    let faulted = Err(DispatchError::Faulted("boom".into()));
    assert_eq!(
        f.store.dispatch_to(&f.mount, CatcherAction::Note(3)),
        faulted
    );
    assert_eq!(
        f.store.drain_events(16),
        Err(DispatchError::Faulted("boom".into()))
    );
    assert!(matches!(
        f.sink.try_send_to(&f.mount, Ping),
        Err(SendError::Closed(_))
    ));
    assert_eq!(f.faults.lock().unwrap().len(), 1);
}

#[test]
fn every_level_that_catches_it_finds_the_store_faulted() {
    let mut f = fixture();
    let result = f
        .store
        .dispatch_to(&f.mount, CatcherAction::Start(Then::CatchNested));
    assert_eq!(result, Err(DispatchError::Faulted("boom".into())));
    let boom = f.boom();
    assert_eq!(
        *f.faults.lock().unwrap(),
        [(Phase::Reducer, Some(boom), "boom".to_owned())]
    );
    // The inner handler caught the panic; the outer one caught what ended
    // the rest of the inner root input's work, not the panic again.
    let seen = f.store.services();
    assert_eq!(seen.caught, ["boom", "another payload"]);
    assert_eq!(
        seen.after_catch,
        [DispatchOutcome::Faulted, DispatchOutcome::Faulted]
    );
    assert_eq!(*f.store.state(), [1, 1]);
    assert_eq!(
        f.started(),
        [
            ("catcher.start", 1),
            ("catcher.run", 2),
            ("catcher.note", 3),
            ("catcher.nested", 3),
            ("catcher.run", 4),
            ("catcher.note", 5),
            ("catcher.boom", 5),
        ]
    );
    assert!(!panic_is_ignored());
}

#[test]
fn the_first_panic_is_the_fault() {
    let mut f = fixture();
    let result = f
        .store
        .dispatch_to(&f.mount, CatcherAction::Start(Then::CatchThenPanic));
    assert_eq!(result, Err(DispatchError::Faulted("boom".into())));
    let boom = f.boom();
    assert_eq!(
        *f.faults.lock().unwrap(),
        [(Phase::Reducer, Some(boom), "boom".to_owned())]
    );
    assert!(!panic_is_ignored());
}

#[test]
fn an_event_s_root_input_returns_the_fault_too() {
    let mut f = fixture();
    f.store.drain_events(0).unwrap(); // this thread owns the Store lane
    f.sink.try_send_to(&f.mount, Ping).unwrap();
    f.sink.try_send_to(&f.mount, Ping).unwrap();
    assert_eq!(
        f.store.drain_events(16),
        Err(DispatchError::Faulted("boom".into()))
    );
    // The second Event never ran.
    assert_eq!(*f.store.state(), [1]);
    assert_eq!(
        f.started(),
        [
            ("catcher.ping", 1),
            ("catcher.start", 2),
            ("catcher.run", 3),
            ("catcher.note", 4),
            ("catcher.boom", 4),
        ]
    );
    assert_eq!(f.faults.lock().unwrap().len(), 1);
    assert!(!panic_is_ignored());
}
