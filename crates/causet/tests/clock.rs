//! The execution clock: execution IDs count executions from 1, apart from
//! time; execution times are `max(previous, now)`, so they never go back;
//! root Actions are admitted at their ingress time, queued Actions at the
//! time of the most recently executed item.

use std::sync::Arc;

use causet::hosting::EventBus;
use causet::observers::TraceLog;
use causet::{
    Action, ActionMeta, Clock, Component, ComponentContext, DispatchOutcome, ManualClock, Never,
    Protocol, Registry, Store, StoreOptions, Timestamp, no_effects, root,
};

#[derive(Clone, Default, Debug, PartialEq)]
/// The gate component's State.
pub struct GateState {
    /// `Late` and `Ping` are admitted only before this time.
    pub deadline: Timestamp,
    /// Each executed Action's kind and `meta.time`.
    pub ran: Vec<(String, Timestamp)>,
}

#[derive(Debug, Action)]
#[causet(type = "gate.action")]
enum GateAction {
    /// Queues a `Late`.
    Start,
    Late,
    Ping,
    /// Its enabling check panics when admitted after this time.
    PanicAfter(Timestamp),
}

struct Gate;

impl Component for Gate {
    type State = GateState;
    type Action = GateAction;
    type Output = Never;
    no_effects!();

    fn is_enabled(state: &GateState, action: &GateAction, now: Timestamp) -> bool {
        match action {
            GateAction::Start => true,
            GateAction::Late | GateAction::Ping => now < state.deadline,
            GateAction::PanicAfter(limit) => {
                assert!(now <= *limit, "admitted at {now}, after {limit}");
                true
            }
        }
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: GateAction, meta: &ActionMeta) {
        ctx.state_mut()
            .ran
            .push((action.kind().to_string(), meta.time));
        if let GateAction::Start = action {
            ctx.into_emitter().emit_action(GateAction::Late);
        }
    }
}

use causet::Message as _;

/// Moves `step` forward on every reading. The Store's first reading, in
/// `Store::new`, is its anchor.
struct StepClock {
    now: Timestamp,
    step: Timestamp,
}

impl Clock for StepClock {
    fn now(&mut self) -> Timestamp {
        self.now += self.step;
        self.now
    }
}

fn protocol() -> Arc<Protocol<GateState, ()>> {
    let mut registry = Registry::<GateState, ()>::new();
    registry.root::<Gate>();
    registry.freeze().expect("valid protocol")
}

fn store(state: GateState, options: StoreOptions) -> (Store<GateState, ()>, causet::Mount<Gate>) {
    let p = protocol();
    let (_sink, inbox) = EventBus::new(&p, 4);
    let mut store = Store::new(p.clone(), state, (), inbox, options).unwrap();
    store.add_observer(TraceLog::new());
    (store, p.resolve(&root::<Gate>()).unwrap())
}

fn with_clock(clock: impl Clock + 'static) -> StoreOptions {
    StoreOptions::with_clock(clock)
}

fn far() -> GateState {
    GateState {
        deadline: Timestamp::MAX,
        ran: Vec::new(),
    }
}

#[test]
fn ids_count_executions_and_a_standing_clock_gives_equal_times() {
    let (mut s, gate) = store(far(), with_clock(ManualClock::new(1000)));
    s.dispatch_to(&gate, GateAction::Ping).unwrap();
    s.dispatch_to(&gate, GateAction::Ping).unwrap();

    let ids: Vec<_> = s
        .observer::<TraceLog>()
        .unwrap()
        .entries()
        .iter()
        .map(|e| (e.id.0, e.time))
        .collect();
    assert_eq!(ids, [(1, 1000), (2, 1000)]);
}

#[test]
fn times_follow_the_clock_forward_and_never_backwards() {
    let clock = ManualClock::new(1000);
    let (mut s, gate) = store(far(), with_clock(clock.clone()));
    clock.set(5000);
    s.dispatch_to(&gate, GateAction::Ping).unwrap();
    clock.set(10);
    s.dispatch_to(&gate, GateAction::Ping).unwrap();

    let ids: Vec<_> = s
        .observer::<TraceLog>()
        .unwrap()
        .entries()
        .iter()
        .map(|e| (e.id.0, e.time))
        .collect();
    assert_eq!(ids, [(1, 5000), (2, 5000)]);
    assert_eq!(
        s.state().ran,
        [
            ("gate.ping".to_string(), 5000),
            ("gate.ping".to_string(), 5000)
        ]
    );
}

#[test]
fn a_root_action_is_admitted_at_its_ingress_time_and_a_refused_time_is_discarded() {
    let clock = ManualClock::new(1000);
    let state = GateState {
        deadline: 1500,
        ran: Vec::new(),
    };
    let (mut s, gate) = store(state, with_clock(clock.clone()));

    // Admission reads the ingress time (1600), not the previous execution's
    // time (1000): past the deadline, refused.
    clock.set(1600);
    assert_eq!(
        s.dispatch_to(&gate, GateAction::Ping),
        Ok(DispatchOutcome::Disabled)
    );
    assert_eq!(s.executions(), 0);

    // The refused reading took no ID and moved no time: the next execution
    // is the first, at the clock's reading.
    clock.set(1200);
    assert_eq!(
        s.dispatch_to(&gate, GateAction::Ping),
        Ok(DispatchOutcome::Executed)
    );
    let first = &s.observer::<TraceLog>().unwrap().entries()[0];
    assert_eq!((first.id.0, first.time), (1, 1200));
}

#[test]
fn a_queued_action_is_admitted_at_the_time_of_the_last_execution() {
    // Readings: 1000 for the anchor, 1100 at Start's ingress, 1200 when Late
    // starts.
    let state = GateState {
        deadline: 1150,
        ran: Vec::new(),
    };
    let (mut s, gate) = store(
        state,
        with_clock(StepClock {
            now: 900,
            step: 100,
        }),
    );
    s.dispatch_to(&gate, GateAction::Start).unwrap();

    // Late was admitted at Start's time (1100 < 1150), not at the clock's
    // next reading, and then took that reading as its own time.
    assert_eq!(
        s.state().ran,
        [
            ("gate.start".to_string(), 1100),
            ("gate.late".to_string(), 1200)
        ]
    );
}

#[test]
fn a_clock_at_its_end_keeps_its_time_and_executions_go_on() {
    let (mut s, gate) = store(far(), with_clock(ManualClock::new(Timestamp::MAX)));
    let last = GateAction::PanicAfter(Timestamp::MAX);
    assert_eq!(s.dispatch_to(&gate, last), Ok(DispatchOutcome::Executed));
    assert_eq!(
        s.dispatch_to(&gate, GateAction::Ping),
        Ok(DispatchOutcome::Disabled)
    );
    assert_eq!(
        s.dispatch_to(&gate, GateAction::Start),
        Ok(DispatchOutcome::Executed)
    );
    let times: Vec<_> = s
        .observer::<TraceLog>()
        .unwrap()
        .entries()
        .iter()
        .map(|e| (e.id.0, e.time))
        .collect();
    // Ping, and the Late that Start queues, are refused: `now < deadline`
    // cannot hold at the end of time. Neither takes an ID.
    assert_eq!(times, [(1, Timestamp::MAX), (2, Timestamp::MAX)]);
}

#[test]
fn the_system_clock_reads_the_real_time_and_its_offset() {
    use causet::{Clock, ClockOffset, SystemClock};
    use std::time::{SystemTime, UNIX_EPOCH};

    let wall = || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    };
    let second = 1_000_000_000;
    let before = wall();
    let reading = SystemClock::new().now();
    assert!(
        reading >= before && reading <= wall() + second,
        "{reading} not near {before}"
    );

    // The application corrects it as it learns the machine's error.
    let offset = ClockOffset::default();
    let mut clock = SystemClock::with_offset(offset.clone());
    offset.set(3_600 * second as i64);
    let ahead = clock.now();
    assert!(
        ahead >= before + 3_600 * second,
        "the offset applies to every reading"
    );
}
