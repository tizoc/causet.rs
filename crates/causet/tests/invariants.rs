//! Invariants on the Store: when a checker runs, what it is told, and how a
//! violation and a checker's own panic are reported without stopping the
//! Store.

use std::sync::{Arc, Mutex};

use causet::hosting::EventBus;
use causet::invariants::InvariantStep;
use causet::observers::{BugLog, BugReport, Category, Phase};
use causet::*;

#[derive(Debug, Action)]
#[causet(type = "tally.action")]
enum TallyAction {
    /// Refused when `n` is 0.
    Add(u64),
    Boom,
}

#[derive(Debug, Event)]
#[causet(type = "tally.ping")]
struct Ping(u64);

struct Tally;

impl Component for Tally {
    type State = u64;
    type Action = TallyAction;
    type Output = Never;
    no_effects!();

    fn is_enabled(_: &u64, action: &TallyAction, _: Timestamp) -> bool {
        !matches!(action, TallyAction::Add(0))
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: TallyAction, _: &ActionMeta) {
        match action {
            TallyAction::Add(n) => *ctx.state_mut() += n,
            TallyAction::Boom => {
                *ctx.state_mut() = 999; // half changed, then the fault
                panic!("boom");
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Ping>();
    }
}

impl Adapts<Ping> for Tally {
    fn adapt(Ping(n): Ping, port: &mut ActionPort<'_, Self>) {
        port.emit_action(TallyAction::Add(n));
    }
}

struct Fixture {
    store: Store<u64, ()>,
    sink: EventSink,
    tally: Mount<Tally>,
}

fn fixture() -> Fixture {
    let mut registry = Registry::<u64, ()>::new();
    registry.root::<Tally>();
    let protocol = registry.freeze().unwrap();
    let (sink, inbox) = EventBus::new(&protocol, 16);
    let options = StoreOptions::with_clock(ManualClock::new(1_000));
    let mut store = Store::new(protocol.clone(), 0, (), inbox, options).unwrap();
    store.add_observer(BugLog::new());
    Fixture {
        store,
        sink,
        tally: protocol.resolve(&root::<Tally>()).unwrap(),
    }
}

/// Every step a checker saw, in words.
fn recording(
    seen: &Arc<Mutex<Vec<String>>>,
) -> impl FnMut(&u64, &InvariantStep<'_>) -> Result<(), String> + Send + 'static {
    let seen = Arc::clone(seen);
    move |state, step| {
        let step = match step {
            InvariantStep::Added => "added".to_owned(),
            InvariantStep::Restored => "restored".to_owned(),
            InvariantStep::Settled {
                category,
                kind,
                path,
                ..
            } => format!("{category:?} {kind} at {path}"),
        };
        seen.lock().unwrap().push(format!("{step}: {state}"));
        Ok(())
    }
}

fn reports(store: &Store<u64, ()>) -> Vec<BugReport> {
    store.observer::<BugLog>().unwrap().reports().to_vec()
}

#[test]
fn a_checker_runs_when_added_and_after_each_settled_root_input_only() {
    let mut f = fixture();
    let seen = Arc::new(Mutex::new(Vec::new()));
    f.store.add_invariant("records", recording(&seen));

    f.store.dispatch_to(&f.tally, TallyAction::Add(2)).unwrap();
    assert_eq!(
        f.store.dispatch_to(&f.tally, TallyAction::Add(0)),
        Ok(DispatchOutcome::Disabled)
    );
    assert_eq!(f.store.drain_events(8), Ok(0)); // an empty drain
    f.sink.try_send_to(&f.tally, Ping(3)).unwrap();
    f.sink.try_send_to(&f.tally, Ping(4)).unwrap();
    assert_eq!(f.store.drain_events(8), Ok(2)); // one check per Event

    assert_eq!(
        *seen.lock().unwrap(),
        [
            "added: 0",
            "Action tally.add at /: 2",
            "Event tally.ping at /: 5",
            "Event tally.ping at /: 9",
        ]
        .map(String::from)
    );
}

#[test]
fn a_settled_step_names_its_root_input() {
    let mut f = fixture();
    let steps = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&steps);
    f.store.add_invariant("steps", move |_, step| {
        if let InvariantStep::Settled {
            execution,
            category,
            time,
            ..
        } = *step
        {
            log.lock().unwrap().push((execution, category, time));
        }
        Ok(())
    });
    f.store.dispatch_to(&f.tally, TallyAction::Add(1)).unwrap();
    let [(execution, category, time)] = steps.lock().unwrap()[..] else {
        panic!()
    };
    assert_eq!(
        (execution, category, time),
        (ExecutionId(1), Category::Action, 1_000)
    );
}

#[test]
fn no_checker_runs_after_a_fault() {
    let mut f = fixture();
    let seen = Arc::new(Mutex::new(Vec::new()));
    f.store.add_invariant("records", recording(&seen));
    assert!(matches!(
        f.store.dispatch_to(&f.tally, TallyAction::Boom),
        Err(DispatchError::Faulted(_))
    ));
    assert_eq!(
        *seen.lock().unwrap(),
        ["added: 0"].map(String::from),
        "never on the half-changed State"
    );
}

#[test]
fn a_violation_is_a_bug_report_and_the_store_goes_on() {
    let mut f = fixture();
    let line = line!() + 1;
    f.store.add_invariant("below ten", |&n, _| {
        if n < 10 {
            Ok(())
        } else {
            Err(format!("{n} is not below ten"))
        }
    });
    f.store.dispatch_to(&f.tally, TallyAction::Add(12)).unwrap();
    f.store.dispatch_to(&f.tally, TallyAction::Add(1)).unwrap();

    let reports = reports(&f.store);
    assert_eq!(reports.len(), 2, "one per root input after which it fails");
    let report = &reports[0];
    let place = report.place.as_ref().unwrap();
    assert_eq!(place.phase, Phase::Invariant("below ten"));
    assert_eq!(place.at, Some(ExecutionId(1)));
    assert_eq!(
        (report.message.as_str(), report.line),
        ("12 is not below ten", line)
    );
    assert!(report.file.ends_with("invariants.rs"), "{}", report.file);
    assert_eq!(*f.store.state(), 13);
}

#[test]
fn a_checker_that_panics_is_reported_apart_and_disabled() {
    let mut f = fixture();
    let calls = Arc::new(Mutex::new(0));
    let counted = Arc::clone(&calls);
    f.store.add_invariant("fragile", move |&n, _| {
        *counted.lock().unwrap() += 1;
        assert!(
            causet::hosting::panic_is_ignored(),
            "the Store ignores a checker's panic"
        );
        if n > 3 {
            panic!("cannot read {n}");
        }
        Ok(())
    });
    f.store.dispatch_to(&f.tally, TallyAction::Add(5)).unwrap();
    f.store.dispatch_to(&f.tally, TallyAction::Add(1)).unwrap();

    let reports = reports(&f.store);
    let [report] = &reports[..] else {
        panic!("{reports:?}")
    };
    assert_eq!(
        report.place.as_ref().unwrap().phase,
        Phase::InvariantPanic("fragile")
    );
    assert!(
        report.message.contains("cannot read 5"),
        "{}",
        report.message
    );
    assert_eq!(
        *calls.lock().unwrap(),
        2,
        "added, then the panic; disabled after it"
    );
}

#[test]
fn invariants_move_into_the_next_store_with_their_memory() {
    let mut f = fixture();
    let seen = Arc::new(Mutex::new(Vec::new()));
    f.store.add_invariant("records", recording(&seen));
    let mut most = 0;
    f.store.add_invariant("never decreases", move |&n, step| {
        if *step == InvariantStep::Restored {
            most = 0; // this application restores from scratch
        }
        let broken = n < most;
        most = most.max(n);
        if broken {
            Err(format!("{n} after {most}"))
        } else {
            Ok(())
        }
    });
    f.store.dispatch_to(&f.tally, TallyAction::Add(7)).unwrap();

    let invariants = f.store.take_invariants();
    assert_eq!(invariants.len(), 2);
    drop(f);
    let mut g = fixture();
    g.store.restore_invariants(invariants);
    g.store.dispatch_to(&g.tally, TallyAction::Add(1)).unwrap();

    assert_eq!(
        seen.lock().unwrap()[2..],
        ["restored: 0", "Action tally.add at /: 1"].map(String::from)
    );
    assert!(
        reports(&g.store).is_empty(),
        "the restored State is told apart"
    );
}
