//! Invariant edges: an Action's kind is read inside the Store's fault
//! boundary, and only once it is admitted or to tell an observer of its
//! refusal; a checker's own bug reports reach observers; a refused Action
//! settles no group; and a failed publication leaves no member behind.

use std::cell::Cell;
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;

use causet::hosting::EventBus;
use causet::observers::{Boundary, BugLog, FaultInfo, Observer, Phase};
use causet::*;
use causet_test::{Group, GroupStep, Node};

#[derive(Debug)]
enum Input {
    Inc,
    Refused,
    PanicKind,
    /// Refused, and its kind panics: a refused Action's kind is never read.
    RefusedKind,
}

impl Message for Input {
    const TYPE: &'static str = "review.invariant.action";
    fn kind(&self) -> &'static str {
        match self {
            Self::Inc => "review.inc",
            Self::Refused => "review.refused",
            Self::PanicKind | Self::RefusedKind => panic!("the kind callback failed"),
        }
    }
}
impl Action for Input {}

struct Counter;

impl Component for Counter {
    type State = u32;
    type Action = Input;
    type Output = Never;
    no_effects!();

    fn is_enabled(_: &u32, action: &Input, _: Timestamp) -> bool {
        !matches!(action, Input::Refused | Input::RefusedKind)
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, _: Input, _: &ActionMeta) {
        *ctx.state_mut() += 1;
    }
}

fn store() -> (Store<u32, ()>, Mount<Counter>) {
    let mut registry = Registry::<u32, ()>::new();
    registry.root::<Counter>();
    let protocol = registry.freeze().unwrap();
    let (_sink, inbox) = EventBus::new(&protocol, 8);
    let mount = protocol.resolve(&root::<Counter>()).unwrap();
    let store = Store::new(
        protocol,
        0u32,
        (),
        inbox,
        StoreOptions::with_clock(ManualClock::new(0)),
    )
    .unwrap();
    (store, mount)
}

#[test]
fn adding_an_invariant_does_not_let_kind_panics_escape_the_store() {
    let (mut ordinary, mount) = store();
    let ordinary_result = panic::catch_unwind(AssertUnwindSafe(|| {
        ordinary.dispatch_to(&mount, Input::PanicKind)
    }));
    assert!(matches!(
        ordinary_result,
        Ok(Err(DispatchError::Faulted(_)))
    ));

    let (mut checked, mount) = store();
    checked.add_invariant("always valid", |_, _| Ok(()));
    let checked_result = panic::catch_unwind(AssertUnwindSafe(|| {
        checked.dispatch_to(&mount, Input::PanicKind)
    }));
    assert!(
        matches!(checked_result, Ok(Err(DispatchError::Faulted(_)))),
        "the kind panic escaped; Store fault: {:?}",
        checked.fault()
    );
}

#[test]
fn invariants_do_not_inspect_the_kind_of_a_refused_action() {
    let (mut ordinary, mount) = store();
    assert_eq!(
        ordinary.dispatch_to(&mount, Input::RefusedKind),
        Ok(DispatchOutcome::Disabled)
    );

    let (mut checked, mount) = store();
    checked.add_invariant("always valid", |_, _| Ok(()));
    assert_eq!(
        checked.dispatch_to(&mount, Input::RefusedKind),
        Ok(DispatchOutcome::Disabled)
    );
    assert!(checked.fault().is_none());
}

/// An observer of no refusal reads no kind; once one is installed, reading
/// the kind is part of telling it, and a kind that panics faults the Store
/// as that observer boundary's fault.
#[test]
fn a_refused_actions_kind_is_read_only_to_tell_an_observer() {
    struct Faults(Vec<Phase>);
    impl Observer for Faults {
        fn fault(&mut self, fault: &FaultInfo<'_>) {
            self.0.push(fault.phase);
        }
    }
    let (mut observed, mount) = store();
    observed.add_observer(Faults(Vec::new()));
    assert!(observed.dispatch_to(&mount, Input::RefusedKind).is_err());
    assert_eq!(
        observed.observer::<Faults>().unwrap().0,
        [Phase::Observer(Boundary::Refused)]
    );
}

#[test]
fn group_checks_do_not_run_for_a_refused_action() {
    let group = Group::new("pair");
    let mut node = Node::new::<Counter>("counter", 0u32, (), SessionId(1), &ManualClock::new(0))
        .publish(&group, |&state| state);
    let calls = Rc::new(Cell::new(0));
    let counted = Rc::clone(&calls);
    group.check("settled roots", move |_, step| {
        if matches!(step, GroupStep::Settled { .. }) {
            counted.set(counted.get() + 1);
        }
        Ok(())
    });
    let mount = node.mount(&root::<Counter>());
    assert_eq!(
        node.dispatch(&mount, Input::Refused),
        DispatchOutcome::Disabled
    );
    assert_eq!(calls.get(), 0, "no root execution settled");
}

#[test]
fn a_bug_report_inside_a_checker_reaches_store_observers() {
    let (mut store, mount) = store();
    store.add_observer(BugLog::new());
    store.add_invariant("diagnostic", |&state, _| {
        if state == 1 {
            bug_condition!("diagnostic from inside a checker");
        }
        Ok(())
    });
    store.dispatch_to(&mount, Input::Inc).unwrap();
    let reports = store.observer::<BugLog>().unwrap().reports();
    assert_eq!(
        reports.len(),
        1,
        "the callback's report was silently discarded"
    );
    // Made while the checker ran, so placed under its phase.
    assert_eq!(
        reports[0].place.as_ref().unwrap().phase,
        Phase::Invariant("diagnostic")
    );
    assert_eq!(reports[0].message, "diagnostic from inside a checker");
}

#[test]
fn failed_publication_does_not_leave_a_ghost_member() {
    let group = Group::new("pair");
    group.check("positive", |views, _| {
        if views.get::<u32>("counter") == Some(&0) {
            Err("zero".into())
        } else {
            Ok(())
        }
    });
    let failed = panic::catch_unwind(AssertUnwindSafe(|| {
        Node::new::<Counter>("counter", 0u32, (), SessionId(1), &ManualClock::new(0))
            .publish(&group, |&state| state)
    }));
    assert!(failed.is_err());
    let replacement = panic::catch_unwind(AssertUnwindSafe(|| {
        Node::new::<Counter>("counter", 1u32, (), SessionId(2), &ManualClock::new(0))
            .publish(&group, |&state| state)
    }));
    assert!(
        replacement.is_ok(),
        "a dropped failed publication still occupies the name"
    );
}
