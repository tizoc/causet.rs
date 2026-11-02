//! Generation edges: the final replay starts a fresh run, a saved case keeps
//! its step limit, and a fault keeps the faulting code's place when something
//! the Store notifies panics too, before the fault or after it, or the
//! application caught a panic of its own, first or in a destructor while the
//! fault unwinds.

use std::any::Any;
use std::panic::{self, AssertUnwindSafe};

use causet::observers::{BugReport, FaultInfo, Observer};
use causet::*;
use causet_test::{Config, Node, Rng, check, search};

#[derive(Debug, Action)]
#[causet(type = "generation.edge")]
enum Input {
    Set(u8),
    CrashA,
    CrashB,
    ReportThenCrash(bool),
    CaughtThenCrash(bool),
    CleanupThenCrash(bool),
}

struct Counter;

impl Component for Counter {
    type State = u8;
    type Action = Input;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, input: Input, _: &ActionMeta) {
        match input {
            Input::Set(n) => *ctx.state_mut() = n,
            Input::CrashA => panic!("fault A"),
            Input::CrashB => panic!("fault B"),
            Input::ReportThenCrash(b) => {
                bug_condition!("expected diagnostic before a queued child");
                ctx.into_emitter()
                    .emit_action(if b { Input::CrashB } else { Input::CrashA });
            }
            Input::CleanupThenCrash(b) => {
                // A destructor that catches its own panic while the fault
                // unwinds through it.
                let _cleanup = CatchInDrop;
                if b {
                    panic!("fault B");
                } else {
                    panic!("fault A");
                }
            }
            Input::CaughtThenCrash(b) => {
                // A panic the application catches itself, then a fault.
                let _ = panic::catch_unwind(|| panic!("handled inside the reducer"));
                ctx.into_emitter()
                    .emit_action(if b { Input::CrashB } else { Input::CrashA });
            }
        }
    }
}

struct CatchInDrop;

impl Drop for CatchInDrop {
    fn drop(&mut self) {
        // Rust lets a destructor catch its own panic while another unwinds.
        let _ = panic::catch_unwind(|| panic!("handled cleanup panic"));
    }
}

fn node() -> Node<u8, ()> {
    Node::new::<Counter>("counter", 0, (), SessionId(1), &ManualClock::new(0)).check(
        "not one",
        |&state| {
            if state == 1 {
                Err("one is invalid".into())
            } else {
                Ok(())
            }
        },
    )
}

fn dispatch(node: &mut Node<u8, ()>, input: Input) {
    let mount = node.mount(&root::<Counter>());
    node.dispatch(&mount, input);
}

fn message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic")
}

fn one_step() -> Config {
    Config {
        runs: 1,
        max_steps: 1,
        max_shrinks: 0,
        ..Config::default()
    }
}

#[test]
fn final_check_replay_starts_with_a_fresh_step_counter() {
    let failed = panic::catch_unwind(|| {
        check(one_step(), |_| dispatch(&mut node(), Input::Set(1)));
    })
    .unwrap_err();
    assert!(
        message(&*failed).contains("invariant \"not one\""),
        "wrong final failure: {}",
        message(&*failed)
    );
}

#[test]
fn a_saved_budget_failure_repeats_through_public_replay() {
    let property = |_: &mut Rng| {
        let mut node = node();
        dispatch(&mut node, Input::Set(2));
        dispatch(&mut node, Input::Set(2));
    };
    let found = search(one_step(), property).unwrap();
    assert_eq!(found.key, "step budget");
    let replayed = panic::catch_unwind(AssertUnwindSafe(|| found.replay(property)));
    assert!(
        replayed.is_err(),
        "the advertised saved failure passed on replay: {found:?}"
    );
}

struct FaultObserverPanics;

impl Observer for FaultObserverPanics {
    fn fault(&mut self, _: &FaultInfo<'_>) {
        panic!("ignored observer failure");
    }
}

#[test]
fn ignored_fault_observer_panics_do_not_merge_distinct_store_faults() {
    let seed = (0..).find(|&seed| Rng::new(seed).below(2) == 1).unwrap();
    let config = Config {
        runs: 1,
        seed,
        max_steps: 10,
        max_shrinks: 50,
        ..Config::default()
    };
    let property = |rng: &mut Rng, observer: bool| {
        let mut node = node();
        if observer {
            node.store_mut().add_observer(FaultObserverPanics);
        }
        let input = if rng.below(2) == 0 {
            Input::CrashA
        } else {
            Input::CrashB
        };
        dispatch(&mut node, input);
    };
    let without = search(config.clone(), |rng| property(rng, false)).unwrap();
    assert_eq!(without.choices, [1], "the original B fault must stay B");
    let with = search(config, |rng| property(rng, true)).unwrap();
    assert_eq!(
        with.choices, without.choices,
        "an ignored observer changed fault identity: {with:?}"
    );
}

#[test]
fn a_bare_replay_still_has_a_step_limit() {
    let endless = panic::catch_unwind(|| {
        causet_test::replay(&[], |_| {
            let mut node = node();
            loop {
                dispatch(&mut node, Input::Set(2));
            }
        })
    })
    .unwrap_err();
    assert!(
        message(&*endless).contains("Config::max_steps"),
        "{}",
        message(&*endless)
    );
}

struct BugObserverPanics;

impl Observer for BugObserverPanics {
    fn bug(&mut self, _: &BugReport) {
        panic!("ignored report observer failure");
    }
}

/// Searches `property` on a seed whose first choice is 1 (fault B), with
/// and without `noise`, and checks both keep fault B.
fn keeps_fault_b(noise: impl Fn(&mut Node<u8, ()>), input: fn(bool) -> Input) {
    let seed = (0..).find(|&seed| Rng::new(seed).below(2) == 1).unwrap();
    let config = Config {
        runs: 1,
        seed,
        max_steps: 10,
        max_shrinks: 50,
        ..Config::default()
    };
    let property = |rng: &mut Rng, noisy: bool| {
        let mut node = node().allow_bugs();
        if noisy {
            noise(&mut node);
        }
        dispatch(&mut node, input(rng.below(2) != 0));
    };
    let quiet = search(config.clone(), |rng| property(rng, false)).unwrap();
    assert_eq!(quiet.choices, [1], "the original B fault must stay B");
    let noisy = search(config, |rng| property(rng, true)).unwrap();
    assert_eq!(noisy.key, quiet.key);
    assert_eq!(
        noisy.choices, quiet.choices,
        "an unrelated panic replaced the real fault key: {noisy:?}"
    );
}

#[test]
fn an_ignored_panic_before_the_fault_does_not_merge_distinct_store_faults() {
    keeps_fault_b(
        |node| node.store_mut().add_observer(BugObserverPanics),
        Input::ReportThenCrash,
    );
}

#[test]
fn a_panic_the_application_caught_does_not_merge_distinct_store_faults() {
    keeps_fault_b(|_| {}, Input::CaughtThenCrash);
}

#[test]
fn a_panic_a_destructor_caught_while_the_fault_unwound_does_not_merge_faults() {
    keeps_fault_b(|_| {}, Input::CleanupThenCrash);
}
