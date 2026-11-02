//! Invariants in the harness: node invariants on the Store, their keys, their
//! memory across a restart, and groups of nodes whose published views shared
//! checks read.

use std::cell::RefCell;
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;

use causet::invariants::InvariantStep;
use causet::*;
use causet_test::{Config, Group, GroupStep, Node, search};

#[derive(Debug, Action)]
#[causet(type = "level.action", kind = "level.set")]
struct Set(u64);

struct Level;

impl Component for Level {
    type State = u64;
    type Action = Set;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, Set(n): Set, _: &ActionMeta) {
        if n == 8 {
            bug_condition!("eight");
        }
        *ctx.state_mut() = n;
    }
}

fn level(name: &str, session: u64) -> Node<u64, ()> {
    Node::new::<Level>(name, 0, (), SessionId(session), &ManualClock::new(1_000))
}

fn set(node: &mut Node<u64, ()>, n: u64) {
    let mount = node.mount(&root::<Level>());
    node.dispatch(&mount, Set(n));
}

fn message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap()
}

fn fails(f: impl FnOnce()) -> String {
    message(panic::catch_unwind(AssertUnwindSafe(f)).unwrap_err())
}

fn quick() -> Config {
    Config {
        runs: 50,
        ..Config::default()
    }
}

// ---------------------------------------------------------------------------
// Node invariants

#[test]
fn allow_bugs_does_not_silence_an_invariant() {
    let mut node = level("level", 1).allow_bugs().check("below ten", |&n| {
        if n < 10 { Ok(()) } else { Err(format!("{n}")) }
    });
    set(&mut node, 8); // a bug report, allowed
    let failure = fails(|| set(&mut node, 12));
    assert_eq!(
        failure,
        "node level, after root input 2: invariant \"below ten\": 12"
    );
}

#[test]
fn a_violation_and_a_panicking_check_have_their_own_keys() {
    let violated = search(quick(), |rng| {
        let mut node = level("level", 1).check("below ten", |&n| {
            if n < 10 { Ok(()) } else { Err(format!("{n}")) }
        });
        set(&mut node, rng.below(20));
    })
    .unwrap();
    assert_eq!(violated.key, "node level: invariant \"below ten\"");
    assert_eq!(violated.choices, [10]);

    let panicked = search(quick(), |rng| {
        let mut node = level("level", 1).check("fragile", |&n| {
            assert!(n < 10, "cannot read {n}");
            Ok(())
        });
        set(&mut node, rng.below(20));
    })
    .unwrap();
    assert_eq!(panicked.key, "node level: invariant \"fragile\" panicked");
}

#[test]
fn an_invariant_keeps_its_memory_and_hears_of_a_restart_first() {
    let never_decreases = || {
        let mut most = 0;
        move |&n: &u64, step: &InvariantStep<'_>| {
            if *step == InvariantStep::Restored {
                most = n; // this application's policy: the restored State is the new floor
            }
            let broken = n < most;
            most = most.max(n);
            if broken {
                Err(format!("{n} after {most}"))
            } else {
                Ok(())
            }
        }
    };
    let mut node = level("level", 1).check_with("never decreases", never_decreases());
    set(&mut node, 5);
    // Restored lower: told first, so no violation.
    let mut node = node.restart(2, (), SessionId(2));
    set(&mut node, 3);
    // Its memory came along: 3 is the floor now.
    let failure = fails(|| set(&mut node, 1));
    assert_eq!(
        failure,
        "node level, after root input 3: invariant \"never decreases\": 1 after 3"
    );
}

// ---------------------------------------------------------------------------
// Groups

/// "b never above a", with every step it ran at, by name.
fn ordered(group: &Group) -> Rc<RefCell<Vec<String>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let log = Rc::clone(&seen);
    group.check("b never above a", move |views, step| {
        log.borrow_mut().push(match step {
            GroupStep::Added => "added".to_owned(),
            GroupStep::Published { node } => format!("{node} published"),
            GroupStep::Settled { node } => format!("{node} settled"),
            GroupStep::Restarted { node } => format!("{node} restarted"),
        });
        match (views.get::<u64>("a"), views.get::<u64>("b")) {
            (Some(a), Some(b)) if b > a => Err(format!("b {b} > a {a}")),
            _ => Ok(()),
        }
    });
    seen
}

#[test]
fn a_group_check_runs_on_publish_and_after_every_member_s_root_input() {
    let group = Group::new("pair");
    let seen = ordered(&group);
    let mut a = level("a", 1).publish(&group, |&n: &u64| n);
    let mut b = level("b", 2).publish(&group, |&n: &u64| n);
    set(&mut a, 5);
    set(&mut b, 3);
    let b = b.restart(1, (), SessionId(3));
    assert_eq!(
        *seen.borrow(),
        [
            "added",
            "a published",
            "b published",
            "a settled",
            "b settled",
            "b restarted"
        ]
    );

    // A missing member is None: once `a` is gone, nothing bounds `b`.
    drop(a);
    drop(b);
    let mut b = level("b", 4).publish(&group, |&n: &u64| n);
    set(&mut b, 9);
}

#[test]
fn a_group_failure_names_the_group_and_when() {
    let group = Group::new("pair");
    let _seen = ordered(&group);
    let mut a = level("a", 1).publish(&group, |&n: &u64| n);
    let mut b = level("b", 2).publish(&group, |&n: &u64| n);
    set(&mut a, 5);
    let failure = fails(|| set(&mut b, 6));
    assert_eq!(
        failure,
        "group pair, after a root input of b: invariant \"b never above a\": b 6 > a 5"
    );
}

#[test]
fn group_keys_tell_two_groups_apart() {
    let found = search(quick(), |rng| {
        let (left, right) = (Group::new("left"), Group::new("right"));
        let _l = ordered(&left);
        let _r = ordered(&right);
        let mut a = level("a", 1)
            .publish(&left, |&n: &u64| n)
            .publish(&right, |&n: &u64| n + 10);
        let mut b = level("b", 2)
            .publish(&left, |&n: &u64| n)
            .publish(&right, |&n: &u64| n);
        set(&mut a, rng.below(20));
        set(&mut b, rng.below(20));
    })
    .unwrap();
    assert_eq!(found.key, "group left: invariant \"b never above a\"");
    assert_eq!(
        found.choices,
        [0, 1],
        "a = 0, then b = 1: above a on the left only"
    );
}

#[test]
fn a_node_publishes_once_per_group() {
    let group = Group::new("pair");
    let _a = level("a", 1).publish(&group, |&n: &u64| n);
    let failure = fails(|| drop(level("a", 2).publish(&group, |&n: &u64| n)));
    assert_eq!(failure, "group pair: node \"a\" has already published");
}

#[test]
fn a_node_in_two_groups_has_each_group_check_once_on_its_new_view() {
    let (left, right) = (Group::new("left"), Group::new("right"));
    let latest = Rc::new(RefCell::new((0, 0)));
    let runs = Rc::new(RefCell::new(0));
    for (group, mine) in [(&left, 0), (&right, 1)] {
        let (latest, runs) = (Rc::clone(&latest), Rc::clone(&runs));
        group.check("views agree", move |views, step| {
            let Some(&n) = views.get::<u64>("a") else {
                return Ok(());
            };
            if let GroupStep::Settled { .. } = step {
                *runs.borrow_mut() += 1;
                let mut latest = latest.borrow_mut();
                if mine == 0 {
                    latest.0 = n
                } else {
                    latest.1 = n
                }
            }
            Ok(())
        });
    }
    let mut a = level("a", 1)
        .publish(&left, |&n: &u64| n)
        .publish(&right, |&n: &u64| n);
    set(&mut a, 4);
    assert_eq!(
        (*latest.borrow(), *runs.borrow()),
        ((4, 4), 2),
        "once per group, each on the new view"
    );
}
