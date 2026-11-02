//! Root guards: the application's checks on a mount below the root,
//! reading the whole State. They check every Action that enters the mount
//! and the root's own inline calls, never an inline call made below the
//! root, and run after availability and before the slot guard and the
//! component's own check.

use std::sync::Arc;

use causet::hosting::EventBus;
use causet::observers::{BugLog, Phase, RefusedBy, SentFrom, TraceLog};
use causet::*;

mod guarded;

use guarded::*;

struct Fixture {
    store: Store<AppState, AppServices>,
    sink: EventSink,
    app: Mount<App>,
    mid: Mount<Mid>,
    other: Mount<Mid>,
    leaf: Mount<Leaf>,
    other_leaf: Mount<Leaf>,
}

fn fixture(state: AppState) -> Fixture {
    let mut registry = Registry::<AppState, AppServices>::new();
    registry.root::<App>();
    registry.admit(mid(), mid_may_forward);
    registry.admit(mid_leaf(), may_add);
    registry.admit(other_leaf(), other_may_add);
    registry.admit(spare(), spare_never);
    let protocol = registry.freeze().unwrap();
    let (sink, inbox) = EventBus::new(&protocol, 16);
    let options = StoreOptions::with_clock(ManualClock::new(1_000));
    let mut store = Store::new(
        Arc::clone(&protocol),
        state,
        AppServices::default(),
        inbox,
        options,
    )
    .unwrap();
    store.add_observer(TraceLog::new());
    Fixture {
        store,
        sink,
        app: protocol.resolve(&root::<App>()).unwrap(),
        mid: protocol.resolve(&root::<App>().child(MID)).unwrap(),
        other: protocol.resolve(&root::<App>().child(OTHER)).unwrap(),
        leaf: protocol.resolve(&mid_leaf()).unwrap(),
        other_leaf: protocol.resolve(&other_leaf()).unwrap(),
    }
}

fn blocking(blocked: &[u32]) -> AppState {
    AppState {
        blocked: blocked.to_vec(),
        limit: 1_000,
        ..AppState::default()
    }
}

impl Fixture {
    fn leaf(&mut self, action: LeafAction) -> DispatchOutcome {
        self.store.dispatch_to(&self.leaf, action).unwrap()
    }

    fn mid(&mut self, action: MidAction) -> DispatchOutcome {
        self.store.dispatch_to(&self.mid, action).unwrap()
    }

    fn bump(&mut self, n: u32) {
        self.sink.try_send_to(&self.leaf, Bumped(n)).unwrap();
    }

    fn total(&self) -> u32 {
        self.store.state().mid.leaf.as_ref().map_or(0, |l| l.total)
    }

    fn other_total(&self) -> u32 {
        self.store
            .state()
            .other
            .leaf
            .as_ref()
            .map_or(0, |l| l.total)
    }

    /// Every refusal so far: its path, kind, what refused it and where it
    /// was sent from, as `root`, `queued`, `effect` or `inline`.
    fn refusals(&self) -> Vec<(String, &'static str, RefusedBy, &'static str)> {
        let log = self.store.observer::<TraceLog>().unwrap();
        log.refusals()
            .iter()
            .map(|r| {
                let sent = match r.sent {
                    SentFrom::Root => "root",
                    SentFrom::Queued(_) => "queued",
                    SentFrom::Effect(_) => "effect",
                    SentFrom::Inline(_) => "inline",
                };
                (r.path.to_string(), r.kind, r.by, sent)
            })
            .collect()
    }
}

fn root_guard(path: &str, sent: &'static str) -> (String, &'static str, RefusedBy, &'static str) {
    (path.into(), "leaf.add", RefusedBy::RootGuard, sent)
}

/// Each route by which an Action enters the leaf, with `n`.
fn every_route(f: &mut Fixture, n: u32) {
    f.leaf(LeafAction::Add(n)); // from outside
    f.leaf(LeafAction::Again(n)); // queued by itself
    f.mid(MidAction::Send(LeafAction::Add(n))); // queued by its parent
    f.bump(n); // adapted from an Event
    f.store.drain_events(16).unwrap();
    f.leaf(LeafAction::Ask(n)); // lowered from its child's Output
    f.leaf(LeafAction::Echo(n)); // dispatched by its Effect handler
}

#[test]
fn a_root_guard_checks_every_action_that_enters_its_mount() {
    let mut f = fixture(blocking(&[5]));
    every_route(&mut f, 5);
    assert_eq!(f.total(), 0, "nothing blocked was added");
    assert_eq!(
        f.refusals(),
        ["root", "queued", "queued", "queued", "queued", "effect"]
            .map(|sent| root_guard("/mid/leaf", sent))
    );

    every_route(&mut f, 1);
    assert_eq!(f.total(), 6, "every route admits what the guard allows");
    assert_eq!(f.refusals().len(), 6);
}

#[test]
fn a_root_guard_reads_the_state_when_the_action_runs() {
    // Blocked after the Event was sent, before its Action ran: refused.
    let mut f = fixture(blocking(&[]));
    f.bump(5);
    let app = f.app;
    f.store.dispatch_to(&app, AppAction::Block(5)).unwrap();
    f.store.drain_events(16).unwrap();
    assert_eq!(f.total(), 0);
    assert_eq!(f.refusals(), [root_guard("/mid/leaf", "queued")]);

    // Unblocked in between: admitted.
    let mut f = fixture(blocking(&[5]));
    f.bump(5);
    let app = f.app;
    f.store.dispatch_to(&app, AppAction::Unblock(5)).unwrap();
    f.store.drain_events(16).unwrap();
    assert_eq!(f.total(), 5);
    assert!(f.refusals().is_empty());

    // The leaf's own State, as it stands: the limit counts what was added.
    let mut f = fixture(AppState {
        limit: 10,
        ..blocking(&[])
    });
    assert_eq!(f.leaf(LeafAction::Add(6)), DispatchOutcome::Executed);
    assert_eq!(f.leaf(LeafAction::Add(6)), DispatchOutcome::Disabled);
    assert_eq!(f.leaf(LeafAction::Add(4)), DispatchOutcome::Executed);
    assert_eq!(f.total(), 10);
}

#[test]
fn two_mounts_of_one_component_have_their_own_root_guards() {
    let mut f = fixture(AppState {
        other_blocked: vec![7],
        ..blocking(&[5])
    });
    let (leaf, other_leaf) = (f.leaf, f.other_leaf);
    for n in [5, 7] {
        f.store.dispatch_to(&leaf, LeafAction::Add(n)).unwrap();
        f.store
            .dispatch_to(&other_leaf, LeafAction::Add(n))
            .unwrap();
    }
    assert_eq!((f.total(), f.other_total()), (7, 5));
    assert_eq!(
        f.refusals(),
        [
            root_guard("/mid/leaf", "root"),
            root_guard("/other/leaf", "root")
        ]
    );
}

#[test]
fn an_inline_call_below_the_root_does_not_run_the_root_guard_and_says_so() {
    let mut f = fixture(AppState {
        other_blocked: vec![7],
        ..blocking(&[])
    });
    let other = f.other;
    let forward = |f: &mut Fixture, n| {
        f.store
            .dispatch_to(&other, MidAction::Forward(LeafAction::Add(n)))
            .unwrap()
    };
    // /other has no root guard for what it forwards: the call is the
    // caller's, and the leaf's root guard does not run.
    forward(&mut f, 7);
    assert_eq!(f.other_total(), 7);
    let log = f.store.observer::<TraceLog>().unwrap();
    let inline = &log.inline()[0];
    assert_eq!(
        (&*inline.path, inline.root_guard_skipped),
        ("/other/leaf", true)
    );
    // The slot guard and the leaf's own check still run inline.
    forward(&mut f, 13);
    forward(&mut f, 100);
    assert_eq!(f.other_total(), 7);
    let inline_refusal = |by| ("/other/leaf".to_string(), "leaf.add", by, "inline");
    assert_eq!(
        f.refusals(),
        [
            inline_refusal(RefusedBy::Guard),
            inline_refusal(RefusedBy::Component)
        ]
    );
    assert_eq!(
        f.store.state().other.outcomes,
        [
            ChildOutcome::Reduced,
            ChildOutcome::Refused,
            ChildOutcome::Refused
        ]
    );
}

#[test]
fn an_inline_call_from_the_root_runs_the_root_guard() {
    let mut f = fixture(blocking(&[5]));
    let app = f.app;
    let call = |f: &mut Fixture, n| {
        f.store
            .dispatch_to(&app, AppAction::Mid(MidAction::Forward(LeafAction::Add(n))))
            .unwrap()
    };
    // The root holds the whole State, so its inline call into /mid is
    // checked by the root guard there.
    call(&mut f, 5);
    assert_eq!(
        f.refusals(),
        [(
            "/mid".to_string(),
            "mid.forward",
            RefusedBy::RootGuard,
            "inline"
        )]
    );
    call(&mut f, 2);
    assert_eq!(f.total(), 2);
    // Told as run (/mid) and as skipped by /mid's own inline call (/mid/leaf).
    let log = f.store.observer::<TraceLog>().unwrap();
    let inline: Vec<_> = log
        .inline()
        .iter()
        .map(|i| (&*i.path, i.root_guard_skipped))
        .collect();
    assert_eq!(inline, [("/mid", false), ("/mid/leaf", true)]);
}

#[test]
fn the_root_guard_on_the_root_s_inline_call_reads_its_changes_so_far() {
    // The root blocks 4 and then calls /mid inline: the root guard there
    // reads the State the root's reducer has made, not the one it began with.
    let mut f = fixture(blocking(&[]));
    let app = f.app;
    f.store
        .dispatch_to(&app, AppAction::BlockThenForward(4))
        .unwrap();
    assert_eq!(f.total(), 0);
    assert_eq!(
        f.refusals(),
        [(
            "/mid".to_string(),
            "mid.forward",
            RefusedBy::RootGuard,
            "inline"
        )]
    );
}

#[test]
fn the_root_s_inline_call_checks_in_the_queued_order() {
    // 131 is blocked (the root guard) and ends in 31 (App's slot guard on
    // /mid); 1031 ends in 31 and is 1000 or more (Mid's own check); 1000 is
    // only the latter. /spare is absent, and its root guard refuses all.
    let mut f = fixture(blocking(&[131]));
    let app = f.app;
    for call in [
        AppAction::Mid(MidAction::Forward(LeafAction::Add(131))),
        AppAction::Mid(MidAction::Forward(LeafAction::Add(1031))),
        AppAction::Mid(MidAction::Forward(LeafAction::Add(1000))),
        AppAction::Spare(MidAction::Forward(LeafAction::Add(5))),
    ] {
        f.store.dispatch_to(&app, call).unwrap();
    }
    let inline = |path: &str, by| (path.to_string(), "mid.forward", by, "inline");
    assert_eq!(
        f.refusals(),
        [
            inline("/mid", RefusedBy::RootGuard),
            inline("/mid", RefusedBy::Guard),
            inline("/mid", RefusedBy::Component),
            inline("/spare", RefusedBy::Absent),
        ]
    );
}

#[test]
fn a_root_guard_s_bug_report_is_placed_at_the_root() {
    let mut f = fixture(blocking(&[]));
    f.store.add_observer(BugLog::new());
    let leaf = f.leaf;
    assert_eq!(
        f.store.dispatch_to(&leaf, LeafAction::Add(777)).unwrap(),
        DispatchOutcome::Disabled
    );
    let log = f.store.observer::<BugLog>().unwrap();
    let place = log.reports()[0].place.as_ref().unwrap();
    assert_eq!((place.phase, &*place.path), (Phase::Admission, "/"));
}

#[test]
fn a_forwarded_action_is_checked_where_it_entered() {
    // The root guard on /mid checks the wrapper, on every route into /mid,
    // so the addition never reaches the leaf.
    let mut f = fixture(blocking(&[5]));
    assert_eq!(
        f.mid(MidAction::Forward(LeafAction::Add(5))),
        DispatchOutcome::Disabled
    );
    assert_eq!(f.total(), 0);
    assert_eq!(
        f.refusals(),
        [(
            "/mid".to_string(),
            "mid.forward",
            RefusedBy::RootGuard,
            "root"
        )]
    );
    assert_eq!(
        f.mid(MidAction::Forward(LeafAction::Add(6))),
        DispatchOutcome::Executed
    );
    assert_eq!(f.total(), 6);
}

#[test]
fn the_checks_run_in_order_and_the_first_refusal_is_told() {
    let refused = |f: &mut Fixture, action| {
        let before = f.refusals().len();
        assert_eq!(f.leaf(action), DispatchOutcome::Disabled);
        f.refusals()[before].2
    };
    // 113 ends in 13 (the slot guard) and is 100 or more (the leaf's own
    // check); 150 is too; 13 is blocked by the root guard as well.
    let mut f = fixture(blocking(&[13, 150]));
    assert_eq!(refused(&mut f, LeafAction::Add(113)), RefusedBy::Guard);
    assert_eq!(refused(&mut f, LeafAction::Add(150)), RefusedBy::RootGuard);
    assert_eq!(refused(&mut f, LeafAction::Add(13)), RefusedBy::RootGuard);

    // With the leaf gone, every check would refuse 13; availability comes
    // first, on the queued path and inline.
    f.mid(MidAction::DropLeaf);
    assert_eq!(refused(&mut f, LeafAction::Add(13)), RefusedBy::Absent);
    let before = f.refusals().len();
    f.mid(MidAction::Send(LeafAction::Add(113)));
    let other = f.other;
    f.store.dispatch_to(&other, MidAction::DropLeaf).unwrap();
    f.store
        .dispatch_to(&other, MidAction::Forward(LeafAction::Add(113)))
        .unwrap();
    let after: Vec<_> = f.refusals()[before..].iter().map(|r| (r.2, r.3)).collect();
    assert_eq!(
        after,
        [(RefusedBy::Absent, "queued"), (RefusedBy::Absent, "inline")]
    );
}

#[test]
fn a_panicking_root_guard_faults_the_store() {
    let mut f = fixture(blocking(&[]));
    let leaf = f.leaf;
    let result = f.store.dispatch_to(&leaf, LeafAction::Add(666));
    assert!(
        matches!(&result, Err(DispatchError::Faulted(m)) if m.contains("a root guard that panics")),
        "{result:?}"
    );
}

#[test]
fn a_path_from_another_root_does_not_resolve() {
    // The names reach /mid's leaf, but the path starts at Mid, not App.
    let f = fixture(blocking(&[]));
    let other_root = ResolveError::OtherComponent { path: "/".into() };
    assert_eq!(
        f.store.mount(&root::<Mid>().child(LEAF)).err(),
        Some(other_root.clone())
    );
    assert_eq!(
        f.store.protocol().resolve(&root::<Mid>().child(LEAF)).err(),
        Some(other_root)
    );
}

#[test]
fn freeze_reports_misplaced_root_guards() {
    const NOWHERE: Child<Mid, Leaf> = Child::new("nowhere");
    const NOT_A_TIP: Child<Mid, Tip> = Child::new("leaf");
    let mut registry = Registry::<AppState, AppServices>::new();
    registry.root::<App>();
    registry.admit(root::<App>().child(MID).child(NOWHERE), |_, _, _, _| false);
    registry.admit(root::<Mid>().child(LEAF), |_, _, _, _| false);
    registry.admit(root::<App>().child(MID).child(NOT_A_TIP), |_, _, _, _| {
        false
    });
    registry.admit(root::<App>(), |_, _, _, _| false);
    // A child of the root is fine.
    registry.admit(root::<App>().child(MID), |_, _, _, _| false);
    registry.admit(mid_leaf(), |_, _, _, _| false);
    registry.admit(mid_leaf(), |_, _, _, _| false);
    let Err(RegistrationError(problems)) = registry.freeze() else {
        panic!("the registration is refused")
    };
    let path = |p: &str| p.to_string();
    assert_eq!(
        problems,
        [
            Problem::RootGuardNoMount {
                path: path("/mid/nowhere")
            },
            Problem::RootGuardOtherComponent { path: path("/") },
            Problem::RootGuardOtherComponent {
                path: path("/mid/leaf")
            },
            Problem::RootGuardOnRoot,
            Problem::RootGuardTwice {
                path: path("/mid/leaf")
            },
        ]
    );
}
