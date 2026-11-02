//! A trace of root guards, slot guards and their order, printed one fact per
//! line, for a port in another language to compare against. The tree is the
//! root-guard tests' (`tests/guarded/mod.rs`).
//!
//! `cargo run -p causet --example conformance_guards > guards.txt`

use std::sync::Arc;

use causet::Origin;
use causet::hosting::EventBus;
use causet::observers::{RefusedBy, SentFrom, TraceLog};
use causet::{ChildOutcome, DispatchOutcome, ManualClock, Registry, Store, StoreOptions, root};

#[path = "../tests/guarded/mod.rs"]
mod guarded;

use guarded::*;

fn origin(o: &Option<Origin>) -> String {
    match o {
        None => "-".into(),
        Some(o) => format!(
            "{}/{:016x}/{}/{}",
            o.causal.0, o.component.0, o.emission_ordinal, o.lowering_ordinal
        ),
    }
}

fn sent(s: &SentFrom) -> String {
    match s {
        SentFrom::Root => "root".into(),
        SentFrom::Effect(at) => format!("effect {}", at.0),
        SentFrom::Queued(o) => format!("queued {}", origin(&Some(*o))),
        SentFrom::Inline(at) => format!("inline {}", at.0),
    }
}

fn by(b: RefusedBy) -> &'static str {
    match b {
        RefusedBy::RootGuard => "root-guard",
        RefusedBy::Guard => "guard",
        RefusedBy::Component => "component",
        RefusedBy::Absent => "absent",
    }
}

fn outcome(o: &DispatchOutcome) -> &'static str {
    match o {
        DispatchOutcome::Executed => "executed",
        DispatchOutcome::Disabled => "disabled",
        DispatchOutcome::Faulted => "faulted",
    }
}

/// A leaf's total, or `-` once dropped.
fn total(m: &MidState) -> String {
    m.leaf.as_ref().map_or("-".into(), |l| l.total.to_string())
}

fn main() {
    let mut registry = Registry::<AppState, AppServices>::new();
    registry.root::<App>();
    registry.admit(mid(), mid_may_forward);
    registry.admit(mid_leaf(), may_add);
    registry.admit(other_leaf(), other_may_add);
    registry.admit(spare(), spare_never);
    let protocol = registry.freeze().unwrap();
    let (sink, inbox) = EventBus::new(&protocol, 16);
    let state = AppState {
        blocked: vec![5],
        other_blocked: vec![7],
        limit: 1_000,
        ..AppState::default()
    };
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
    let app = protocol.resolve(&root::<App>()).unwrap();
    let mid = protocol.resolve(&root::<App>().child(MID)).unwrap();
    let other = protocol.resolve(&root::<App>().child(OTHER)).unwrap();
    let leaf = protocol.resolve(&mid_leaf()).unwrap();
    let other_leaf = protocol.resolve(&other_leaf()).unwrap();

    let mut outcomes = Vec::new();
    // Every route into /mid/leaf, blocked and admitted.
    for n in [5, 1] {
        outcomes.push(store.dispatch_to(&leaf, LeafAction::Add(n)).unwrap());
        outcomes.push(store.dispatch_to(&leaf, LeafAction::Again(n)).unwrap());
        outcomes.push(
            store
                .dispatch_to(&mid, MidAction::Send(LeafAction::Add(n)))
                .unwrap(),
        );
        sink.try_send_to(&leaf, Bumped(n)).unwrap();
        store.drain_events(16).unwrap();
        outcomes.push(store.dispatch_to(&leaf, LeafAction::Ask(n)).unwrap());
        outcomes.push(store.dispatch_to(&leaf, LeafAction::Echo(n)).unwrap());
    }
    // Forwarded: checked where it entered, by the root guard on /mid, from
    // outside and on the root's own inline calls.
    outcomes.push(
        store
            .dispatch_to(&mid, MidAction::Forward(LeafAction::Add(5)))
            .unwrap(),
    );
    for n in [5, 2] {
        let call = AppAction::Mid(MidAction::Forward(LeafAction::Add(n)));
        outcomes.push(store.dispatch_to(&app, call).unwrap());
    }
    // The root's changes so far, seen by the root guard on its inline call.
    outcomes.push(
        store
            .dispatch_to(&app, AppAction::BlockThenForward(4))
            .unwrap(),
    );
    // The root's inline calls check in the queued order: the root guard
    // (131, blocked and ending in 31), the slot guard (1031, ending in 31 and
    // 1000 or more), the component's own check (1000), and availability
    // before all of them (/spare).
    outcomes.push(store.dispatch_to(&app, AppAction::Block(131)).unwrap());
    for n in [131, 1031, 1000] {
        let call = AppAction::Mid(MidAction::Forward(LeafAction::Add(n)));
        outcomes.push(store.dispatch_to(&app, call).unwrap());
    }
    let call = AppAction::Spare(MidAction::Forward(LeafAction::Add(5)));
    outcomes.push(store.dispatch_to(&app, call).unwrap());
    // Inline into /other/leaf, from /other: its root guard does not run;
    // the slot guard and the leaf's own check do.
    for n in [7, 13, 100] {
        outcomes.push(
            store
                .dispatch_to(&other, MidAction::Forward(LeafAction::Add(n)))
                .unwrap(),
        );
    }
    // The other mount's own root guard.
    outcomes.push(store.dispatch_to(&other_leaf, LeafAction::Add(7)).unwrap());
    // Order: the slot guard before the leaf's own check (113), the root
    // guard before both (150, 13, once blocked).
    for n in [150, 13] {
        outcomes.push(store.dispatch_to(&app, AppAction::Block(n)).unwrap());
    }
    for n in [113, 150, 13] {
        outcomes.push(store.dispatch_to(&leaf, LeafAction::Add(n)).unwrap());
    }
    let totals = format!(
        "mid {} other {}",
        total(&store.state().mid),
        total(&store.state().other)
    );
    // Absent first, from outside, queued and inline.
    outcomes.push(store.dispatch_to(&mid, MidAction::DropLeaf).unwrap());
    outcomes.push(store.dispatch_to(&leaf, LeafAction::Add(13)).unwrap());
    outcomes.push(
        store
            .dispatch_to(&mid, MidAction::Send(LeafAction::Add(113)))
            .unwrap(),
    );
    outcomes.push(store.dispatch_to(&other, MidAction::DropLeaf).unwrap());
    outcomes.push(
        store
            .dispatch_to(&other, MidAction::Forward(LeafAction::Add(113)))
            .unwrap(),
    );

    let log = store.observer::<TraceLog>().expect("a trace log");
    println!("scenario root-guards");
    println!(
        "outcomes {}",
        outcomes.iter().map(outcome).collect::<Vec<_>>().join(" ")
    );
    for e in log.entries() {
        println!(
            "exec {} time {} {:?} {} {} causal {} origin {} answers {} depth {}",
            e.id.0,
            e.time,
            e.category,
            e.path,
            e.kind,
            e.causal.0,
            origin(&e.origin),
            e.answers.map_or("-".into(), |a| a.0.to_string()),
            e.depth
        );
    }
    for r in log.refusals() {
        println!(
            "refused {} {} {} by {} at {}",
            r.path,
            r.kind,
            sent(&r.sent),
            by(r.by),
            r.time
        );
    }
    for i in log.inline() {
        println!(
            "inline {} {} within {}{}",
            i.path,
            i.kind,
            i.within.0,
            if i.root_guard_skipped {
                " root-guard-skipped"
            } else {
                ""
            }
        );
    }
    let s = store.state();
    let calls = |m: &MidState| -> String {
        let calls = m
            .outcomes
            .iter()
            .map(|o| match o {
                ChildOutcome::Reduced => "reduced",
                ChildOutcome::Refused => "refused",
            })
            .collect::<Vec<_>>()
            .join(" ");
        if calls.is_empty() { "-".into() } else { calls }
    };
    println!("totals before the drops {totals}");
    println!("state mid {} other {}", total(&s.mid), total(&s.other));
    println!("calls mid {}", calls(&s.mid));
    println!("calls other {}", calls(&s.other));
}
