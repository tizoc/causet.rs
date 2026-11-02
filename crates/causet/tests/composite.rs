//! A leaf with no Effects, a composite holding it, and a root mounting the
//! composite twice: `/primary` and `/secondary`, each with its own `leaf`.

use std::sync::Arc;

use causet::hosting::EventBus;
use causet::observers::TraceLog;
use causet::{
    Action, ActionMeta, ActionPort, Child, ChildOutcome, Component, ComponentContext,
    ComponentRegistry, DispatchError, DispatchOutcome, Never, Output, Protocol, Registry,
    RequestId, Store, StoreOptions, Timestamp, lens, no_effects, root,
};

// ---------------------------------------------------------------------------
// Leaf: no Effects, reports one Output.

struct Leaf;

#[derive(Default, Debug)]
struct LeafState {
    value: u64,
    requests: Vec<RequestId>,
}

#[derive(Debug, Action)]
#[causet(type = "leaf.action")]
enum LeafAction {
    Bump(u64),
    Request,
    Panic,
}

#[derive(Debug, Output)]
#[causet(type = "leaf.output")]
enum LeafOutput {
    Bumped { value: u64 },
}

impl Component for Leaf {
    type State = LeafState;
    type Action = LeafAction;
    type Output = LeafOutput;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: LeafAction, _: &ActionMeta) {
        match action {
            LeafAction::Bump(n) => {
                ctx.state_mut().value += n;
                let (mut out, view) = ctx.into_emitter_with_view();
                out.emit_output(LeafOutput::Bumped { value: view.value });
            }
            LeafAction::Request => {
                let request = ctx.new_request();
                ctx.state_mut().requests.push(request);
            }
            LeafAction::Panic => panic!("leaf panicked"),
        }
    }
}

// ---------------------------------------------------------------------------
// Composite: holds a Leaf, calls it inline or queued, lowers its Outputs.

struct Composite;

const LEAF: Child<Composite, Leaf> = Child::new("leaf");
const GHOST: Child<Composite, Leaf> = Child::new("ghost"); // never bound

#[derive(Default, Debug)]
struct CompositeState {
    leaf: LeafState,
    requests: Vec<RequestId>,
    after_child: bool,
    seen: Vec<u64>,
}

#[derive(Debug, Action)]
#[causet(type = "composite.action")]
enum CompositeAction {
    Work,
    BumpInline(u64),
    BumpQueued(u64),
    LeafBumped(u64),
    PanicInChild,
    UseGhost,
}

#[derive(Debug, Output)]
#[causet(type = "composite.output")]
enum CompositeOutput {
    Reported { value: u64 },
}

fn lower_leaf(
    _slot: Child<Composite, Leaf>,
    out: LeafOutput,
    port: &mut ActionPort<'_, Composite>,
) {
    match out {
        LeafOutput::Bumped { value } => port.emit_action(CompositeAction::LeafBumped(value)),
    }
}

impl Component for Composite {
    type State = CompositeState;
    type Action = CompositeAction;
    type Output = CompositeOutput;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: CompositeAction, _: &ActionMeta) {
        match action {
            CompositeAction::Work => {
                let first = ctx.new_request();
                ctx.state_mut().requests.push(first);
                ctx.reduce_child(LEAF, LeafAction::Request);
                let last = ctx.new_request();
                ctx.state_mut().requests.push(last);
            }
            CompositeAction::BumpInline(n) => {
                ctx.reduce_child(LEAF, LeafAction::Bump(n));
                ctx.state_mut().after_child = true;
            }
            CompositeAction::BumpQueued(n) => {
                ctx.into_emitter().to_child(LEAF, LeafAction::Bump(n));
            }
            CompositeAction::LeafBumped(value) => {
                ctx.state_mut().seen.push(value);
                ctx.into_emitter()
                    .emit_output(CompositeOutput::Reported { value });
            }
            CompositeAction::PanicInChild => {
                ctx.reduce_child(LEAF, LeafAction::Panic);
                ctx.state_mut().after_child = true;
            }
            CompositeAction::UseGhost => {
                ctx.reduce_child(GHOST, LeafAction::Bump(1));
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(LEAF, lens!(CompositeState => leaf))
            .on_output(lower_leaf)
            .bind();
    }
}

// ---------------------------------------------------------------------------
// App: the root, mounting Composite twice.

struct App;

const PRIMARY: Child<App, Composite> = Child::new("primary");
const SECONDARY: Child<App, Composite> = Child::new("secondary");

#[derive(Default, Debug)]
struct AppState {
    primary: CompositeState,
    secondary: CompositeState,
    secondary_open: bool,
    reports: Vec<(String, u64)>,
    inline_outcomes: Vec<ChildOutcome>,
}

#[derive(Debug, Action)]
#[causet(type = "app.action")]
enum AppAction {
    Reported { slot: String, value: u64 },
    OpenSecondary,
    WorkSecondaryInline,
    WorkSecondaryQueued,
}

/// One lowerer for both mounts: the slot says which one reported.
fn lower_composite(
    slot: Child<App, Composite>,
    out: CompositeOutput,
    port: &mut ActionPort<'_, App>,
) {
    match out {
        CompositeOutput::Reported { value } => port.emit_action(AppAction::Reported {
            slot: slot.name().into(),
            value,
        }),
    }
}

fn admit_secondary(app: &AppState, _: &CompositeAction, _: Timestamp) -> bool {
    app.secondary_open
}

impl Component for App {
    type State = AppState;
    type Action = AppAction;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: AppAction, _: &ActionMeta) {
        match action {
            AppAction::Reported { slot, value } => ctx.state_mut().reports.push((slot, value)),
            AppAction::OpenSecondary => ctx.state_mut().secondary_open = true,
            AppAction::WorkSecondaryInline => {
                let outcome = ctx.reduce_child(SECONDARY, CompositeAction::Work);
                ctx.state_mut().inline_outcomes.push(outcome);
            }
            AppAction::WorkSecondaryQueued => {
                ctx.into_emitter()
                    .to_child(SECONDARY, CompositeAction::Work);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(PRIMARY, lens!(AppState => primary))
            .on_output(lower_composite)
            .bind();
        r.child(SECONDARY, lens!(AppState => secondary))
            .admit(admit_secondary)
            .on_output(lower_composite)
            .bind();
    }
}

fn protocol() -> Arc<Protocol<AppState, ()>> {
    let mut registry = Registry::<AppState, ()>::new();
    registry.root::<App>();
    registry.freeze().expect("valid protocol")
}

fn store(protocol: &Arc<Protocol<AppState, ()>>) -> Store<AppState, ()> {
    let (_sink, inbox) = EventBus::new(protocol, 16);
    let mut store = Store::new(
        protocol.clone(),
        AppState::default(),
        (),
        inbox,
        StoreOptions::default(),
    )
    .expect("the bus is this Protocol's");
    store.add_observer(TraceLog::new());
    store
}

// ---------------------------------------------------------------------------

#[test]
fn composite_mounted_twice_gets_distinct_paths_and_ids() {
    let p = protocol();
    let primary_leaf = p
        .resolve(&root::<App>().child(PRIMARY).child(LEAF))
        .unwrap();
    let secondary_leaf = p
        .resolve(&root::<App>().child(SECONDARY).child(LEAF))
        .unwrap();
    assert_eq!(p.path_of(&primary_leaf), "/primary/leaf");
    assert_eq!(p.path_of(&secondary_leaf), "/secondary/leaf");
    assert_ne!(p.instance_of(&primary_leaf), p.instance_of(&secondary_leaf));
    // xxh3-64, seed 0, of the path: a fixed value that recordings depend on.
    assert_eq!(
        p.instance_of(&primary_leaf),
        causet::InstanceId(0xdb38_8269_9235_059d)
    );
}

#[test]
fn leaf_without_effects_reports_through_both_lowerers() {
    let p = protocol();
    let mut s = store(&p);
    let leaf = p
        .resolve(&root::<App>().child(PRIMARY).child(LEAF))
        .unwrap();
    let composite = p.resolve(&root::<App>().child(PRIMARY)).unwrap();

    assert_eq!(
        s.dispatch_to(&leaf, LeafAction::Bump(3)),
        Ok(DispatchOutcome::Executed)
    );

    assert_eq!(s.state().primary.leaf.value, 3);
    assert_eq!(s.state().secondary.leaf.value, 0);
    assert_eq!(s.state().primary.seen, vec![3]);
    assert_eq!(s.state().reports, vec![("primary".to_string(), 3)]);

    let kinds: Vec<_> = s
        .observer::<TraceLog>()
        .unwrap()
        .entries()
        .iter()
        .map(|e| (&*e.path, e.kind))
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("/primary/leaf", "leaf.bump"),
            ("/primary", "composite.leaf_bumped"),
            ("/", "app.reported"),
        ]
    );
    // The lowered Action keeps the leaf as provenance, with the Output's
    // emission ordinal and its own lowering ordinal.
    let lowered = s.observer::<TraceLog>().unwrap().entries()[1]
        .origin
        .unwrap();
    assert_eq!(
        lowered.causal,
        s.observer::<TraceLog>().unwrap().entries()[0].id
    );
    assert_eq!(lowered.component, p.instance_of(&leaf));
    assert_eq!((lowered.emission_ordinal, lowered.lowering_ordinal), (0, 0));
    let reported = s.observer::<TraceLog>().unwrap().entries()[2]
        .origin
        .unwrap();
    assert_eq!(reported.component, p.instance_of(&composite));
}

#[test]
fn parent_and_inline_child_share_one_request_counter() {
    let p = protocol();
    let mut s = store(&p);
    let primary = p.resolve(&root::<App>().child(PRIMARY)).unwrap();

    s.dispatch_to(&primary, CompositeAction::Work).unwrap();

    let parent = &s.state().primary.requests;
    let child = &s.state().primary.leaf.requests;
    assert_eq!(parent.len(), 2);
    assert_eq!(child.len(), 1);
    // One execution: the parent's first request, the child's, the parent's last.
    let execution = parent[0].execution;
    assert!(
        parent
            .iter()
            .chain(child)
            .all(|r| r.execution == execution && r.session == s.session())
    );
    assert_eq!(
        (parent[0].index, child[0].index, parent[1].index),
        (0, 1, 2)
    );
    // The inline call added no execution of its own.
    assert_eq!(s.observer::<TraceLog>().unwrap().entries().len(), 1);
}

#[test]
fn inline_and_queued_calls_differ_in_executions() {
    let p = protocol();
    let mut s = store(&p);
    let primary = p.resolve(&root::<App>().child(PRIMARY)).unwrap();

    s.dispatch_to(&primary, CompositeAction::BumpInline(2))
        .unwrap();
    let inline: Vec<_> = s
        .observer::<TraceLog>()
        .unwrap()
        .entries()
        .iter()
        .map(|e| e.kind)
        .collect();
    assert_eq!(
        inline,
        vec![
            "composite.bump_inline",
            "composite.leaf_bumped",
            "app.reported"
        ]
    );
    assert!(s.state().primary.after_child);

    s.dispatch_to(&primary, CompositeAction::BumpQueued(5))
        .unwrap();
    let queued: Vec<_> = s.observer::<TraceLog>().unwrap().entries()[3..]
        .iter()
        .map(|e| e.kind)
        .collect();
    assert_eq!(
        queued,
        vec![
            "composite.bump_queued",
            "leaf.bump",
            "composite.leaf_bumped",
            "app.reported"
        ]
    );
    assert_eq!(s.state().primary.leaf.value, 7);
}

#[test]
fn one_lowerer_serves_both_mounts() {
    let p = protocol();
    let mut s = store(&p);
    let app = p.resolve(&root::<App>()).unwrap();
    let primary = p.resolve(&root::<App>().child(PRIMARY)).unwrap();
    let secondary = p.resolve(&root::<App>().child(SECONDARY)).unwrap();

    s.dispatch_to(&app, AppAction::OpenSecondary).unwrap();
    s.dispatch_to(&primary, CompositeAction::BumpInline(1))
        .unwrap();
    s.dispatch_to(&secondary, CompositeAction::BumpInline(4))
        .unwrap();

    assert_eq!(
        s.state().reports,
        vec![("primary".to_string(), 1), ("secondary".to_string(), 4)]
    );
    assert_eq!(
        (s.state().primary.leaf.value, s.state().secondary.leaf.value),
        (1, 4)
    );
}

#[test]
fn parent_guard_refuses_every_route() {
    let p = protocol();
    let mut s = store(&p);
    let app = p.resolve(&root::<App>()).unwrap();
    let secondary = p.resolve(&root::<App>().child(SECONDARY)).unwrap();

    // External dispatch.
    assert_eq!(
        s.dispatch_to(&secondary, CompositeAction::Work),
        Ok(DispatchOutcome::Disabled)
    );
    // Inline call from the parent.
    s.dispatch_to(&app, AppAction::WorkSecondaryInline).unwrap();
    assert_eq!(s.state().inline_outcomes, vec![ChildOutcome::Refused]);
    // Queued send from the parent: refused at dequeue, so no execution.
    s.dispatch_to(&app, AppAction::WorkSecondaryQueued).unwrap();
    assert!(
        s.observer::<TraceLog>()
            .unwrap()
            .entries()
            .iter()
            .all(|e| &*e.path != "/secondary")
    );
    assert!(s.state().secondary.requests.is_empty());

    s.dispatch_to(&app, AppAction::OpenSecondary).unwrap();
    s.dispatch_to(&app, AppAction::WorkSecondaryInline).unwrap();
    assert_eq!(
        s.state().inline_outcomes,
        vec![ChildOutcome::Refused, ChildOutcome::Reduced]
    );
    assert_eq!(s.state().secondary.requests.len(), 2);
}

#[test]
fn an_inline_call_makes_no_execution_of_its_own() {
    let p = protocol();
    let mut s = store(&p);
    let primary = p.resolve(&root::<App>().child(PRIMARY)).unwrap();

    s.dispatch_to(&primary, CompositeAction::BumpInline(1))
        .unwrap();

    let log = s.observer::<TraceLog>().unwrap();
    let executed: Vec<_> = log
        .entries()
        .iter()
        .map(|e| format!("{} {}", e.path, e.kind))
        .collect();
    assert_eq!(
        executed,
        [
            "/primary composite.bump_inline",
            "/primary composite.leaf_bumped",
            "/ app.reported"
        ]
    );
    // The leaf's Action ran inside the composite's execution.
    let inline: Vec<_> = log
        .inline()
        .iter()
        .map(|i| (format!("{} {}", i.path, i.kind), i.within))
        .collect();
    assert_eq!(
        inline,
        [("/primary/leaf leaf.bump".to_owned(), log.entries()[0].id)]
    );
}

#[test]
fn panic_in_inline_child_faults_and_parent_never_resumes() {
    let p = protocol();
    let mut s = store(&p);
    let primary = p.resolve(&root::<App>().child(PRIMARY)).unwrap();

    let result = s.dispatch_to(&primary, CompositeAction::PanicInChild);
    assert_eq!(result, Err(DispatchError::Faulted("leaf panicked".into())));
    assert!(!s.state().primary.after_child);
    assert_eq!(
        s.dispatch_to(&primary, CompositeAction::Work),
        Err(DispatchError::Faulted("leaf panicked".into()))
    );
}

#[test]
fn unbound_slot_faults_at_the_call() {
    let p = protocol();
    let mut s = store(&p);
    let primary = p.resolve(&root::<App>().child(PRIMARY)).unwrap();

    let Err(DispatchError::Faulted(message)) = s.dispatch_to(&primary, CompositeAction::UseGhost)
    else {
        panic!("expected a fault");
    };
    assert!(
        message.contains("slot ghost is not bound under /primary"),
        "{message}"
    );
}

#[test]
fn mount_from_another_protocol_is_rejected() {
    let first = protocol();
    let second = protocol();
    let mut s = store(&second);
    let foreign = first.resolve(&root::<App>().child(PRIMARY)).unwrap();
    assert_eq!(
        s.dispatch_to(&foreign, CompositeAction::Work),
        Err(DispatchError::ForeignMount)
    );

    // The same Protocol serves several Stores.
    let mut other = store(&first);
    assert_eq!(
        other.dispatch_to(&foreign, CompositeAction::Work),
        Ok(DispatchOutcome::Executed)
    );
}

#[test]
fn freeze_rejects_an_unlowered_output_and_a_reporting_root() {
    struct Bare;
    impl Component for Bare {
        type State = AppState;
        type Action = AppAction;
        type Output = Never;
        no_effects!();
        fn reduce(_: ComponentContext<'_, Self>, _: AppAction, _: &ActionMeta) {}
        fn register(r: &mut ComponentRegistry<'_, Self>) {
            // Composite reports Outputs, but no lowerer is bound.
            r.child(
                Child::<Bare, Composite>::new("c"),
                lens!(AppState => primary),
            )
            .bind();
        }
    }
    let mut registry = Registry::<AppState, ()>::new();
    registry.root::<Bare>();
    let error = registry.freeze().err().expect("must fail");
    assert!(
        error
            .0
            .contains(&causet::Problem::NoLowerer { path: "/c".into() }),
        "{error}"
    );

    struct Reporting;
    impl Component for Reporting {
        type State = CompositeState;
        type Action = CompositeAction;
        type Output = CompositeOutput;
        no_effects!();
        fn reduce(_: ComponentContext<'_, Self>, _: CompositeAction, _: &ActionMeta) {}
    }
    let mut registry = Registry::<CompositeState, ()>::new();
    registry.root::<Reporting>();
    assert!(registry.freeze().is_err());
}
