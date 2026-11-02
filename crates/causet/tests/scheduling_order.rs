//! The order work runs in, and the metadata each execution carries: one FIFO
//! per execution, mixing Actions and Effects in emission order; fresh,
//! globally linked metadata for each item; an Effect's dispatched Action
//! completing before the outer FIFO resumes; Output lowering keeping emission
//! order; refused items keeping their reserved emission ordinals; a child
//! Action starting its own causal sequence. Emissions outside any component
//! carry the root's `InstanceId`, since the application is the root
//! component.
//!
//! The observer tests at the end check that lifecycle observers wrap their
//! handlers only, that a refused Action reaches none of them, that
//! `action_settled` runs after the whole queued FIFO, and that observers run
//! in the order added until `clear_observers` removes them.

use causet::ExecutionId;
use causet::InstanceId;
use causet::hosting::EventBus;
use causet::observers::{Category, Execution, MessageView, Observer, TraceEntry, TraceLog};
use causet::{
    Action, ActionMeta, ActionPort, Child, Component, ComponentContext, ComponentRegistry,
    DispatchOutcome, Effect, EffectContext, EffectMeta, Mount, Never, Output, Registry, Store,
    StoreOptions, Timestamp, lens, root,
};

// ---------------------------------------------------------------------------
// The root's protocol

#[derive(Clone, Copy, Debug, PartialEq, Eq, Action)]
#[causet(type = "test.action")]
enum TestAction {
    RootMixed,
    Followup,
    RootNested,
    RootComponentFanout,
    RootDisabledGap,
    RootChildEffect,
    ChildEmitsEffect,
    Nested,
    OuterQueued,
    Disabled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Effect)]
#[causet(type = "test.effect")]
enum TestEffect {
    First,
    Second,
    DispatchNested,
}

// ---------------------------------------------------------------------------
// The component, mounted at /kid

#[derive(Clone, Copy, Debug, PartialEq, Eq, Action)]
#[causet(type = "kid.action")]
enum KidAction {
    /// What the root asks of it inline: emit Action, Output, Effect.
    Emit,
    Followup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Effect)]
#[causet(type = "kid.effect")]
enum KidEffect {
    Second,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Output)]
#[causet(type = "kid.output")]
enum KidOutput {
    Fanout,
}

struct Kid;

#[derive(Default)]
struct KidService {
    handled: u32,
}

impl Component for Kid {
    type State = ();
    type Service = KidService;
    type Action = KidAction;
    type Effect = KidEffect;
    type Output = KidOutput;

    fn reduce(ctx: ComponentContext<'_, Self>, action: KidAction, _: &ActionMeta) {
        if action == KidAction::Emit {
            // An Action, an Output and an Effect, in that order.
            let mut out = ctx.into_emitter();
            out.emit_action(KidAction::Followup);
            out.emit_output(KidOutput::Fanout);
            out.emit_effect(KidEffect::Second);
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, _: KidEffect, _: &EffectMeta) {
        ctx.service().handled += 1;
    }
}

const KID: Child<TestRoot, Kid> = Child::new("kid");

/// Lowers the kid's Output into the root's Actions.
fn lower_component_output(
    _: Child<TestRoot, Kid>,
    output: KidOutput,
    actions: &mut ActionPort<'_, TestRoot>,
) {
    match output {
        KidOutput::Fanout => {
            actions.emit_action(TestAction::Nested);
            actions.emit_action(TestAction::OuterQueued);
        }
    }
}

// ---------------------------------------------------------------------------
// The root: its reducer and Effect handler

struct TestRoot;

#[derive(Default)]
struct TestState {
    reductions: usize,
    kid: (),
}

#[derive(Default)]
struct TestService {
    handled_effects: usize,
    state_reductions_seen: Vec<usize>,
    kid: KidService,
}

impl Component for TestRoot {
    type State = TestState;
    type Service = TestService;
    type Action = TestAction;
    type Effect = TestEffect;
    type Output = Never;

    fn is_enabled(_: &TestState, action: &TestAction, _: Timestamp) -> bool {
        *action != TestAction::Disabled
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: TestAction, _: &ActionMeta) {
        ctx.state_mut().reductions += 1;
        match action {
            TestAction::RootMixed => {
                let mut out = ctx.into_emitter();
                out.emit_effect(TestEffect::First);
                out.emit_action(TestAction::Followup);
                out.emit_effect(TestEffect::Second);
            }
            TestAction::RootNested => {
                let mut out = ctx.into_emitter();
                out.emit_effect(TestEffect::DispatchNested);
                out.emit_action(TestAction::OuterQueued);
            }
            TestAction::RootComponentFanout => {
                ctx.reduce_child(KID, KidAction::Emit);
            }
            TestAction::RootDisabledGap => {
                let mut out = ctx.into_emitter();
                out.emit_action(TestAction::Disabled);
                out.emit_effect(TestEffect::Second);
            }
            TestAction::RootChildEffect => {
                ctx.into_emitter().emit_action(TestAction::ChildEmitsEffect)
            }
            TestAction::ChildEmitsEffect => ctx.into_emitter().emit_effect(TestEffect::Second),
            TestAction::Followup
            | TestAction::Nested
            | TestAction::OuterQueued
            | TestAction::Disabled => {}
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: TestEffect, _: &EffectMeta) {
        let reductions = ctx.state().expect("root State").reductions;
        let service = ctx.service();
        service.handled_effects += 1;
        service.state_reductions_seen.push(reductions);
        if effect == TestEffect::DispatchNested {
            assert_eq!(ctx.dispatch(TestAction::Nested), DispatchOutcome::Executed);
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(KID, lens!(TestState => kid))
            .service(lens!(TestService => kid))
            .on_output(lower_component_output)
            .bind();
    }
}

// ---------------------------------------------------------------------------
// Harness

struct Fixture {
    store: Store<TestState, TestService>,
    root: Mount<TestRoot>,
    root_instance: InstanceId,
    kid_instance: InstanceId,
}

fn make_store() -> Fixture {
    let mut registry = Registry::<TestState, TestService>::new();
    registry.root::<TestRoot>();
    let p = registry.freeze().unwrap();
    let (_sink, inbox) = EventBus::new(&p, 4);
    let mut store = Store::new(
        p.clone(),
        TestState::default(),
        TestService::default(),
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    store.add_observer(TraceLog::new());
    let kid = p.resolve(&root::<TestRoot>().child(KID)).unwrap();
    let root_mount = p.resolve(&root::<TestRoot>()).unwrap();
    Fixture {
        store,
        root: root_mount,
        root_instance: p.instance_of(&root_mount),
        kid_instance: p.instance_of(&kid),
    }
}

/// One entry per executed Action or Effect.
fn executions(store: &Store<TestState, TestService>) -> Vec<TraceEntry> {
    store.observer::<TraceLog>().unwrap().entries().to_vec()
}

fn start(category: Category, kind: &'static str) -> (Category, &'static str) {
    (category, kind)
}

fn order(store: &Store<TestState, TestService>) -> Vec<(Category, &'static str)> {
    executions(store)
        .iter()
        .map(|e| (e.category, e.kind))
        .collect()
}

/// (causal, component, emission ordinal, lowering ordinal); a root input has
/// no origin and is self-caused.
fn provenance(entry: &TraceEntry) -> (ExecutionId, Option<InstanceId>, u32, u32) {
    match entry.origin {
        Some(o) => (
            o.causal,
            Some(o.component),
            o.emission_ordinal,
            o.lowering_ordinal,
        ),
        None => (entry.causal, None, 0, 0),
    }
}

const A: Category = Category::Action;
const E: Category = Category::Effect;

// ---------------------------------------------------------------------------

#[test]
fn typed_dispatcher_preserves_one_mixed_fifo() {
    let mut f = make_store();

    assert_eq!(
        f.store.dispatch_to(&f.root, TestAction::RootMixed),
        Ok(DispatchOutcome::Executed)
    );

    assert_eq!(
        order(&f.store),
        [
            start(A, "test.root_mixed"),
            start(E, "test.first"),
            start(A, "test.followup"),
            start(E, "test.second")
        ]
    );
    assert_eq!(f.store.state().reductions, 2);
    assert_eq!(f.store.services().handled_effects, 2);
}

#[test]
fn typed_items_receive_fresh_globally_linked_metadata() {
    let mut f = make_store();

    f.store.dispatch_to(&f.root, TestAction::RootMixed).unwrap();

    let executions = executions(&f.store);
    assert_eq!(executions.len(), 4);
    assert_eq!(
        executions.iter().map(|e| e.depth).collect::<Vec<_>>(),
        [1, 2, 2, 2]
    );
    for pair in executions.windows(2) {
        // IDs count executions, in the order they start.
        assert_eq!(pair[1].id.0, pair[0].id.0 + 1);
    }

    let root = &executions[0];
    assert_eq!(root.causal, root.id);
    assert_eq!(provenance(root), (root.id, None, 0, 0));
    assert_eq!(
        executions[1..].iter().map(provenance).collect::<Vec<_>>(),
        [
            (root.id, Some(f.root_instance), 0, 0),
            (root.id, Some(f.root_instance), 1, 0),
            (root.id, Some(f.root_instance), 2, 0),
        ]
    );
}

#[test]
fn pooled_dispatcher_resets_each_root_emission_sequence() {
    let mut f = make_store();

    f.store.dispatch_to(&f.root, TestAction::RootMixed).unwrap();
    f.store.dispatch_to(&f.root, TestAction::RootMixed).unwrap();

    let executions = executions(&f.store);
    let [root, first, followup, second] = &executions[4..] else {
        panic!("expected the second root and its three queued items");
    };
    assert_eq!(root.causal, root.id);
    assert_eq!(root.origin, None);
    assert_eq!(
        [first, followup, second].map(|e| (provenance(e).0, provenance(e).2)),
        [(root.id, 0), (root.id, 1), (root.id, 2)]
    );
}

#[test]
fn effect_dispatched_action_completes_before_outer_fifo_resumes() {
    let mut f = make_store();

    f.store
        .dispatch_to(&f.root, TestAction::RootNested)
        .unwrap();

    let executions = executions(&f.store);
    assert_eq!(
        executions
            .iter()
            .map(|e| (e.category, e.kind, e.depth))
            .collect::<Vec<_>>(),
        [
            (A, "test.root_nested", 1),
            (E, "test.dispatch_nested", 2),
            (A, "test.nested", 3),
            (A, "test.outer_queued", 2),
        ]
    );
    assert_eq!(f.store.state().reductions, 3);
    assert_eq!(f.store.services().handled_effects, 1);
    assert_eq!(f.store.services().state_reductions_seen, [1]);

    let [root, effect, nested, outer] = executions.as_slice() else {
        panic!("expected four executions");
    };
    assert_eq!(root.causal, root.id);
    assert_eq!((provenance(effect).0, provenance(effect).2), (root.id, 0));
    assert_eq!((provenance(nested).0, provenance(nested).2), (effect.id, 0));
    assert_eq!((provenance(outer).0, provenance(outer).2), (root.id, 1));
    // It runs right after the nested Action, which did not cause it.
    assert_eq!(outer.id.0, nested.id.0 + 1);
    assert_ne!(outer.causal, nested.id);
}

#[test]
fn component_output_fanout_retains_one_emission_and_lowering_order() {
    let mut f = make_store();

    f.store
        .dispatch_to(&f.root, TestAction::RootComponentFanout)
        .unwrap();

    let executions = executions(&f.store);
    let [root, first, lowered_action, second_lowered_action, last] = executions.as_slice() else {
        panic!("expected root and four component-lowered executions, got {executions:#?}");
    };
    assert_eq!(root.causal, root.id);
    assert_eq!(
        [first, lowered_action, second_lowered_action, last]
            .map(|e| (e.category, e.kind, &*e.path)),
        [
            (A, "kid.followup", "/kid"),
            (A, "test.nested", "/"),
            (A, "test.outer_queued", "/"),
            (E, "kid.second", "/kid"),
        ]
    );
    assert_eq!(
        [first, lowered_action, second_lowered_action, last].map(provenance),
        [
            (root.id, Some(f.kid_instance), 0, 0),
            (root.id, Some(f.kid_instance), 1, 0),
            (root.id, Some(f.kid_instance), 1, 1),
            (root.id, Some(f.kid_instance), 2, 0),
        ]
    );
    assert_eq!(f.store.services().kid.handled, 1);
}

#[test]
fn drain_disabled_item_keeps_its_reserved_emission_gap() {
    let mut f = make_store();

    f.store
        .dispatch_to(&f.root, TestAction::RootDisabledGap)
        .unwrap();

    let executions = executions(&f.store);
    let [root, enabled_effect] = executions.as_slice() else {
        panic!("the disabled Action must not allocate execution metadata");
    };
    assert_eq!(
        (enabled_effect.category, enabled_effect.kind),
        (E, "test.second")
    );
    // The refused Action took no ID.
    assert_eq!(enabled_effect.id.0, root.id.0 + 1);
    assert_eq!(enabled_effect.causal, root.id);
    assert_eq!(provenance(enabled_effect).2, 1);
}

#[test]
fn child_action_starts_a_fresh_causal_emission_sequence() {
    let mut f = make_store();

    f.store
        .dispatch_to(&f.root, TestAction::RootChildEffect)
        .unwrap();

    let executions = executions(&f.store);
    let [root, child, effect] = executions.as_slice() else {
        panic!("expected root Action, child Action, and child Effect");
    };
    assert_eq!((provenance(child).0, provenance(child).2), (root.id, 0));
    assert_eq!((provenance(effect).0, provenance(effect).2), (child.id, 0));
}

#[test]
fn disabled_action_invokes_no_lifecycle_observer() {
    let mut f = make_store();

    assert_eq!(
        f.store.dispatch_to(&f.root, TestAction::Disabled),
        Ok(DispatchOutcome::Disabled)
    );
    assert!(executions(&f.store).is_empty());
}

// ---------------------------------------------------------------------------
// Observers

/// Every lifecycle callback, in order, with the execution's kind and ID.
#[derive(Default)]
struct Lifecycle {
    seen: Vec<(&'static str, &'static str, ExecutionId)>,
}

impl Lifecycle {
    fn calls(&self) -> Vec<(&'static str, &'static str)> {
        self.seen
            .iter()
            .map(|&(call, kind, _)| (call, kind))
            .collect()
    }
}

impl Observer for Lifecycle {
    fn action_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        self.seen.push(("action_start", exec.kind, exec.meta.id));
    }
    fn action_finish(&mut self, exec: &Execution<'_>) {
        self.seen.push(("action_finish", exec.kind, exec.meta.id));
    }
    fn action_settled(&mut self, exec: &Execution<'_>) {
        self.seen.push(("action_settled", exec.kind, exec.meta.id));
    }
    fn effect_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        self.seen.push(("effect_start", exec.kind, exec.meta.id));
    }
    fn effect_finish(&mut self, exec: &Execution<'_>) {
        self.seen.push(("effect_finish", exec.kind, exec.meta.id));
    }
}

#[test]
fn lifecycle_observers_wrap_only_their_typed_handlers() {
    let mut f = make_store();
    f.store.add_observer(Lifecycle::default());

    f.store.dispatch_to(&f.root, TestAction::RootMixed).unwrap();

    let lifecycle = f.store.observer::<Lifecycle>().unwrap();
    assert_eq!(
        lifecycle.calls(),
        [
            ("action_start", "test.root_mixed"),
            ("action_finish", "test.root_mixed"),
            ("effect_start", "test.first"),
            ("effect_finish", "test.first"),
            ("action_start", "test.followup"),
            ("action_finish", "test.followup"),
            ("action_settled", "test.followup"),
            ("effect_start", "test.second"),
            ("effect_finish", "test.second"),
            ("action_settled", "test.root_mixed"),
        ]
    );
    // Each pair of callbacks names one execution.
    for window in lifecycle.seen.windows(2) {
        if let [(start, _, a), (finish, _, b)] = window
            && start.ends_with("_start")
            && finish.ends_with("_finish")
        {
            assert_eq!(a, b);
        }
    }
}

#[test]
fn action_settled_observer_runs_after_the_entire_queued_fifo() {
    let mut f = make_store();
    f.store.add_observer(Lifecycle::default());

    f.store.dispatch_to(&f.root, TestAction::RootMixed).unwrap();

    let entries = executions(&f.store);
    let id_of = |kind| {
        entries
            .iter()
            .find(|e| e.kind == kind)
            .map(|e| e.id)
            .expect("executed")
    };
    let settled: Vec<_> = f
        .store
        .observer::<Lifecycle>()
        .unwrap()
        .seen
        .iter()
        .filter(|(call, _, _)| *call == "action_settled")
        .map(|&(_, _, id)| id)
        .collect();
    assert_eq!(settled, [id_of("test.followup"), id_of("test.root_mixed")]);
    // Observers have no State or Service authority; the test reads the
    // Service afterwards. Both Effects ran before the root settled:
    assert_eq!(f.store.services().handled_effects, 2);
}

/// Pushes its name on every Action start.
struct Named(
    &'static str,
    std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>,
);

impl Observer for Named {
    fn action_start(&mut self, _: &Execution<'_>, _: &dyn MessageView) {
        self.1.lock().unwrap().push(self.0);
    }
}

#[test]
fn additive_observers_run_in_order_and_clear_removes_them() {
    let mut f = make_store();
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    f.store.add_observer(Named("first", calls.clone()));
    f.store.add_observer(Named("second", calls.clone()));

    f.store.dispatch_to(&f.root, TestAction::Followup).unwrap();
    assert_eq!(*calls.lock().unwrap(), ["first", "second"]);

    calls.lock().unwrap().clear();
    f.store.clear_observers();
    f.store.add_observer(Named("replacement", calls.clone()));
    f.store.dispatch_to(&f.root, TestAction::Followup).unwrap();
    assert_eq!(*calls.lock().unwrap(), ["replacement"]);

    calls.lock().unwrap().clear();
    f.store.clear_observers();
    f.store.dispatch_to(&f.root, TestAction::Followup).unwrap();
    assert!(calls.lock().unwrap().is_empty());
}
