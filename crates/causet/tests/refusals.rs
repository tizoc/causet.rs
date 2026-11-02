//! Observers see every refused Action, wherever it was sent from, and every
//! child's Action reduced inline.

use std::sync::{Arc, Mutex};

use causet::hosting::EventBus;
use causet::observers::{
    Boundary, BugLog, Category, FaultInfo, InlineCall, MessageView, Observer, Phase, Refusal,
    RefusedBy, SentFrom, TraceLog,
};
use causet::*;

// ---------------------------------------------------------------------------
// Kid: refuses `Refuse`.

struct Kid;

#[derive(Debug, Action)]
#[causet(type = "kid.action")]
enum KidAction {
    Add(u32),
    Refuse,
}

impl Component for Kid {
    type State = u32;
    type Action = KidAction;
    type Output = Never;
    no_effects!();

    fn is_enabled(_: &u32, action: &KidAction, _: Timestamp) -> bool {
        !matches!(action, KidAction::Refuse)
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: KidAction, _: &ActionMeta) {
        if let KidAction::Add(n) = action {
            *ctx.state_mut() += n;
        }
    }
}

// ---------------------------------------------------------------------------
// Parent: sends to its kid inline or queued, and refuses `Refused` itself; its
// guard refuses an addition over 100.

struct Parent;

const KID: Child<Parent, Kid> = Child::new("kid");

#[derive(Debug, Default)]
struct ParentState {
    kid: u32,
    outcomes: Vec<ChildOutcome>,
}

#[derive(Debug, Action)]
#[causet(type = "parent.action")]
enum ParentAction {
    Inline(KidAction),
    Queue(KidAction),
    Poke,
    Refused,
}

#[derive(Debug, Effect)]
#[causet(type = "parent.effect")]
enum ParentEffect {
    Poke,
}

impl Component for Parent {
    type State = ParentState;
    type Service = ();
    type Action = ParentAction;
    type Effect = ParentEffect;
    type Output = Never;

    fn is_enabled(_: &ParentState, action: &ParentAction, _: Timestamp) -> bool {
        !matches!(action, ParentAction::Refused)
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: ParentAction, _: &ActionMeta) {
        match action {
            ParentAction::Inline(action) => {
                let outcome = ctx.reduce_child(KID, action);
                ctx.state_mut().outcomes.push(outcome);
            }
            ParentAction::Queue(action) => ctx.into_emitter().to_child(KID, action),
            ParentAction::Poke => ctx.into_emitter().emit_effect(ParentEffect::Poke),
            ParentAction::Refused => {}
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: ParentEffect, _: &EffectMeta) {
        let ParentEffect::Poke = effect;
        assert_eq!(
            ctx.dispatch(ParentAction::Refused),
            DispatchOutcome::Disabled
        );
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(KID, lens!(ParentState => kid))
            .admit(|_, action, _| !matches!(action, KidAction::Add(n) if *n > 100))
            .bind();
    }
}

struct Fixture {
    store: Store<ParentState, ()>,
    protocol: Arc<Protocol<ParentState, ()>>,
    parent: Mount<Parent>,
}

fn fixture() -> Fixture {
    let mut registry = Registry::<ParentState, ()>::new();
    registry.root::<Parent>();
    let protocol = registry.freeze().unwrap();
    let (_sink, inbox) = EventBus::new(&protocol, 16);
    let options = StoreOptions::with_clock(ManualClock::new(1_000));
    let mut store = Store::new(
        Arc::clone(&protocol),
        ParentState::default(),
        (),
        inbox,
        options,
    )
    .unwrap();
    store.add_observer(TraceLog::new());
    let parent = protocol.resolve(&root::<Parent>()).unwrap();
    Fixture {
        store,
        protocol,
        parent,
    }
}

impl Fixture {
    fn send(&mut self, action: ParentAction) -> DispatchOutcome {
        self.store.dispatch_to(&self.parent, action).unwrap()
    }

    fn trace(&self) -> &TraceLog {
        self.store.observer::<TraceLog>().unwrap()
    }

    /// The last root execution's ID.
    fn last_root(&self) -> ExecutionId {
        let entries = self.trace().entries();
        entries.iter().rev().find(|e| e.depth == 1).unwrap().id
    }
}

/// What a refusal says: where, what, by what and from where.
fn refusals(f: &Fixture) -> Vec<(String, String, RefusedBy, SentFrom)> {
    f.trace()
        .refusals()
        .iter()
        .map(|r| (r.path.to_string(), r.kind.to_owned(), r.by, r.sent))
        .collect()
}

#[test]
fn a_refused_root_action_is_told_with_no_execution() {
    let mut f = fixture();
    assert_eq!(f.send(ParentAction::Refused), DispatchOutcome::Disabled);
    assert!(f.trace().entries().is_empty(), "no execution");
    assert_eq!(
        refusals(&f),
        [(
            "/".into(),
            "parent.refused".into(),
            RefusedBy::Component,
            SentFrom::Root
        )]
    );
}

#[test]
fn a_refused_queued_action_is_told_with_its_origin() {
    let mut f = fixture();
    f.send(ParentAction::Queue(KidAction::Refuse));
    let sender = f.last_root();
    f.send(ParentAction::Queue(KidAction::Add(500)));
    let second = f.last_root();
    let [(path, kind, by, sent), (_, _, guard, guarded)] = &refusals(&f)[..] else {
        panic!("two refusals: {:?}", refusals(&f));
    };
    assert_eq!(
        (path.as_str(), kind.as_str(), *by),
        ("/kid", "kid.refuse", RefusedBy::Component)
    );
    let SentFrom::Queued(origin) = sent else {
        panic!("queued: {sent:?}");
    };
    assert_eq!((origin.causal, origin.emission_ordinal), (sender, 0));
    assert_eq!(*guard, RefusedBy::Guard);
    assert_eq!(guarded.execution(), Some(second));
    // Still no execution of their own: only the two parents ran.
    assert_eq!(f.trace().entries().len(), 2);
    assert_eq!(f.store.state().kid, 0);
}

#[test]
fn a_refused_inline_action_is_told_within_its_parent() {
    let mut f = fixture();
    f.send(ParentAction::Inline(KidAction::Refuse));
    let first = f.last_root();
    f.send(ParentAction::Inline(KidAction::Add(500)));
    let second = f.last_root();
    assert_eq!(
        refusals(&f),
        [
            (
                "/kid".into(),
                "kid.refuse".into(),
                RefusedBy::Component,
                SentFrom::Inline(first)
            ),
            (
                "/kid".into(),
                "kid.add".into(),
                RefusedBy::Guard,
                SentFrom::Inline(second)
            ),
        ]
    );
    assert_eq!(
        f.store.state().outcomes,
        [ChildOutcome::Refused, ChildOutcome::Refused]
    );
    assert!(f.trace().inline().is_empty(), "nothing reduced inline");
}

#[test]
fn a_refused_effect_dispatch_is_told_from_its_effect() {
    let mut f = fixture();
    f.send(ParentAction::Poke);
    let effect = f
        .trace()
        .entries()
        .iter()
        .find(|e| e.category == Category::Effect)
        .unwrap()
        .id;
    assert_eq!(
        refusals(&f),
        [(
            "/".into(),
            "parent.refused".into(),
            RefusedBy::Component,
            SentFrom::Effect(effect)
        )]
    );
}

#[test]
fn an_inline_action_is_told_within_its_parent_with_its_payload() {
    #[derive(Default)]
    struct Payloads(Vec<String>);
    impl Observer for Payloads {
        fn inline_action(&mut self, inline: &InlineCall<'_>, payload: &dyn MessageView) {
            let add = payload.downcast_ref::<KidAction>().unwrap();
            self.0
                .push(format!("{} {} {add:?}", inline.path, inline.kind));
        }
    }
    let mut f = fixture();
    f.store.add_observer(Payloads::default());
    f.send(ParentAction::Inline(KidAction::Add(2)));
    let parent = f.last_root();
    let inline = f.trace().inline();
    assert_eq!(inline.len(), 1);
    assert_eq!(
        (&*inline[0].path, inline[0].kind, inline[0].within),
        ("/kid", "kid.add", parent)
    );
    assert_eq!(
        f.store.observer::<Payloads>().unwrap().0,
        ["/kid kid.add Add(2)"]
    );
    // InlineCall, so no execution of its own; and a queued one is no inline call.
    assert_eq!(f.trace().entries().len(), 1);
    f.send(ParentAction::Queue(KidAction::Add(3)));
    assert_eq!(f.trace().inline().len(), 1);
    assert_eq!(f.store.state().kid, 5);
}

/// A panic in either callback faults the Store at its boundary, naming the
/// execution that sent the Action.
#[test]
fn a_panic_in_either_callback_faults_at_its_boundary() {
    type Faults = Arc<Mutex<Vec<(Phase, Option<ExecutionId>)>>>;
    struct Panics(Faults);
    impl Observer for Panics {
        fn action_refused(&mut self, _: &Refusal<'_>, _: &dyn MessageView) {
            panic!("refused");
        }
        fn inline_action(&mut self, _: &InlineCall<'_>, _: &dyn MessageView) {
            panic!("inline");
        }
        fn fault(&mut self, fault: &FaultInfo<'_>) {
            self.0.lock().unwrap().push((fault.phase, fault.at));
        }
    }
    let faults = |action: ParentAction| {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut f = fixture();
        f.store.add_observer(Panics(Arc::clone(&seen)));
        assert!(matches!(
            f.store.dispatch_to(&f.parent, action),
            Err(DispatchError::Faulted(_))
        ));
        seen.lock().unwrap().clone()
    };
    assert_eq!(
        faults(ParentAction::Refused),
        [(Phase::Observer(Boundary::Refused), None)]
    );
    let [(phase, at)] = faults(ParentAction::Inline(KidAction::Add(1)))[..] else {
        panic!("one fault");
    };
    assert_eq!(
        (phase, at.is_some()),
        (Phase::Observer(Boundary::Inline), true)
    );
    let [(phase, at)] = faults(ParentAction::Queue(KidAction::Refuse))[..] else {
        panic!("one fault");
    };
    assert_eq!(
        (phase, at.is_some()),
        (Phase::Observer(Boundary::Refused), true)
    );
}

#[test]
fn a_refusal_and_an_inline_call_carry_their_mount_type_and_time() {
    let mut f = fixture();
    f.send(ParentAction::Queue(KidAction::Refuse));
    let sender = f.last_root();
    f.send(ParentAction::Inline(KidAction::Add(1)));
    let kid = f.protocol.resolve(&root::<Parent>().child(KID)).unwrap();
    let instance = f.protocol.instance_of(&kid);
    let [refusal] = f.trace().refusals() else {
        panic!("one refusal")
    };
    // Checked when its turn came: right after its sender, at its time.
    let sent_at = f
        .trace()
        .entries()
        .iter()
        .find(|e| e.id == sender)
        .unwrap()
        .time;
    assert_eq!(refusal.time, sent_at);
    // An inline call is told at its parent's time.
    let parent = f.last_root();
    let parent_at = f
        .trace()
        .entries()
        .iter()
        .find(|e| e.id == parent)
        .unwrap()
        .time;
    let [inline] = f.trace().inline() else {
        panic!("one inline call")
    };
    assert_eq!((inline.within, inline.time), (parent, parent_at));
    assert_ne!(inline.time, parent.0, "a time, not the parent's ID");
    #[derive(Default)]
    struct Fields(Vec<(InstanceId, &'static str)>);
    impl Observer for Fields {
        fn action_refused(&mut self, refusal: &Refusal<'_>, _: &dyn MessageView) {
            self.0.push((refusal.instance, refusal.message));
        }
        fn inline_action(&mut self, inline: &InlineCall<'_>, _: &dyn MessageView) {
            self.0.push((inline.instance, inline.message));
        }
    }
    f.store.add_observer(Fields::default());
    f.send(ParentAction::Queue(KidAction::Refuse));
    f.send(ParentAction::Inline(KidAction::Add(1)));
    assert_eq!(
        f.store.observer::<Fields>().unwrap().0,
        [(instance, "kid.action"), (instance, "kid.action")]
    );
}

// ---------------------------------------------------------------------------
// Top, Mid and Leaf: inline calls two deep, a Leaf whose kind panics from 90
// up, and a Top that catches a panic.

struct Top;
struct Mid;
struct Leaf;

const MID: Child<Top, Mid> = Child::new("mid");
const LEAF: Child<Mid, Leaf> = Child::new("leaf");

#[derive(Debug, Default)]
struct TopState {
    mid: MidState,
}

#[derive(Debug, Default)]
struct MidState {
    leaf: u32,
}

/// Its kind is application code that panics from 90 up.
#[derive(Debug)]
struct LeafAction(u32);

impl Message for LeafAction {
    const TYPE: &'static str = "leaf.action";
    fn kind(&self) -> &'static str {
        assert!(self.0 < 90, "the kind callback failed");
        "leaf.go"
    }
}
impl Action for LeafAction {}

#[derive(Debug, Action)]
#[causet(type = "mid.action")]
enum MidAction {
    Inline(u32),
    Queue(u32),
}

#[derive(Debug, Clone, Copy, Action)]
#[causet(type = "top.action")]
enum TopAction {
    Inline(u32),
    Queue(u32),
    /// Calls inline, catching any panic, then does `then`.
    Catching {
        then: Then,
    },
    Panic,
}

/// What a reducer does after catching a panic.
#[derive(Debug, Clone, Copy)]
enum Then {
    Nothing,
    /// Panics with its own text.
    Panic,
    /// Panics with the very text the observer panicked with.
    PanicAlike,
    /// Panics with a payload that is not text.
    PanicWithNumber,
}

/// What an observer panics with.
#[derive(Clone, Copy)]
enum Panics {
    Text,
    Number,
}

impl Component for Leaf {
    type State = u32;
    type Action = LeafAction;
    type Output = Never;
    no_effects!();

    // Refuses 0 and every odd number from 91.
    fn is_enabled(_: &u32, action: &LeafAction, _: Timestamp) -> bool {
        action.0 != 0 && !(action.0 >= 91 && action.0 % 2 == 1)
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: LeafAction, _: &ActionMeta) {
        *ctx.state_mut() += action.0;
    }
}

impl Component for Mid {
    type State = MidState;
    type Action = MidAction;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: MidAction, _: &ActionMeta) {
        match action {
            MidAction::Inline(n) => drop(ctx.reduce_child(LEAF, LeafAction(n))),
            MidAction::Queue(n) => ctx.into_emitter().to_child(LEAF, LeafAction(n)),
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(LEAF, lens!(MidState => leaf)).bind();
    }
}

impl Component for Top {
    type State = TopState;
    type Action = TopAction;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: TopAction, _: &ActionMeta) {
        match action {
            TopAction::Inline(n) => drop(ctx.reduce_child(MID, MidAction::Inline(n))),
            TopAction::Queue(n) => drop(ctx.reduce_child(MID, MidAction::Queue(n))),
            TopAction::Catching { then } => {
                let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    ctx.reduce_child(MID, MidAction::Inline(1))
                }));
                assert!(caught.is_err(), "the observer did not panic");
                match then {
                    Then::Nothing => {}
                    Then::Panic => panic!("the reducer panics afterwards"),
                    Then::PanicAlike => panic!("the observer panicked"),
                    Then::PanicWithNumber => std::panic::panic_any(2_u32),
                }
            }
            TopAction::Panic => panic!("the observer panicked"),
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(MID, lens!(TopState => mid)).bind();
    }
}

fn top_store() -> (Store<TopState, ()>, Mount<Top>) {
    let mut registry = Registry::<TopState, ()>::new();
    registry.root::<Top>();
    let protocol = registry.freeze().unwrap();
    let (_sink, inbox) = EventBus::new(&protocol, 16);
    let options = StoreOptions::with_clock(ManualClock::new(1_000));
    let store = Store::new(
        Arc::clone(&protocol),
        TopState::default(),
        (),
        inbox,
        options,
    )
    .unwrap();
    (store, protocol.resolve(&root::<Top>()).unwrap())
}

/// Every fault, with its phase and execution.
type Faults = Arc<Mutex<Vec<(Phase, Option<ExecutionId>, String)>>>;

/// Records faults; panics in `inline_action` if told to.
struct Watcher {
    faults: Faults,
    panics: Option<Panics>,
}

impl Observer for Watcher {
    fn inline_action(&mut self, _: &InlineCall<'_>, _: &dyn MessageView) {
        match self.panics {
            None => {}
            Some(Panics::Text) => panic!("the observer panicked"),
            Some(Panics::Number) => std::panic::panic_any(1_u32),
        }
    }
    fn fault(&mut self, fault: &FaultInfo<'_>) {
        let entry = (fault.phase, fault.at, fault.message.to_owned());
        self.faults.lock().unwrap().push(entry);
    }
}

#[test]
fn inline_calls_two_deep_are_told_within_the_root_execution() {
    let (mut store, top) = top_store();
    store.add_observer(TraceLog::new());
    store.dispatch_to(&top, TopAction::Inline(2)).unwrap();
    store.dispatch_to(&top, TopAction::Inline(0)).unwrap();
    let trace = store.observer::<TraceLog>().unwrap();
    let [first, second] = trace.entries() else {
        panic!("two executions")
    };
    let inline: Vec<_> = trace
        .inline()
        .iter()
        .map(|i| (&*i.path, i.kind, i.within))
        .collect();
    assert_eq!(
        inline,
        [
            ("/mid", "mid.inline", first.id),
            ("/mid/leaf", "leaf.go", first.id),
            ("/mid", "mid.inline", second.id),
        ]
    );
    let [refusal] = trace.refusals() else {
        panic!("one refusal")
    };
    assert_eq!(
        (&*refusal.path, refusal.by, refusal.sent),
        (
            "/mid/leaf",
            RefusedBy::Component,
            SentFrom::Inline(second.id)
        )
    );
    assert_eq!(store.state().mid.leaf, 2);
}

/// A refused Action's kind is read only to tell an observer of it, and an
/// inline Action's only to tell one of the call: with none, a kind that
/// panics changes nothing.
#[test]
fn with_no_observer_a_kind_is_not_read_to_tell_one() {
    let (mut store, top) = top_store();
    // Admitted inline, refused inline, refused queued.
    for action in [
        TopAction::Inline(90),
        TopAction::Inline(91),
        TopAction::Queue(93),
    ] {
        assert_eq!(
            store.dispatch_to(&top, action),
            Ok(DispatchOutcome::Executed)
        );
    }
    assert_eq!(store.state().mid.leaf, 90);
}

/// With an observer, reading the kind is part of telling it: a kind that
/// panics faults the Store as that observer boundary's fault.
#[test]
fn with_an_observer_a_kind_that_panics_faults_at_its_boundary() {
    let faults = |action: TopAction| {
        let (mut store, top) = top_store();
        let seen = Faults::default();
        store.add_observer(TraceLog::new());
        store.add_observer(Watcher {
            faults: Arc::clone(&seen),
            panics: None,
        });
        assert!(store.dispatch_to(&top, action).is_err());
        let root = store.observer::<TraceLog>().unwrap().entries()[0].id;
        let (phase, at, _) = seen.lock().unwrap()[0].clone();
        (phase, at == Some(root))
    };
    assert_eq!(
        faults(TopAction::Inline(90)),
        (Phase::Observer(Boundary::Inline), true)
    );
    assert_eq!(
        faults(TopAction::Inline(91)),
        (Phase::Observer(Boundary::Refused), true)
    );
    // Queued by Mid, inline within the root execution: its refusal names that
    // execution too.
    assert_eq!(
        faults(TopAction::Queue(93)),
        (Phase::Observer(Boundary::Refused), true)
    );
}

/// A reducer that catches an observer's panic swallows it, as it would any
/// panic; a later fault is then placed where it happens, not at the
/// observer, whatever its payload.
#[test]
fn an_observer_panic_a_reducer_catches_leaves_no_stale_place() {
    // The first fault's phase and message, after `actions` with an observer
    // that panics with `panics`.
    let fault = |panics: Panics, actions: &[TopAction]| {
        let (mut store, top) = top_store();
        let seen = Faults::default();
        store.add_observer(Watcher {
            faults: Arc::clone(&seen),
            panics: Some(panics),
        });
        for action in actions {
            let _ = store.dispatch_to(&top, *action);
        }
        let (phase, _, message) = seen.lock().unwrap()[0].clone();
        (phase, message)
    };
    let catching = |then| TopAction::Catching { then };
    // Caught, then the reducer panics: with its own text, the very text
    // the observer panicked with, or a number after the observer's number.
    for (panics, then, message) in [
        (Panics::Text, Then::Panic, "the reducer panics afterwards"),
        (Panics::Text, Then::PanicAlike, "the observer panicked"),
        (Panics::Number, Then::PanicWithNumber, "panic"),
    ] {
        assert_eq!(
            fault(panics, &[catching(then)]),
            (Phase::Reducer, message.to_owned()),
            "{then:?}"
        );
    }
    // Caught and swallowed; a later root input faulting with the very same
    // text as the observer's is placed at its reducer.
    assert_eq!(
        fault(Panics::Text, &[catching(Then::Nothing), TopAction::Panic]).0,
        Phase::Reducer
    );
    // Uncaught, it is the observer's fault, with its message.
    for (panics, message) in [
        (Panics::Text, "the observer panicked"),
        (Panics::Number, "panic"),
    ] {
        assert_eq!(
            fault(panics, &[TopAction::Inline(1)]),
            (Phase::Observer(Boundary::Inline), message.to_owned())
        );
    }
}

/// A bug an observer reports while told of an inline call or a refusal is
/// placed at that boundary, in the execution that sent the Action, at the
/// child's mount.
#[test]
fn a_bug_an_observer_reports_is_placed_at_its_boundary() {
    struct Reports;
    impl Observer for Reports {
        fn inline_action(&mut self, _: &InlineCall<'_>, _: &dyn MessageView) {
            bug_condition!("seen inline");
        }
        fn action_refused(&mut self, _: &Refusal<'_>, _: &dyn MessageView) {
            bug_condition!("seen refused");
        }
    }
    let (mut store, top) = top_store();
    store.add_observer(TraceLog::new());
    store.add_observer(BugLog::new());
    store.add_observer(Reports);
    store.dispatch_to(&top, TopAction::Queue(0)).unwrap();
    let root = store.observer::<TraceLog>().unwrap().entries()[0].id;
    let places: Vec<_> = store
        .observer::<BugLog>()
        .unwrap()
        .reports()
        .iter()
        .map(|r| {
            let place = r.place.as_ref().unwrap();
            (
                r.message.as_str(),
                place.phase,
                place.at,
                place.path.to_string(),
            )
        })
        .collect();
    assert_eq!(
        places,
        [
            (
                "seen inline",
                Phase::Observer(Boundary::Inline),
                Some(root),
                "/mid".into()
            ),
            (
                "seen refused",
                Phase::Observer(Boundary::Refused),
                Some(root),
                "/mid/leaf".into()
            ),
        ]
    );
}
