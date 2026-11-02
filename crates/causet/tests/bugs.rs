//! Bug reports (`bug_condition!`): the caller's fallback runs, the report
//! names its place, and observers get it.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use causet::hosting::EventBus;
use causet::observers::{
    Boundary, BugLog, BugReport, Execution, MessageView, Observer, Phase, set_bug_hook,
};
use causet::*;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record as SpanRecord};
use tracing::{Event as TraceEvent, Level, Metadata, Subscriber};

// ---------------------------------------------------------------------------
// A parent and a child, with every place a report can come from.

#[derive(Clone, Default, Debug, PartialEq)]
struct KidState {
    pings: u32,
}

#[derive(Debug, Action)]
#[causet(type = "kid.action")]
enum KidAction {
    Ping {
        report: bool,
    },
    /// Its enabling check reports and refuses it.
    Refuse,
    /// The parent's guard reports and refuses it.
    Guarded,
    /// Reported as an Output the parent's lowerer reports about.
    Shout,
    CrashReport,
}

#[derive(Debug, Output)]
#[causet(type = "kid.output")]
enum KidOutput {
    Shouted,
}

struct Kid;

impl Component for Kid {
    type State = KidState;
    type Action = KidAction;
    type Output = KidOutput;
    no_effects!();

    fn is_enabled(_: &KidState, action: &KidAction, _: Timestamp) -> bool {
        if let KidAction::Refuse = action {
            bug_condition!("kid refuses");
            return false;
        }
        true
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: KidAction, _: &ActionMeta) {
        match action {
            KidAction::Ping { report } => {
                if report {
                    let pings = ctx.state().pings;
                    bug_condition!(pings = pings; "kid ping");
                }
                ctx.state_mut().pings += 1;
            }
            KidAction::CrashReport => {
                bug_condition!("kid before panic");
                panic!("kid panic");
            }
            KidAction::Shout => ctx.into_emitter().emit_output(KidOutput::Shouted),
            KidAction::Refuse | KidAction::Guarded => unreachable!("refused"),
        }
    }
}

const KID: Child<Tree, Kid> = Child::new("kid");

#[derive(Clone, Default, Debug, PartialEq)]
struct TreeState {
    kid: KidState,
    notes: u32,
}

#[derive(Debug, Action)]
#[causet(type = "tree.action")]
enum TreeAction {
    Inline {
        before: bool,
        kid: bool,
        after: bool,
    },
    Queue(KidActionTag),
    Fx,
    Noted,
    Crash,
    AdmissionBug {
        refuse: bool,
    },
    FxAdmission {
        refuse: bool,
    },
    InlineCrash,
}

#[derive(Debug)]
enum KidActionTag {
    Refuse,
    Guarded,
    Shout,
}

#[derive(Debug, Effect)]
#[causet(type = "tree.effect", kind = "tree.work")]
struct Work {
    mode: u8,
}

#[derive(Debug, Event)]
#[causet(type = "tree.poked")]
struct Poked;

struct Tree;

fn guard(_: &TreeState, action: &KidAction, _: Timestamp) -> bool {
    if let KidAction::Guarded = action {
        bug_condition!("guard refuses");
        return false;
    }
    true
}

fn lower(_: Child<Tree, Kid>, output: KidOutput, port: &mut ActionPort<'_, Tree>) {
    match output {
        KidOutput::Shouted => {
            bug_condition!("lowered");
            port.emit_action(TreeAction::Noted);
        }
    }
}

impl Component for Tree {
    type State = TreeState;
    type Service = ();
    type Action = TreeAction;
    type Effect = Work;
    type Output = Never;

    fn is_enabled(_: &TreeState, action: &TreeAction, _: Timestamp) -> bool {
        if let TreeAction::AdmissionBug { refuse } = action {
            bug_condition!("sync admission");
            !refuse
        } else {
            true
        }
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: TreeAction, _: &ActionMeta) {
        match action {
            TreeAction::Inline { before, kid, after } => {
                if before {
                    bug_condition!("parent before");
                }
                ctx.reduce_child(KID, KidAction::Ping { report: kid });
                if after {
                    bug_condition!("parent after");
                }
            }
            TreeAction::Queue(tag) => {
                let action = match tag {
                    KidActionTag::Refuse => KidAction::Refuse,
                    KidActionTag::Guarded => KidAction::Guarded,
                    KidActionTag::Shout => KidAction::Shout,
                };
                ctx.into_emitter().to_child(KID, action);
            }
            TreeAction::Fx => ctx.into_emitter().emit_effect(Work { mode: 0 }),
            TreeAction::FxAdmission { refuse } => ctx.into_emitter().emit_effect(Work {
                mode: if refuse { 2 } else { 1 },
            }),
            TreeAction::InlineCrash => {
                ctx.reduce_child(KID, KidAction::CrashReport);
            }
            TreeAction::AdmissionBug { .. } => ctx.state_mut().notes += 1,
            TreeAction::Noted => ctx.state_mut().notes += 1,
            TreeAction::Crash => {
                bug_condition!("about to crash");
                panic!("crashed");
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, work: Work, _: &EffectMeta) {
        if work.mode == 1 || work.mode == 2 {
            ctx.dispatch(TreeAction::AdmissionBug {
                refuse: work.mode == 2,
            });
            return;
        }
        bug_condition!("handler before");
        ctx.dispatch(TreeAction::Noted);
        bug_condition!("handler after");
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Poked>();
        r.child(KID, lens!(TreeState => kid))
            .admit(guard)
            .on_output(lower)
            .bind();
    }
}

impl Adapts<Poked> for Tree {
    fn adapt(_: Poked, port: &mut ActionPort<'_, Self>) {
        bug_condition!("adapter");
        port.emit_action(TreeAction::Noted);
    }
}

/// Everything observers see, in order: starts and reports.
#[derive(Default)]
struct Order(Arc<Mutex<Vec<String>>>);

impl Observer for Order {
    fn action_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        self.0.lock().unwrap().push(format!("start {}", exec.kind));
    }
    fn effect_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        self.0.lock().unwrap().push(format!("start {}", exec.kind));
    }
    fn bug(&mut self, report: &BugReport) {
        self.0
            .lock()
            .unwrap()
            .push(format!("bug {}", report.message));
    }
}

struct Fixture {
    store: Store<TreeState, ()>,
    sink: EventSink,
    tree: Mount<Tree>,
    protocol: Arc<Protocol<TreeState, ()>>,
}

fn protocol() -> Arc<Protocol<TreeState, ()>> {
    let mut registry = Registry::<TreeState, ()>::new();
    registry.root::<Tree>();
    registry.freeze().unwrap()
}

fn fixture() -> Fixture {
    fixture_with_clock(ManualClock::new(1000))
}

fn fixture_with_clock(clock: impl Clock + 'static) -> Fixture {
    let protocol = protocol();
    let (sink, inbox) = EventBus::new(&protocol, 16);
    let mut store = Store::new(
        Arc::clone(&protocol),
        TreeState::default(),
        (),
        inbox,
        StoreOptions::with_clock(clock),
    )
    .unwrap();
    store.add_observer(BugLog::new());
    let tree = protocol.resolve(&root::<Tree>()).unwrap();
    Fixture {
        store,
        sink,
        tree,
        protocol,
    }
}

fn reports(f: &Fixture) -> Vec<BugReport> {
    f.store.observer::<BugLog>().unwrap().reports().to_vec()
}

fn summary(report: &BugReport) -> (String, Phase, Option<u64>, String) {
    let place = report.place.as_ref().expect("made inside a Store");
    (
        report.message.clone(),
        place.phase,
        place.at.map(|id| id.0),
        place.path.to_string(),
    )
}

// ---------------------------------------------------------------------------

#[test]
fn a_report_is_placed_and_the_fallback_runs() {
    let mut f = fixture();
    let outcome = f.store.dispatch_to(
        &f.tree,
        TreeAction::Inline {
            before: false,
            kid: true,
            after: false,
        },
    );
    assert_eq!(outcome, Ok(DispatchOutcome::Executed));
    assert_eq!(
        f.store.state().kid.pings,
        1,
        "the code after the report ran"
    );

    let [report] = reports(&f).try_into().unwrap();
    assert_eq!(
        summary(&report),
        ("kid ping".into(), Phase::Reducer, Some(1), "/kid".into())
    );
    assert_eq!(report.fields, [("pings".into(), "0".into())]);
    assert!(report.file.ends_with("bugs.rs"));
    let place = report.place.unwrap();
    assert_eq!(place.sequence, 0);
    assert_eq!(
        place.component,
        f.protocol
            .instance_of(&f.protocol.resolve(&root::<Tree>().child(KID)).unwrap())
    );
}

#[test]
fn reports_name_the_component_whose_code_made_them() {
    let mut f = fixture();
    f.store
        .dispatch_to(
            &f.tree,
            TreeAction::Inline {
                before: true,
                kid: true,
                after: true,
            },
        )
        .unwrap();
    f.store
        .dispatch_to(&f.tree, TreeAction::Queue(KidActionTag::Guarded))
        .unwrap();
    f.store
        .dispatch_to(&f.tree, TreeAction::Queue(KidActionTag::Refuse))
        .unwrap();
    f.store
        .dispatch_to(&f.tree, TreeAction::Queue(KidActionTag::Shout))
        .unwrap();

    let all: Vec<_> = reports(&f).iter().map(summary).collect();
    assert_eq!(
        all,
        [
            // One execution: the parent, the child it called inline, the parent.
            ("parent before".into(), Phase::Reducer, Some(1), "/".into()),
            ("kid ping".into(), Phase::Reducer, Some(1), "/kid".into()),
            ("parent after".into(), Phase::Reducer, Some(1), "/".into()),
            // Queued Actions refused at admission: the guard is the parent's
            // code, the enabling check the child's; both name the execution
            // that queued the Action.
            (
                "guard refuses".into(),
                Phase::Admission,
                Some(2),
                "/".into()
            ),
            (
                "kid refuses".into(),
                Phase::Admission,
                Some(3),
                "/kid".into()
            ),
            // The child's Output, lowered by the parent, in the child's execution.
            ("lowered".into(), Phase::Reducer, Some(5), "/".into()),
        ]
    );
    let sequences: Vec<_> = reports(&f)
        .iter()
        .map(|r| r.place.as_ref().unwrap().sequence)
        .collect();
    assert_eq!(sequences, [0, 1, 2, 3, 4, 5]);
    assert_eq!(f.store.state().notes, 1);
}

#[test]
fn a_root_action_refused_at_admission_names_no_execution() {
    let mut f = fixture();
    let kid = f.protocol.resolve(&root::<Tree>().child(KID)).unwrap();
    assert_eq!(
        f.store.dispatch_to(&kid, KidAction::Refuse),
        Ok(DispatchOutcome::Disabled)
    );
    let [report] = reports(&f).try_into().unwrap();
    assert_eq!(
        summary(&report),
        ("kid refuses".into(), Phase::Admission, None, "/kid".into())
    );
}

#[test]
fn a_handler_report_keeps_its_order_around_a_synchronous_dispatch() {
    let mut f = fixture();
    let order = Arc::new(Mutex::new(Vec::new()));
    f.store.add_observer(Order(Arc::clone(&order)));
    f.store.dispatch_to(&f.tree, TreeAction::Fx).unwrap();
    assert_eq!(
        *order.lock().unwrap(),
        [
            "start tree.fx",
            "start tree.work",
            "bug handler before",
            "start tree.noted",
            "bug handler after"
        ]
    );
    let places: Vec<_> = reports(&f).iter().map(summary).collect();
    assert_eq!(
        places[0],
        ("handler before".into(), Phase::Handler, Some(2), "/".into())
    );
    assert_eq!(
        places[1],
        ("handler after".into(), Phase::Handler, Some(2), "/".into())
    );
}

#[test]
fn an_adapter_report_is_placed_in_the_event() {
    let mut f = fixture();
    f.sink.try_send_to(&f.tree, Poked).unwrap();
    f.store.drain_events(4).unwrap();
    let [report] = reports(&f).try_into().unwrap();
    assert_eq!(
        summary(&report),
        ("adapter".into(), Phase::Adapter, Some(1), "/".into())
    );
}

#[test]
fn reports_made_before_a_fault_are_kept() {
    let mut f = fixture();
    assert!(f.store.dispatch_to(&f.tree, TreeAction::Crash).is_err());
    let [report] = reports(&f).try_into().unwrap();
    assert_eq!(
        summary(&report),
        ("about to crash".into(), Phase::Reducer, Some(1), "/".into())
    );
}

#[test]
fn an_observer_report_is_placed() {
    struct Nosy;
    impl Observer for Nosy {
        fn action_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
            if exec.kind == "tree.noted" {
                bug_condition!("observer");
            }
        }
    }
    let mut f = fixture();
    f.store.add_observer(Nosy);
    f.store.dispatch_to(&f.tree, TreeAction::Noted).unwrap();
    let [report] = reports(&f).try_into().unwrap();
    assert_eq!(
        summary(&report),
        (
            "observer".into(),
            Phase::Observer(Boundary::Start),
            Some(1),
            "/".into()
        )
    );
}

#[test]
fn the_harness_keeps_reports_without_a_place() {
    let mut harness = Harness::<Kid>::new();
    let mut state = KidState::default();
    harness.reduce(&mut state, KidAction::Ping { report: true });
    assert!(!harness.is_enabled(&state, &KidAction::Refuse));
    let messages: Vec<_> = harness
        .bug_reports()
        .iter()
        .map(|r| (r.message.as_str(), r.place.is_none()))
        .collect();
    assert_eq!(messages, [("kid ping", true), ("kid refuses", true)]);
}

#[test]
fn outside_a_store_a_report_goes_to_the_hook() {
    static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());
    set_bug_hook(|report| {
        if report.place.is_none() && report.message.starts_with("outside") {
            SEEN.lock().unwrap().push(report.message.clone());
        }
    });
    let helper = |n: u32| -> u32 {
        if n == 0 {
            bug_condition!("outside, from a helper");
            return 1;
        }
        n
    };
    assert_eq!(helper(0), 1);
    std::thread::spawn(|| bug_condition!("outside, from a Service thread"))
        .join()
        .unwrap();
    let mut seen = SEEN.lock().unwrap().clone();
    seen.sort();
    assert_eq!(
        seen,
        ["outside, from a Service thread", "outside, from a helper"]
    );
}

#[test]
fn a_store_run_inside_another_keeps_its_reports_apart() {
    // The outer Store's handler runs a whole inner Store, which reports too.
    struct Outer;
    #[derive(Debug, Action)]
    #[causet(type = "outer.action", kind = "outer.go")]
    struct Go;
    #[derive(Debug, Effect)]
    #[causet(type = "outer.effect", kind = "outer.nest")]
    struct Nest;
    impl Component for Outer {
        type State = ();
        type Service = ();
        type Action = Go;
        type Effect = Nest;
        type Output = Never;
        fn reduce(ctx: ComponentContext<'_, Self>, _: Go, _: &ActionMeta) {
            ctx.into_emitter().emit_effect(Nest);
        }
        fn handle(_: EffectContext<'_, Self>, _: Nest, _: &EffectMeta) {
            bug_condition!("outer before");
            let mut inner = fixture();
            inner
                .store
                .dispatch_to(
                    &inner.tree,
                    TreeAction::Inline {
                        before: true,
                        kid: false,
                        after: false,
                    },
                )
                .unwrap();
            let inner_reports: Vec<_> = reports(&inner).into_iter().map(|r| r.message).collect();
            assert_eq!(inner_reports, ["parent before"]);
            bug_condition!("outer after");
        }
    }
    let mut registry = Registry::<(), ()>::new();
    registry.root::<Outer>();
    let protocol = registry.freeze().unwrap();
    let (_sink, inbox) = EventBus::new(&protocol, 4);
    let mut store = Store::new(
        Arc::clone(&protocol),
        (),
        (),
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    store.add_observer(BugLog::new());
    store
        .dispatch_to(&protocol.resolve(&root::<Outer>()).unwrap(), Go)
        .unwrap();
    store
        .observer::<BugLog>()
        .unwrap()
        .assert_messages(&["outer before", "outer after"]);
}

#[test]
fn a_test_checks_reports_after_the_run() {
    let mut f = fixture();
    f.store.dispatch_to(&f.tree, TreeAction::Noted).unwrap();
    f.store.observer::<BugLog>().unwrap().assert_none();
    f.store
        .dispatch_to(
            &f.tree,
            TreeAction::Inline {
                before: false,
                kid: true,
                after: false,
            },
        )
        .unwrap();
    let log = f.store.observer::<BugLog>().unwrap();
    let unexpected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| log.assert_none()));
    assert!(unexpected.is_err());
    f.store
        .observer::<BugLog>()
        .unwrap()
        .assert_messages(&["kid ping"]);
}

#[test]
fn a_child_that_reports_then_panics_keeps_its_report() {
    let mut f = fixture();
    assert!(
        f.store
            .dispatch_to(&f.tree, TreeAction::InlineCrash)
            .is_err()
    );
    let all = reports(&f);
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].place.as_ref().unwrap().path.as_ref(), "/kid");
}

#[derive(Debug)]
struct Captured {
    target: String,
    level: Level,
    fields: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Captured>>>);

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
}

impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &SpanRecord<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &TraceEvent<'_>) {
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        let metadata = event.metadata();
        self.0.lock().unwrap().push(Captured {
            target: metadata.target().into(),
            level: *metadata.level(),
            fields,
        });
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

fn captured(f: impl FnOnce()) -> Vec<Captured> {
    let capture = Capture::default();
    tracing::subscriber::with_default(capture.clone(), f);
    std::mem::take(&mut *capture.0.lock().unwrap())
}

#[test]
fn the_logger_logs_bug_reports_at_error_level() {
    let mut f = fixture();
    f.store.clear_observers();
    f.store.add_observer(causet::observers::Logger::new());
    let events = captured(|| {
        f.store
            .dispatch_to(
                &f.tree,
                TreeAction::Inline {
                    before: true,
                    kid: false,
                    after: false,
                },
            )
            .unwrap();
    });
    let bug = events
        .iter()
        .find(|e| e.level == Level::ERROR)
        .unwrap_or_else(|| panic!("no error-level event: {events:?}"));
    assert_eq!(bug.target, "causet::exec");
    assert_eq!(bug.fields["message"], "bug condition: parent before");
    assert_eq!(bug.fields["path"], "/");
    assert_eq!(bug.fields["phase"], "Some(Reducer)");
}

/// Reports while the synchronous child starts: its fourth reading follows
/// Store construction, the root Action and the Effect.
struct ReportingClock(u32);

impl Clock for ReportingClock {
    fn now(&mut self) -> Timestamp {
        self.0 += 1;
        if self.0 == 4 {
            bug_condition!("clock at child start");
        }
        1000
    }
}

#[test]
fn an_observer_that_reports_then_panics_keeps_the_observed_mount() {
    struct Crashing;
    impl Observer for Crashing {
        fn action_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
            if exec.path.as_ref() == "/kid" {
                bug_condition!("observer before panic");
                panic!("observer panic");
            }
        }
    }
    let mut f = fixture();
    f.store.add_observer(Crashing);
    assert!(
        f.store
            .dispatch_to(&f.tree, TreeAction::Queue(KidActionTag::Shout))
            .is_err()
    );
    let [report] = reports(&f).try_into().unwrap();
    let place = report.place.unwrap();
    assert_eq!(
        (place.phase, place.path.as_ref()),
        (Phase::Observer(Boundary::Start), "/kid")
    );
}

#[test]
fn an_effect_childs_admission_report_names_the_effect() {
    let mut f = fixture();
    f.store
        .dispatch_to(&f.tree, TreeAction::FxAdmission { refuse: false })
        .unwrap();
    f.store
        .dispatch_to(&f.tree, TreeAction::FxAdmission { refuse: true })
        .unwrap();
    let all: Vec<_> = reports(&f).iter().map(summary).collect();
    assert_eq!(
        all,
        [
            // The Effects were executions 2 and 5: the admission reports name them.
            (
                "sync admission".into(),
                Phase::Admission,
                Some(2),
                "/".into()
            ),
            (
                "sync admission".into(),
                Phase::Admission,
                Some(5),
                "/".into()
            ),
        ]
    );
    assert_eq!(f.store.state().notes, 1, "only the admitted child ran");
}

#[test]
fn a_clock_report_is_placed_in_the_clock_phase() {
    let mut f = fixture_with_clock(ReportingClock(0));
    f.store
        .dispatch_to(&f.tree, TreeAction::FxAdmission { refuse: false })
        .unwrap();
    let all: Vec<_> = reports(&f).iter().map(summary).collect();
    assert_eq!(
        all,
        [
            (
                "sync admission".into(),
                Phase::Admission,
                Some(2),
                "/".into()
            ),
            (
                "clock at child start".into(),
                Phase::Clock,
                None,
                "/".into()
            ),
        ]
    );
}
