//! Observers: callbacks at each execution's start, finish and settlement, and
//! at refusals, inline calls, faults and bug reports. They inspect kinds,
//! causality and timing; they have no dispatch and no State authority. With
//! none installed, the Store skips every callback.

use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use crate::{BugReport, Category, ExecMeta, ExecutionId, InstanceId, Message, Origin, Timestamp};

/// What was running when a fault happened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// An enabling check or a guard.
    Admission,
    /// A reducer, or a child it called inline, or a lowerer.
    Reducer,
    /// An Effect handler.
    Handler,
    /// An Event adapter.
    Adapter,
    /// Reading the clock and allocating the next execution time, before any
    /// execution exists: a clock that panics, or IDs running out.
    Clock,
    /// An observer callback, at this boundary of the execution the fault
    /// names (none for `Bus`).
    Observer(Boundary),
    /// The named invariant found the State invalid. Only in bug reports: an
    /// invariant never faults the Store.
    Invariant(&'static str),
    /// The named invariant's checker panicked, and is disabled from now on.
    /// Only in bug reports.
    InvariantPanic(&'static str),
}

/// Where observers are called.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Boundary {
    /// When the execution starts, before its handler runs.
    Start,
    /// After the handler; before an Action's or Event's FIFO drains.
    Finish,
    /// After everything an Action or Event caused.
    Settled,
    /// The lane report at the start of a drain, outside any execution.
    Bus,
    /// An Action refused at admission; the fault names the execution that
    /// sent it, if any.
    Refused,
    /// A child's Action admitted inline, before its reducer runs; the fault
    /// names its parent's execution.
    Inline,
}

/// One execution, as observers see it.
#[derive(Clone, Copy, Debug)]
pub struct Execution<'a> {
    /// Action, Effect or Event.
    pub category: Category,
    /// The destination mount.
    pub path: &'a Arc<str>,
    /// The destination mount's identity.
    pub instance: InstanceId,
    /// The message's type name (`Message::TYPE`).
    pub message: &'static str,
    /// The message's kind.
    pub kind: &'static str,
    /// Its metadata.
    pub meta: &'a ExecMeta,
    /// For an Event, the Effect it answers.
    pub answers: Option<ExecutionId>,
}

/// Where an Action was sent from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SentFrom {
    /// A root input (`Store::dispatch_to`).
    Root,
    /// Dispatched by this Effect's handler, during it.
    Effect(ExecutionId),
    /// Queued, with the origin it would have carried: the execution that
    /// sent it and its ordinals there.
    Queued(Origin),
    /// Called inline by its parent's reducer, during this execution.
    Inline(ExecutionId),
}

impl SentFrom {
    /// The execution that sent it; none for a root input.
    pub fn execution(&self) -> Option<ExecutionId> {
        match self {
            SentFrom::Root => None,
            SentFrom::Effect(at) | SentFrom::Inline(at) => Some(*at),
            SentFrom::Queued(origin) => Some(origin.causal),
        }
    }
}

/// What refused an Action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusedBy {
    /// The root guard the application set on its mount
    /// (`Registry::admit`).
    RootGuard,
    /// Its parent's guard on the slot.
    Guard,
    /// Its component's own enabling check.
    Component,
    /// Its State is absent: the slot's lens reaches nothing.
    Absent,
}

/// An Action refused at admission, as observers see it. It has no
/// execution: no ID, no metadata, no ordinal.
#[derive(Clone, Copy, Debug)]
pub struct Refusal<'a> {
    /// The destination mount.
    pub path: &'a Arc<str>,
    /// The destination mount's identity.
    pub instance: InstanceId,
    /// The message's type name (`Message::TYPE`).
    pub message: &'static str,
    /// The message's kind.
    pub kind: &'static str,
    /// Where it was sent from.
    pub sent: SentFrom,
    /// What refused it.
    pub by: RefusedBy,
    /// The time it was checked at.
    pub time: Timestamp,
}

/// A child's Action reduced inline, within its parent's execution, as
/// observers see it. It has no execution of its own.
#[derive(Clone, Copy, Debug)]
pub struct InlineCall<'a> {
    /// The child's mount.
    pub path: &'a Arc<str>,
    /// The child's mount's identity.
    pub instance: InstanceId,
    /// The message's type name (`Message::TYPE`).
    pub message: &'static str,
    /// The message's kind.
    pub kind: &'static str,
    /// The parent's execution, which the child's reducer runs within.
    pub within: ExecutionId,
    /// The parent's execution time, which the child's reducer reads too.
    pub time: Timestamp,
    /// The child's mount has a root guard that this call did not run: an
    /// inline call made below the root, whose caller takes on what the guard
    /// checks (`Registry::admit`).
    pub root_guard_skipped: bool,
}

/// A payload at start, before its handler consumes it: its `Debug` form,
/// what logging records for it, and the value itself, read-only, through
/// `downcast_ref::<T>()` on `dyn MessageView`.
pub trait MessageView: Debug {
    /// The level to log the value at.
    fn log_level(&self) -> LogLevel;
    /// Hands the selected fields to `fields`.
    fn log_fields(&self, fields: &mut dyn FieldVisitor);
    /// The value, to downcast to its type.
    fn as_any(&self) -> &dyn Any;
}

impl<M: Message> MessageView for M {
    fn log_level(&self) -> LogLevel {
        Message::log_level(self)
    }
    fn log_fields(&self, fields: &mut dyn FieldVisitor) {
        Message::log_fields(self, fields);
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl dyn MessageView + '_ {
    /// The payload as a `T`, if it is one: an observer that knows a
    /// message type reads its fields exactly. Read-only; an observer still
    /// changes nothing.
    pub fn downcast_ref<T: Message>(&self) -> Option<&T> {
        self.as_any().downcast_ref()
    }
}

/// The level a message is logged at, chosen per message type or variant
/// with `#[causet(log(level = ...))]`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogLevel {
    /// The finest detail.
    Trace,
    /// Detail for debugging; the default.
    #[default]
    Debug,
    /// Routine facts worth keeping.
    Info,
    /// Something unexpected the code handles.
    Warn,
    /// A failure.
    Error,
}

/// Receives the fields a message selects for logging
/// (`#[causet(log(fields(...)))]`).
pub trait FieldVisitor {
    /// A field recorded by its `Debug` form.
    fn debug(&mut self, name: &'static str, value: &dyn Debug);
    /// A field recorded by its `Display` form (`display(...)`).
    fn display(&mut self, name: &'static str, value: &dyn Display);
}

/// What observers learn about a fault.
#[derive(Clone, Copy, Debug)]
pub struct FaultInfo<'a> {
    /// What was running: admission, a reducer, a handler, an observer, ...
    pub phase: Phase,
    /// The innermost execution, if one was allocated.
    pub at: Option<ExecutionId>,
    /// The panic message.
    pub message: &'a str,
}

/// The bus at the start of a drain.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BusDepths {
    /// Events waiting in the Store lane (sent from the Store's thread).
    pub store_lane: usize,
    /// Events waiting in the worker lane (sent from other threads).
    pub worker_lane: usize,
    /// Sends refused with `SendError::Full` since the previous report.
    pub rejected_full: u64,
}

/// Every callback defaults to nothing. Only start callbacks see the payload:
/// after start, its handler owns it. A panic in a callback faults the Store,
/// as `Phase::Observer` at that boundary of that execution. A panic in
/// `fault` or `bug` is ignored: those report something already decided.
pub trait Observer: Any + Send {
    /// An Action was admitted and starts: its payload is still readable.
    fn action_start(&mut self, _exec: &Execution<'_>, _payload: &dyn MessageView) {}
    /// After the reducer, before the queue drains.
    fn action_finish(&mut self, _exec: &Execution<'_>) {}
    /// After the Action's whole queue, and everything it caused, has run.
    fn action_settled(&mut self, _exec: &Execution<'_>) {}
    /// An Effect starts, before its handler.
    fn effect_start(&mut self, _exec: &Execution<'_>, _payload: &dyn MessageView) {}
    /// After the handler and its synchronous children.
    fn effect_finish(&mut self, _exec: &Execution<'_>) {}
    /// An Event starts, before its adapter.
    fn event_start(&mut self, _exec: &Execution<'_>, _payload: &dyn MessageView) {}
    /// After the adapter, before its queue drains.
    fn event_finish(&mut self, _exec: &Execution<'_>) {}
    /// After everything the Event caused has run.
    fn event_settled(&mut self, _exec: &Execution<'_>) {}
    /// An Action was refused at admission, wherever it was sent from: it
    /// runs nothing, and its payload is still readable. Called on every
    /// refusal whenever any observer is installed, so even an observer that
    /// ignores it costs a few nanoseconds a refusal. Its kind is read to tell
    /// the observers, and only then: a `kind()` that panics faults the Store
    /// at `Boundary::Refused`.
    fn action_refused(&mut self, _refusal: &Refusal<'_>, _payload: &dyn MessageView) {}
    /// A child's Action was admitted inline (`reduce_child`), and its
    /// reducer runs next, within its parent's execution: no `action_start`
    /// follows. Its payload is still readable; its kind is read to tell the
    /// observers, and only then.
    fn inline_action(&mut self, _inline: &InlineCall<'_>, _payload: &dyn MessageView) {}
    /// The Store faulted, after the fault was committed.
    fn fault(&mut self, _fault: &FaultInfo<'_>) {}
    /// The bus's depths at the start of a drain.
    fn bus(&mut self, _depths: &BusDepths) {}
    /// A bug was reported (`bug_condition!`), at its place in the run. A
    /// panic here is ignored: the report is already made.
    fn bug(&mut self, _report: &BugReport) {}
}

/// Keeps every bug report, so a test can check them after a run: none, or
/// exactly those a recovery test expects. It never panics while the run goes
/// on, so the test exercises the fallback production takes.
#[derive(Default)]
pub struct BugLog {
    reports: Vec<BugReport>,
}

impl BugLog {
    /// An empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every report, in order.
    pub fn reports(&self) -> &[BugReport] {
        &self.reports
    }

    /// Panics, listing them, if any bug was reported.
    #[track_caller]
    pub fn assert_none(&self) {
        assert!(
            self.reports.is_empty(),
            "unexpected bug reports:\n{}",
            list(&self.reports)
        );
    }

    /// Panics unless the reports' messages are exactly `messages`, in order.
    #[track_caller]
    pub fn assert_messages(&self, messages: &[&str]) {
        let actual: Vec<&str> = self.reports.iter().map(|r| r.message.as_str()).collect();
        assert!(
            actual == messages,
            "expected bug reports {messages:?}, got:\n{}",
            list(&self.reports)
        );
    }
}

fn list(reports: &[BugReport]) -> String {
    reports
        .iter()
        .map(|r| format!("  {}:{}: {}", r.file, r.line, r.message))
        .collect::<Vec<_>>()
        .join("\n")
}

impl Observer for BugLog {
    fn bug(&mut self, report: &BugReport) {
        self.reports.push(report.clone());
    }
}

/// One started execution, as `TraceLog` keeps it.
#[derive(Clone, Debug)]
pub struct TraceEntry {
    /// The execution.
    pub id: ExecutionId,
    /// Its time.
    pub time: Timestamp,
    /// Action, Effect or Event.
    pub category: Category,
    /// The destination mount's path.
    pub path: Arc<str>,
    /// The message's kind.
    pub kind: &'static str,
    /// The execution that caused it; its own ID for a root input.
    pub causal: ExecutionId,
    /// Where it was emitted; `None` for a root input.
    pub origin: Option<Origin>,
    /// For an Event, the Effect it answers.
    pub answers: Option<ExecutionId>,
    /// Active frames including it: a root input is at depth 1.
    pub depth: u32,
}

/// One refused Action, as `TraceLog` keeps it.
#[derive(Clone, Debug)]
pub struct RefusalEntry {
    /// The destination mount's path.
    pub path: Arc<str>,
    /// The Action's kind.
    pub kind: &'static str,
    /// Where it was sent from.
    pub sent: SentFrom,
    /// What refused it.
    pub by: RefusedBy,
    /// The time it was checked at.
    pub time: Timestamp,
}

/// One child's Action reduced inline, as `TraceLog` keeps it.
#[derive(Clone, Debug)]
pub struct InlineEntry {
    /// The child's mount path.
    pub path: Arc<str>,
    /// The Action's kind.
    pub kind: &'static str,
    /// The parent's execution.
    pub within: ExecutionId,
    /// The parent's execution time.
    pub time: Timestamp,
    /// The child's mount has a root guard, which the call did not run.
    pub root_guard_skipped: bool,
}

/// Keeps every started execution in memory, for tests and debugging, with
/// the Actions refused and those reduced inline. It grows without bound.
#[derive(Default)]
pub struct TraceLog {
    entries: Vec<TraceEntry>,
    refusals: Vec<RefusalEntry>,
    inline: Vec<InlineEntry>,
}

impl TraceLog {
    /// An empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every started execution, in order.
    pub fn entries(&self) -> &[TraceEntry] {
        &self.entries
    }

    /// Every refused Action, in order.
    pub fn refusals(&self) -> &[RefusalEntry] {
        &self.refusals
    }

    /// Every child's Action reduced inline, in order.
    pub fn inline(&self) -> &[InlineEntry] {
        &self.inline
    }

    fn push(&mut self, exec: &Execution<'_>) {
        self.entries.push(TraceEntry {
            id: exec.meta.id,
            time: exec.meta.time,
            category: exec.category,
            path: Arc::clone(exec.path),
            kind: exec.kind,
            causal: exec.meta.causal_execution_id(),
            origin: exec.meta.origin,
            answers: exec.answers,
            depth: exec.meta.depth,
        });
    }
}

impl Observer for TraceLog {
    fn action_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        self.push(exec);
    }
    fn effect_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        self.push(exec);
    }
    fn event_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        self.push(exec);
    }
    fn action_refused(&mut self, refusal: &Refusal<'_>, _: &dyn MessageView) {
        self.refusals.push(RefusalEntry {
            path: Arc::clone(refusal.path),
            kind: refusal.kind,
            sent: refusal.sent,
            by: refusal.by,
            time: refusal.time,
        });
    }
    fn inline_action(&mut self, inline: &InlineCall<'_>, _: &dyn MessageView) {
        self.inline.push(InlineEntry {
            path: Arc::clone(inline.path),
            kind: inline.kind,
            within: inline.within,
            time: inline.time,
            root_guard_skipped: inline.root_guard_skipped,
        });
    }
}

pub(crate) type ViewFn = fn(&dyn Any) -> &dyn MessageView;

pub(crate) fn view_as<M: Message>(payload: &dyn Any) -> &dyn MessageView {
    payload.downcast_ref::<M>().expect("message type")
}
