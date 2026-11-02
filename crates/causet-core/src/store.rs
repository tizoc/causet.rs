//! Scheduling, invariant checks and the owning Store.

use crate::{
    Boundary, BugPlace, BusDepths, Category, Clock, Component, Envelope, EventBus, EventInbox,
    EventReadiness, EventSink, Exec, ExecMeta, Execution, ExecutionId, FaultInfo, FaultUnwind,
    IGNORING, Message, Mount, Observer, ObserverPanic, Origin, Path, Payload, Phase, Protocol,
    ProtocolDefs, Queued, Refusal, RefusedBy, RegistrationError, Registry, ResolveError, SentFrom,
    SessionId, SystemClock, Timestamp, Watch, bug, fault_unwind, ignoring_panics, notify,
    panic_message,
};
use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::thread;

// ---------------------------------------------------------------------------
// Store

/// What a dispatch did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchOutcome {
    /// Admitted and run, with everything it caused.
    Executed,
    /// Refused by an admission check: nothing ran.
    Disabled,
    /// The Store faulted earlier in this root input: nothing ran. Only an
    /// Effect's `dispatch` returns it, after its handler caught the panic of
    /// an earlier dispatch; a Store call returns `DispatchError::Faulted`.
    Faulted,
}

/// Why a Store call did nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DispatchError {
    /// The Mount was resolved against another Protocol.
    ForeignMount,
    /// The Event bus was made for another Protocol.
    ForeignBus,
    /// The Store faulted, now or earlier, with this message.
    Faulted(String),
}

/// How a Store runs: `StoreOptions::default()`, or `with_clock`. More
/// settings may come; they will have defaults.
#[non_exhaustive]
pub struct StoreOptions {
    /// The execution clock: the only source of `meta.time`. Execution IDs
    /// count separately. `SystemClock` by default.
    pub clock: Box<dyn Clock>,
}

impl StoreOptions {
    /// The default options with `clock` as the execution clock.
    pub fn with_clock(clock: impl Clock + 'static) -> Self {
        Self {
            clock: Box::new(clock),
        }
    }
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            clock: Box::new(SystemClock::new()),
        }
    }
}

/// The non-generic execution core. Effect contexts hold `&mut Core`, so a
/// synchronous child can run through the same scheduler.
pub(super) struct Core {
    /// Assigned once, when the Core is built, and never replaced: `protocol`
    /// relies on it.
    pub(super) proto: Arc<ProtocolDefs>,
    pub(super) state: Box<dyn Any>,
    pub(super) services: Box<dyn Any>,
    pub(super) session: SessionId,
    /// Owned here, so the bus closes when the Store goes.
    pub(super) inbox: EventInbox,
    pub(super) clock: Box<dyn Clock>,
    /// The latest execution time: the anchor before the first execution.
    pub(super) last: Timestamp,
    /// The next execution's ID: 1 before the first.
    pub(super) next_id: u64,
    pub(super) depth: u32,
    pub(super) executions: usize,
    /// Bug reports so far: the next one's sequence number.
    pub(super) bugs: u64,
    /// Called at every lifecycle boundary, in the order they were added.
    pub(super) observers: Vec<Box<dyn Observer>>,
    /// Emptied queue buffers, reused by later executions.
    pub(super) fifo_pool: Vec<Vec<Queued>>,
    /// Active executions and what they are doing, innermost last. A fault
    /// names the top entry.
    pub(super) open: Vec<(Option<ExecutionId>, Phase, usize)>,
    /// A phase that runs no application code and never nests (reading the
    /// clock, calling observers): set and cleared around it, and when set it
    /// names a fault instead of the top of `open`.
    pub(super) special: Option<(Option<ExecutionId>, Phase, usize)>,
    /// The fault's message, once the Store has faulted. It is recorded where
    /// the panic first leaves the work an Effect's `dispatch` ran, or at the
    /// root-input boundary: the first one wins.
    pub(super) fault: Option<String>,
    /// The fault was recorded inside a root input that is still going on (a
    /// handler caught the panic): this thread counts as ignoring panics
    /// until the root input returns.
    pub(super) ignoring: bool,
}

impl Core {
    /// Takes the bug reports waiting on this thread, places the untagged ones
    /// at `phase` of `at` in `mount`, and tells the observers.
    #[inline(always)]
    pub(super) fn flush_bugs(&mut self, phase: Phase, at: Option<ExecutionId>, mount: usize) {
        if bug::pending() {
            self.flush_bugs_slow(phase, at, mount);
        }
    }

    #[inline(never)]
    fn flush_bugs_slow(&mut self, phase: Phase, at: Option<ExecutionId>, mount: usize) {
        for pending in bug::take() {
            let (phase, mount) = pending.tag.unwrap_or((phase, mount));
            let sequence = self.bugs;
            self.bugs += 1;
            let def = &self.proto.mounts[mount];
            let place = BugPlace {
                phase,
                at,
                component: def.instance,
                path: Arc::clone(&def.path),
                sequence,
            };
            let report = pending.into_report(Some(place));
            notify(&mut self.observers, |o| o.bug(&report));
        }
    }

    /// The Protocol, borrowed apart from `self`, so a method can read its
    /// definitions while it mutates the Core. Cloning the `Arc` for each
    /// execution instead costs two atomic operations, about 7 ns.
    ///
    /// # Safety
    ///
    /// The caller keeps the reference only within its call on this Core. That
    /// is enough: `proto` is never replaced, so the Core's own `Arc` keeps the
    /// Protocol alive for as long as the Core.
    unsafe fn protocol<'p>(&self) -> &'p ProtocolDefs {
        // SAFETY: the pointer comes from a live `Arc`; the caller bounds the
        // lifetime as documented.
        unsafe { &*Arc::as_ptr(&self.proto) }
    }

    /// Calls the observers at `boundary` of execution `at`, in the
    /// `Observer` phase, so a panic in one faults the Store as that
    /// boundary's fault.
    #[inline(always)]
    fn observe(
        &mut self,
        at: ExecutionId,
        mount: usize,
        boundary: Boundary,
        call: impl FnMut(&mut dyn Observer),
    ) {
        if self.observers.is_empty() {
            return;
        }
        self.observe_slow(Some(at), mount, boundary, call);
    }

    /// `observe`, for a boundary that may have no execution: a refused root
    /// Action.
    #[inline(always)]
    fn observe_at(
        &mut self,
        at: Option<ExecutionId>,
        mount: usize,
        boundary: Boundary,
        call: impl FnMut(&mut dyn Observer),
    ) {
        if self.observers.is_empty() {
            return;
        }
        self.observe_slow(at, mount, boundary, call);
    }

    #[inline(never)]
    fn observe_slow(
        &mut self,
        at: Option<ExecutionId>,
        mount: usize,
        boundary: Boundary,
        mut call: impl FnMut(&mut dyn Observer),
    ) {
        self.special = Some((at, Phase::Observer(boundary), mount));
        for observer in &mut self.observers {
            call(&mut **observer);
        }
        self.special = None;
        self.flush_bugs(Phase::Observer(boundary), at, mount);
    }

    /// Where a fault happened: the special phase if one is running, else the
    /// innermost open execution.
    fn fault_place(&self) -> (Option<ExecutionId>, Phase) {
        self.special
            .or_else(|| self.open.last().copied())
            .map_or((None, Phase::Admission), |(at, phase, _)| (at, phase))
    }

    /// The mount whose code was running at a fault: the one an observer was
    /// told about, or the innermost open execution's.
    fn fault_mount(&self) -> usize {
        self.special
            .or_else(|| self.open.last().copied())
            .map_or(0, |(_, _, mount)| mount)
    }

    /// Records the fault of the panic unwinding with `payload`, unless one
    /// is recorded already: the first wins. Its message is the panic's and
    /// its place where it was raised (an observer's panic inside a reducer
    /// carries its own); the bug reports made before it are told first, the
    /// bus closes, and the observers are told, their panics ignored: nothing
    /// they do can undo or unwind past it.
    #[cold]
    #[inline(never)]
    fn record_fault(&mut self, payload: &(dyn Any + Send)) {
        if self.fault.is_some() {
            return;
        }
        let (payload, place) = match payload.downcast_ref::<ObserverPanic>() {
            Some(observer) => (
                &*observer.payload,
                Some((
                    Some(observer.at),
                    Phase::Observer(observer.boundary),
                    observer.mount,
                )),
            ),
            None => (payload, None),
        };
        let message = panic_message(payload);
        let (at, phase, mount) = place.unwrap_or_else(|| {
            let (at, phase) = self.fault_place();
            (at, phase, self.fault_mount())
        });
        // Reported before the fault; nothing here may unwind past it.
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            ignoring_panics(|| self.flush_bugs(phase, at, mount))
        }));
        self.fault = Some(message);
        // A faulted Store takes no more input: its bus closes as if it had
        // been dropped, so Services' sends fail rather than fill a lane.
        self.inbox.sink.bus.close();
        let Self {
            fault, observers, ..
        } = self;
        let info = FaultInfo {
            phase,
            at,
            message: fault.as_deref().unwrap_or_default(),
        };
        notify(observers, |o| o.fault(&info));
    }

    /// A panic left the work an Effect's `dispatch` ran, and the handler
    /// may catch it: the fault is recorded now, at its place, and the
    /// execution stack and depth are restored to the dispatch's, before the
    /// panic goes on. Until the root input returns, this thread counts as
    /// ignoring panics (`panic_is_ignored`): the Store has its fault.
    #[cold]
    #[inline(never)]
    pub(super) fn unwound(&mut self, payload: Box<dyn Any + Send>, open: usize, depth: u32) -> ! {
        self.record_fault(&*payload);
        if !self.ignoring {
            self.ignoring = true;
            IGNORING.with(|ignoring| ignoring.set(ignoring.get() + 1));
        }
        self.open.truncate(open);
        self.depth = depth;
        self.special = None;
        panic::resume_unwind(payload)
    }

    /// The root input ended: everything it left open is closed, and this
    /// thread no longer ignores panics for it.
    fn end_fault(&mut self) {
        self.depth = 0;
        self.open.clear();
        self.special = None;
        if self.ignoring {
            self.ignoring = false;
            IGNORING.with(|ignoring| ignoring.set(ignoring.get() - 1));
        }
    }

    /// The next execution time from the clock, in the `Clock` phase, so a
    /// fault here names it.
    fn clock_time(&mut self) -> Timestamp {
        self.special = Some((None, Phase::Clock, 0));
        let now = self.clock.now();
        let time = self.tick(now);
        self.special = None;
        self.flush_bugs(Phase::Clock, None, 0);
        time
    }

    /// The next execution time for a clock reading: `max(last, now)`, so
    /// times never go back, whatever the clock does.
    fn tick(&self, now: Timestamp) -> Timestamp {
        self.last.max(now)
    }

    /// Depth counts the active frames including this one: a root input is at
    /// depth 1. `time` is a root Action's ingress time;
    /// everything else reads the clock as it starts.
    fn next_meta(&mut self, origin: Option<Origin>, time: Option<Timestamp>) -> ExecMeta {
        let time = match time {
            Some(time) => time,
            None => self.clock_time(),
        };
        self.last = time;
        let id = ExecutionId(self.next_id);
        // Exhaustion is a fault, never a wrap-around.
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("execution IDs exhausted");
        ExecMeta {
            id,
            depth: self.depth + 1,
            time,
            origin,
        }
    }

    /// Admission at `now`: a root Action's ingress time, otherwise the time of
    /// the most recently executed item.
    ///
    /// The checks, in order: the mount's State is there, its root guard,
    /// its parent's slot guard, its own `is_enabled`.
    fn admitted(&self, target: usize, action: &dyn Any, now: Timestamp) -> Result<(), RefusedBy> {
        let proto = &self.proto;
        let def = &proto.mounts[target];
        let root = &*self.state;
        // The parent's State, and the mount's through the parent's slot
        // lens, in one walk.
        let (parent_state, own) = match def.parent {
            None => (None, root),
            Some(parent) => {
                let Some(parent_state) = proto.state_ref(root, parent) else {
                    return Err(RefusedBy::Absent);
                };
                let lens = def.lens.as_ref().expect("a child mount always has a lens");
                let Some(own) = lens.get(parent_state) else {
                    return Err(RefusedBy::Absent);
                };
                (Some((parent, parent_state)), own)
            }
        };
        if let Some(guard) = &def.root_guard {
            let admitted = {
                let _root = bug::Attribute(Phase::Admission, 0);
                guard.admit(root, own, action, now)
            };
            if !admitted {
                return Err(RefusedBy::RootGuard);
            }
        }
        if let (Some((parent, parent_state)), Some(guard)) = (parent_state, &def.guard) {
            let admitted = {
                let _parent = bug::Attribute(Phase::Admission, parent);
                guard.admit(parent_state, action, now)
            };
            if !admitted {
                return Err(RefusedBy::Guard);
            }
        }
        if (def.admit)(own, action, now) {
            Ok(())
        } else {
            Err(RefusedBy::Component)
        }
    }

    /// Admission with its place on the open stack: `at` is the Effect for a
    /// synchronous child, `None` otherwise.
    fn admitted_tracked(
        &mut self,
        target: usize,
        action: &dyn Any,
        at: Option<ExecutionId>,
        now: Timestamp,
    ) -> Result<(), RefusedBy> {
        self.open.push((at, Phase::Admission, target));
        let admitted = self.admitted(target, action, now);
        self.open.pop();
        admitted
    }

    /// Admission of an Action sent from `sent`, with its bug reports placed
    /// at `at`; a refusal is told to the observers.
    pub(super) fn admit(
        &mut self,
        target: usize,
        action: &dyn Any,
        sent: SentFrom,
        at: Option<ExecutionId>,
        now: Timestamp,
    ) -> bool {
        let admitted = self.admitted_tracked(target, action, at, now);
        self.flush_bugs(Phase::Admission, sent.execution(), target);
        match admitted {
            Ok(()) => true,
            Err(by) => {
                if !self.observers.is_empty() {
                    self.refused(target, action, sent, by, now);
                }
                false
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn refused(
        &mut self,
        target: usize,
        action: &dyn Any,
        sent: SentFrom,
        by: RefusedBy,
        now: Timestamp,
    ) {
        // SAFETY: used only within this call.
        let proto = unsafe { self.protocol() };
        let def = &proto.mounts[target];
        // The kind is read only to tell an observer, in its phase.
        self.observe_at(sent.execution(), target, Boundary::Refused, |o| {
            let refusal = Refusal {
                path: &def.path,
                instance: def.instance,
                message: def.action_type,
                kind: (def.action_kind)(action),
                sent,
                by,
                time: now,
            };
            o.action_refused(&refusal, (def.view_action)(action))
        });
    }

    fn execute_action(&mut self, target: usize, payload: Payload, origin: Origin) {
        let now = self.last;
        if self.admit(
            target,
            payload.as_any(),
            SentFrom::Queued(origin),
            None,
            now,
        ) {
            self.run_action(target, payload, Some(origin), None);
        }
    }

    pub(super) fn run_action(
        &mut self,
        target: usize,
        payload: Payload,
        origin: Option<Origin>,
        time: Option<Timestamp>,
    ) -> ExecutionId {
        // SAFETY: used only within this call.
        let proto = unsafe { self.protocol() };
        let def = &proto.mounts[target];
        let meta = self.next_meta(origin, time);
        let kind = (def.action_kind)(payload.as_any());
        self.executions += 1;
        let observed = Execution {
            category: Category::Action,
            path: &def.path,
            instance: def.instance,
            message: def.action_type,
            kind,
            meta: &meta,
            answers: None,
        };
        self.observe(meta.id, target, Boundary::Start, |o| {
            o.action_start(&observed, (def.view_action)(payload.as_any()))
        });

        let mut exec = Exec::new(
            self.session,
            meta.id,
            self.fifo_pool.pop().unwrap_or_default(),
        );
        self.open.push((Some(meta.id), Phase::Reducer, target));
        {
            let state = proto
                .state_mut(&mut *self.state, target)
                .expect("admitted, so available");
            let watch = Watch {
                observers: &mut self.observers,
            };
            (def.run)(proto, target, state, &mut exec, watch, payload, &meta);
        }
        self.open.pop();
        self.flush_bugs(Phase::Reducer, Some(meta.id), target);
        self.observe(meta.id, target, Boundary::Finish, |o| {
            o.action_finish(&observed)
        });
        self.drain_fifo(exec.fifo);
        self.observe(meta.id, target, Boundary::Settled, |o| {
            o.action_settled(&observed)
        });
        meta.id
    }

    fn run_effect(&mut self, target: usize, payload: Payload, origin: Origin) {
        // SAFETY: used only within this call.
        let proto = unsafe { self.protocol() };
        let def = &proto.mounts[target];
        let meta = self.next_meta(Some(origin), None);
        let kind = (def.effect_kind)(payload.as_any());
        self.executions += 1;
        let observed = Execution {
            category: Category::Effect,
            path: &def.path,
            instance: def.instance,
            message: def.effect_type,
            kind,
            meta: &meta,
            answers: None,
        };
        self.observe(meta.id, target, Boundary::Start, |o| {
            o.effect_start(&observed, (def.view_effect)(payload.as_any()))
        });

        self.depth += 1;
        self.open.push((Some(meta.id), Phase::Handler, target));
        (def.handle)(self, target, payload, &meta);
        if self.fault.is_some() {
            // The handler caught a dispatch's panic: nothing that was
            // interrupted resumes, and nothing after it runs.
            fault_unwind();
        }
        self.open.pop();
        self.flush_bugs(Phase::Handler, Some(meta.id), target);
        self.depth -= 1;
        self.observe(meta.id, target, Boundary::Finish, |o| {
            o.effect_finish(&observed)
        });
    }

    /// Runs one Event as a root input; returns its execution, its mount and
    /// its kind.
    fn run_event(&mut self, envelope: Envelope) -> (ExecutionId, Timestamp, usize, &'static str) {
        // SAFETY: used only within this call.
        let proto = unsafe { self.protocol() };
        assert!(
            envelope.session == self.session,
            "Event from another session"
        );
        let target = proto
            .mount_by_instance(envelope.instance)
            .unwrap_or_else(|| panic!("no mount with instance {:?}", envelope.instance));
        let def = &proto.mounts[target];
        let binding = def
            .events
            .iter()
            .find(|b| b.event == envelope.event)
            .unwrap_or_else(|| panic!("{} never registered Event {}", def.path, envelope.kind));
        let meta = self.next_meta(None, None);
        let kind = (binding.kind)(envelope.payload.as_any());
        self.executions += 1;
        let observed = Execution {
            category: Category::Event,
            path: &def.path,
            instance: def.instance,
            message: binding.name,
            kind,
            meta: &meta,
            answers: envelope.effect,
        };
        self.observe(meta.id, target, Boundary::Start, |o| {
            o.event_start(&observed, (binding.view)(envelope.payload.as_any()))
        });
        let mut exec = Exec::new(
            self.session,
            meta.id,
            self.fifo_pool.pop().unwrap_or_default(),
        );
        self.open.push((Some(meta.id), Phase::Adapter, target));
        (binding.adapt)(def.instance, target, &mut exec, envelope.payload);
        self.open.pop();
        self.flush_bugs(Phase::Adapter, Some(meta.id), target);
        self.observe(meta.id, target, Boundary::Finish, |o| {
            o.event_finish(&observed)
        });
        self.drain_fifo(exec.fifo);
        self.observe(meta.id, target, Boundary::Settled, |o| {
            o.event_settled(&observed)
        });
        (meta.id, meta.time, target, kind)
    }

    fn drain_fifo(&mut self, mut fifo: Vec<Queued>) {
        self.depth += 1;
        for queued in fifo.drain(..) {
            match queued.category {
                Category::Action => {
                    self.execute_action(queued.target, queued.payload, queued.origin);
                }
                Category::Effect => self.run_effect(queued.target, queued.payload, queued.origin),
                Category::Event => unreachable!("Events are root inputs, never queued"),
            }
        }
        self.depth -= 1;
        self.fifo_pool.push(fifo);
    }
}

/// Runs an application: owns its State, Services and inbox, and executes
/// root inputs (dispatched Actions, drained Events) one at a time, each to
/// completion, on the calling thread. Dropping it closes the bus, and so does
/// a fault: from then on every send fails with `SendError::Closed`.
///
/// A Store can move to another thread when its State and Services are `Send`.
/// It does not require them to be `Sync`, and it is not itself `Sync`: an
/// execution always has exclusive access. Local, non-`Send` State and Services
/// are supported too; their Store stays on its thread.
pub struct Store<S, Svc> {
    pub(super) core: Core,
    pub(super) protocol: Arc<Protocol<S, Svc>>,
    pub(super) invariants: Vec<Invariant<S>>,
    pub(super) _types: PhantomData<fn() -> (S, Svc)>,
}

// SAFETY: only `Store::new` constructs a Store, and it erases exactly `S` and
// `Svc` into `Core::state` and `Core::services`. Their types never change.
// Those are the only fields that are not already Send: the Protocol's erased
// lenses/guards/lowerers are Send + Sync, payloads and observers are Send,
// the clock is Send, and invariant closures are Send. State/Service borrows
// are tied to a borrow of the Store or an active context, so Rust prevents
// moving it while they or an exclusive execution are active. `enter` updates
// the bus's owner thread
// before each root input. This grants Send only, never Sync.
unsafe impl<S: Send, Svc: Send> Send for Store<S, Svc> {}

// ---------------------------------------------------------------------------
// Invariants

/// Why an invariant's checker runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvariantStep<'a> {
    /// It was just registered, on the State as it is.
    Added,
    /// It was moved into a new Store, with its memory, after a restart: the
    /// State is the restored one, and no root input has run yet.
    Restored,
    /// A root input ran with everything it caused: an admitted root Action,
    /// or one drained Event.
    Settled {
        /// The root input's execution.
        execution: ExecutionId,
        /// Action or Event.
        category: Category,
        /// Its mount's path.
        path: &'a str,
        /// Its kind.
        kind: &'static str,
        /// Its time.
        time: Timestamp,
    },
}

/// An invariant's checker: what is wrong with the State, if anything. Its
/// captures are its memory.
pub type Checker<S> = Box<dyn FnMut(&S, &InvariantStep<'_>) -> Result<(), String> + Send>;

pub(super) struct Invariant<S> {
    pub(super) name: &'static str,
    pub(super) file: &'static str,
    pub(super) line: u32,
    pub(super) check: Checker<S>,
    /// Its checker panicked: its memory may be half updated.
    pub(super) disabled: bool,
}

/// A Store's invariants with their memory, taken out to move into the Store
/// that replaces it after a restart (`Store::take_invariants`).
pub struct Invariants<S>(Vec<Invariant<S>>);

impl<S> Invariants<S> {
    /// How many there are.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The bus closes before State and Services drop, so a Service that joins its
/// workers as it drops finds their sends already failing with `Closed`.
impl<S, Svc> Drop for Store<S, Svc> {
    fn drop(&mut self) {
        self.core.inbox.sink.bus.close();
    }
}

/// The worker lane's capacity on a bus `Store::start` and `Store::open`
/// make: how many Events sent from outside the Store's own calls (from
/// other threads, or before its first call) wait before a send fails.
pub const DEFAULT_WORKER_CAPACITY: usize = 1024;

impl<S: 'static, Svc: 'static> Store<S, Svc> {
    /// A Store whose root component is `R`, starting from `state` with
    /// `services`, and the sink for Events nobody asked for: the host's input,
    /// or a Service's that listens (an answer to an Effect goes through its
    /// `Reply`). `R`'s tree is registered and frozen, and the Store drains a
    /// bus of its own (a worker lane of `DEFAULT_WORKER_CAPACITY` Events, a
    /// fresh session). Fails with
    /// the registration's problems. For root guards or several Stores on one
    /// Protocol, freeze a `Registry` and use `open`; for a chosen session or
    /// a held bus, `EventBus` and `new`.
    pub fn start<R: Component<State = S, Service = Svc>>(
        state: S,
        services: Svc,
        options: StoreOptions,
    ) -> Result<(Self, EventSink), RegistrationError> {
        let mut registry = Registry::<S, Svc>::new();
        registry.root::<R>();
        let protocol = registry.freeze()?;
        Ok(Self::open(&protocol, state, services, options))
    }

    /// A Store for `protocol`, starting from `state` with `services`, and the
    /// sink for Events nobody asked for. The Store drains a bus of its own, as
    /// `start`'s does.
    pub fn open(
        protocol: &Arc<Protocol<S, Svc>>,
        state: S,
        services: Svc,
        options: StoreOptions,
    ) -> (Self, EventSink) {
        let (sink, inbox) = EventBus::new(protocol, DEFAULT_WORKER_CAPACITY);
        let store = Self::new(Arc::clone(protocol), state, services, inbox, options)
            .expect("the bus is made for this Protocol");
        (store, sink)
    }

    /// The mount at `path`, resolved with this Store's Protocol: a handle for
    /// `dispatch_to` and the Event sink.
    pub fn mount<C: Component>(&self, path: &Path<C>) -> Result<Mount<C>, ResolveError> {
        self.core.proto.resolve(path)
    }

    /// The Protocol this Store runs. A Store that replaces it (after a
    /// fault, say) opens on the same one, so the mounts resolved so far
    /// stay valid: `Store::open(store.protocol(), ...)`.
    pub fn protocol(&self) -> &Arc<Protocol<S, Svc>> {
        &self.protocol
    }

    /// A Store for `proto`, starting from `state` with `services`, draining
    /// `inbox`. The Protocol's root types are `S` and `Svc`, so a State or
    /// Services of another type does not compile. An inbox made for another
    /// Protocol is refused with `ForeignBus`.
    pub fn new(
        proto: Arc<Protocol<S, Svc>>,
        state: S,
        services: Svc,
        inbox: EventInbox,
        options: StoreOptions,
    ) -> Result<Self, DispatchError> {
        // A rejected inbox is dropped here, which closes its bus.
        if inbox.sink.protocol.id != proto.defs.id {
            return Err(DispatchError::ForeignBus);
        }
        let mut clock = options.clock;
        let anchor = clock.now();
        let core = Core {
            proto: Arc::clone(&proto.defs),
            state: Box::new(state),
            services: Box::new(services),
            session: inbox.sink.session,
            inbox,
            clock,
            last: anchor,
            next_id: 1,
            depth: 0,
            executions: 0,
            bugs: 0,
            observers: Vec::new(),
            fifo_pool: Vec::new(),
            open: Vec::new(),
            special: None,
            fault: None,
            ignoring: false,
        };
        Ok(Self {
            core,
            protocol: proto,
            invariants: Vec::new(),
            _types: PhantomData,
        })
    }

    /// Registers an invariant: `check` says what
    /// is wrong with the whole State, if anything. It runs now (`Added`),
    /// then after every root input that settles; never after a refused
    /// Action, an empty drain or a fault. A violation, or a panic of the
    /// checker, is a bug report placed at the invariant's registration (a
    /// panic also disables it); the Store goes on.
    #[track_caller]
    pub fn add_invariant(
        &mut self,
        name: &'static str,
        check: impl FnMut(&S, &InvariantStep<'_>) -> Result<(), String> + Send + 'static,
    ) {
        let at = std::panic::Location::caller();
        let invariant = Invariant {
            name,
            file: at.file(),
            line: at.line(),
            check: Box::new(check),
            disabled: false,
        };
        self.invariants.push(invariant);
        let first = self.invariants.len() - 1;
        self.check_invariants(first, InvariantStep::Added);
    }

    /// Takes the invariants out, with their memory, for the Store that
    /// replaces this one after a restart.
    pub fn take_invariants(&mut self) -> Invariants<S> {
        Invariants(std::mem::take(&mut self.invariants))
    }

    /// Moves invariants taken from a previous Store into this one and runs
    /// each on the restored State (`Restored`), before any root input.
    /// Those disabled there stay disabled.
    pub fn restore_invariants(&mut self, invariants: Invariants<S>) {
        let first = self.invariants.len();
        self.invariants.extend(invariants.0);
        self.check_invariants(first, InvariantStep::Restored);
    }

    /// Runs the invariants from index `first` on, unless the Store faulted.
    #[inline(always)]
    fn check_invariants(&mut self, first: usize, step: InvariantStep<'_>) {
        if self.invariants.len() > first && self.core.fault.is_none() {
            self.check_invariants_slow(first, step);
        }
    }

    #[inline(never)]
    fn check_invariants_slow(&mut self, first: usize, step: InvariantStep<'_>) {
        let state: &S = self.core.state.downcast_ref().expect("root State type");
        let at = match step {
            InvariantStep::Settled { execution, .. } => Some(execution),
            InvariantStep::Added | InvariantStep::Restored => None,
        };
        let root = &self.core.proto.mounts[0];
        let (bugs, observers) = (&mut self.core.bugs, &mut self.core.observers);
        let mut deliver = |phase: Phase, report: bug::Pending| {
            let place = BugPlace {
                phase,
                at,
                component: root.instance,
                path: Arc::clone(&root.path),
                sequence: *bugs,
            };
            *bugs += 1;
            let report = report.into_report(Some(place));
            notify(observers, |o| o.bug(&report));
        };
        // A checker's own `bug_condition!` reports are collected here, apart
        // from any root input's, and delivered under its phase.
        let _reports = bug::Scope::enter();
        for invariant in self.invariants[first..].iter_mut().filter(|i| !i.disabled) {
            let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
                ignoring_panics(|| (invariant.check)(state, &step))
            }));
            for made in bug::take() {
                deliver(Phase::Invariant(invariant.name), made);
            }
            let (phase, message) = match outcome {
                Ok(Ok(())) => continue,
                Ok(Err(message)) => (Phase::Invariant(invariant.name), message),
                Err(payload) => {
                    invariant.disabled = true;
                    let message = format!(
                        "the checker panicked and is disabled: {}",
                        panic_message(&*payload)
                    );
                    (Phase::InvariantPanic(invariant.name), message)
                }
            };
            let fields = vec![("invariant", invariant.name.to_owned())];
            deliver(
                phase,
                bug::Pending {
                    message,
                    fields,
                    file: invariant.file,
                    line: invariant.line,
                    tag: None,
                },
            );
        }
    }

    /// The whole application State.
    pub fn state(&self) -> &S {
        self.core.state.downcast_ref().expect("root State type")
    }

    /// Adds an observer after those already installed.
    pub fn add_observer(&mut self, observer: impl Observer) {
        self.core.observers.push(Box::new(observer));
    }

    /// Removes every observer.
    pub fn clear_observers(&mut self) {
        self.core.observers.clear();
    }

    /// The first installed observer of type `T`, for reading what it gathered.
    pub fn observer<T: Observer>(&self) -> Option<&T> {
        self.core.observers.iter().find_map(|o| {
            let any: &dyn Any = &**o;
            any.downcast_ref()
        })
    }

    /// The first installed observer of type `T`, to change.
    pub fn observer_mut<T: Observer>(&mut self) -> Option<&mut T> {
        self.core.observers.iter_mut().find_map(|o| {
            let any: &mut dyn Any = &mut **o;
            any.downcast_mut()
        })
    }

    /// How many Actions, Effects and Events have run.
    pub fn executions(&self) -> usize {
        self.core.executions
    }

    /// The application's Services.
    pub fn services(&self) -> &Svc {
        self.core
            .services
            .downcast_ref()
            .expect("root Services type")
    }

    /// The fault's message, once the Store has faulted.
    pub fn fault(&self) -> Option<&str> {
        self.core.fault.as_deref()
    }

    /// This Store run's identity.
    pub fn session(&self) -> SessionId {
        self.core.session
    }

    /// Synchronous readiness for this Store's Event bus. Notifications signal
    /// the host's driver; only `drain_events` runs the Events it announces.
    pub fn readiness(&self) -> EventReadiness {
        self.core.inbox.readiness()
    }

    /// Dispatches an Action to `mount` as a root input: reads the clock,
    /// checks admission, and runs it with everything it causes before
    /// returning. A panic anywhere in that work faults the Store.
    pub fn dispatch_to<C: Component>(
        &mut self,
        mount: &Mount<C>,
        action: C::Action,
    ) -> Result<DispatchOutcome, DispatchError> {
        if mount.protocol != self.core.proto.id {
            return Err(DispatchError::ForeignMount);
        }
        self.enter()?;
        let _reports = bug::Scope::enter();

        // The clock is read once, at ingress: that time is the admission time
        // and, if admitted, its `meta.time`; if refused, it is discarded.
        // An invariant reads the kind only of an admitted Action: it is
        // application code, so it runs inside the catch, and after admission.
        // A refused Action's kind is read only to tell observers of it.
        let wants_kind = !self.invariants.is_empty();
        let core = &mut self.core;
        let admitted = panic::catch_unwind(AssertUnwindSafe(|| {
            let now = core.clock_time();
            if !core.admit(mount.index, &action, SentFrom::Root, None, now) {
                return None;
            }
            Some((now, if wants_kind { action.kind() } else { "" }))
        }));
        let (now, kind) = match admitted {
            Err(payload) => return Err(self.faulted(payload)),
            Ok(None) => return Ok(DispatchOutcome::Disabled),
            Ok(Some(admitted)) => admitted,
        };

        // The root-input boundary: one catch for the whole tree.
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            core.run_action(mount.index, Payload::new(action), None, Some(now))
        }));
        match result {
            Ok(execution) if self.core.fault.is_none() => {
                self.settled(execution, now, Category::Action, mount.index, kind);
                Ok(DispatchOutcome::Executed)
            }
            Ok(_) => Err(self.faulted(Box::new(FaultUnwind))),
            Err(payload) => Err(self.faulted(payload)),
        }
    }

    /// Delivers up to `max_events` Events, Store lane first. Each Event is a
    /// root input execution with its own catch.
    pub fn drain_events(&mut self, max_events: usize) -> Result<usize, DispatchError> {
        self.enter()?;
        let _reports = bug::Scope::enter();
        if !self.core.observers.is_empty() {
            let depths = {
                let mut lanes = self.core.inbox.sink.bus.lock();
                let depths = BusDepths {
                    store_lane: lanes.store.len(),
                    worker_lane: lanes.worker.len(),
                    rejected_full: lanes.rejected_full,
                };
                lanes.rejected_full = 0;
                depths
            };
            let core = &mut self.core;
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                core.special = Some((None, Phase::Observer(Boundary::Bus), 0));
                for observer in &mut core.observers {
                    observer.bus(&depths);
                }
                core.special = None;
                core.flush_bugs(Phase::Observer(Boundary::Bus), None, 0);
            }));
            if let Err(payload) = result {
                return Err(self.faulted(payload));
            }
        }
        let mut delivered = 0;
        while delivered < max_events {
            let Some(envelope) = self.core.inbox.sink.bus.pop() else {
                break;
            };
            let core = &mut self.core;
            let result = panic::catch_unwind(AssertUnwindSafe(|| core.run_event(envelope)));
            match result {
                Ok((execution, time, mount, kind)) if self.core.fault.is_none() => {
                    self.settled(execution, time, Category::Event, mount, kind)
                }
                Ok(_) => return Err(self.faulted(Box::new(FaultUnwind))),
                Err(payload) => return Err(self.faulted(payload)),
            }
            delivered += 1;
        }
        Ok(delivered)
    }

    /// A root input settled: the invariants check the State it left.
    #[inline(always)]
    fn settled(
        &mut self,
        execution: ExecutionId,
        time: Timestamp,
        category: Category,
        mount: usize,
        kind: &'static str,
    ) {
        if !self.invariants.is_empty() {
            self.settled_slow(execution, time, category, mount, kind);
        }
    }

    #[inline(never)]
    fn settled_slow(
        &mut self,
        execution: ExecutionId,
        time: Timestamp,
        category: Category,
        mount: usize,
        kind: &'static str,
    ) {
        let proto = Arc::clone(&self.core.proto);
        let path = &proto.mounts[mount].path;
        let step = InvariantStep::Settled {
            execution,
            category,
            path,
            kind,
            time,
        };
        self.check_invariants(0, step);
    }

    fn enter(&mut self) -> Result<(), DispatchError> {
        if let Some(fault) = &self.core.fault {
            return Err(DispatchError::Faulted(fault.clone()));
        }
        // Sends from this thread are Store-lane sends.
        self.core.inbox.sink.bus.lock().owner = Some(thread::current().id());
        Ok(())
    }

    /// The root input ended with a panic, or after its fault: the fault is
    /// recorded unless it was already, where an Effect's `dispatch` let the
    /// panic go (the first one wins), and what the root input left open is
    /// closed.
    fn faulted(&mut self, payload: Box<dyn Any + Send>) -> DispatchError {
        self.core.record_fault(&*payload);
        self.core.end_fault();
        let message = self.core.fault.clone().unwrap_or_default();
        DispatchError::Faulted(message)
    }
}
