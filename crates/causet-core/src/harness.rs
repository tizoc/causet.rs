//! A single-component transition harness.

use crate::{
    Action, Backend, BugReport, Component, ComponentContext, ExecMeta, ExecutionId, Payload,
    SessionId, Timestamp, bug,
};
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;

// ---------------------------------------------------------------------------
// Component test harness

pub(super) struct Collected {
    pub(super) session: SessionId,
    pub(super) execution: ExecutionId,
    pub(super) next_request: u32,
    pub(super) emissions: Vec<RawEmission>,
}

pub(super) enum RawEmission {
    Action(Payload),
    Effect(Payload),
    Output(Payload),
    InlineChild(ChildCall),
    ToChild(ChildCall),
}

/// A call to a child, recorded rather than run: the child is not mounted
/// under a `Harness`.
pub struct ChildCall {
    /// The slot's name.
    pub slot: &'static str,
    /// The child Action's kind.
    pub kind: &'static str,
    pub(super) action: Payload,
}

impl ChildCall {
    pub(super) fn new<A: Action>(slot: &'static str, action: A) -> Self {
        Self {
            slot,
            kind: action.kind(),
            action: Payload::new(action),
        }
    }

    /// The child's Action, if it has type `A`.
    pub fn action<A: Action>(&self) -> Option<&A> {
        self.action.downcast_ref()
    }
}

impl Debug for ChildCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.slot, self.kind)
    }
}

impl PartialEq for ChildCall {
    fn eq(&self, other: &Self) -> bool {
        self.slot == other.slot && self.kind == other.kind
    }
}

/// One emission of a component under test, in emission order.
pub enum Emission<C: Component> {
    /// `emit_action`.
    Action(C::Action),
    /// `emit_effect`.
    Effect(C::Effect),
    /// `emit_output`.
    Output(C::Output),
    /// `reduce_child`: recorded, reported to the reducer as `Reduced`.
    InlineChild(ChildCall),
    /// `to_child`.
    ToChild(ChildCall),
}

impl<C: Component> Debug for Emission<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Action(a) => f.debug_tuple("Action").field(a).finish(),
            Self::Effect(e) => f.debug_tuple("Effect").field(e).finish(),
            Self::Output(o) => f.debug_tuple("Output").field(o).finish(),
            Self::InlineChild(c) => f.debug_tuple("InlineChild").field(c).finish(),
            Self::ToChild(c) => f.debug_tuple("ToChild").field(c).finish(),
        }
    }
}

impl<C: Component> PartialEq for Emission<C>
where
    C::Action: PartialEq,
    C::Effect: PartialEq,
    C::Output: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Action(a), Self::Action(b)) => a == b,
            (Self::Effect(a), Self::Effect(b)) => a == b,
            (Self::Output(a), Self::Output(b)) => a == b,
            (Self::InlineChild(a), Self::InlineChild(b)) | (Self::ToChild(a), Self::ToChild(b)) => {
                a == b
            }
            _ => false,
        }
    }
}

/// Drives one component's transitions with local State and records what it
/// emits, with no Protocol, Store or registration: State, an Action and a
/// time in; admission, the new State and the ordered emissions out. Children
/// are not run: inline and queued child calls are recorded as emissions.
/// Executions are numbered and stamped as a Store does it: IDs from 1, times
/// `max(previous, now)`, with `now` set by `at` (0 until then).
pub struct Harness<C: Component> {
    pub(super) session: SessionId,
    /// The last execution's time; 0 before the first.
    pub(super) last: Timestamp,
    /// The next execution's ID.
    pub(super) next_id: u64,
    /// The clock reading the next execution is stamped from.
    pub(super) now: Timestamp,
    pub(super) bugs: Vec<BugReport>,
    pub(super) _c: PhantomData<fn() -> C>,
}

/// `Harness::dispatch` refused the Action: the component's enabling check
/// said no. Nothing ran and no execution time was used, as in a Store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Disabled;

impl<C: Component> Default for Harness<C> {
    fn default() -> Self {
        Self {
            session: SessionId(0),
            last: 0,
            next_id: 1,
            now: 0,
            bugs: Vec::new(),
            _c: PhantomData,
        }
    }
}

impl<C: Component> Harness<C> {
    /// A harness in session 0 whose executions are numbered from 1.
    pub fn new() -> Self {
        Self::default()
    }

    /// A harness whose request IDs carry `session`, to match a fixture's.
    pub fn with_session(session: SessionId) -> Self {
        Self {
            session,
            ..Self::default()
        }
    }

    /// Sets the clock reading the next executions are stamped from. Like a
    /// Store's clock it may step back; execution times never do.
    pub fn at(&mut self, now: Timestamp) -> &mut Self {
        self.now = now;
        self
    }

    /// The time the next execution would be stamped with.
    pub fn next_time(&self) -> Timestamp {
        self.last.max(self.now)
    }

    /// The component's own enabling check, at the time the next execution
    /// would have.
    pub fn is_enabled(&mut self, state: &C::State, action: &C::Action) -> bool {
        let scope = bug::Scope::enter();
        let enabled = C::is_enabled(state, action, self.next_time());
        self.keep_bugs(scope);
        enabled
    }

    /// One root transition as a Store runs it: the enabling check, then, if
    /// it admits the Action, the reducer, whose emissions are returned in
    /// order. A refusal is `Err(Disabled)` and leaves State and time alone.
    pub fn dispatch(
        &mut self,
        state: &mut C::State,
        action: C::Action,
    ) -> Result<Vec<Emission<C>>, Disabled> {
        if !self.is_enabled(state, &action) {
            return Err(Disabled);
        }
        Ok(self.reduce(state, action))
    }

    /// The bug reports the checks and reducers run so far made, in order,
    /// with no place: there is no Store.
    pub fn bug_reports(&self) -> &[BugReport] {
        &self.bugs
    }

    fn keep_bugs(&mut self, scope: bug::Scope) {
        self.bugs.extend(
            bug::take()
                .into_iter()
                .map(|pending| pending.into_report(None)),
        );
        drop(scope);
    }

    /// Runs the reducer as one root execution, skipping the enabling check,
    /// and returns its emissions in order. Each call is a new execution, so
    /// request counters restart.
    pub fn reduce(&mut self, state: &mut C::State, action: C::Action) -> Vec<Emission<C>> {
        let time = self.next_time();
        let id = ExecutionId(self.next_id);
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("execution IDs exhausted");
        self.last = time;
        let meta = ExecMeta {
            id,
            depth: 1,
            time,
            origin: None,
        };
        let mut collected = Collected {
            session: self.session,
            execution: id,
            next_request: 0,
            emissions: Vec::new(),
        };
        let ctx = ComponentContext::<C> {
            backend: Backend::Harness(&mut collected),
            state,
            meta: &meta,
            _c: PhantomData,
        };
        let scope = bug::Scope::enter();
        C::reduce(ctx, action, &meta);
        self.keep_bugs(scope);
        collected
            .emissions
            .into_iter()
            .map(|raw| match raw {
                RawEmission::Action(a) => Emission::Action(a.take().expect("Action type")),
                RawEmission::Effect(e) => Emission::Effect(e.take().expect("Effect type")),
                RawEmission::Output(o) => Emission::Output(o.take().expect("Output type")),
                RawEmission::InlineChild(c) => Emission::InlineChild(c),
                RawEmission::ToChild(c) => Emission::ToChild(c),
            })
            .collect()
    }
}
