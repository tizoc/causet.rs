//! The four message categories and the empty protocol.

use crate::{FieldVisitor, LogLevel};
use std::any::Any;
use std::fmt::Debug;

// ---------------------------------------------------------------------------
// Messages

/// Implemented by the `causet` derives (`Action`, `Effect`, `Event`,
/// `Output`); a hand-written impl is equally valid.
pub trait Message: Any + Send + Debug + 'static {
    /// The type's name, as logs, stats and observers show it.
    const TYPE: &'static str;
    /// This value's kind: its variant's name as logs and stats show it
    /// (`cart.add`). The derives default it to the type's prefix and the
    /// variant in snake case.
    fn kind(&self) -> &'static str;
    /// The level logging records this value at (`#[causet(log(level = ...))]`).
    fn log_level(&self) -> LogLevel {
        LogLevel::Debug
    }
    /// The fields logging records, captured at start
    /// (`#[causet(log(fields(...)))]`).
    fn log_fields(&self, _fields: &mut dyn FieldVisitor) {}
}

/// A request to examine and maybe change a component's State: admitted by
/// the enabling checks, then reduced.
pub trait Action: Message {}
/// A command for a Service: emitted only by a reducer, carried out by its
/// component's Effect handler at its place in the FIFO.
pub trait Effect: Message {}
/// A fact from outside the Store, such as a Service's answer: sent through
/// the Event bus, delivered by `Store::drain_events`, and turned into Actions
/// by the receiving component's `Adapts` impl.
pub trait Event: Message {}
/// A fact a child reports to its parent, turned into parent Actions by the
/// parent's lowerer for that slot.
pub trait Output: Message {}

/// Fills `Effect` and `Output` for components that have none.
#[derive(Debug)]
pub enum Never {}

impl Message for Never {
    const TYPE: &'static str = "never";
    fn kind(&self) -> &'static str {
        match *self {}
    }
}
impl Action for Never {}
impl Effect for Never {}
impl Event for Never {}
impl Output for Never {}
