//! Execution metadata and causal origins.

use crate::{ExecutionId, InstanceId, Timestamp};
use std::fmt::Debug;

// ---------------------------------------------------------------------------
// Execution metadata

/// Who emitted an item: the execution, the component (an inline child's
/// emissions carry the child), and the item's position among the emissions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Origin {
    /// The execution that emitted it.
    pub causal: ExecutionId,
    /// The mount that emitted it.
    pub component: InstanceId,
    /// Its position in the emitting execution's emissions, inline children's
    /// included. A refused queued Action or an ignored Output leaves its gap.
    pub emission_ordinal: u32,
    /// Its position among the Actions one Output was lowered to; 0 otherwise.
    pub lowering_ordinal: u32,
}

/// Every execution's metadata: identity, timeline, depth, time and cause.
#[derive(Clone, Copy, Debug)]
pub struct ExecMeta {
    /// This execution.
    pub id: ExecutionId,
    /// Active frames including this one: a root input is at depth 1.
    pub depth: u32,
    /// The execution's time: the Store's clock as it started, never before
    /// the execution before it. Reducers read time here.
    pub time: Timestamp,
    /// `None` for a root input, which is self-caused.
    pub origin: Option<Origin>,
}

/// An Action's metadata.
pub type ActionMeta = ExecMeta;
/// An Effect's metadata.
pub type EffectMeta = ExecMeta;
/// An Event's metadata.
pub type EventMeta = ExecMeta;

impl ExecMeta {
    /// The execution that caused this one; its own ID for a root input.
    pub fn causal_execution_id(&self) -> ExecutionId {
        self.origin.map_or(self.id, |o| o.causal)
    }
}

/// What an inline child call did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildOutcome {
    /// The child was admitted and its reducer ran.
    Reduced,
    /// An admission check refused it; nothing ran.
    Refused,
}

/// The kind of an execution. Outputs are lowered in place and are never
/// executions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Category {
    /// An Action.
    Action,
    /// An Effect.
    Effect,
    /// An Event.
    Event,
}
