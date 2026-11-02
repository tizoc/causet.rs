//! Run, instance, execution and request identities.

use std::fmt::Debug;

/// A reading of the Store's clock. With `SystemClock`, nanoseconds since the
/// Unix epoch. An execution's ID is separate from its timestamp.
pub type Timestamp = u64;

/// One execution's number in its Store's run: 1, 2, 3, ... in the order
/// executions start, so it orders the whole run. Its time is separate
/// ([`crate::ExecMeta::time`]); a new Store counts from 1 again, and its session
/// tells the two runs apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ExecutionId(pub u64);

/// A mount's identity: the 64-bit xxh3 hash (seed 0) of its path, the same
/// in every run and every build. Two paths that collide are refused at
/// `freeze`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstanceId(pub u64);

/// One Store run's identity, made with its Event bus. Request IDs and reply
/// routes carry it, so an answer meant for another run is told apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SessionId(pub u64);

/// A request identity created during mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestId {
    /// The Store run that made it.
    pub session: SessionId,
    /// The execution whose reducer made it.
    pub execution: ExecutionId,
    /// Its number within that execution, shared with children called inline.
    pub index: u32,
}
