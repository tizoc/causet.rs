//! Per-execution FIFO and emission counters.

use crate::{Category, ExecutionId, Origin, Payload, SessionId};

// ---------------------------------------------------------------------------
// Per-execution scratch: the private FIFO and the counters an inline child
// shares with its parent.

pub(crate) struct Exec {
    pub(super) session: SessionId,
    pub(super) execution: ExecutionId,
    pub(super) next_emission: u32,
    pub(super) next_request: u32,
    pub(super) fifo: Vec<Queued>,
}

pub(super) struct Queued {
    pub(super) category: Category,
    pub(super) target: usize,
    pub(super) payload: Payload,
    pub(super) origin: Origin,
}

impl Exec {
    /// `fifo` comes empty from the Store's pool, so queue buffers are reused.
    pub(super) fn new(session: SessionId, execution: ExecutionId, fifo: Vec<Queued>) -> Self {
        Self {
            session,
            execution,
            next_emission: 0,
            next_request: 0,
            fifo,
        }
    }

    pub(super) fn claim_emission(&mut self) -> u32 {
        let ordinal = self.next_emission;
        self.next_emission = self
            .next_emission
            .checked_add(1)
            .expect("emission ordinal overflow");
        ordinal
    }
}
