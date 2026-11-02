//! The log's records: what nodes report, typed. A record has its sender, the
//! sender's run, an event ID unique within the run, and its event time.

use causet::projection::LogEntry;

/// Who reported a record: a node, in one run of it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventIdentity {
    pub sender: String,
    pub run_id: String,
    pub event_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub sender: String,
    pub run_id: String,
    pub event_id: u64,
    /// When it happened, by the sender's clock.
    pub timestamp_ms: i64,
    pub event: Event,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A node produced a block and injected it.
    Produced(ProducedBlockObservation),
    /// A node adopted a block as its best tip.
    TipAdopted(ChainBlockContextObservation),
    /// Anything else: chain placement ignores it.
    Other,
}

/// A produced block, as its producer reports it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProducedBlockObservation {
    pub produced_at_ms: i64,
    pub block_hash: String,
    pub parent_hash: Option<String>,
    pub block_height: i64,
    pub global_slot: i64,
}

/// A block a node adopted as its best tip.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainBlockContextObservation {
    pub block_hash: String,
    pub parent_hash: Option<String>,
    pub height: i64,
    pub global_slot: i64,
}

impl Record {
    pub fn identity(&self) -> EventIdentity {
        EventIdentity {
            sender: self.sender.clone(),
            run_id: self.run_id.clone(),
            event_id: self.event_id,
        }
    }
}

impl LogEntry for Record {
    fn time_ms(&self) -> u64 {
        u64::try_from(self.timestamp_ms).unwrap_or(0)
    }
}
