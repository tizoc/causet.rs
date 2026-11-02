//! Chain placement as a projection: it keeps a bounded context of the chain
//! reports it has seen and the blocks produced recently, and after each
//! record recomputes the blocks the record may have changed ("dirty"),
//! outputting a block's status when it changes and its placement once when it
//! becomes final. Blocks are kept `CONTEXT_RETENTION_MS` behind the newest
//! event time seen; past that, a block stays a while as a competitor for the
//! blocks of its slot still kept, and chain reports stay as long as a kept
//! block or a competitor reads them. Facts are compared by event time;
//! records arrive in log order.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use causet::projection::{Keyed, Projection};

use crate::classify::{
    ADOPTION_WINDOW_MS, CLOCK_SKEW_TOLERANCE_MS, CONTEXT_RETENTION_MS, CONTEXT_WINDOW_MS,
    ChainPlacementFacts, ChainPlacementResult, ChainTipObservation, PROGRESS_SETTLE_BLOCKS,
    classify, finalized_key,
};
use crate::record::{
    ChainBlockContextObservation, Event, EventIdentity, ProducedBlockObservation, Record,
};

/// A produced block's status: its facts as of now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    pub identity: EventIdentity,
    pub produced: ProducedBlockObservation,
    pub reference_tip: Option<ChainTipObservation>,
    pub parent_seen_recently: Option<bool>,
    pub parent_seen_at_ms: Option<i64>,
    pub adoption_sender_count: i64,
    pub producer_self_adopted: bool,
    pub first_adopted_at_ms: Option<i64>,
    pub last_adopted_at_ms: Option<i64>,
    pub settlement_tip: Option<ChainTipObservation>,
    pub slot_block_count: i64,
    pub slot_external_adoption_rank: i64,
    pub updated_at_ms: i64,
}

/// A produced block's placement, once final.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finalized {
    pub identity: EventIdentity,
    pub produced_at_ms: i64,
    pub chain_placement: ChainPlacementResult,
}

/// What the projection outputs, each keyed as the application stores it: a
/// status by its block, a finalized placement by its idempotency key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    Status {
        key: String,
        status: Box<Status>,
    },
    Finalized {
        key: String,
        finalized: Box<Finalized>,
    },
}

impl Keyed for Output {
    fn key(&self) -> &str {
        match self {
            Output::Status { key, .. } | Output::Finalized { key, .. } => key,
        }
    }
}

impl Output {
    fn status(status: Status) -> Self {
        let key = status_key(&status.identity);
        Output::Status {
            key,
            status: Box::new(status),
        }
    }

    fn finalized(finalized: Finalized) -> Self {
        let id = &finalized.identity;
        let key = finalized_key(&id.sender, &id.run_id, id.event_id);
        Output::Finalized {
            key,
            finalized: Box::new(finalized),
        }
    }
}

#[derive(Clone, Debug)]
struct ContextEntry {
    /// Arrival order among the context entries, for ties.
    sequence: u64,
    sender: String,
    run_id: String,
    timestamp_ms: i64,
    observation: ChainBlockContextObservation,
}

#[derive(Clone, Debug)]
struct ProducedBlockState {
    produced: ProducedBlockObservation,
    last_status: Option<Status>,
    placement_finalized: bool,
    finalized_output_emitted: bool,
}

#[derive(Clone, Debug, Default)]
struct AdoptionSummary {
    sender_count: i64,
    first_seen_at_ms: Option<i64>,
    last_seen_at_ms: Option<i64>,
    producer_self_adopted: bool,
}

/// A production as the projection reads it: produced no later than the
/// clock skew tolerance after its report's own time. A report timed further
/// before the production it claims would be read by its production time but
/// replayed by a warm-up by its own: kept, as a block or a competitor, past
/// where a restart can find it, so an eviction would change a kept block's
/// status only where there was no restart. A producer times its report of
/// a production at the production itself (its later updates are other
/// events), so the original never sees such a report: this changes nothing
/// it reads, and keeps a projection fed one deterministic across restarts.
fn as_read(produced: &ProducedBlockObservation, record_ms: i64) -> ProducedBlockObservation {
    let latest = record_ms.saturating_add(CLOCK_SKEW_TOLERANCE_MS);
    ProducedBlockObservation {
        produced_at_ms: produced.produced_at_ms.min(latest),
        ..produced.clone()
    }
}

/// How much longer than kept blocks the context they read is kept: a kept
/// block's competitors were produced up to `ADOPTION_WINDOW_MS` before it,
/// and a competitor's adoptions are read from `CLOCK_SKEW_TOLERANCE_MS`
/// before it; a kept block's own reference and parent, `CONTEXT_WINDOW_MS`
/// before it.
const CONTEXT_REACH_MS: i64 = if CONTEXT_WINDOW_MS > ADOPTION_WINDOW_MS + CLOCK_SKEW_TOLERANCE_MS {
    CONTEXT_WINDOW_MS
} else {
    ADOPTION_WINDOW_MS + CLOCK_SKEW_TOLERANCE_MS
};

/// The chain-placement projection.
pub struct ChainPlacement {
    contexts: VecDeque<ContextEntry>,
    produced_blocks: BTreeMap<EventIdentity, ProducedBlockState>,
    /// Each kept block, by the key of its status output.
    status_keys: BTreeMap<String, EventIdentity>,
    /// Blocks no longer kept, still read as same-slot competitors by the
    /// blocks that are.
    competitors: BTreeMap<EventIdentity, ProducedBlockObservation>,
    next_sequence: u64,
    max_timestamp_ms: Option<i64>,
    /// How far behind the newest event time context is kept; `None` keeps
    /// everything.
    retention_ms: Option<i64>,
}

impl Default for ChainPlacement {
    fn default() -> Self {
        Self {
            contexts: VecDeque::new(),
            produced_blocks: BTreeMap::new(),
            status_keys: BTreeMap::new(),
            competitors: BTreeMap::new(),
            next_sequence: 0,
            max_timestamp_ms: None,
            retention_ms: Some(CONTEXT_RETENTION_MS),
        }
    }
}

impl Projection<Record> for ChainPlacement {
    type Output = Output;

    fn name(&self) -> &str {
        crate::classify::PROCESSOR
    }

    /// How far back a warm-up must reach: as far as context is kept.
    fn retention_ms(&self) -> Option<u64> {
        self.retention_ms.map(|ms| (ms + CONTEXT_REACH_MS) as u64)
    }

    fn process(&mut self, record: &Record, out: &mut Vec<Output>) {
        let mut dirty = BTreeSet::new();
        self.observe_timestamp(record.timestamp_ms);
        match &record.event {
            Event::Produced(reported) => {
                let produced = &as_read(reported, record.timestamp_ms);
                let identity = record.identity();
                self.status_keys
                    .insert(status_key(&identity), identity.clone());
                self.produced_blocks.insert(
                    identity.clone(),
                    ProducedBlockState {
                        produced: produced.clone(),
                        last_status: None,
                        placement_finalized: false,
                        finalized_output_emitted: false,
                    },
                );
                self.mark_related_produced_blocks(produced, &mut dirty);
                dirty.insert(identity);
            }
            Event::TipAdopted(observation) => {
                let entry = ContextEntry {
                    sequence: self.next_sequence,
                    sender: record.sender.clone(),
                    run_id: record.run_id.clone(),
                    timestamp_ms: record.timestamp_ms,
                    observation: observation.clone(),
                };
                self.mark_produced_blocks_affected_by_context(&entry, &mut dirty);
                self.mark_competitors_of_adopted(&entry.observation.block_hash, &mut dirty);
                self.contexts.push_back(entry);
                self.next_sequence = self.next_sequence.saturating_add(1);
            }
            Event::Other => {}
        }
        self.emit_dirty(dirty, out);
        self.evict_retained();
    }
}

impl ChainPlacement {
    /// The same projection keeping everything it sees: the definition the
    /// bounded one approximates, for tests to compare it with.
    pub fn unbounded() -> Self {
        Self {
            retention_ms: None,
            ..Self::default()
        }
    }

    /// The status output `key` names, recomputed now from the context kept,
    /// whether or not anything marked its block; `None` when the key names
    /// no block kept (one evicted, or a finalized placement's key). What a
    /// check of incremental against from-scratch asks (`check_incremental`).
    pub fn status_now(&self, key: &str) -> Option<Output> {
        let identity = self.status_keys.get(key)?;
        let state = self.produced_blocks.get(identity)?;
        Some(Output::status(self.status(identity, &state.produced)))
    }

    fn observe_timestamp(&mut self, timestamp_ms: i64) {
        self.max_timestamp_ms = Some(
            self.max_timestamp_ms
                .map_or(timestamp_ms, |max| max.max(timestamp_ms)),
        );
    }

    fn evict_retained(&mut self) {
        let (Some(max_timestamp_ms), Some(retention_ms)) =
            (self.max_timestamp_ms, self.retention_ms)
        else {
            return;
        };
        let cutoff = max_timestamp_ms.saturating_sub(retention_ms);
        // What a kept block reads is kept as long as it is: evicted sooner,
        // a later recompute of the block would find less than it had.
        let competitor_cutoff = cutoff.saturating_sub(ADOPTION_WINDOW_MS);
        let context_cutoff = cutoff.saturating_sub(CONTEXT_REACH_MS);
        self.contexts
            .retain(|entry| entry.timestamp_ms >= context_cutoff);
        let expired: Vec<EventIdentity> = self
            .produced_blocks
            .iter()
            .filter(|(_, state)| state.produced.produced_at_ms < cutoff)
            .map(|(identity, _)| identity.clone())
            .collect();
        for identity in expired {
            if let Some(state) = self.produced_blocks.remove(&identity) {
                self.competitors.insert(identity, state.produced);
            }
        }
        self.competitors
            .retain(|_, produced| produced.produced_at_ms >= competitor_cutoff);
        let kept = &self.produced_blocks;
        self.status_keys
            .retain(|_, identity| kept.contains_key(identity));
    }

    /// An adoption of a block changes the rank of every block competing
    /// for its slot, kept or not: the kept ones are recomputed.
    fn mark_competitors_of_adopted(&self, block_hash: &str, dirty: &mut BTreeSet<EventIdentity>) {
        let adopted: Vec<&ProducedBlockObservation> = self
            .produced_blocks
            .values()
            .map(|state| &state.produced)
            .chain(self.competitors.values())
            .filter(|produced| produced.block_hash == block_hash)
            .collect();
        for (identity, state) in &self.produced_blocks {
            if adopted
                .iter()
                .any(|produced| same_slot_candidate(produced, &state.produced))
            {
                dirty.insert(identity.clone());
            }
        }
    }

    fn mark_produced_blocks_affected_by_context(
        &self,
        context: &ContextEntry,
        dirty: &mut BTreeSet<EventIdentity>,
    ) {
        let from_ms = context.timestamp_ms.saturating_sub(ADOPTION_WINDOW_MS);
        let to_ms = context.timestamp_ms.saturating_add(CONTEXT_WINDOW_MS);
        for (identity, state) in &self.produced_blocks {
            if state.produced.produced_at_ms >= from_ms && state.produced.produced_at_ms <= to_ms {
                dirty.insert(identity.clone());
                continue;
            }

            let settle_height = state
                .produced
                .block_height
                .saturating_add(PROGRESS_SETTLE_BLOCKS);
            let first_settlement_at_ms = state
                .last_status
                .as_ref()
                .and_then(|status| status.settlement_tip.as_ref())
                .map(|tip| tip.timestamp_ms);
            let improves_settlement_tip = first_settlement_at_ms
                .is_none_or(|timestamp_ms| context.timestamp_ms < timestamp_ms);
            if context.timestamp_ms >= state.produced.produced_at_ms
                && context.observation.height >= settle_height
                && (!state.placement_finalized || improves_settlement_tip)
            {
                dirty.insert(identity.clone());
            }
        }
    }

    fn mark_related_produced_blocks(
        &self,
        produced: &ProducedBlockObservation,
        dirty: &mut BTreeSet<EventIdentity>,
    ) {
        for (identity, state) in &self.produced_blocks {
            if same_slot_candidate(produced, &state.produced) {
                dirty.insert(identity.clone());
            }
        }
    }

    fn emit_dirty(&mut self, dirty: BTreeSet<EventIdentity>, out: &mut Vec<Output>) {
        let mut pending = Vec::new();
        for identity in dirty {
            let Some(state) = self.produced_blocks.get(&identity) else {
                continue;
            };
            let status = self.status(&identity, &state.produced);
            let chain_placement = self.chain_placement(&status);
            let status_changed = state.last_status.as_ref() != Some(&status);
            let finalized_output_needed =
                chain_placement.placement_finalized && !state.finalized_output_emitted;
            if status_changed || finalized_output_needed {
                pending.push((
                    identity,
                    status,
                    chain_placement,
                    status_changed,
                    finalized_output_needed,
                ));
            }
        }

        for (identity, status, chain_placement, status_changed, finalized_output_needed) in pending
        {
            let placement_finalized = chain_placement.placement_finalized;
            let produced_at_ms = status.produced.produced_at_ms;
            if status_changed {
                out.push(Output::status(status.clone()));
            }
            if finalized_output_needed {
                out.push(Output::finalized(Finalized {
                    identity: identity.clone(),
                    produced_at_ms,
                    chain_placement,
                }));
            }
            let Some(state) = self.produced_blocks.get_mut(&identity) else {
                continue;
            };
            if status_changed {
                state.last_status = Some(status);
            }
            if placement_finalized {
                state.placement_finalized = true;
            }
            if finalized_output_needed {
                state.finalized_output_emitted = true;
            }
        }
    }

    fn status(&self, identity: &EventIdentity, produced: &ProducedBlockObservation) -> Status {
        let reference_tip = self.reference_tip(produced);
        let parent_tip = self.parent_tip(produced);
        let settlement_tip = self.settlement_tip(produced);
        let adoption = self.adoption_summary(identity, produced);
        let parent_seen_recently = produced.parent_hash.as_ref().map(|_| parent_tip.is_some());
        let parent_seen_at_ms = parent_tip.as_ref().map(|tip| tip.timestamp_ms);
        let (slot_block_count, slot_external_adoption_rank) =
            self.same_slot_competition(produced, &adoption);
        let updated_at_ms = [
            Some(produced.produced_at_ms),
            reference_tip.as_ref().map(|tip| tip.timestamp_ms),
            parent_seen_at_ms,
            adoption.first_seen_at_ms,
            adoption.last_seen_at_ms,
            settlement_tip.as_ref().map(|tip| tip.timestamp_ms),
        ]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(produced.produced_at_ms);

        Status {
            identity: identity.clone(),
            produced: produced.clone(),
            reference_tip,
            parent_seen_recently,
            parent_seen_at_ms,
            adoption_sender_count: adoption.sender_count,
            producer_self_adopted: adoption.producer_self_adopted,
            first_adopted_at_ms: adoption.first_seen_at_ms,
            last_adopted_at_ms: adoption.last_seen_at_ms,
            settlement_tip,
            slot_block_count,
            slot_external_adoption_rank,
            updated_at_ms,
        }
    }

    fn chain_placement(&self, status: &Status) -> ChainPlacementResult {
        let adoption_tip = status
            .first_adopted_at_ms
            .or(status.last_adopted_at_ms)
            .map(|timestamp_ms| ChainTipObservation {
                block_hash: status.produced.block_hash.clone(),
                height: status.produced.block_height,
                global_slot: status.produced.global_slot,
                timestamp_ms,
            });
        let parent_tip = status
            .parent_seen_at_ms
            .map(|timestamp_ms| ChainTipObservation {
                block_hash: status.produced.parent_hash.clone().unwrap_or_default(),
                height: status.produced.block_height.saturating_sub(1),
                global_slot: status.produced.global_slot.saturating_sub(1),
                timestamp_ms,
            });
        classify(
            status.produced.clone(),
            ChainPlacementFacts {
                reference_tip: status.reference_tip.clone(),
                parent_tip,
                settlement_tip: status.settlement_tip.clone(),
                adoption_tip,
                parent_seen_recently: status.parent_seen_recently,
                adoption_sender_count: status.adoption_sender_count,
                producer_self_adopted: status.producer_self_adopted,
                slot_block_count: status.slot_block_count,
                slot_external_adoption_rank: status.slot_external_adoption_rank,
                status_updated_at_ms: status.updated_at_ms,
                as_of_timestamp_ms: self.max_timestamp_ms.unwrap_or(status.updated_at_ms),
            },
        )
    }

    /// The highest report in the window around the block.
    fn reference_tip(&self, produced: &ProducedBlockObservation) -> Option<ChainTipObservation> {
        let from_ms = produced.produced_at_ms.saturating_sub(CONTEXT_WINDOW_MS);
        let to_ms = produced
            .produced_at_ms
            .saturating_add(CLOCK_SKEW_TOLERANCE_MS);
        self.contexts
            .iter()
            .filter(|entry| entry.timestamp_ms >= from_ms && entry.timestamp_ms <= to_ms)
            .max_by_key(|entry| (entry.observation.height, entry.timestamp_ms, entry.sequence))
            .map(chain_tip)
    }

    /// The latest report of the block's parent in the window.
    fn parent_tip(&self, produced: &ProducedBlockObservation) -> Option<ChainTipObservation> {
        let parent_hash = produced.parent_hash.as_deref()?;
        let from_ms = produced.produced_at_ms.saturating_sub(CONTEXT_WINDOW_MS);
        let to_ms = produced
            .produced_at_ms
            .saturating_add(CLOCK_SKEW_TOLERANCE_MS);
        self.contexts
            .iter()
            .filter(|entry| {
                entry.observation.block_hash == parent_hash
                    && entry.timestamp_ms >= from_ms
                    && entry.timestamp_ms <= to_ms
            })
            .max_by_key(|entry| (entry.timestamp_ms, entry.sequence))
            .map(chain_tip)
    }

    /// The first report at least `PROGRESS_SETTLE_BLOCKS` above the block.
    fn settlement_tip(&self, produced: &ProducedBlockObservation) -> Option<ChainTipObservation> {
        let settle_height = produced.block_height.saturating_add(PROGRESS_SETTLE_BLOCKS);
        self.contexts
            .iter()
            .filter(|entry| {
                entry.observation.height >= settle_height
                    && entry.timestamp_ms >= produced.produced_at_ms
            })
            .min_by_key(|entry| (entry.timestamp_ms, entry.sequence))
            .map(chain_tip)
    }

    /// Who else adopted the block within the adoption window: each other
    /// sender and run once. The producer's own run does not count.
    fn adoption_summary(
        &self,
        producer: &EventIdentity,
        produced: &ProducedBlockObservation,
    ) -> AdoptionSummary {
        let from_ms = produced
            .produced_at_ms
            .saturating_sub(CLOCK_SKEW_TOLERANCE_MS);
        let to_ms = produced.produced_at_ms.saturating_add(ADOPTION_WINDOW_MS);
        let mut external = BTreeSet::new();
        let mut summary = AdoptionSummary::default();
        for entry in self.contexts.iter().filter(|entry| {
            entry.observation.block_hash == produced.block_hash
                && entry.timestamp_ms >= from_ms
                && entry.timestamp_ms <= to_ms
        }) {
            if entry.sender == producer.sender && entry.run_id == producer.run_id {
                summary.producer_self_adopted = true;
            } else {
                external.insert((entry.sender.as_str(), entry.run_id.as_str()));
                summary.first_seen_at_ms = Some(
                    summary
                        .first_seen_at_ms
                        .map_or(entry.timestamp_ms, |at| at.min(entry.timestamp_ms)),
                );
                summary.last_seen_at_ms = Some(
                    summary
                        .last_seen_at_ms
                        .map_or(entry.timestamp_ms, |at| at.max(entry.timestamp_ms)),
                );
            }
        }
        summary.sender_count = i64::try_from(external.len()).unwrap_or(i64::MAX);
        summary
    }

    /// How many blocks compete for the slot, and this one's rank among
    /// those with external adoption (1 is best).
    fn same_slot_competition(
        &self,
        produced: &ProducedBlockObservation,
        adoption: &AdoptionSummary,
    ) -> (i64, i64) {
        let mut same_slot_blocks = BTreeSet::new();
        let mut better_slot_blocks = BTreeSet::new();
        let current_rank = AdoptionRank {
            sender_count: adoption.sender_count,
            last_seen_at_ms: adoption.last_seen_at_ms.unwrap_or(0),
            produced_at_ms: produced.produced_at_ms,
            block_hash: produced.block_hash.clone(),
        };
        let kept = self
            .produced_blocks
            .iter()
            .map(|(identity, state)| (identity, &state.produced));
        for (other_identity, other) in kept.chain(&self.competitors) {
            if !same_slot_candidate(produced, other) {
                continue;
            }
            same_slot_blocks.insert(other.block_hash.clone());
            if other.block_hash == produced.block_hash {
                continue;
            }
            let other_adoption = self.adoption_summary(other_identity, other);
            if other_adoption.sender_count == 0 {
                continue;
            }
            let other_rank = AdoptionRank {
                sender_count: other_adoption.sender_count,
                last_seen_at_ms: other_adoption.last_seen_at_ms.unwrap_or(0),
                produced_at_ms: other.produced_at_ms,
                block_hash: other.block_hash.clone(),
            };
            if other_rank.is_better_than(&current_rank) {
                better_slot_blocks.insert(other.block_hash.clone());
            }
        }
        (
            i64::try_from(same_slot_blocks.len()).unwrap_or(i64::MAX),
            i64::try_from(better_slot_blocks.len().saturating_add(1)).unwrap_or(i64::MAX),
        )
    }
}

#[derive(Clone, Debug)]
struct AdoptionRank {
    sender_count: i64,
    last_seen_at_ms: i64,
    produced_at_ms: i64,
    block_hash: String,
}

impl AdoptionRank {
    /// More external senders first, then the latest adoption, then the
    /// earliest produced, then the smaller hash.
    fn is_better_than(&self, other: &Self) -> bool {
        self.sender_count > other.sender_count
            || (self.sender_count == other.sender_count
                && self.last_seen_at_ms > other.last_seen_at_ms)
            || (self.sender_count == other.sender_count
                && self.last_seen_at_ms == other.last_seen_at_ms
                && self.produced_at_ms < other.produced_at_ms)
            || (self.sender_count == other.sender_count
                && self.last_seen_at_ms == other.last_seen_at_ms
                && self.produced_at_ms == other.produced_at_ms
                && self.block_hash < other.block_hash)
    }
}

fn same_slot_candidate(left: &ProducedBlockObservation, right: &ProducedBlockObservation) -> bool {
    left.global_slot == right.global_slot
        && left.block_height == right.block_height
        && (left.parent_hash.is_none()
            || right.parent_hash.is_none()
            || left.parent_hash == right.parent_hash)
        && left.produced_at_ms.abs_diff(right.produced_at_ms) <= ADOPTION_WINDOW_MS as u64
}

fn chain_tip(entry: &ContextEntry) -> ChainTipObservation {
    ChainTipObservation {
        block_hash: entry.observation.block_hash.clone(),
        height: entry.observation.height,
        global_slot: entry.observation.global_slot,
        timestamp_ms: entry.timestamp_ms,
    }
}

fn status_key(identity: &EventIdentity) -> String {
    format!(
        "status:{}:{}:{}",
        identity.sender, identity.run_id, identity.event_id
    )
}
