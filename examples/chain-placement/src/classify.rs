//! Where a produced block landed, from event-time facts: a pure function,
//! with the windows it reads its facts in.
//!
//! The classifier assumes trusted reporters, out-of-order arrival, 5-second
//! slots and chain reports roughly every 30 seconds. It is an
//! eventually-correct observability model, not a consensus proof. Its
//! classification and reason strings are its output, and stay stable.

use crate::record::ProducedBlockObservation;

/// Event-time look-behind/look-ahead window for reference and parent context.
pub const CONTEXT_WINDOW_MS: i64 = 5 * 60 * 1_000;
/// Event-time window for external adoption reports of a produced block.
pub const ADOPTION_WINDOW_MS: i64 = 5 * 60 * 1_000;
/// Worst-case tolerated clock skew between senders for event-time
/// comparisons. Delayed arrival is handled by the projection, not by
/// widening these matches.
pub const CLOCK_SKEW_TOLERANCE_MS: i64 = 2_000;
/// How long context is kept, behind the newest event time.
pub const CONTEXT_RETENTION_MS: i64 = 15 * 60 * 1_000;
/// Diagnostic age for callers that flag stale observations. Wall-clock age
/// never finalizes placement; later chain progress does.
pub const ADOPTION_SETTLE_MS: i64 = 2 * 60 * 1_000;
/// How many blocks past a produced block the chain must progress before its
/// placement is final: a guard against short reorgs.
pub const PROGRESS_SETTLE_BLOCKS: i64 = 2;
/// Height lag, relative to the reference tip, considered suspicious.
pub const HEIGHT_LAG_THRESHOLD: i64 = 2;
/// The processor and output names of a finalized placement.
pub const PROCESSOR: &str = "chain_placement";
pub const FINALIZED_OUTPUT: &str = "produced_block_chain_placement";

/// A chain tip selected from event-time context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainTipObservation {
    pub block_hash: String,
    pub height: i64,
    pub global_slot: i64,
    pub timestamp_ms: i64,
}

/// The event-time facts needed to classify one produced block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainPlacementFacts {
    pub reference_tip: Option<ChainTipObservation>,
    pub parent_tip: Option<ChainTipObservation>,
    pub settlement_tip: Option<ChainTipObservation>,
    pub adoption_tip: Option<ChainTipObservation>,
    pub parent_seen_recently: Option<bool>,
    pub adoption_sender_count: i64,
    pub producer_self_adopted: bool,
    pub slot_block_count: i64,
    pub slot_external_adoption_rank: i64,
    pub status_updated_at_ms: i64,
    pub as_of_timestamp_ms: i64,
}

/// One produced block's placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainPlacementResult {
    pub produced_at_ms: i64,
    pub block_hash: String,
    pub parent_hash: Option<String>,
    pub block_height: i64,
    pub global_slot: i64,
    pub reference_tip: Option<ChainTipObservation>,
    pub parent_tip: Option<ChainTipObservation>,
    pub settlement_tip: Option<ChainTipObservation>,
    pub adoption_tip: Option<ChainTipObservation>,
    pub height_delta: Option<i64>,
    pub chain_progress_blocks: Option<i64>,
    pub parent_seen_recently: Option<bool>,
    pub adoption_sender_count: i64,
    pub external_adoption_seen: bool,
    pub producer_self_adopted: bool,
    pub slot_block_count: i64,
    pub slot_external_adoption_rank: i64,
    pub canonical: bool,
    pub produced_block_adopted: bool,
    pub adoption_settled: bool,
    pub placement_finalized: bool,
    pub finalization_reason: Option<String>,
    pub classification: String,
    pub reason: String,
    pub updated_at_ms: i64,
}

/// Classifies one produced block from its facts. Missing reference or
/// parent evidence gives `unknown` rather than invented context; a block
/// nobody else adopted becomes `orphaned` only once chain progress settles
/// it.
pub fn classify(
    produced: ProducedBlockObservation,
    facts: ChainPlacementFacts,
) -> ChainPlacementResult {
    let ChainPlacementFacts {
        reference_tip,
        parent_tip,
        settlement_tip,
        adoption_tip,
        parent_seen_recently,
        adoption_sender_count,
        producer_self_adopted,
        slot_block_count,
        slot_external_adoption_rank,
        status_updated_at_ms,
        as_of_timestamp_ms: _,
    } = facts;

    let height_delta = reference_tip
        .as_ref()
        .map(|tip| produced.block_height.saturating_sub(tip.height));
    let external_adoption_seen = adoption_sender_count > 0;
    let canonical = external_adoption_seen && slot_external_adoption_rank == 1;
    let produced_block_adopted = external_adoption_seen;
    let chain_progress_blocks = settlement_tip
        .as_ref()
        .map(|tip| tip.height.saturating_sub(produced.block_height));
    let chain_progress_settled =
        chain_progress_blocks.is_some_and(|blocks| blocks >= PROGRESS_SETTLE_BLOCKS);
    let placement_finalized = chain_progress_settled;
    let adoption_settled = placement_finalized;
    let finalization_reason = chain_progress_settled.then_some("chain_progress");

    let max_stale_delta = HEIGHT_LAG_THRESHOLD.saturating_neg();
    let stale_height = height_delta.is_some_and(|delta| delta <= max_stale_delta);
    let parent_seen = parent_seen_recently.unwrap_or(false);

    let (classification, reason) = if canonical {
        ("canonical", "slot_best_external_adoption")
    } else if external_adoption_seen {
        ("orphaned", "same_slot_block_has_stronger_external_adoption")
    } else if slot_external_adoption_rank > 1 {
        // Another block of the same slot has external adoption already: this
        // one lost its slot, whatever the chain does next.
        ("orphaned", "same_slot_block_has_stronger_external_adoption")
    } else {
        match (reference_tip.as_ref(), produced.parent_hash.as_ref()) {
            _ if chain_progress_settled => ("orphaned", "chain_advanced_without_external_adoption"),
            (None, _) => ("unknown", "missing_reference_tip"),
            (_, None) => ("unknown", "missing_parent_hash"),
            (Some(_), Some(_)) if stale_height && !parent_seen => {
                ("likely_side_chain", "height_lag_and_parent_not_recent")
            }
            (Some(_), Some(_)) if stale_height => ("stale_parent", "height_lag"),
            (Some(_), Some(_)) if !parent_seen => ("stale_parent", "parent_not_recent"),
            (Some(_), Some(_)) => ("likely_main_chain", "parent_seen_recently"),
        }
    };

    let updated_at_ms = adoption_tip.as_ref().map_or_else(
        || {
            settlement_tip.as_ref().map_or(status_updated_at_ms, |tip| {
                status_updated_at_ms.max(tip.timestamp_ms)
            })
        },
        |tip| tip.timestamp_ms,
    );

    ChainPlacementResult {
        produced_at_ms: produced.produced_at_ms,
        block_hash: produced.block_hash,
        parent_hash: produced.parent_hash,
        block_height: produced.block_height,
        global_slot: produced.global_slot,
        reference_tip,
        parent_tip,
        settlement_tip,
        adoption_tip,
        height_delta,
        chain_progress_blocks,
        parent_seen_recently,
        adoption_sender_count,
        external_adoption_seen,
        producer_self_adopted,
        slot_block_count,
        slot_external_adoption_rank,
        canonical,
        produced_block_adopted,
        adoption_settled,
        placement_finalized,
        finalization_reason: finalization_reason.map(ToOwned::to_owned),
        classification: classification.to_owned(),
        reason: reason.to_owned(),
        updated_at_ms,
    }
}

/// The stable idempotency key of one block's finalized placement.
pub fn finalized_key(sender: &str, run_id: &str, event_id: u64) -> String {
    format!("{PROCESSOR}:{FINALIZED_OUTPUT}:{sender}:{run_id}:{event_id}")
}
