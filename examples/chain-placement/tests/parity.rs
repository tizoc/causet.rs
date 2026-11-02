//! The original's tests of the classifier and of chain-placement
//! projection, asserting what they asserted. The original fed a whole batch
//! of records before outputting; a `Projection` outputs after each record,
//! so the batch test counts finalized placements, which are once per block
//! either way.

use causet::projection::Projection;
use chain_placement::classify::{
    ADOPTION_SETTLE_MS, ADOPTION_WINDOW_MS, ChainPlacementFacts, ChainTipObservation, classify,
    finalized_key,
};
use chain_placement::projection::{ChainPlacement, Output};
use chain_placement::record::{
    ChainBlockContextObservation, Event, ProducedBlockObservation, Record,
};

// ---------------------------------------------------------------------------
// The classifier

fn produced_block() -> ProducedBlockObservation {
    ProducedBlockObservation {
        produced_at_ms: 1_000,
        block_hash: "block-hash-41".to_owned(),
        parent_hash: Some("block-hash-40".to_owned()),
        block_height: 41,
        global_slot: 1041,
    }
}

fn tip(height: i64, timestamp_ms: i64) -> ChainTipObservation {
    ChainTipObservation {
        block_hash: format!("block-hash-{height}"),
        height,
        global_slot: height + 1_000,
        timestamp_ms,
    }
}

fn facts() -> ChainPlacementFacts {
    ChainPlacementFacts {
        reference_tip: None,
        parent_tip: None,
        settlement_tip: None,
        adoption_tip: None,
        parent_seen_recently: None,
        adoption_sender_count: 0,
        producer_self_adopted: false,
        slot_block_count: 1,
        slot_external_adoption_rank: 0,
        status_updated_at_ms: 1_000,
        as_of_timestamp_ms: 1_100,
    }
}

trait FactBuilder {
    fn with_reference(self) -> Self;
    fn with_reference_height(self, height: i64) -> Self;
    fn with_parent_seen(self, seen: bool) -> Self;
    fn with_external_adoption(self, senders: i64, rank: i64) -> Self;
    fn with_settlement_height(self, height: i64) -> Self;
    fn with_as_of(self, timestamp_ms: i64) -> Self;
}

impl FactBuilder for ChainPlacementFacts {
    fn with_reference(mut self) -> Self {
        self.reference_tip = Some(tip(40, 900));
        self
    }

    fn with_reference_height(mut self, height: i64) -> Self {
        self.reference_tip = Some(tip(height, 900));
        self
    }

    fn with_parent_seen(mut self, seen: bool) -> Self {
        self.parent_seen_recently = Some(seen);
        self.parent_tip = seen.then(|| tip(40, 950));
        self
    }

    fn with_external_adoption(mut self, senders: i64, rank: i64) -> Self {
        self.adoption_sender_count = senders;
        self.slot_external_adoption_rank = rank;
        self.adoption_tip = Some(tip(41, 1_050));
        self
    }

    fn with_settlement_height(mut self, height: i64) -> Self {
        self.settlement_tip = Some(tip(height, 1_100));
        self
    }

    fn with_as_of(mut self, timestamp_ms: i64) -> Self {
        self.as_of_timestamp_ms = timestamp_ms;
        self
    }
}

#[test]
fn classifies_core_chain_placement_cases() {
    let produced = produced_block();
    let canonical = classify(
        produced.clone(),
        facts()
            .with_reference()
            .with_parent_seen(true)
            .with_external_adoption(2, 1),
    );
    assert_eq!(canonical.classification, "canonical");
    assert!(canonical.produced_block_adopted);
    assert!(!canonical.placement_finalized);
    assert_eq!(canonical.finalization_reason, None);

    let finalized_canonical = classify(
        produced.clone(),
        facts()
            .with_reference()
            .with_parent_seen(true)
            .with_external_adoption(2, 1)
            .with_settlement_height(44),
    );
    assert_eq!(finalized_canonical.classification, "canonical");
    assert!(finalized_canonical.placement_finalized);
    assert_eq!(
        finalized_canonical.finalization_reason.as_deref(),
        Some("chain_progress")
    );

    let orphaned = classify(
        produced.clone(),
        facts().with_reference().with_external_adoption(1, 2),
    );
    assert_eq!(orphaned.classification, "orphaned");
    assert_eq!(
        orphaned.reason,
        "same_slot_block_has_stronger_external_adoption"
    );

    let likely_main = classify(
        produced.clone(),
        facts().with_reference().with_parent_seen(true),
    );
    assert_eq!(likely_main.classification, "likely_main_chain");
    assert!(!likely_main.placement_finalized);

    let mut competing_adopted = facts().with_reference().with_parent_seen(true);
    competing_adopted.slot_external_adoption_rank = 2;
    let competing_adopted = classify(produced.clone(), competing_adopted);
    assert_eq!(competing_adopted.classification, "orphaned");
    assert_eq!(
        competing_adopted.reason,
        "same_slot_block_has_stronger_external_adoption"
    );

    let aged_without_progress = classify(
        produced.clone(),
        facts()
            .with_reference()
            .with_parent_seen(true)
            .with_as_of(produced.produced_at_ms + ADOPTION_SETTLE_MS + 1),
    );
    assert_eq!(aged_without_progress.classification, "likely_main_chain");
    assert!(!aged_without_progress.placement_finalized);

    let chain_progress_orphaned = classify(
        produced.clone(),
        facts()
            .with_reference()
            .with_parent_seen(true)
            .with_settlement_height(44),
    );
    assert_eq!(chain_progress_orphaned.classification, "orphaned");
    assert_eq!(
        chain_progress_orphaned.reason,
        "chain_advanced_without_external_adoption"
    );
    assert!(chain_progress_orphaned.placement_finalized);
    assert_eq!(
        chain_progress_orphaned.finalization_reason.as_deref(),
        Some("chain_progress")
    );

    let stale_parent = classify(
        produced.clone(),
        facts().with_reference_height(44).with_parent_seen(true),
    );
    assert_eq!(stale_parent.classification, "stale_parent");

    let missing_reference = classify(produced, facts());
    assert_eq!(missing_reference.classification, "unknown");
    assert_eq!(missing_reference.reason, "missing_reference_tip");
}

#[test]
fn builds_a_stable_finalized_key() {
    assert_eq!(
        finalized_key("sender", "run", 91),
        "chain_placement:produced_block_chain_placement:sender:run:91"
    );
}

// ---------------------------------------------------------------------------
// The projection

fn block(hash: &str, parent_hash: &str, height: i64) -> ChainBlockContextObservation {
    ChainBlockContextObservation {
        block_hash: hash.to_owned(),
        parent_hash: Some(parent_hash.to_owned()),
        height,
        global_slot: height + 1_000,
    }
}

fn record_from_run(
    sender: &str,
    run_id: &str,
    event_id: u64,
    timestamp_ms: i64,
    event: Event,
) -> Record {
    Record {
        sender: sender.to_owned(),
        run_id: run_id.to_owned(),
        event_id,
        timestamp_ms,
        event,
    }
}

fn record_from(sender: &str, event_id: u64, timestamp_ms: i64, event: Event) -> Record {
    record_from_run(sender, "run", event_id, timestamp_ms, event)
}

fn record(event_id: u64, timestamp_ms: i64, event: Event) -> Record {
    record_from("sender", event_id, timestamp_ms, event)
}

fn adopted(hash: &str, parent_hash: &str, height: i64) -> Event {
    Event::TipAdopted(block(hash, parent_hash, height))
}

/// A block injected at the record's time, at slot 1041.
fn produced(timestamp_ms: i64, hash: &str, parent_hash: &str, height: i64) -> Event {
    Event::Produced(ProducedBlockObservation {
        produced_at_ms: timestamp_ms,
        block_hash: hash.to_owned(),
        parent_hash: Some(parent_hash.to_owned()),
        block_height: height,
        global_slot: height + 1_000,
    })
}

fn process(projection: &mut ChainPlacement, record: Record, out: &mut Vec<Output>) {
    projection.process(&record, out);
}

fn status_count(out: &[Output]) -> usize {
    out.iter()
        .filter(|output| matches!(output, Output::Status { .. }))
        .count()
}

fn finalized_count(out: &[Output]) -> usize {
    out.iter()
        .filter(|output| matches!(output, Output::Finalized { .. }))
        .count()
}

#[test]
fn materializes_produced_block_after_chain_progress() {
    let mut projection = ChainPlacement::default();
    let mut out = Vec::new();
    let p = &mut projection;
    process(
        p,
        record(1, 1_000, adopted("parent", "grandparent", 40)),
        &mut out,
    );
    process(
        p,
        record(2, 1_100, produced(1_100, "produced", "parent", 41)),
        &mut out,
    );
    process(
        p,
        record(3, 1_200, adopted("produced", "parent", 41)),
        &mut out,
    );
    process(
        p,
        record(4, 1_300, adopted("settled", "produced", 44)),
        &mut out,
    );
    assert!(finalized_count(&out) > 0);
}

#[test]
fn late_chain_progress_finalizes_produced_block_outside_adoption_window() {
    let mut projection = ChainPlacement::default();
    let mut out = Vec::new();
    let p = &mut projection;
    process(
        p,
        record(1, 1_000, adopted("parent", "grandparent", 40)),
        &mut out,
    );
    process(
        p,
        record(2, 1_100, produced(1_100, "produced", "parent", 41)),
        &mut out,
    );
    out.clear();
    let late = 1_100 + ADOPTION_WINDOW_MS + 1;
    process(
        p,
        record(3, late, adopted("settled", "other-parent", 44)),
        &mut out,
    );
    assert!(finalized_count(&out) > 0);
}

#[test]
fn ignores_unrelated_events() {
    let mut projection = ChainPlacement::default();
    let mut out = Vec::new();
    process(&mut projection, record(1, 1_000, Event::Other), &mut out);
    assert!(out.is_empty());
}

#[test]
fn finalized_produced_block_ignores_later_settlement_progress() {
    let mut projection = ChainPlacement::default();
    let mut out = Vec::new();
    let p = &mut projection;
    process(
        p,
        record(1, 1_000, adopted("parent", "grandparent", 40)),
        &mut out,
    );
    process(
        p,
        record(2, 1_100, produced(1_100, "produced", "parent", 41)),
        &mut out,
    );
    out.clear();

    process(
        p,
        record(3, 1_300, adopted("settled", "produced", 44)),
        &mut out,
    );
    assert_eq!(status_count(&out), 1);
    assert_eq!(finalized_count(&out), 1);
    out.clear();

    let later = 1_100 + ADOPTION_WINDOW_MS + 1;
    process(
        p,
        record(4, later, adopted("later-settled", "settled", 45)),
        &mut out,
    );
    assert_eq!(status_count(&out), 0);
    assert_eq!(finalized_count(&out), 0);
}

#[test]
fn finalized_produced_block_refreshes_late_adoption_in_window_once() {
    let mut projection = ChainPlacement::default();
    let mut out = Vec::new();
    let p = &mut projection;
    process(
        p,
        record_from("producer", 1, 1_000, adopted("parent", "grandparent", 40)),
        &mut out,
    );
    process(
        p,
        record_from(
            "producer",
            2,
            1_100,
            produced(1_100, "produced", "parent", 41),
        ),
        &mut out,
    );
    process(
        p,
        record_from("observer", 3, 1_300, adopted("settled", "produced", 44)),
        &mut out,
    );
    assert_eq!(finalized_count(&out), 1);
    out.clear();

    process(
        p,
        record_from("observer", 4, 1_200, adopted("produced", "parent", 41)),
        &mut out,
    );
    assert_eq!(status_count(&out), 1);
    assert_eq!(finalized_count(&out), 0);
}

#[test]
fn same_sender_different_run_adoption_counts_as_external_instance() {
    let mut projection = ChainPlacement::default();
    let mut out = Vec::new();
    let p = &mut projection;
    let made = produced(1_100, "produced", "parent", 41);
    process(
        p,
        record_from_run("producer", "producer-run", 1, 1_100, made),
        &mut out,
    );
    out.clear();
    let seen = adopted("produced", "parent", 41);
    process(
        p,
        record_from_run("producer", "observer-run", 2, 1_200, seen),
        &mut out,
    );

    let status = out
        .iter()
        .filter_map(|output| match output {
            Output::Status { status, .. } if status.identity.event_id == 1 => Some(status),
            _ => None,
        })
        .next_back()
        .expect("produced block status");
    assert_eq!(status.adoption_sender_count, 1);
    assert!(!status.producer_self_adopted);
    assert_eq!(status.first_adopted_at_ms, Some(1_200));
}

#[test]
fn every_block_of_a_batch_is_finalized_once() {
    let mut projection = ChainPlacement::default();
    let mut records = vec![record_from(
        "observer",
        1,
        1_000,
        adopted("parent", "grandparent", 40),
    )];
    for offset in 0..5 {
        let height = 41 + offset;
        let made = produced(
            1_100 + offset,
            &format!("produced-{height}"),
            "parent",
            height,
        );
        records.push(record_from(
            "producer",
            10 + offset as u64,
            1_100 + offset,
            made,
        ));
    }
    for offset in 0..20 {
        let height = 100 + offset;
        let seen = adopted(&format!("settled-{height}"), "settled-parent", height);
        records.push(record_from(
            "observer",
            100 + offset as u64,
            1_300 + offset,
            seen,
        ));
    }
    let mut out = Vec::new();
    for record in records {
        process(&mut projection, record, &mut out);
    }
    assert_eq!(finalized_count(&out), 5);
}
