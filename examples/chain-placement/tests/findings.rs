//! What generation found in chain placement, each as the shortest story that
//! shows it.

use causet::projection::{Projection, Projector, check_incremental, check_restarts};
use chain_placement::classify::CONTEXT_RETENTION_MS;
use chain_placement::projection::{ChainPlacement, Output, Status};
use chain_placement::record::{
    ChainBlockContextObservation, Event, ProducedBlockObservation, Record,
};

fn record(sender: &str, event_id: u64, timestamp_ms: i64, event: Event) -> Record {
    Record {
        sender: sender.to_owned(),
        run_id: "run".to_owned(),
        event_id,
        timestamp_ms,
        event,
    }
}

fn adopted(hash: &str, parent_hash: &str, height: i64) -> Event {
    Event::TipAdopted(ChainBlockContextObservation {
        block_hash: hash.to_owned(),
        parent_hash: Some(parent_hash.to_owned()),
        height,
        global_slot: height,
    })
}

fn produced(at: i64, hash: &str, parent_hash: &str, height: i64) -> Event {
    Event::Produced(ProducedBlockObservation {
        produced_at_ms: at,
        block_hash: hash.to_owned(),
        parent_hash: Some(parent_hash.to_owned()),
        block_height: height,
        global_slot: height,
    })
}

/// The canonical placements finalized, by block.
fn finalized_canonical(out: &[Output]) -> Vec<String> {
    out.iter()
        .filter_map(|output| match output {
            Output::Finalized { finalized, .. } if finalized.chain_placement.canonical => {
                Some(finalized.chain_placement.block_hash.clone())
            }
            _ => None,
        })
        .collect()
}

/// Finality comes from chain progress, but a slot's ranking can still
/// change after it: a competitor whose production report arrives late, with
/// a later external adoption, is finalized canonical beside the block
/// already finalized canonical for the same slot. The original behaves so;
/// this pins it.
#[test]
fn a_late_production_report_finalizes_a_second_canonical_block_for_its_slot() {
    let mut projection = ChainPlacement::default();
    let mut out = Vec::new();
    let log = [
        record("a", 1, 1_000, adopted("parent", "root", 1)),
        // B is produced, adopted by an observer, and settled by progress.
        record("b", 1, 5_000, produced(5_000, "B", "parent", 2)),
        record("observer", 1, 5_500, adopted("B", "parent", 2)),
        // A's adoption by another observer arrives, later than B's...
        record("watcher", 1, 6_000, adopted("A", "parent", 2)),
        record("observer", 2, 15_000, adopted("next", "B", 4)),
        // ...and A's own production report only now.
        record("a", 2, 4_900, produced(4_900, "A", "parent", 2)),
    ];
    for record in &log {
        projection.process(record, &mut out);
    }
    assert_eq!(finalized_canonical(&out), ["B", "A"]);
}

/// Context is evicted 15 minutes behind the newest event time, but a block
/// kept that long reads context up to 5 minutes before it. When the newest
/// time jumps ahead, a kept block's parent reports are gone; a late, older
/// parent report then recomputes it from less than it had, and its stored
/// status loses the newer report: with the original's eviction, the last
/// status names the report at 1,000. Section 25.7 keeps context as far back
/// as a kept block reads.
#[test]
fn a_kept_block_is_not_recomputed_from_evicted_context() {
    let minute = 60_000;
    let mut projection = ChainPlacement::default();
    let mut out = Vec::new();
    let log = [
        record("a", 1, 0, adopted("parent", "root", 1)),
        record("b", 1, 4_000, adopted("parent", "root", 1)),
        record(
            "a",
            2,
            3 * minute,
            produced(3 * minute, "block", "parent", 2),
        ),
        // The newest time jumps 12 minutes past the block.
        record("c", 1, 15 * minute + 30_000, adopted("far", "elsewhere", 9)),
        // A late and older report of the parent.
        record("c", 2, 1_000, adopted("parent", "root", 1)),
    ];
    for record in &log {
        projection.process(record, &mut out);
    }
    let parent_seen_at: Vec<Option<i64>> = out
        .iter()
        .filter_map(|output| match output {
            Output::Status { status, .. } if status.produced.block_hash == "block" => {
                Some(status.parent_seen_at_ms)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        parent_seen_at.last(),
        Some(&Some(4_000)),
        "the newest parent report stays the block's: {parent_seen_at:?}"
    );
}

/// What the projection stores agrees with every record so far.
fn agrees_with_every_record(log: &[Record]) {
    let checked = check_incremental(
        ChainPlacement::default(),
        ChainPlacement::unbounded(),
        log,
        |kept, everything, key| {
            kept.status_now(key)?;
            everything.status_now(key)
        },
    );
    assert!(checked.is_ok(), "{checked:?}");
}

fn competing(at: i64, hash: &str) -> Event {
    Event::Produced(ProducedBlockObservation {
        produced_at_ms: at,
        block_hash: hash.to_owned(),
        parent_hash: Some("P".to_owned()),
        block_height: 2,
        global_slot: 2,
    })
}

/// Regression: a kept block's competitors are part of what it reads.
/// A, produced a second before B, leaves the kept blocks first; a later
/// recompute of B must still count it.
#[test]
fn a_kept_block_keeps_the_competitor_its_rank_depends_on() {
    agrees_with_every_record(&[
        record("producer-a", 1, 0, competing(0, "A")),
        record("producer-b", 1, 1_000, competing(1_000, "B")),
        record("clock", 1, CONTEXT_RETENTION_MS + 500, Event::Other),
        record("observer", 1, 500, adopted("P", "root", 1)),
    ]);
}

/// Regression: an adoption of A, inside A's adoption window but past
/// B's marking window, changes B's rank: B is recomputed too.
#[test]
fn adopting_a_block_recomputes_the_other_blocks_of_its_slot() {
    agrees_with_every_record(&[
        record("producer-b", 1, 0, competing(0, "B")),
        record("producer-a", 1, 1_000, competing(1_000, "A")),
        record("observer", 1, 300_500, adopted("A", "P", 2)),
    ]);
}

/// Regression: competitors up to five minutes apart compete. A,
/// five minutes older than B, leaves the kept blocks first; B, still kept,
/// must keep counting it. (Generation made competitors at most a couple of
/// seconds apart, so it passed with them kept 3 seconds.)
#[test]
fn a_competitor_five_minutes_older_stays_a_competitor() {
    agrees_with_every_record(&[
        record("producer-a", 1, 0, competing(0, "A")),
        record("producer-b", 1, 300_000, competing(300_000, "B")),
        record("clock", 1, CONTEXT_RETENTION_MS + 200_000, Event::Other),
        record("observer", 1, 299_000, adopted("P", "root", 1)),
    ]);
}

/// Regression: a competitor five minutes older than a kept block,
/// adopted a second before its own production, within the clock skew: that
/// adoption is still read, so it is kept the skew longer than the window.
#[test]
fn a_competitor_s_early_adoption_stays_in_context() {
    agrees_with_every_record(&[
        record("producer-a", 1, 10_000, competing(10_000, "A")),
        record("producer-b", 1, 310_000, competing(310_000, "B")),
        record("observer", 1, 9_000, adopted("A", "P", 2)),
        record("clock", 1, CONTEXT_RETENTION_MS + 310_000, Event::Other),
        record("observer", 2, 309_000, adopted("P", "root", 1)),
    ]);
}

/// Regression: competitors ten seconds apart, and an adoption of A
/// at the end of A's window, nine seconds past B's: B is recomputed.
#[test]
fn adopting_a_competitor_ten_seconds_newer_recomputes_the_older() {
    agrees_with_every_record(&[
        record("producer-b", 1, 0, competing(0, "B")),
        record("producer-a", 1, 10_000, competing(10_000, "A")),
        record("observer", 1, 309_000, adopted("A", "P", 2)),
    ]);
}

/// Regression: a production report timed three seconds before the
/// production it claims, past the skew tolerance. Blocks were kept by
/// production time and replayed by record time, so a warm-up missed A, and
/// after a restart B's next status no longer counted it. A production is now
/// read as no later than the tolerance after its report's own time: A as
/// produced at 9,000, 301 seconds before B, so it competes with B in neither
/// pass: the story above no longer happens, which is the fix.
#[test]
fn a_production_claimed_after_its_report_competes_alike_after_a_restart() {
    let log = [
        record("producer-a", 1, 7_000, competing(10_000, "A")),
        record("producer-b", 1, 310_000, competing(310_000, "B")),
        record("clock", 1, CONTEXT_RETENTION_MS + 310_000, Event::Other),
        record("observer", 1, 309_000, adopted("P", "root", 1)),
    ];
    let fresh = || {
        let mut projector = Projector::new();
        projector.add_projection(ChainPlacement::default());
        projector
    };
    let restarted = check_restarts(fresh, &log, &[3]);
    assert!(restarted.is_ok(), "{restarted:?}");
    let b = last_status(&log, "B");
    assert_eq!(b.slot_block_count, 1, "A, read at 9,000, does not compete");
}

/// The status a block was last output with, over `log`.
fn last_status(log: &[Record], hash: &str) -> Status {
    let mut projection = ChainPlacement::default();
    let mut out = Vec::new();
    for record in log {
        projection.process(record, &mut out);
    }
    out.into_iter()
        .rev()
        .find_map(|output| match output {
            Output::Status { status, .. } if status.produced.block_hash == hash => Some(*status),
            _ => None,
        })
        .expect("a status")
}

/// Regression: no test or generated log had a production report
/// timed after its production, so reading such a report differently passed
/// every suite. (A producer times its report at the production; one timed
/// later is no more than a projection must read alike.) Injected at 1,000,
/// reported a minute later, adopted at 1,500: read at its production.
#[test]
fn a_production_reported_a_minute_late_is_read_at_its_production() {
    let log = [
        record("observer", 1, 1_500, adopted("A", "P", 2)),
        record("producer", 1, 61_000, competing(1_000, "A")),
    ];
    let status = last_status(&log, "A");
    assert_eq!(status.produced.produced_at_ms, 1_000);
    assert_eq!(status.adoption_sender_count, 1);
    assert_eq!(status.first_adopted_at_ms, Some(1_500));
}
