//! Generated logs of a chain and its nodes' reports: blocks every 5 seconds,
//! sometimes two competing for a slot, at once, seconds apart or about the
//! adoption window apart, sometimes one built on a stale tip; production
//! reports timed at their production, as a producer sends them, in half the
//! logs, and in the others some timed after it (seconds to minutes) or before
//! it (within the clock skew tolerance or past it), which no producer sends,
//! for the projection's determinism whatever it is fed; nodes with skewed
//! clocks adopting them, some about five minutes late, around the adoption
//! window's end, and restarting under new runs; idle stretches, and jumps of
//! the newest time to the last competing pair's retention edge; stalls after
//! which a block's production report arrives past its retention, settled only
//! by reports kept longer; and reports that arrive late, some minutes late, a
//! few past the retention. The log is in arrival order.
//!
//! Checked on every log:
//! 1. restart equivalence: restarted at the generated points, warmed up in
//!    log order, the projection outputs what the uninterrupted pass did;
//! 2. after every record, each retained block's last status is the status
//!    recomputed from every record so far;
//! 3. the contract: at most one finalized placement per block, and one for
//!    every block the chain progressed past while it was retained, or
//!    arrived settled by a report still kept.
//!
//! "No slot has two blocks finalized canonical" is not part of it: a
//! competitor whose production report arrives late can outrank a block
//! already finalized (`tests/findings.rs`).

use std::collections::{BTreeMap, BTreeSet};

use causet::projection::{Projector, check_incremental, check_restarts};
use causet_test::{Config, Rng, Swarm, check};
use chain_placement::classify::{
    ADOPTION_WINDOW_MS, CLOCK_SKEW_TOLERANCE_MS, CONTEXT_RETENTION_MS, CONTEXT_WINDOW_MS,
    PROGRESS_SETTLE_BLOCKS,
};
use chain_placement::projection::{ChainPlacement, Output};
use chain_placement::record::{
    ChainBlockContextObservation, Event, EventIdentity, ProducedBlockObservation, Record,
};

const START: i64 = 1_000_000_000;
const SLOT_MS: i64 = 5_000;
const MINUTE: i64 = 60_000;

#[derive(Clone, Copy, Debug)]
enum Step {
    /// One to three slots, a block in each.
    Slots,
    /// Two blocks competing for one slot, produced at once, seconds apart,
    /// or about the adoption window apart.
    Compete,
    /// A block built on a stale tip.
    Stale,
    /// No block for a while.
    Idle,
    /// The newest time jumps to the last competing blocks' retention edge.
    Edge,
    /// A node restarts, under a new run.
    Restart,
    /// A block whose production report is held back; two slots settle it;
    /// the chain stalls past its retention, but not past what the reports
    /// that settled it are kept for; then the report arrives.
    Stalled,
}

const STEPS: [(Step, u64); 7] = [
    (Step::Slots, 6),
    (Step::Compete, 2),
    (Step::Stale, 1),
    (Step::Idle, 1),
    (Step::Edge, 1),
    (Step::Restart, 1),
    (Step::Stalled, 1),
];

struct Node {
    name: String,
    run: u32,
    /// Its clock's offset from true time.
    skew: i64,
    next_event: u64,
}

#[derive(Clone)]
struct Block {
    hash: String,
    height: i64,
    slot: i64,
}

/// Two blocks that competed for a slot.
#[derive(Clone)]
struct Competition {
    /// When the first was produced, by true time, and how long after it
    /// the second was.
    at: i64,
    gap: i64,
    /// Their production times as read; the older's as claimed instead,
    /// half the time, for the edges it would have read as given.
    older_ms: i64,
    newer_ms: i64,
    /// When another node adopted the first before its production, if one
    /// did.
    early_ms: Option<i64>,
    /// The tip they were built on.
    parent: Block,
    /// The newer block, and when its production report arrives.
    newer: Block,
    newer_arrival: i64,
}

/// The chain, its nodes, and the reports in flight to the log.
struct Chain {
    /// The world's own stream: timings, delays, who adopts.
    world: Rng,
    nodes: Vec<Node>,
    now: i64,
    slot: i64,
    tip: Block,
    /// The tip's parent, for a block built on a stale tip.
    behind: Block,
    /// Reports, by arrival time then generation order.
    arrivals: Vec<(i64, usize, Record)>,
    /// The last two blocks competing for a slot.
    last_competition: Option<Competition>,
    /// Whether production reports are timed after their production: by up
    /// to 3 s, one in ten minutes later. A producer times its report at the
    /// production; an eighth of the logs.
    lags: bool,
    /// How far before the production it claims a production report can be
    /// timed: within the tolerance in an eighth of the logs, past it in a
    /// quarter.
    lead_limit: i64,
    /// Records to restart just before, where an edge is about to be read.
    restart_before: Vec<EventIdentity>,
    /// Each Edge step's recompute report, and the newer block, which it
    /// must reach.
    edge_recomputes: Vec<(EventIdentity, String)>,
}

impl Chain {
    fn new(rng: &mut Rng) -> Self {
        let mut world = rng.split();
        let nodes = (0..world.between(3, 4))
            .map(|n| Node {
                name: format!("node-{n}"),
                run: 0,
                skew: world.between(0, 2_000) as i64 - 1_000,
                next_event: 0,
            })
            .collect();
        let (lags, lead_limit) = match world.below(8) {
            0..=3 => (false, 0),
            4 => (true, 0),
            5 => (false, CLOCK_SKEW_TOLERANCE_MS),
            _ => (false, 4 * CLOCK_SKEW_TOLERANCE_MS),
        };
        let genesis = Block {
            hash: "genesis".to_owned(),
            height: 0,
            slot: 0,
        };
        Self {
            world,
            nodes,
            now: START,
            slot: 0,
            tip: genesis.clone(),
            behind: genesis,
            arrivals: Vec::new(),
            last_competition: None,
            lags,
            lead_limit,
            restart_before: Vec::new(),
            edge_recomputes: Vec::new(),
        }
    }

    /// A node's report of `event`, `after` ms from now by true time, sent to
    /// the log with a delay.
    fn report(&mut self, node: usize, after: i64, event: Event) {
        let delay = if self.world.chance(1, 30) {
            15 * MINUTE + self.world.below(15 * MINUTE as u64) as i64
        } else if self.world.chance(1, 10) {
            MINUTE + self.world.below(9 * MINUTE as u64) as i64
        } else {
            self.world.below(5_000) as i64
        };
        let at = self.now + after;
        self.report_arriving(node, at, at + delay, event);
    }

    /// A node's report of `event` at true time `at`, arriving at `arrival`.
    fn report_arriving(
        &mut self,
        node: usize,
        at: i64,
        arrival: i64,
        event: Event,
    ) -> EventIdentity {
        let node = &mut self.nodes[node];
        node.next_event += 1;
        let record = Record {
            sender: node.name.clone(),
            run_id: format!("run-{}", node.run),
            event_id: node.next_event,
            timestamp_ms: at + node.skew,
            event,
        };
        let order = self.arrivals.len();
        let identity = record.identity();
        self.arrivals.push((arrival, order, record));
        identity
    }

    /// `producer` produces a block on `parent` in the current slot, `after`
    /// ms from now, and adopts it itself. Its report is timed at the
    /// production, or after it in a log with lags; in a log with leads, one
    /// report in three is timed up
    /// to the lead limit before it, and with `past`, one in two past the
    /// tolerance. Returns the block, and its production time as the
    /// projection reads it and as claimed.
    fn produce(
        &mut self,
        producer: usize,
        parent: &Block,
        after: i64,
        past: bool,
    ) -> (Block, i64, i64) {
        let block = Block {
            hash: format!("h{}s{}n{producer}", parent.height + 1, self.slot),
            height: parent.height + 1,
            slot: self.slot,
        };
        let produced = ProducedBlockObservation {
            produced_at_ms: self.now + after + self.nodes[producer].skew,
            block_hash: block.hash.clone(),
            parent_hash: Some(parent.hash.clone()),
            block_height: block.height,
            global_slot: block.slot,
        };
        let tolerance = CLOCK_SKEW_TOLERANCE_MS;
        let lead = if self.lags {
            if self.world.chance(1, 10) {
                -(MINUTE + self.world.below(4 * MINUTE as u64) as i64)
            } else {
                -(self.world.below(3_000) as i64)
            }
        } else if past && self.lead_limit > tolerance && self.world.chance(1, 2) {
            tolerance + 1 + self.world.below((self.lead_limit - tolerance) as u64) as i64
        } else if self.lead_limit > 0 && self.world.chance(1, 3) {
            self.world.below(self.lead_limit as u64 + 1) as i64
        } else {
            0
        };
        let claimed = produced.produced_at_ms;
        let read = claimed.min(claimed - lead + tolerance);
        self.report(producer, after - lead, Event::Produced(produced));
        self.report(producer, after + 10, adopted(&block, parent));
        (block, read, claimed)
    }

    /// Every node but the producers adopts one of `blocks` after a short
    /// while; one in ten misses it, and one in twenty, coming back online,
    /// adopts it about five minutes later, within seconds of the adoption
    /// window's end, where competing blocks' windows differ.
    /// Each block comes with how long after now it was produced.
    fn adopt(&mut self, blocks: &[(Block, Block, i64)], producers: &[usize]) {
        for node in 0..self.nodes.len() {
            if producers.contains(&node) || self.world.chance(1, 10) {
                continue;
            }
            let (block, parent, produced) =
                blocks[self.world.below(blocks.len() as u64) as usize].clone();
            let after = if self.world.chance(1, 20) {
                ADOPTION_WINDOW_MS - 3_000 + self.world.below(6_000) as i64
            } else {
                100 + self.world.below(3_000) as i64
            };
            self.report(node, produced + after, adopted(&block, &parent));
        }
    }

    fn next_slot(&mut self) {
        self.now += SLOT_MS;
        self.slot += 1;
    }

    fn producer(&self, rng: &mut Rng) -> usize {
        rng.below(self.nodes.len() as u64) as usize
    }

    fn step(&mut self, rng: &mut Rng, step: Step) {
        match step {
            Step::Slots => {
                for _ in 0..rng.between(1, 3) {
                    self.next_slot();
                    let producer = self.producer(rng);
                    let parent = self.tip.clone();
                    let (block, ..) = self.produce(producer, &parent, 0, false);
                    self.adopt(&[(block.clone(), parent.clone(), 0)], &[producer]);
                    self.behind = parent;
                    self.tip = block;
                }
            }
            Step::Compete => {
                self.next_slot();
                let first = self.producer(rng);
                let second = (first + 1 + rng.below(self.nodes.len() as u64 - 1) as usize)
                    % self.nodes.len();
                let parent = self.tip.clone();
                // Same-slot blocks compete up to the adoption window apart:
                // its edge is drawn too, a few seconds either side, or
                // reported up to a second inside it, where the newer is
                // still kept when the older is barely a competitor.
                let (gap, inside) = match self.world.below(8) {
                    0..=2 => (0, None),
                    3 | 4 => (self.world.below(30_000) as i64, None),
                    5 => {
                        let gap = ADOPTION_WINDOW_MS - 5_000 + self.world.below(10_000) as i64;
                        (gap, None)
                    }
                    _ => (0, Some(self.world.below(1_000) as i64)),
                };
                let aimed = inside.is_some();
                let (a, older_ms, claimed) = self.produce(first, &parent, 0, aimed);
                // Inside the window by the times as read, or, half the
                // time, as claimed: the edges of a projection reading a
                // report past the tolerance as given.
                let edge_ms = if rng.chance(1, 2) { claimed } else { older_ms };
                let skew_b = self.nodes[second].skew;
                let gap = inside.map_or(gap, |inside| {
                    edge_ms + ADOPTION_WINDOW_MS - inside - self.now - skew_b
                });
                let (b, newer_ms, _) = self.produce(second, &parent, gap, false);
                // Its production report, pushed before its own adoption.
                let newer_arrival = self.arrivals[self.arrivals.len() - 2].0;
                // Half the time, and always just inside the window, another
                // node adopts the first up to the tolerance before its
                // production as read, as its clock reads.
                let others: Vec<usize> = (0..self.nodes.len())
                    .filter(|node| *node != first && *node != second)
                    .collect();
                let mut early_ms = None;
                if !others.is_empty() && (aimed || self.world.chance(1, 2)) {
                    let node = others[self.world.below(others.len() as u64) as usize];
                    let timed = older_ms - 1 - self.world.below(1_900) as i64;
                    let at = timed - self.nodes[node].skew;
                    let arrival = at + self.world.below(5_000) as i64;
                    self.report_arriving(node, at, arrival, adopted(&a, &parent));
                    early_ms = Some(timed);
                }
                self.adopt(
                    &[
                        (a.clone(), parent.clone(), 0),
                        (b.clone(), parent.clone(), gap),
                    ],
                    &[first, second],
                );
                self.last_competition = Some(Competition {
                    at: self.now,
                    gap,
                    older_ms: edge_ms,
                    newer_ms,
                    early_ms,
                    parent: parent.clone(),
                    newer: b.clone(),
                    newer_arrival,
                });
                // The next block decides; the chain goes on from the winner.
                self.behind = parent;
                self.tip = if rng.chance(1, 2) { b } else { a };
                // Half the ones just inside the window go straight to their
                // competitor edge, before later steps take the newest time
                // past it.
                if aimed && rng.chance(1, 2) {
                    self.edge(rng, true);
                }
            }
            Step::Stale => {
                self.next_slot();
                let producer = self.producer(rng);
                let parent = self.behind.clone();
                self.produce(producer, &parent, 0, false);
            }
            Step::Idle => self.now += rng.between(1, 30) as i64 * MINUTE,
            // The newest event time moves to an edge of the last competing
            // blocks' retention: between their two production times (give
            // or take a second or two), where one is no longer kept and the
            // other still is; or up to four seconds inside where the older
            // stops being the newer's competitor, the newer still kept.
            // Then a report from their slot arrives, which recomputes the
            // one kept, and half the time the projection restarts just
            // before it. A random idle stretch would rarely land there.
            Step::Edge => {
                let competitor = rng.chance(1, 2);
                self.edge(rng, competitor);
            }
            Step::Restart => {
                let node = self.producer(rng);
                self.nodes[node].run += 1;
                self.nodes[node].next_event = 0;
            }
            // Only the reports before the block's own, past the blocks'
            // retention but still kept, settle it (without this step, nothing
            // generated needed the contract to look that far).
            Step::Stalled => {
                self.next_slot();
                let producer = self.producer(rng);
                let parent = self.tip.clone();
                let held = self.arrivals.len();
                let (block, ..) = self.produce(producer, &parent, 0, false);
                self.adopt(&[(block.clone(), parent.clone(), 0)], &[producer]);
                self.behind = parent;
                self.tip = block;
                for _ in 0..PROGRESS_SETTLE_BLOCKS {
                    self.next_slot();
                    let producer = self.producer(rng);
                    let parent = self.tip.clone();
                    let (block, ..) = self.produce(producer, &parent, 0, false);
                    self.adopt(&[(block.clone(), parent.clone(), 0)], &[producer]);
                    self.behind = parent;
                    self.tip = block;
                }
                self.now += CONTEXT_RETENTION_MS
                    + 1_000
                    + rng.below((REPORTS_REACH_MS - 5_000) as u64) as i64;
                // A report that settles nothing moves the newest time.
                let node = self.producer(rng);
                let now = self.now;
                self.report_arriving(node, now, now, Event::Other);
                self.arrivals[held].0 = now + 1_000;
                self.now += 1_000 + self.lead_limit;
            }
        }
    }

    /// Moves the newest event time to an edge of the last competition:
    /// `competitor`, where the older stops being the newer's competitor.
    fn edge(&mut self, rng: &mut Rng, competitor: bool) {
        let Some(Competition {
            at,
            gap,
            older_ms,
            newer_ms,
            early_ms,
            parent,
            newer,
            newer_arrival,
        }) = self.last_competition.clone()
        else {
            return;
        };
        let node = self.producer(rng);
        let edge = if !competitor {
            // The cutoff at most at the newer's production as read: it is
            // still kept (with clock skews the range overshoots it by up to
            // about 3.5 s, and without this bound 65 of 1,093 recomputes
            // reached no block kept).
            let drawn = at + CONTEXT_RETENTION_MS - 1_500 + rng.below(gap as u64 + 3_000) as i64;
            drawn.min(newer_ms + CONTEXT_RETENTION_MS - self.nodes[node].skew)
        } else {
            // The newest record's own time, so the cutoff lands there:
            // past the early adoption's reach, when there was one.
            let highest = newer_ms.min(older_ms + ADOPTION_WINDOW_MS);
            let lowest = early_ms.map_or(highest - 2_000, |early| {
                (early + ADOPTION_WINDOW_MS + 1).min(highest)
            });
            let cutoff = lowest + rng.below((highest - lowest) as u64 + 1) as i64;
            cutoff + CONTEXT_RETENTION_MS - self.nodes[node].skew
        };
        // Not before the newer block's production has arrived, nor when a
        // report arriving first, timed later (a lagging one), has already
        // taken the cutoff past it: the recompute could not reach it.
        let kept_until = newer_ms + CONTEXT_RETENTION_MS;
        let overtaken = self.arrivals.iter().any(|(arrival, _, record)| {
            *arrival <= edge + 1_000 && record.timestamp_ms > kept_until
        });
        if edge <= self.now || edge <= newer_arrival || overtaken {
            return;
        }
        self.now = edge;
        // Arriving now, before the recompute below: the cutoff has moved
        // when it is read.
        let (tip, behind) = (self.tip.clone(), self.behind.clone());
        let now = self.now;
        self.report_arriving(node, now, now, adopted(&tip, &behind));
        let late = self.producer(rng);
        let grandparent = Block {
            hash: "earlier".to_owned(),
            ..parent.clone()
        };
        // Timed, by its sender's clock, a second before the newer block's
        // production as read: within the windows it reads, so it is
        // recomputed.
        let timed = newer_ms - 1_000;
        let arrival = self.now + 1_000;
        let at = timed - self.nodes[late].skew;
        let recompute = self.report_arriving(late, at, arrival, adopted(&parent, &grandparent));
        // What comes next arrives after it, even a report timed before its
        // production.
        self.now = arrival + self.lead_limit;
        self.edge_recomputes
            .push((recompute.clone(), newer.hash.clone()));
        if rng.chance(1, 2) {
            self.restart_before.push(recompute);
        }
    }

    /// The log: every report, in arrival order.
    fn log(&mut self) -> Vec<Record> {
        self.arrivals.sort_by_key(|(at, order, _)| (*at, *order));
        std::mem::take(&mut self.arrivals)
            .into_iter()
            .map(|(_, _, record)| record)
            .collect()
    }
}

fn adopted(block: &Block, parent: &Block) -> Event {
    Event::TipAdopted(ChainBlockContextObservation {
        block_hash: block.hash.clone(),
        parent_hash: Some(parent.hash.clone()),
        height: block.height,
        global_slot: block.slot,
    })
}

fn fresh() -> Projector<Record, Output> {
    let mut projector = Projector::new();
    projector.add_projection(ChainPlacement::default());
    projector
}

/// A generated log and its restart points.
fn generate(rng: &mut Rng) -> (Vec<Record>, Vec<usize>) {
    let mut chain = Chain::new(rng);
    let swarm = Swarm::new(rng, &STEPS);
    while rng.more() {
        let step = swarm.pick(rng);
        chain.step(rng, step);
    }
    let log = chain.log();
    let mut restarts: BTreeSet<usize> = (0..rng.below(4))
        .map(|_| rng.below(log.len() as u64 + 1) as usize)
        .collect();
    let aimed = log
        .iter()
        .enumerate()
        .filter(|(_, record)| chain.restart_before.contains(&record.identity()))
        .map(|(position, _)| position);
    restarts.extend(aimed);
    (log, restarts.into_iter().collect())
}

/// Check 2: what the projection stores for each kept block is what every
/// record so far gives: the same projection keeping everything, asked for
/// the block's status from scratch.
fn incremental_equals_scratch(log: &[Record]) {
    let checked = check_incremental(
        ChainPlacement::default(),
        ChainPlacement::unbounded(),
        log,
        |kept, everything, key| {
            kept.status_now(key)?;
            everything.status_now(key)
        },
    );
    if let Err(mismatch) = checked {
        panic!("{mismatch:?}");
    }
}

/// Check 3: the output's contract, on the uninterrupted pass.
fn contract(log: &[Record]) {
    let mut projector = fresh();
    let mut finalized = BTreeMap::new();
    for record in log {
        for output in projector.process(record) {
            let Output::Finalized {
                key,
                finalized: placement,
            } = output
            else {
                continue;
            };
            let placement = placement.chain_placement;
            assert!(
                finalized.insert(key.clone(), placement).is_none(),
                "{key} finalized twice"
            );
        }
    }
    let settled = settled_while_retained(log, REPORTS_REACH_MS);
    for key in &settled {
        assert!(
            finalized.contains_key(key),
            "{key} was settled while retained, and never finalized"
        );
    }
    // And only those (without this, a projection finalizing every block
    // passed).
    for (key, placement) in &finalized {
        assert!(
            placement.placement_finalized && settled.contains(key),
            "{key} was finalized, and never settled while retained"
        );
    }
}

/// How much longer than blocks the projection keeps reports: as far as a
/// kept block reads.
const REPORTS_REACH_MS: i64 = if CONTEXT_WINDOW_MS > ADOPTION_WINDOW_MS + CLOCK_SKEW_TOLERANCE_MS {
    CONTEXT_WINDOW_MS
} else {
    ADOPTION_WINDOW_MS + CLOCK_SKEW_TOLERANCE_MS
};

/// The finalized keys of the blocks the chain progressed past while they
/// were retained: a report two heights above, timed at or after the
/// block, processed while the block was still kept, or kept when the
/// block arrived (reports are kept `reach`, `REPORTS_REACH_MS`, past
/// blocks).
fn settled_while_retained(log: &[Record], reach: i64) -> Vec<String> {
    let mut newest = Vec::with_capacity(log.len());
    let mut max = i64::MIN;
    for record in log {
        max = max.max(record.timestamp_ms);
        newest.push(max);
    }
    let cutoff_before = |position: usize| {
        position
            .checked_sub(1)
            .map_or(i64::MIN, |before| newest[before] - CONTEXT_RETENTION_MS)
    };
    let mut settled = Vec::new();
    for (b, record) in log.iter().enumerate() {
        let Event::Produced(produced) = &record.event else {
            continue;
        };
        // As the projection reads it: no later than the tolerance after its
        // record's own time.
        let produced_at = produced
            .produced_at_ms
            .min(record.timestamp_ms + CLOCK_SKEW_TOLERANCE_MS);
        let settles = log.iter().enumerate().any(|(c, context)| {
            let Event::TipAdopted(tip) = &context.event else {
                return false;
            };
            tip.height >= produced.block_height + PROGRESS_SETTLE_BLOCKS
                && context.timestamp_ms >= produced_at
                && if c > b {
                    produced_at >= cutoff_before(c)
                } else {
                    context.timestamp_ms >= cutoff_before(b) - reach
                }
        });
        if settles {
            let id = record.identity();
            settled.push(chain_placement::classify::finalized_key(
                &id.sender,
                &id.run_id,
                id.event_id,
            ));
        }
    }
    settled
}

fn generated(rng: &mut Rng) {
    let (log, restarts) = generate(rng);
    if let Err(difference) = check_restarts(fresh, &log, &restarts) {
        panic!("{difference:?}");
    }
    incremental_equals_scratch(&log);
    contract(&log);
}

#[test]
fn generated_logs_keep_restart_equivalence_and_the_contract() {
    check(
        Config {
            runs: 2_000,
            ..Config::default()
        },
        generated,
    );
}

/// The generator itself: every Edge step's recompute reaches the newer
/// block, read from the log at the recompute: arrived, still kept, and
/// within the windows the recompute marks (timed from the older block, one
/// in eleven was not within them; and a check that only restated the timing
/// let 151 of 1,431 reach a block evicted or not yet arrived). The windows hold by how the recompute is
/// timed; arrived and kept are what this tests.
#[test]
fn every_edge_recompute_reaches_the_kept_block() {
    let (mut edges, mut missed) = (0, 0);
    for seed in 0..2_000 {
        let mut rng = Rng::new(seed);
        let mut chain = Chain::new(&mut rng);
        let swarm = Swarm::new(&mut rng, &STEPS);
        while rng.more() {
            let step = swarm.pick(&mut rng);
            chain.step(&mut rng, step);
        }
        let recomputes = std::mem::take(&mut chain.edge_recomputes);
        let log = chain.log();
        for (identity, newer) in &recomputes {
            edges += 1;
            let r = log
                .iter()
                .position(|record| record.identity() == *identity)
                .expect("the recompute is in the log");
            let newest = log[..=r]
                .iter()
                .map(|record| record.timestamp_ms)
                .max()
                .unwrap();
            let timed = log[r].timestamp_ms;
            let reached = log[..r].iter().any(|record| {
                let Event::Produced(produced) = &record.event else {
                    return false;
                };
                let read = produced
                    .produced_at_ms
                    .min(record.timestamp_ms + CLOCK_SKEW_TOLERANCE_MS);
                produced.block_hash == *newer
                    && read >= newest - CONTEXT_RETENTION_MS
                    && read >= timed - ADOPTION_WINDOW_MS
                    && read <= timed + CONTEXT_WINDOW_MS
            });
            missed += usize::from(!reached);
        }
    }
    assert!(edges > 1_000, "only {edges} Edge steps");
    assert_eq!(
        missed, 0,
        "{missed} of {edges} Edge recomputes miss the newer block"
    );
}

/// The generator itself: an Edge whose newer block a report arriving
/// first, timed later, has already evicted is skipped (that report comes
/// about once in 300,000 logs, so no run of seeds would see the skip
/// removed).
#[test]
fn an_edge_past_an_overtaking_report_is_skipped() {
    for overtaking in [false, true] {
        let mut rng = Rng::new(0);
        let mut chain = Chain::new(&mut rng);
        let parent = chain.tip.clone();
        let newer = Block {
            hash: "newer".to_owned(),
            height: 1,
            slot: 1,
        };
        chain.last_competition = Some(Competition {
            at: START,
            gap: 1_000,
            older_ms: START,
            newer_ms: START + 1_000,
            early_ms: None,
            parent: parent.clone(),
            newer,
            newer_arrival: START + 1_000,
        });
        if overtaking {
            let past = START + 1_000 + CONTEXT_RETENTION_MS + 1 - chain.nodes[0].skew;
            chain.report_arriving(0, past, START + 2_000, adopted(&parent, &parent));
        }
        chain.edge(&mut rng, false);
        assert_eq!(chain.edge_recomputes.len(), usize::from(!overtaking));
    }
}

/// The generator itself: Stalled steps make blocks that only reports kept
/// past the blocks' retention settle, which the contract must then require
/// (without them no generated log had one, and only the hand-written test
/// below tested the contract's reach).
#[test]
fn stalled_blocks_are_settled_only_by_reports_kept_longer() {
    let mut reached = 0;
    for seed in 0..1_000 {
        let mut rng = Rng::new(seed);
        let (log, _) = generate(&mut rng);
        let within = settled_while_retained(&log, 0).len();
        let kept = settled_while_retained(&log, REPORTS_REACH_MS).len();
        reached += usize::from(kept > within);
    }
    // Stalled is on in half the swarms, and then about one step in five:
    // some 1,600 Stalled steps in 1,000 logs.
    assert!(
        reached >= 200,
        "only {reached} of 1,000 logs need the reach"
    );
}

/// The contract model: a block arriving past retention, settled by a report
/// still kept, is finalized (a model that asked it only of reports within
/// the blocks' retention missed it).
#[test]
fn the_contract_requires_a_block_settled_by_a_report_still_kept() {
    let record = |sender: &str, timestamp_ms: i64, event: Event| Record {
        sender: sender.to_owned(),
        run_id: "run".to_owned(),
        event_id: 1,
        timestamp_ms,
        event,
    };
    let tip = ChainBlockContextObservation {
        block_hash: "h3".to_owned(),
        parent_hash: Some("h2".to_owned()),
        height: 3,
        global_slot: 3,
    };
    let block = ProducedBlockObservation {
        produced_at_ms: 0,
        block_hash: "h1".to_owned(),
        parent_hash: Some("h0".to_owned()),
        block_height: 1,
        global_slot: 1,
    };
    let log = [
        record("observer", 50_000, Event::TipAdopted(tip)),
        record("clock", CONTEXT_RETENTION_MS + 100_000, Event::Other),
        record("producer", 0, Event::Produced(block)),
    ];
    let mut projector = fresh();
    let finalized: Vec<String> = log
        .iter()
        .flat_map(|record| projector.process(record))
        .filter_map(|output| match output {
            Output::Finalized { key, .. } => Some(key),
            Output::Status { .. } => None,
        })
        .collect();
    let key = chain_placement::classify::finalized_key("producer", "run", 1);
    assert_eq!(
        finalized,
        std::slice::from_ref(&key),
        "the projection finalizes it"
    );
    assert!(settled_while_retained(&log, REPORTS_REACH_MS).contains(&key));
}
