//! Restart equivalence on generated logs: a block adoption projection, with
//! records arriving late and out of order, keeps it; a retention shorter than
//! its context, and a warm-up that re-sorts the log by event time while live
//! processing keeps log order, break it, and the check finds and shrinks
//! each.

use std::collections::BTreeMap;

use causet_projection::{
    Difference, Keyed, LogEntry, Projection, Projector, check_restarts, check_restarts_with,
};
use causet_test::{Config, Rng, search};

/// How long a produced block waits for its adoption, in event time.
const WINDOW: u64 = 50;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Produced,
    Adopted,
}

#[derive(Clone, Debug)]
struct Rec {
    time: u64,
    kind: Kind,
    block: u64,
}

impl LogEntry for Rec {
    fn time_ms(&self) -> u64 {
        self.time
    }
}

/// A produced block's fate.
#[derive(Clone, Debug, PartialEq)]
struct Placement {
    key: String,
    adopted: bool,
}

impl Keyed for Placement {
    fn key(&self) -> &str {
        &self.key
    }
}

/// Places each produced block: adopted if its adoption arrives while it is
/// pending, orphaned once the newest event time seen is a window past it.
struct Adoption {
    retention: u64,
    /// The newest event time seen.
    now: u64,
    pending: BTreeMap<u64, u64>,
}

impl Adoption {
    fn new(retention: u64) -> Self {
        Self {
            retention,
            now: 0,
            pending: BTreeMap::new(),
        }
    }
}

impl Projection<Rec> for Adoption {
    type Output = Placement;

    fn name(&self) -> &str {
        "adoption"
    }

    fn retention_ms(&self) -> Option<u64> {
        Some(self.retention)
    }

    fn process(&mut self, record: &Rec, out: &mut Vec<Placement>) {
        let place = |block: u64, adopted| Placement {
            key: format!("block {block}"),
            adopted,
        };
        self.now = self.now.max(record.time);
        let horizon = self.now.saturating_sub(WINDOW);
        match record.kind {
            // Too old to wait for: never pending.
            Kind::Produced if record.time < horizon => {}
            Kind::Produced => {
                self.pending.insert(record.block, record.time);
            }
            Kind::Adopted => {
                if self.pending.remove(&record.block).is_some() {
                    out.push(place(record.block, true));
                }
            }
        }
        let expired: Vec<u64> = self
            .pending
            .iter()
            .filter(|&(_, &t)| t < horizon)
            .map(|(&b, _)| b)
            .collect();
        for block in expired {
            self.pending.remove(&block);
            out.push(place(block, false));
        }
    }
}

fn projector(retention: u64) -> Projector<Rec, Placement> {
    let mut projector = Projector::new();
    projector.add_projection(Adoption::new(retention));
    projector
}

/// A log: blocks produced as time goes on, some adopted a little later, each
/// record arriving after a delay, so the log is in arrival order, not event
/// order; and restarts, each at some step's arrival. A step's choices are
/// drawn together, so shrinking that deletes them deletes the
/// step.
fn generate(rng: &mut Rng) -> (Vec<Rec>, Vec<usize>) {
    let (mut time, mut next, mut produced) = (0, 0, Vec::new());
    let (mut arrivals, mut restart_times) = (Vec::new(), Vec::new());
    while rng.more() {
        time += rng.below(30);
        let record = if produced.is_empty() || !rng.chance(1, 2) {
            produced.push(next);
            next += 1;
            Rec {
                time,
                kind: Kind::Produced,
                block: next - 1,
            }
        } else {
            // Counted back from the newest, so deleting an older step while
            // shrinking does not change which block this adopts.
            let block = produced[produced.len() - 1 - rng.below(produced.len() as u64) as usize];
            Rec {
                time,
                kind: Kind::Adopted,
                block,
            }
        };
        let arrival = time + rng.below(40);
        if rng.chance(1, 4) {
            restart_times.push(arrival);
        }
        arrivals.push((arrival, arrivals.len(), record));
    }
    arrivals.sort_by_key(|&(arrival, step, _)| (arrival, step));
    // A restart at time t: before the first record arriving at t or later.
    let restarts = restart_times
        .iter()
        .map(|&t| arrivals.iter().take_while(|(a, _, _)| *a < t).count())
        .collect();
    (
        arrivals.into_iter().map(|(_, _, record)| record).collect(),
        restarts,
    )
}

/// The log a list of choices makes.
fn log_of(choices: &[u64]) -> Vec<Rec> {
    let mut log = Vec::new();
    causet_test::replay(choices, |rng| log = generate(rng).0);
    log
}

fn quick() -> Config {
    Config {
        runs: 300,
        ..Config::default()
    }
}

#[test]
fn a_processor_whose_retention_covers_its_context_keeps_restart_equivalence() {
    causet_test::check(quick(), |rng| {
        let (log, restarts) = generate(rng);
        check_restarts(|| projector(WINDOW), &log, &restarts).unwrap();
    });
}

#[test]
fn a_retention_shorter_than_the_context_is_found() {
    let found = search(quick(), |rng| {
        let (log, restarts) = generate(rng);
        if let Err(difference) = check_restarts(|| projector(WINDOW / 2), &log, &restarts) {
            panic!("{difference:?}");
        }
    })
    .expect("a short retention breaks a restart");
    // Shrunk to a few records and a restart.
    assert!(
        log_of(&found.choices).len() <= 4,
        "{found:?}: {:?}",
        log_of(&found.choices)
    );
}

#[test]
fn a_warm_up_sorted_by_event_time_is_found() {
    // The warm-up reads the stored log in (timestamp, row) order while live
    // processing reads it in row order: a different order.
    let sorted_warm_up = |prefix: &[Rec]| {
        let mut by_time: Vec<Rec> = prefix.to_vec();
        by_time.sort_by_key(|r| r.time);
        let mut projector = projector(WINDOW);
        projector.warm_up(&by_time);
        projector
    };
    let found = search(quick(), |rng| {
        let (log, restarts) = generate(rng);
        if let Err(difference) =
            check_restarts_with(|| projector(WINDOW), sorted_warm_up, &log, &restarts)
        {
            panic!("{difference:?}");
        }
    })
    .expect("re-sorting the warm-up breaks a restart");
    assert!(
        log_of(&found.choices).len() <= 4,
        "{found:?}: {:?}",
        log_of(&found.choices)
    );
}

// ---------------------------------------------------------------------------
// The projector's own contracts

/// Outputs one placement per record, keyed by the projection and the block.
struct Echo(&'static str, Option<u64>);

impl Projection<Rec> for Echo {
    type Output = Placement;
    fn name(&self) -> &str {
        self.0
    }
    fn retention_ms(&self) -> Option<u64> {
        self.1
    }
    fn process(&mut self, record: &Rec, out: &mut Vec<Placement>) {
        out.push(Placement {
            key: format!("{} {}", self.0, record.block),
            adopted: true,
        });
    }
}

fn rec(time: u64, block: u64) -> Rec {
    Rec {
        time,
        kind: Kind::Produced,
        block,
    }
}

#[test]
fn processors_run_in_the_order_added() {
    let mut projector = Projector::new();
    projector.add_projection(Echo("first", None));
    projector.add_projection(Echo("second", None));
    let keys: Vec<String> = projector
        .process(&rec(0, 7))
        .into_iter()
        .map(|p| p.key)
        .collect();
    assert_eq!(keys, ["first 7", "second 7"]);
    assert_eq!(projector.names(), ["first", "second"]);
}

#[test]
fn each_processor_warms_up_on_its_own_window_and_outputs_nothing() {
    use std::sync::{Arc, Mutex};

    /// Records the blocks it is fed.
    struct Fed(&'static str, u64, Arc<Mutex<Vec<(&'static str, u64)>>>);
    impl Projection<Rec> for Fed {
        type Output = Placement;
        fn name(&self) -> &str {
            self.0
        }
        fn retention_ms(&self) -> Option<u64> {
            Some(self.1)
        }
        fn process(&mut self, record: &Rec, out: &mut Vec<Placement>) {
            self.2.lock().unwrap().push((self.0, record.block));
            out.push(Placement {
                key: String::new(),
                adopted: true,
            });
        }
    }

    let fed = Arc::new(Mutex::new(Vec::new()));
    let mut projector = Projector::new();
    projector.add_projection(Fed("short", 10, Arc::clone(&fed)));
    projector.add_projection(Fed("long", 100, Arc::clone(&fed)));
    projector.add_projection(Echo("none", None));
    // Log order, not time order: 95 arrived before 80.
    let log = [rec(0, 0), rec(95, 1), rec(80, 2), rec(100, 3)];
    assert_eq!(
        projector.warm_up_from(100),
        Some(0),
        "the longest window starts at 0"
    );
    for record in &log {
        projector.warm(record);
    }
    assert_eq!(
        *fed.lock().unwrap(),
        [
            ("long", 0),
            ("short", 1),
            ("long", 1),
            ("long", 2),
            ("short", 3),
            ("long", 3)
        ],
        "short sees 90..; long sees all; none, without context, nothing"
    );
}

#[test]
fn a_difference_without_a_restart_means_the_processor_is_not_deterministic() {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Outputs a number that differs between instances.
    struct Counter(u64);
    impl Projection<Rec> for Counter {
        type Output = Placement;
        fn name(&self) -> &str {
            "counter"
        }
        fn process(&mut self, _: &Rec, out: &mut Vec<Placement>) {
            out.push(Placement {
                key: self.0.to_string(),
                adopted: true,
            });
        }
    }
    static INSTANCES: AtomicU64 = AtomicU64::new(0);
    let fresh = || {
        let mut projector = Projector::new();
        projector.add_projection(Counter(INSTANCES.fetch_add(1, Ordering::Relaxed)));
        projector
    };
    let Err(Difference {
        restart, record, ..
    }) = check_restarts(fresh, &[rec(0, 0)], &[])
    else {
        panic!("two instances differ");
    };
    assert_eq!((restart, record), (None, 0));
}

// ---------------------------------------------------------------------------
// The crate's documented example, under generated logs: its hand-picked logs
// missed a late record that broke it.

const MINUTE: u64 = 60_000;

struct Seen {
    at: u64,
    id: u64,
}

impl LogEntry for Seen {
    fn time_ms(&self) -> u64 {
        self.at
    }
}

#[derive(Debug, PartialEq)]
struct Repeat(String);

impl Keyed for Repeat {
    fn key(&self) -> &str {
        &self.0
    }
}

/// As in the crate's documentation.
#[derive(Default)]
struct Repeats {
    watermark: u64,
    last: BTreeMap<u64, u64>,
}

impl Projection<Seen> for Repeats {
    type Output = Repeat;
    fn name(&self) -> &str {
        "repeats"
    }
    fn retention_ms(&self) -> Option<u64> {
        Some(MINUTE)
    }
    fn process(&mut self, seen: &Seen, out: &mut Vec<Repeat>) {
        self.watermark = self.watermark.max(seen.at);
        let horizon = self.watermark.saturating_sub(MINUTE);
        if seen.at < horizon {
            return;
        }
        if let Some(&before) = self.last.get(&seen.id)
            && seen.at.abs_diff(before) <= MINUTE
        {
            out.push(Repeat(format!("{}@{}", seen.id, seen.at)));
        }
        let newest = self.last.entry(seen.id).or_default();
        *newest = (*newest).max(seen.at);
        self.last.retain(|_, at| *at >= horizon);
    }
}

#[test]
fn the_documented_example_keeps_restart_equivalence_with_late_records() {
    causet_test::check(quick(), |rng| {
        let mut time = 0;
        let (mut log, mut restarts) = (Vec::new(), Vec::new());
        while rng.more() {
            time += rng.below(40_000);
            // Some records arrive long after their time.
            let at = if rng.chance(1, 4) {
                time.saturating_sub(rng.below(3 * MINUTE))
            } else {
                time
            };
            log.push(Seen {
                at,
                id: rng.below(3),
            });
            if rng.chance(1, 3) {
                restarts.push(log.len());
            }
        }
        let fresh = || {
            let mut projector = Projector::new();
            projector.add_projection(Repeats::default());
            projector
        };
        if let Err(difference) = check_restarts(fresh, &log, &restarts) {
            panic!("{difference:?}");
        }
    });
}
