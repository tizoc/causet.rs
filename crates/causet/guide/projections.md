The causet guide's Rust examples for its projections page, run as doc tests
(`cargo test -p causet --doc`). The guide's pages are in its own
repository and show each block here by the name above it.

<!-- snippet: bursts -->
```
use std::collections::BTreeMap;

use causet::projection::{Keyed, LogEntry, Projection, Projector};

const MINUTE: u64 = 60_000;

/// A stored report: a source failed, at an event time.
struct Failed { at: u64, source: &'static str }
impl LogEntry for Failed {
    fn time_ms(&self) -> u64 { self.at }
}

/// A source failed three times or more within a minute.
#[derive(Debug, PartialEq)]
struct Burst { key: String, failures: usize }
impl Keyed for Burst {
    fn key(&self) -> &str { &self.key }
}

#[derive(Default)]
struct Bursts {
    /// The newest event time seen: records can arrive late, so the window
    /// is measured from here, never from the record at hand.
    newest: u64,
    /// Each source's failures within the window.
    recent: BTreeMap<&'static str, Vec<u64>>,
}
impl Projection<Failed> for Bursts {
    type Output = Burst;
    fn name(&self) -> &str { "bursts" }
    fn retention_ms(&self) -> Option<u64> { Some(MINUTE) }
    fn process(&mut self, failed: &Failed, out: &mut Vec<Burst>) {
        self.newest = self.newest.max(failed.at);
        let times = self.recent.entry(failed.source).or_default();
        times.push(failed.at);
        let failures = times.iter().filter(|t| **t <= failed.at && *t + MINUTE >= failed.at).count();
        if failures >= 3 {
            out.push(Burst { key: format!("{}@{}", failed.source, failed.at), failures });
        }
        // Keep only the window: what a warm-up would feed back.
        let horizon = self.newest.saturating_sub(MINUTE);
        for times in self.recent.values_mut() {
            times.retain(|t| *t >= horizon);
        }
        self.recent.retain(|_, times| !times.is_empty());
    }
}

let log: Vec<Failed> = [(0, "db"), (40_000, "db"), (70_000, "db"), (80_000, "cache"), (100_000, "db")]
    .into_iter()
    .map(|(at, source)| Failed { at, source })
    .collect();
let mut projector = Projector::new();
projector.add_projection(Bursts::default());
let out: Vec<Vec<Burst>> = log.iter().map(|failed| projector.process(failed)).collect();
// At 70 s, the failure at 0 is more than a minute old: only at 100 s are
// there three within a minute.
assert_eq!(out, [vec![], vec![], vec![], vec![], vec![Burst { key: "db@100000".into(), failures: 3 }]]);
```

<!-- snippet: warm-up -->
```
# use std::collections::BTreeMap;
# use causet::projection::{Keyed, LogEntry, Projection, Projector};
# const MINUTE: u64 = 60_000;
# struct Failed { at: u64, source: &'static str }
# impl LogEntry for Failed {
#     fn time_ms(&self) -> u64 { self.at }
# }
# #[derive(Debug, PartialEq)]
# struct Burst { key: String, failures: usize }
# impl Keyed for Burst {
#     fn key(&self) -> &str { &self.key }
# }
# #[derive(Default)]
# struct Bursts {
#     newest: u64,
#     recent: BTreeMap<&'static str, Vec<u64>>,
# }
# impl Projection<Failed> for Bursts {
#     type Output = Burst;
#     fn name(&self) -> &str { "bursts" }
#     fn retention_ms(&self) -> Option<u64> { Some(MINUTE) }
#     fn process(&mut self, failed: &Failed, out: &mut Vec<Burst>) {
#         self.newest = self.newest.max(failed.at);
#         let times = self.recent.entry(failed.source).or_default();
#         times.push(failed.at);
#         let failures = times.iter().filter(|t| **t <= failed.at && *t + MINUTE >= failed.at).count();
#         if failures >= 3 {
#             out.push(Burst { key: format!("{}@{}", failed.source, failed.at), failures });
#         }
#         let horizon = self.newest.saturating_sub(MINUTE);
#         for times in self.recent.values_mut() {
#             times.retain(|t| *t >= horizon);
#         }
#         self.recent.retain(|_, times| !times.is_empty());
#     }
# }
# let log: Vec<Failed> = [(0, "db"), (40_000, "db"), (70_000, "db"), (80_000, "cache"), (100_000, "db")]
#     .into_iter()
#     .map(|(at, source)| Failed { at, source })
#     .collect();
// The process restarts with its cursor past the first four records, the
// latest of them at 80 s.
let (stored, next) = (&log[..4], &log[4]);
let mut projector = Projector::new();
projector.add_projection(Bursts::default());
// A minute of context: read the stored log from 20 s on, and feed it back
// in log order. What the projection derives meanwhile is dropped.
let from = projector.warm_up_from(80_000).unwrap();
assert_eq!(from, 20_000);
for failed in stored.iter().filter(|failed| failed.at >= from) {
    projector.warm(failed);
}
// The failures at 40 s and 70 s are back in context.
assert_eq!(projector.process(next), [Burst { key: "db@100000".into(), failures: 3 }]);

// The same, for a log held in memory.
let mut projector = Projector::new();
projector.add_projection(Bursts::default());
projector.warm_up(stored);
assert_eq!(projector.process(next).len(), 1);
```

<!-- snippet: check-restarts -->
```
# use std::collections::BTreeMap;
# use causet::projection::{Keyed, LogEntry, Projection, Projector};
# const MINUTE: u64 = 60_000;
# struct Failed { at: u64, source: &'static str }
# impl LogEntry for Failed {
#     fn time_ms(&self) -> u64 { self.at }
# }
# #[derive(Debug, PartialEq)]
# struct Burst { key: String, failures: usize }
# impl Keyed for Burst {
#     fn key(&self) -> &str { &self.key }
# }
# #[derive(Default)]
# struct Bursts {
#     newest: u64,
#     recent: BTreeMap<&'static str, Vec<u64>>,
# }
# impl Projection<Failed> for Bursts {
#     type Output = Burst;
#     fn name(&self) -> &str { "bursts" }
#     fn retention_ms(&self) -> Option<u64> { Some(MINUTE) }
#     fn process(&mut self, failed: &Failed, out: &mut Vec<Burst>) {
#         self.newest = self.newest.max(failed.at);
#         let times = self.recent.entry(failed.source).or_default();
#         times.push(failed.at);
#         let failures = times.iter().filter(|t| **t <= failed.at && *t + MINUTE >= failed.at).count();
#         if failures >= 3 {
#             out.push(Burst { key: format!("{}@{}", failed.source, failed.at), failures });
#         }
#         let horizon = self.newest.saturating_sub(MINUTE);
#         for times in self.recent.values_mut() {
#             times.retain(|t| *t >= horizon);
#         }
#         self.recent.retain(|_, times| !times.is_empty());
#     }
# }
# let log: Vec<Failed> = [(0, "db"), (40_000, "db"), (70_000, "db"), (80_000, "cache"), (100_000, "db")]
#     .into_iter()
#     .map(|(at, source)| Failed { at, source })
#     .collect();
use causet::projection::{Difference, check_restarts};

/// The planted bug: it reads a minute back, but says it keeps 30 seconds.
#[derive(Default)]
struct Short(Bursts);
impl Projection<Failed> for Short {
    type Output = Burst;
    fn name(&self) -> &str { "short bursts" }
    fn retention_ms(&self) -> Option<u64> { Some(30_000) }
    fn process(&mut self, failed: &Failed, out: &mut Vec<Burst>) { self.0.process(failed, out) }
}

fn fresh<P: Projection<Failed, Output = Burst> + Default + Send + 'static>() -> Projector<Failed, Burst> {
    let mut projector = Projector::new();
    projector.add_projection(P::default());
    projector
}
// Restart before every record but the first, one after another.
let restarts: Vec<usize> = (1..log.len()).collect();
assert_eq!(check_restarts(fresh::<Bursts>, &log, &restarts), Ok(()));
// Restarted at 80 s, the short one warms up from 50 s on, and misses the
// failure at 40 s.
assert_eq!(
    check_restarts(fresh::<Short>, &log, &restarts),
    Err(Difference {
        restart: Some(4),
        record: 4,
        expected: vec![Burst { key: "db@100000".into(), failures: 3 }],
        actual: vec![],
    })
);
```

<!-- snippet: check-incremental -->
```
use std::collections::BTreeMap;

use causet::projection::{Keyed, LogEntry, Projection, check_incremental};

const MINUTE: u64 = 60_000;

struct Seen { at: u64, id: u64 }
impl LogEntry for Seen {
    fn time_ms(&self) -> u64 { self.at }
}

/// How many times an id was seen within a minute of the newest sighting.
#[derive(Clone, Debug, PartialEq)]
struct Recent { key: String, count: usize }
impl Keyed for Recent {
    fn key(&self) -> &str { &self.key }
}

#[derive(Default)]
struct Counts {
    newest: u64,
    seen: BTreeMap<u64, Vec<u64>>,
    /// The planted bug: recomputes only the record's own id.
    careless: bool,
}
impl Counts {
    fn count(&self, id: u64) -> usize {
        self.seen[&id].iter().filter(|at| *at + MINUTE >= self.newest).count()
    }
    fn counts(&self) -> BTreeMap<u64, usize> {
        self.seen.keys().map(|id| (*id, self.count(*id))).collect()
    }
}
impl Projection<Seen> for Counts {
    type Output = Recent;
    fn name(&self) -> &str { "recent" }
    fn process(&mut self, seen: &Seen, out: &mut Vec<Recent>) {
        let before = self.counts();
        self.newest = self.newest.max(seen.at);
        self.seen.entry(seen.id).or_default().push(seen.at);
        // Every id whose count this record changed.
        for (id, count) in self.counts() {
            let judged = !self.careless || id == seen.id;
            if judged && before.get(&id) != Some(&count) {
                out.push(Recent { key: id.to_string(), count });
            }
        }
    }
}

// From scratch: the reference's count for the key.
let recompute = |_: &Counts, reference: &Counts, key: &str| {
    let id = key.parse().unwrap();
    reference.seen.contains_key(&id).then(|| Recent { key: key.to_owned(), count: reference.count(id) })
};
let log: Vec<Seen> = [(0, 1), (10_000, 2), (70_000, 2)].into_iter().map(|(at, id)| Seen { at, id }).collect();

assert_eq!(check_incremental(Counts::default(), Counts::default(), &log, recompute), Ok(()));
let careless = Counts { careless: true, ..Counts::default() };
let mismatch = check_incremental(careless, Counts::default(), &log, recompute).unwrap_err();
// At 70 s, id 1's sighting at 0 is past the minute: it is stored as 1.
assert_eq!((mismatch.record, mismatch.key.as_str(), mismatch.stored.count, mismatch.recomputed.count), (2, "1", 1, 0));
```
