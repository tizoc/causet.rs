//! Projections of an ordered log of records into derived facts: the receiving
//! side of observability, where stored reports are turned into the facts an
//! application queries or alerts on.
//!
//! - A [`Projection`] folds records, in log order, into outputs, each with a
//!   key ([`Keyed`]), keeping recent context bounded by a retention window of
//!   event time. It follows the determinism guide: ordered collections, no
//!   I/O, no wall clock; time is the records' own ([`LogEntry`]).
//! - A [`Projector`] feeds each record to its projections, and after a restart
//!   warms their context up from the stored window, dropping what that would
//!   output.
//! - [`check_restarts`] checks the property restarts rely on: after a
//!   restart, a projection outputs exactly what an uninterrupted pass did.
//! - [`check_incremental`] checks the property incremental projections rely
//!   on: what a projection stores for a key, having recomputed only what
//!   each record changed, is what recomputing it from every record gives.
//!
//! Storage, the cursor, checkpoints and delivery are the application's, and
//! so are the contracts that keep them sound. A record's outputs are stored
//! before, or atomically with, the cursor's advance past it, and processing
//! resumes strictly after the cursor. A receiver that stores outputs before
//! advancing the cursor writes some again after a crash, which is safe only
//! if its writes are duplicate-safe and restart equivalence holds. A restart
//! rebuilds context, never history, so what a warm-up outputs is dropped. A
//! rebuild recomputes outputs only where their records are still stored:
//! from the log's beginning, or from a checkpoint.
//!
//! ```
//! use causet_projection::{Projector, Keyed, Projection, LogEntry, check_restarts};
//!
//! struct Seen { at: u64, id: u64 }
//! impl LogEntry for Seen {
//!     fn time_ms(&self) -> u64 { self.at }
//! }
//!
//! /// Says when an id is seen again within a minute of its last sighting.
//! #[derive(Debug, PartialEq)]
//! struct Repeat(String);
//! impl Keyed for Repeat {
//!     fn key(&self) -> &str { &self.0 }
//! }
//!
//! const WINDOW: u64 = 60_000;
//!
//! #[derive(Default)]
//! struct Repeats {
//!     /// The newest event time seen: records arrive late and out of order,
//!     /// so the window is measured from here, never from the record at hand.
//!     watermark: u64,
//!     /// Each id's newest sighting within the window.
//!     last: std::collections::BTreeMap<u64, u64>,
//! }
//! impl Projection<Seen> for Repeats {
//!     type Output = Repeat;
//!     fn name(&self) -> &str { "repeats" }
//!     fn retention_ms(&self) -> Option<u64> { Some(WINDOW) }
//!     fn process(&mut self, seen: &Seen, out: &mut Vec<Repeat>) {
//!         self.watermark = self.watermark.max(seen.at);
//!         let horizon = self.watermark.saturating_sub(WINDOW);
//!         // Older than the window: a warm-up would not see it either.
//!         if seen.at < horizon {
//!             return;
//!         }
//!         if let Some(&before) = self.last.get(&seen.id)
//!             && seen.at.abs_diff(before) <= WINDOW
//!         {
//!             out.push(Repeat(format!("{}@{}", seen.id, seen.at)));
//!         }
//!         let newest = self.last.entry(seen.id).or_default();
//!         *newest = (*newest).max(seen.at);
//!         self.last.retain(|_, at| *at >= horizon);
//!     }
//! }
//!
//! let fresh = || {
//!     let mut projector = Projector::new();
//!     projector.add_projection(Repeats::default());
//!     projector
//! };
//! let seen = |log: &[(u64, u64)]| -> Vec<Seen> { log.iter().map(|&(at, id)| Seen { at, id }).collect() };
//! let every = |log: &[Seen]| -> Vec<usize> { (1..log.len()).collect() };
//!
//! let log = seen(&[(0, 1), (10_000, 2), (20_000, 1), (90_000, 1), (100_000, 2)]);
//! assert_eq!(fresh().process(&log[0]), []);
//! // Restarting after any record changes nothing that follows.
//! assert_eq!(check_restarts(fresh, &log, &every(&log)), Ok(()));
//!
//! // A record arriving long after its time is ignored live, as a warm-up
//! // ignores it: it must not stand in for the newer sighting of its id.
//! let late = seen(&[(100_000, 1), (0, 1), (100_001, 1)]);
//! assert_eq!(check_restarts(fresh, &late, &every(&late)), Ok(()));
//! ```

use std::collections::BTreeMap;
use std::fmt::{self, Debug};

/// A record of the log, with its event time: when it happened, not when it
/// arrived.
pub trait LogEntry {
    /// Event time, in milliseconds.
    fn time_ms(&self) -> u64;
}

/// An output's idempotency key: the same derivation always gives the same
/// key and value, so writing it twice is harmless.
pub trait Keyed {
    /// The key.
    fn key(&self) -> &str;
}

/// Folds records, in log order, into outputs.
pub trait Projection<R> {
    /// What it derives.
    type Output: Keyed;

    /// Its name, for diagnostics.
    fn name(&self) -> &str;

    /// How far back in event time its context reaches: a warm-up refeeds it
    /// the records of this window. `None`: it keeps no context, and is not
    /// warmed up.
    fn retention_ms(&self) -> Option<u64> {
        None
    }

    /// Processes one record, pushing what it derives.
    fn process(&mut self, record: &R, out: &mut Vec<Self::Output>);
}

struct Slot<R, O> {
    projection: Box<dyn Projection<R, Output = O> + Send>,
    /// During a warm-up, the event time its window starts at.
    warm_from: Option<u64>,
}

/// Feeds records to projections, in the order they were added, and warms them
/// up after a restart.
pub struct Projector<R, O> {
    slots: Vec<Slot<R, O>>,
    /// What a warm-up derives, dropped.
    dropped: Vec<O>,
}

impl<R, O> Default for Projector<R, O> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            dropped: Vec::new(),
        }
    }
}

impl<R, O: Keyed> Debug for Projector<R, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.slots.iter().map(|slot| slot.projection.name()))
            .finish()
    }
}

impl<R: LogEntry, O: Keyed> Projector<R, O> {
    /// A projector with no projection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a projection after those already added.
    pub fn add_projection<P>(&mut self, projection: P)
    where
        P: Projection<R, Output = O> + Send + 'static,
    {
        self.slots.push(Slot {
            projection: Box::new(projection),
            warm_from: None,
        });
    }

    /// The projections' names, in order.
    pub fn names(&self) -> Vec<&str> {
        self.slots
            .iter()
            .map(|slot| slot.projection.name())
            .collect()
    }

    /// Processes a record: what every projection derives from it, in
    /// projection order.
    pub fn process(&mut self, record: &R) -> Vec<O> {
        let mut out = Vec::new();
        self.process_into(record, &mut out);
        out
    }

    /// `process`, pushing into `out`.
    pub fn process_into(&mut self, record: &R, out: &mut Vec<O>) {
        for slot in &mut self.slots {
            slot.projection.process(record, out);
        }
    }

    /// The longest retention of any projection.
    pub fn max_retention_ms(&self) -> Option<u64> {
        self.slots
            .iter()
            .filter_map(|slot| slot.projection.retention_ms())
            .max()
    }

    /// Starts a warm-up after a restart: `last_time_ms` is
    /// the latest event time among the records up to the cursor. Each
    /// projection's window starts at `last_time_ms` minus its retention.
    /// Returns the earliest of them: the event time from which the
    /// application reads its stored log to feed `warm`. `None` when no
    /// projection keeps context.
    pub fn warm_up_from(&mut self, last_time_ms: u64) -> Option<u64> {
        for slot in &mut self.slots {
            slot.warm_from = slot
                .projection
                .retention_ms()
                .map(|retention| last_time_ms.saturating_sub(retention));
        }
        self.slots.iter().filter_map(|slot| slot.warm_from).min()
    }

    /// Feeds a stored record during a warm-up, in log order, to the
    /// projections whose window it falls in, and drops what they derive: a
    /// replayed record may lack context older than the window, so its
    /// outputs can differ from those stored, and must not overwrite them.
    pub fn warm(&mut self, record: &R) {
        let time = record.time_ms();
        for slot in &mut self.slots {
            if slot.warm_from.is_some_and(|from| time >= from) {
                slot.projection.process(record, &mut self.dropped);
                self.dropped.clear();
            }
        }
    }

    /// The whole warm-up over the records up to the cursor, in log order,
    /// for a log held in memory.
    pub fn warm_up(&mut self, log: &[R]) {
        let Some(last) = log.iter().map(LogEntry::time_ms).max() else {
            return;
        };
        if self.warm_up_from(last).is_some() {
            for record in log {
                self.warm(record);
            }
        }
    }
}

/// Where a restarted pass first differs from the uninterrupted one.
#[derive(Clone, PartialEq, Eq)]
pub struct Difference<O> {
    /// How many records the latest restart before it had processed (its
    /// cursor); `None` when the two passes differ without any restart: the
    /// projections are not deterministic.
    pub restart: Option<usize>,
    /// The index of the record whose outputs differ.
    pub record: usize,
    /// What the uninterrupted pass derived from it.
    pub expected: Vec<O>,
    /// What the restarted pass did.
    pub actual: Vec<O>,
}

impl<O: Debug> Debug for Difference<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.restart {
            Some(at) => write!(f, "after a restart at {at}, ")?,
            None => write!(f, "without a restart, ")?,
        }
        write!(
            f,
            "record {} derived {:?} instead of {:?}",
            self.record, self.actual, self.expected
        )
    }
}

/// Restart equivalence, with a restart set up the default way:
/// a fresh projector warmed up from the records before it ([`Projector::warm_up`]).
pub fn check_restarts<R, O>(
    fresh: impl Fn() -> Projector<R, O>,
    records: &[R],
    restarts: &[usize],
) -> Result<(), Difference<O>>
where
    R: LogEntry,
    O: Keyed + PartialEq,
{
    check_restarts_with(
        &fresh,
        |prefix| {
            let mut projector = fresh();
            projector.warm_up(prefix);
            projector
        },
        records,
        restarts,
    )
}

/// Restart equivalence, with the application's own restart: `restarted` is
/// given the records up to the cursor and returns the projector ready to go on
/// (seeded from a checkpoint, warmed up). Runs `records` once without a
/// restart and once restarting before each index in `restarts`, one after
/// another, and returns the first record after a restart whose outputs
/// differ, in order.
pub fn check_restarts_with<R, O>(
    fresh: impl Fn() -> Projector<R, O>,
    restarted: impl Fn(&[R]) -> Projector<R, O>,
    records: &[R],
    restarts: &[usize],
) -> Result<(), Difference<O>>
where
    R: LogEntry,
    O: Keyed + PartialEq,
{
    let mut uninterrupted = fresh();
    let mut expected: Vec<Vec<O>> = records
        .iter()
        .map(|record| uninterrupted.process(record))
        .collect();
    let mut projector = fresh();
    let mut restart = None;
    for (index, record) in records.iter().enumerate() {
        if restarts.contains(&index) {
            projector = restarted(&records[..index]);
            restart = Some(index);
        }
        let actual = projector.process(record);
        if actual != expected[index] {
            let expected = std::mem::take(&mut expected[index]);
            return Err(Difference {
                restart,
                record: index,
                expected,
                actual,
            });
        }
    }
    Ok(())
}

/// Where what a projection stores for a key differs from what recomputing it
/// from every record gives.
#[derive(Clone, PartialEq, Eq)]
pub struct Mismatch<O> {
    /// The index of the record after which they differ.
    pub record: usize,
    /// The output's key.
    pub key: String,
    /// The key's last output: what the application stores.
    pub stored: O,
    /// The key's output recomputed from every record so far.
    pub recomputed: O,
}

impl<O: Debug> Debug for Mismatch<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "after record {}, {} is stored as {:?}, but every record so far gives {:?}",
            self.record, self.key, self.stored, self.recomputed
        )
    }
}

/// Incremental equals from scratch: a projection that recomputes, after each
/// record, only the keys it judges the record to change must store for every
/// key what recomputing it from every record would give.
///
/// Feeds `records` to `projection` and to `reference` (usually the same
/// projection keeping everything it sees, so that nothing it reads has been
/// evicted), keeps each key's last output as the application stores it, and
/// after each record asks `recompute` for each stored key's output now,
/// given both: the projection, to say whether it still maintains the key,
/// and the reference, to recompute it. `None` skips the key: one no longer
/// maintained (evicted, or an output made once and never revised). Returns
/// the first stored output that differs. Only keys the projection has output
/// are compared: one that outputs nothing passes, so a test checks that the
/// keys expected are output separately.
///
/// A missed recomputation leaves a stale output; context evicted while a
/// key that reads it is still maintained makes a later recomputation lose
/// facts. Restart equivalence finds neither when the restarted pass is wrong
/// the same way as the uninterrupted one.
pub fn check_incremental<R, P, Q>(
    mut projection: P,
    mut reference: Q,
    records: &[R],
    recompute: impl Fn(&P, &Q, &str) -> Option<P::Output>,
) -> Result<(), Mismatch<P::Output>>
where
    P: Projection<R>,
    Q: Projection<R>,
    P::Output: Clone + PartialEq,
{
    let mut stored: BTreeMap<String, P::Output> = BTreeMap::new();
    let (mut out, mut dropped) = (Vec::new(), Vec::new());
    for (index, record) in records.iter().enumerate() {
        projection.process(record, &mut out);
        reference.process(record, &mut dropped);
        dropped.clear();
        for output in out.drain(..) {
            stored.insert(output.key().to_owned(), output);
        }
        for (key, last) in &stored {
            if let Some(now) = recompute(&projection, &reference, key)
                && now != *last
            {
                return Err(Mismatch {
                    record: index,
                    key: key.clone(),
                    stored: last.clone(),
                    recomputed: now,
                });
            }
        }
    }
    Ok(())
}

#[cfg(feature = "reporting")]
impl LogEntry for causet_reporting::Record {
    fn time_ms(&self) -> u64 {
        self.timestamp_ms
    }
}
