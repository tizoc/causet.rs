//! The execution clock. It belongs to the Store and is the only source of
//! `meta.time`; execution IDs count executions independently. Services keep
//! their own clocks for their
//! I/O.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use web_time::{Instant, SystemTime, UNIX_EPOCH};

use crate::Timestamp;

/// Readings in nanoseconds. They need not increase: the Store makes each
/// execution time `max(previous, now)`.
pub trait Clock: Send {
    /// The current reading.
    fn now(&mut self) -> Timestamp;
}

/// Nanoseconds since the Unix epoch, as near the real time as the machine
/// knows it: the system (wall) clock at construction, plus the time elapsed
/// since. Elapsed time is the monotonic clock's, plus every stretch it
/// missed: between two readings, when the wall clock gained more than the
/// monotonic one, the difference counts too. So:
///
/// - a sleep or suspend, during which the monotonic clock stops on some
///   systems (macOS, iOS, Android), is counted: time does not fall behind by
///   every sleep;
/// - a wall clock stepped back is ignored: the clock goes on with the
///   monotonic clock, rather than standing still until the wall clock
///   catches up, and a later sleep still counts whole;
/// - readings never go back.
///
/// A stretch counts as missed only from 100 ms: below that, the difference
/// between the two clocks is their sampling error, which would otherwise add
/// up reading after reading.
///
/// The wall clock is read at most once per millisecond of monotonic time,
/// which keeps a reading as cheap as one monotonic read; after a sleep, the
/// clock catches up within the first millisecond awake.
///
/// The application can correct the result with a [`ClockOffset`] measured
/// against a source it trusts (NTP, a server, GPS); causet measures none.
/// A wall clock stepped *forward* by mistake is followed too: correct it with
/// the offset. Execution times never go back in any case: the Store stamps
/// `max(previous, now)`.
pub struct SystemClock {
    /// Both clocks at construction: the wall clock in nanoseconds since the
    /// epoch, the monotonic one from its own origin.
    start_wall: Timestamp,
    start_monotonic: Timestamp,
    /// Both clocks, at the last reading of the wall clock.
    last_wall: Timestamp,
    last_monotonic: Timestamp,
    /// The time the monotonic clock missed, summed.
    missed: Timestamp,
    offset: Option<ClockOffset>,
    readings: Box<dyn Readings>,
}

/// Where a `SystemClock` reads time: the real clocks, or a test's.
trait Readings: Send {
    /// The wall clock, nanoseconds since the epoch.
    fn wall(&mut self) -> Timestamp;
    /// The monotonic clock, nanoseconds since construction.
    fn monotonic(&mut self) -> Timestamp;
}

struct RealClocks {
    started: Instant,
}

impl Readings for RealClocks {
    fn wall(&mut self) -> Timestamp {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        saturating_nanos(since_epoch.as_nanos())
    }

    fn monotonic(&mut self) -> Timestamp {
        saturating_nanos(self.started.elapsed().as_nanos())
    }
}

impl SystemClock {
    /// A clock on the machine's clocks, with no offset.
    pub fn new() -> Self {
        Self::from_readings(Box::new(RealClocks {
            started: Instant::now(),
        }))
    }

    /// A clock corrected by `offset`, which the application keeps a clone
    /// of and sets as it learns the machine's error.
    pub fn with_offset(offset: ClockOffset) -> Self {
        Self {
            offset: Some(offset),
            ..Self::new()
        }
    }

    fn from_readings(mut readings: Box<dyn Readings>) -> Self {
        // The wall sample lies between two monotonic ones: elapsed time is
        // counted from the later (nothing before the anchor counts), gains
        // against the earlier (the most that can have passed since).
        let before = readings.monotonic();
        let wall = readings.wall();
        let after = readings.monotonic();
        Self {
            start_wall: wall,
            start_monotonic: after,
            last_wall: wall,
            last_monotonic: before,
            missed: 0,
            offset: None,
            readings,
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&mut self) -> Timestamp {
        let mut monotonic = self.readings.monotonic();
        if monotonic.saturating_sub(self.last_monotonic) >= WALL_CHECK_NANOS {
            // The wall sample lies between `monotonic` and `after`: a pause
            // of the thread between the reads shows in both clocks. Since
            // the previous wall sample, the monotonic clock gained at most
            // `after - last_monotonic` (both bracket ends taken wide); what
            // the wall clock gained beyond that, the monotonic clock missed
            // (a sleep). Below the threshold the difference is the clocks'
            // rate error, not a sleep, and adding it would accumulate. A
            // wall clock that gained less, or went back, adds nothing.
            let wall = self.readings.wall();
            let after = self.readings.monotonic();
            let wall_gained = wall.saturating_sub(self.last_wall);
            let monotonic_gained = after.saturating_sub(self.last_monotonic);
            let ahead = wall_gained.saturating_sub(monotonic_gained);
            if ahead >= MIN_SLEEP_NANOS {
                self.missed = self.missed.saturating_add(ahead);
            }
            (self.last_wall, self.last_monotonic) = (wall, monotonic);
            monotonic = after;
        }
        let real = self
            .start_wall
            .saturating_add(monotonic.saturating_sub(self.start_monotonic))
            .saturating_add(self.missed);
        match &self.offset {
            Some(offset) => real.saturating_add_signed(offset.get()),
            None => real,
        }
    }
}

/// How much monotonic time passes between two readings of the wall clock.
const WALL_CHECK_NANOS: Timestamp = 1_000_000;

/// The least the wall clock must gain on the monotonic clock between two
/// readings to count as a sleep: far more than sampling error, far less than
/// a device's sleep.
const MIN_SLEEP_NANOS: Timestamp = 100_000_000;

/// A correction to a [`SystemClock`], in nanoseconds: positive when the
/// machine's clock is behind the true time. Clones share one value, so the
/// application keeps one and sets it from whatever it trusts, while the
/// Store's clock reads it.
#[derive(Clone, Debug, Default)]
pub struct ClockOffset(Arc<AtomicI64>);

impl ClockOffset {
    /// An offset of `nanos`.
    pub fn new(nanos: i64) -> Self {
        Self(Arc::new(AtomicI64::new(nanos)))
    }

    /// Sets the correction. A smaller one can move readings back; execution
    /// times still never go back.
    pub fn set(&self, nanos: i64) {
        self.0.store(nanos, Ordering::Relaxed);
    }

    /// The current correction.
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn saturating_nanos(nanos: u128) -> Timestamp {
    Timestamp::try_from(nanos).unwrap_or(Timestamp::MAX)
}

/// A clock that moves only when told. Clones share one reading, so a test
/// keeps a clone and moves the Store's clock from outside.
#[derive(Clone, Default)]
pub struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    /// A clock standing at `now`.
    pub fn new(now: Timestamp) -> Self {
        Self(Arc::new(AtomicU64::new(now)))
    }

    /// Moves the clock to `now`, forwards or back. Execution times never go
    /// back: the Store stamps `max(previous, now)`.
    pub fn set(&self, now: Timestamp) {
        self.0.store(now, Ordering::Relaxed);
    }

    /// Moves the clock forward by `by`.
    pub fn advance(&self, by: Timestamp) {
        self.0.fetch_add(by, Ordering::Relaxed);
    }

    /// The current reading.
    pub fn get(&self) -> Timestamp {
        self.0.load(Ordering::Relaxed)
    }
}

impl Clock for ManualClock {
    fn now(&mut self) -> Timestamp {
        self.get()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    const S: Timestamp = 1_000_000_000;
    const START: Timestamp = 1_700_000_000 * S;

    /// Clocks a test moves: (wall, monotonic since construction).
    #[derive(Clone)]
    struct Fake(Arc<Mutex<(Timestamp, Timestamp)>>);

    impl Fake {
        fn at(&self, wall: Timestamp, monotonic: Timestamp) {
            *self.0.lock().unwrap() = (wall, monotonic);
        }
    }

    impl Readings for Fake {
        fn wall(&mut self) -> Timestamp {
            self.0.lock().unwrap().0
        }
        fn monotonic(&mut self) -> Timestamp {
            self.0.lock().unwrap().1
        }
    }

    fn clock() -> (SystemClock, Fake) {
        let fake = Fake(Arc::new(Mutex::new((START, 0))));
        (SystemClock::from_readings(Box::new(fake.clone())), fake)
    }

    #[test]
    fn it_runs_with_the_monotonic_clock_while_the_two_agree() {
        let (mut clock, fake) = clock();
        assert_eq!(clock.now(), START);
        fake.at(START + 5 * S, 5 * S);
        assert_eq!(clock.now(), START + 5 * S);
    }

    #[test]
    fn a_sleep_the_monotonic_clock_missed_is_followed_and_kept() {
        let (mut clock, fake) = clock();
        // An hour asleep: the wall clock moved, the monotonic one did not.
        fake.at(START + 3_610 * S, 10 * S);
        assert_eq!(clock.now(), START + 3_610 * S);
        // Later readings keep the gap.
        fake.at(START + 3_620 * S, 20 * S);
        assert_eq!(clock.now(), START + 3_620 * S);
    }

    #[test]
    fn the_wall_clock_is_read_once_per_millisecond_of_monotonic_time() {
        const MS: Timestamp = 1_000_000;
        let (mut clock, fake) = clock();
        fake.at(START + 10 * S, 10 * S);
        assert_eq!(clock.now(), START + 10 * S);
        // Woken from an hour's sleep: until a millisecond has passed awake,
        // the wall clock is not read again, and the sleep not yet counted.
        fake.at(START + 3_610 * S, 10 * S + MS / 2);
        assert_eq!(clock.now(), START + 10 * S + MS / 2);
        fake.at(START + 3_610 * S + MS / 2, 10 * S + MS);
        assert_eq!(clock.now(), START + 3_610 * S + MS / 2);
    }

    #[test]
    fn sampling_jitter_does_not_add_up() {
        const US: Timestamp = 1_000;
        const MS: Timestamp = 1_000_000;
        let (mut clock, fake) = clock();
        // Both clocks run together; the wall clock is sampled up to 1 µs
        // late, every other time.
        for check in 1..=200 {
            let jitter = if check % 2 == 0 { US } else { 0 };
            fake.at(START + check * MS + jitter, check * MS);
            clock.now();
        }
        fake.at(START + 201 * MS, 201 * MS);
        assert_eq!(clock.now(), START + 201 * MS, "no time made up from jitter");
    }

    #[test]
    fn time_before_the_anchor_is_not_counted() {
        // The monotonic clock had run 5 s before the wall clock was read.
        let fake = Fake(Arc::new(Mutex::new((START, 5 * S))));
        let mut clock = SystemClock::from_readings(Box::new(fake.clone()));
        assert_eq!(clock.now(), START);
        fake.at(START + S, 6 * S);
        assert_eq!(clock.now(), START + S);
    }

    #[test]
    fn a_pause_between_the_reads_is_not_counted_twice() {
        // Two clocks that always agree (wall = START + elapsed); the thread
        // is descheduled for half a second just before the wall read, and
        // both clocks advance meanwhile.
        #[derive(Clone)]
        struct DelayedWall {
            elapsed: Arc<AtomicU64>,
            pause: Arc<AtomicU64>,
        }
        impl Readings for DelayedWall {
            fn wall(&mut self) -> Timestamp {
                let pause = self.pause.swap(0, Ordering::Relaxed);
                START + self.elapsed.fetch_add(pause, Ordering::Relaxed) + pause
            }
            fn monotonic(&mut self) -> Timestamp {
                self.elapsed.load(Ordering::Relaxed)
            }
        }

        let source = DelayedWall {
            elapsed: Arc::default(),
            pause: Arc::default(),
        };
        let mut clock = SystemClock::from_readings(Box::new(source.clone()));
        source.elapsed.store(S, Ordering::Relaxed);
        source.pause.store(S / 2, Ordering::Relaxed);
        let actual = START + S + S / 2;
        assert_eq!(clock.now(), actual, "the reading sees the pause once");
        assert_eq!(clock.now(), actual, "and the next does not add it again");
    }

    #[test]
    fn a_wall_clock_stepped_back_is_ignored() {
        let (mut clock, fake) = clock();
        fake.at(START - 3_600 * S, 10 * S);
        assert_eq!(clock.now(), START + 10 * S, "on with the monotonic clock");
        fake.at(START - 3_590 * S, 20 * S);
        assert_eq!(clock.now(), START + 20 * S);
    }

    #[test]
    fn a_sleep_after_a_step_back_still_counts_whole() {
        let (mut clock, fake) = clock();
        fake.at(START - 60 * S, 10 * S);
        assert_eq!(clock.now(), START + 10 * S);
        // Then 90 s asleep and 10 s awake: the wall clock gains 100 s, the
        // monotonic clock 10 s. The step back does not eat into the sleep.
        fake.at(START + 40 * S, 20 * S);
        assert_eq!(clock.now(), START + 110 * S);
    }

    #[test]
    fn the_offset_corrects_every_reading() {
        let fake = Fake(Arc::new(Mutex::new((START, 0))));
        let offset = ClockOffset::default();
        let mut clock = SystemClock {
            offset: Some(offset.clone()),
            ..SystemClock::from_readings(Box::new(fake.clone()))
        };
        assert_eq!(clock.now(), START);
        offset.set(2 * S as i64);
        assert_eq!(clock.now(), START + 2 * S);
        offset.set(-(3 * S as i64));
        fake.at(START + 10 * S, 10 * S);
        assert_eq!(clock.now(), START + 7 * S);
    }
}
