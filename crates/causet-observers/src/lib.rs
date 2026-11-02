//! Logging and execution statistics as observers.
//!
//! - [`Logger`]: one `tracing` event per execution on the [`TARGET`] target,
//!   at the level the message chooses, with the fields it selects, captured at
//!   start.
//! - [`Stats`]: per-type, per-kind counts and durations on one combined
//!   execution timeline.
//! - `trace!` to `error!`: application logs stamped with the scheduler time,
//!   for reducers and handlers, which never read system time.

use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Display, Write as _};
use std::hash::{BuildHasherDefault, Hasher};

use causet_core::{
    BugReport, Category, Execution, FieldVisitor, InlineCall, LogLevel, MessageView, Observer,
    Refusal, RefusedBy, SentFrom, Timestamp,
};
use serde::{Deserialize, Serialize};

pub use tracing;

/// The target every execution event is logged on.
pub const TARGET: &str = "causet::exec";

// ---------------------------------------------------------------------------
// Logging

/// Logs every execution as it starts. Events carry the scheduler time, the
/// optional label (a node's name, say), the category, `type`, kind,
/// destination path, metadata and, if the message selects any, its `fields`.
/// A refused Action is logged with `refused` (what refused it), `sent` and
/// `causal` (where it came from); a child's Action reduced inline with
/// `within`, its parent's execution, at that execution's time, and
/// `root_guard_skipped` when its mount has a root guard the call did not run. Both at
/// the level the message chooses.
#[derive(Default)]
pub struct Logger {
    label: Option<String>,
}

impl Logger {
    /// A logger with no label.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `label` to every event, to tell several Stores apart.
    pub fn with_label(label: impl Into<String>) -> Self {
        Self {
            label: Some(label.into()),
        }
    }

    fn log(&self, exec: &Execution<'_>, payload: &dyn MessageView) {
        macro_rules! at {
            ($level:expr) => {
                if tracing::enabled!(target: TARGET, $level) {
                    let fields = Fields::of(payload);
                    tracing::event!(
                        target: TARGET,
                        $level,
                        time = exec.meta.time,
                        label = self.label.as_deref(),
                        category = category_name(exec.category),
                        "type" = exec.message,
                        kind = exec.kind,
                        path = &**exec.path,
                        id = exec.meta.id.0,
                        causal = exec.meta.causal_execution_id().0,
                        depth = exec.meta.depth,
                        answers = exec.answers.map(|a| a.0),
                        fields = fields.as_deref(),
                    );
                }
            };
        }
        match payload.log_level() {
            LogLevel::Trace => at!(tracing::Level::TRACE),
            LogLevel::Debug => at!(tracing::Level::DEBUG),
            LogLevel::Info => at!(tracing::Level::INFO),
            LogLevel::Warn => at!(tracing::Level::WARN),
            LogLevel::Error => at!(tracing::Level::ERROR),
        }
    }
}

impl Logger {
    fn log_refused(&self, refusal: &Refusal<'_>, payload: &dyn MessageView) {
        macro_rules! at {
            ($level:expr) => {
                if tracing::enabled!(target: TARGET, $level) {
                    let fields = Fields::of(payload);
                    tracing::event!(
                        target: TARGET,
                        $level,
                        time = refusal.time,
                        label = self.label.as_deref(),
                        category = "action",
                        "type" = refusal.message,
                        kind = refusal.kind,
                        path = &**refusal.path,
                        refused = refused_name(refusal.by),
                        sent = sent_name(refusal.sent),
                        causal = refusal.sent.execution().map(|id| id.0),
                        fields = fields.as_deref(),
                    );
                }
            };
        }
        match payload.log_level() {
            LogLevel::Trace => at!(tracing::Level::TRACE),
            LogLevel::Debug => at!(tracing::Level::DEBUG),
            LogLevel::Info => at!(tracing::Level::INFO),
            LogLevel::Warn => at!(tracing::Level::WARN),
            LogLevel::Error => at!(tracing::Level::ERROR),
        }
    }

    fn log_inline(&self, inline: &InlineCall<'_>, payload: &dyn MessageView) {
        macro_rules! at {
            ($level:expr) => {
                if tracing::enabled!(target: TARGET, $level) {
                    let fields = Fields::of(payload);
                    tracing::event!(
                        target: TARGET,
                        $level,
                        time = inline.time,
                        label = self.label.as_deref(),
                        category = "action",
                        "type" = inline.message,
                        kind = inline.kind,
                        path = &**inline.path,
                        within = inline.within.0,
                        root_guard_skipped = inline.root_guard_skipped.then_some(true),
                        fields = fields.as_deref(),
                    );
                }
            };
        }
        match payload.log_level() {
            LogLevel::Trace => at!(tracing::Level::TRACE),
            LogLevel::Debug => at!(tracing::Level::DEBUG),
            LogLevel::Info => at!(tracing::Level::INFO),
            LogLevel::Warn => at!(tracing::Level::WARN),
            LogLevel::Error => at!(tracing::Level::ERROR),
        }
    }
}

impl Observer for Logger {
    /// Every bug report, at error level, with its place and fields.
    fn bug(&mut self, report: &BugReport) {
        let place = report.place.as_ref();
        let fields: Vec<String> = report
            .fields
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect();
        tracing::error!(
            target: TARGET,
            label = self.label.as_deref(),
            phase = ?place.map(|p| p.phase),
            id = place.and_then(|p| p.at).map(|id| id.0),
            path = place.map(|p| &*p.path),
            sequence = place.map(|p| p.sequence),
            file = &*report.file,
            line = report.line,
            fields = (!fields.is_empty()).then(|| fields.join(" ")),
            "bug condition: {}",
            report.message,
        );
    }
    fn action_start(&mut self, exec: &Execution<'_>, payload: &dyn MessageView) {
        self.log(exec, payload);
    }
    fn effect_start(&mut self, exec: &Execution<'_>, payload: &dyn MessageView) {
        self.log(exec, payload);
    }
    fn event_start(&mut self, exec: &Execution<'_>, payload: &dyn MessageView) {
        self.log(exec, payload);
    }
    fn action_refused(&mut self, refusal: &Refusal<'_>, payload: &dyn MessageView) {
        self.log_refused(refusal, payload);
    }
    fn inline_action(&mut self, inline: &InlineCall<'_>, payload: &dyn MessageView) {
        self.log_inline(inline, payload);
    }
}

fn refused_name(by: RefusedBy) -> &'static str {
    match by {
        RefusedBy::RootGuard => "root-guard",
        RefusedBy::Guard => "guard",
        RefusedBy::Component => "component",
        RefusedBy::Absent => "absent",
    }
}

fn sent_name(sent: SentFrom) -> &'static str {
    match sent {
        SentFrom::Root => "root",
        SentFrom::Effect(_) => "effect",
        SentFrom::Queued(_) => "queued",
        SentFrom::Inline(_) => "inline",
    }
}

fn category_name(category: Category) -> &'static str {
    match category {
        Category::Action => "action",
        Category::Effect => "effect",
        Category::Event => "event",
    }
}

/// A message's selected fields as `name=value` pairs, space-separated.
struct Fields(String);

impl Fields {
    fn of(payload: &dyn MessageView) -> Option<String> {
        let mut fields = Fields(String::new());
        payload.log_fields(&mut fields);
        (!fields.0.is_empty()).then_some(fields.0)
    }

    fn separate(&mut self) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
    }
}

impl FieldVisitor for Fields {
    fn debug(&mut self, name: &'static str, value: &dyn Debug) {
        self.separate();
        let _ = write!(self.0, "{name}={value:?}");
    }
    fn display(&mut self, name: &'static str, value: &dyn Display) {
        self.separate();
        let _ = write!(self.0, "{name}={value}");
    }
}

/// Logs at `level` stamped with a scheduler time, never the system clock:
/// `causet_observers::debug!(meta.time; job = id, "stale job")`.
#[macro_export]
macro_rules! log_at {
    ($level:ident, $time:expr; $($tts:tt)*) => {
        $crate::tracing::$level!(time = $time, $($tts)*)
    };
}

/// `log_at!` at trace level: `trace!(meta.time; ...)`.
#[macro_export]
macro_rules! trace {
    ($time:expr; $($tts:tt)*) => { $crate::log_at!(trace, $time; $($tts)*) };
}

/// `log_at!` at debug level: `debug!(meta.time; ...)`.
#[macro_export]
macro_rules! debug {
    ($time:expr; $($tts:tt)*) => { $crate::log_at!(debug, $time; $($tts)*) };
}

/// `log_at!` at info level: `info!(meta.time; ...)`.
#[macro_export]
macro_rules! info {
    ($time:expr; $($tts:tt)*) => { $crate::log_at!(info, $time; $($tts)*) };
}

/// `log_at!` at warn level: `warn!(meta.time; ...)`.
#[macro_export]
macro_rules! warn {
    ($time:expr; $($tts:tt)*) => { $crate::log_at!(warn, $time; $($tts)*) };
}

/// `log_at!` at error level: `error!(meta.time; ...)`.
#[macro_export]
macro_rules! error {
    ($time:expr; $($tts:tt)*) => { $crate::log_at!(error, $time; $($tts)*) };
}

// ---------------------------------------------------------------------------
// Stats

/// A kind's slot: its category and its `'static` names as address and
/// length, so that finding it hashes words, not strings. The length matters:
/// a name and its prefix share an address. Equal names at different
/// addresses get two slots, which `snapshot` merges by name.
type Key = (Category, (usize, usize), (usize, usize));

fn name_key(name: &'static str) -> (usize, usize) {
    (name.as_ptr() as usize, name.len())
}

struct Slot {
    message: &'static str,
    kind: &'static str,
    ranges: DurationRanges,
}

/// A multiply-rotate hash for `Key`: its words are addresses, never chosen by
/// an outsider, so SipHash's flood resistance buys nothing here.
#[derive(Default)]
struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn write(&mut self, bytes: &[u8]) {
        bytes.iter().for_each(|&b| self.write_u64(u64::from(b)));
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = (self.0.rotate_left(5) ^ n).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
    fn write_usize(&mut self, n: usize) {
        self.write_u64(n as u64);
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

/// Execution counts and durations by category, type and kind.
///
/// Durations use one combined timeline: each execution, whatever its
/// category, is charged from its start until the next execution starts. That
/// is a scheduler attribution interval, not handler self-time. An execution is counted when it is
/// charged, so the latest one stays out until another starts.
#[derive(Default)]
pub struct Stats {
    /// The open execution: its slot, names and start time.
    last: Option<(Key, &'static str, &'static str, Timestamp)>,
    by_kind: HashMap<Key, Slot, BuildHasherDefault<KeyHasher>>,
}

impl Stats {
    /// Empty statistics.
    pub fn new() -> Self {
        Self::default()
    }

    fn start(&mut self, exec: &Execution<'_>) {
        let key = (exec.category, name_key(exec.message), name_key(exec.kind));
        let now = exec.meta.time;
        if let Some((previous, message, kind, started)) =
            self.last.replace((key, exec.message, exec.kind, now))
        {
            let slot = self.by_kind.entry(previous).or_insert_with(|| Slot {
                message,
                kind,
                ranges: DurationRanges::default(),
            });
            slot.ranges.add(now.saturating_sub(started));
        }
    }

    /// The statistics so far, by category, type and kind.
    pub fn snapshot(&self) -> StatsSnapshot {
        let mut snapshot = StatsSnapshot::default();
        for (&(category, _, _), slot) in &self.by_kind {
            let by_type = match category {
                Category::Action => &mut snapshot.actions,
                Category::Effect => &mut snapshot.effects,
                Category::Event => &mut snapshot.events,
            };
            let ranges = by_type
                .entry(slot.message.to_string())
                .or_default()
                .entry(slot.kind.to_string())
                .or_default();
            ranges.merge(&slot.ranges);
        }
        snapshot
    }
}

impl Observer for Stats {
    fn action_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        self.start(exec);
    }
    fn effect_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        self.start(exec);
    }
    fn event_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        self.start(exec);
    }
}

/// Type name, then kind, then durations, per category. Serving it is the
/// application's business.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StatsSnapshot {
    /// Actions, by type and kind.
    pub actions: BTreeMap<String, BTreeMap<String, DurationRanges>>,
    /// Effects, by type and kind.
    pub effects: BTreeMap<String, BTreeMap<String, DurationRanges>>,
    /// Events, by type and kind.
    pub events: BTreeMap<String, BTreeMap<String, DurationRanges>>,
}

/// Charged durations in buckets from under 1 µs to above 50 ms, in
/// nanoseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurationRanges {
    /// Durations up to 1 µs.
    pub under_1_us: RangeStats,
    /// Durations over 1 µs, up to 10 µs.
    pub under_10_us: RangeStats,
    /// Durations over 10 µs, up to 50 µs.
    pub under_50_us: RangeStats,
    /// Durations over 50 µs, up to 100 µs.
    pub under_100_us: RangeStats,
    /// Durations over 100 µs, up to 500 µs.
    pub under_500_us: RangeStats,
    /// Durations over 500 µs, up to 1 ms.
    pub under_1_ms: RangeStats,
    /// Durations over 1 ms, up to 5 ms.
    pub under_5_ms: RangeStats,
    /// Durations over 5 ms, up to 50 ms.
    pub under_50_ms: RangeStats,
    /// Durations over 50 ms.
    pub above_50_ms: RangeStats,
}

/// Calls and durations in one duration range, in nanoseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeStats {
    /// Executions charged to the range.
    pub total_calls: u64,
    /// Sum of the charged durations.
    pub total_duration: u64,
    /// The longest of them.
    pub max_duration: u64,
}

impl DurationRanges {
    /// Charges one execution of `duration` nanoseconds to its range.
    pub fn add(&mut self, duration: u64) {
        let range = match duration {
            0..=1_000 => &mut self.under_1_us,
            1_001..=10_000 => &mut self.under_10_us,
            10_001..=50_000 => &mut self.under_50_us,
            50_001..=100_000 => &mut self.under_100_us,
            100_001..=500_000 => &mut self.under_500_us,
            500_001..=1_000_000 => &mut self.under_1_ms,
            1_000_001..=5_000_000 => &mut self.under_5_ms,
            5_000_001..=50_000_000 => &mut self.under_50_ms,
            50_000_001.. => &mut self.above_50_ms,
        };
        range.total_calls = range.total_calls.saturating_add(1);
        range.total_duration = range.total_duration.saturating_add(duration);
        range.max_duration = range.max_duration.max(duration);
    }

    /// Adds `other`'s calls and durations to these.
    pub fn merge(&mut self, other: &DurationRanges) {
        for (mine, theirs) in self.ranges_mut().into_iter().zip(other.ranges()) {
            mine.total_calls = mine.total_calls.saturating_add(theirs.total_calls);
            mine.total_duration = mine.total_duration.saturating_add(theirs.total_duration);
            mine.max_duration = mine.max_duration.max(theirs.max_duration);
        }
    }

    fn ranges_mut(&mut self) -> [&mut RangeStats; 9] {
        [
            &mut self.under_1_us,
            &mut self.under_10_us,
            &mut self.under_50_us,
            &mut self.under_100_us,
            &mut self.under_500_us,
            &mut self.under_1_ms,
            &mut self.under_5_ms,
            &mut self.under_50_ms,
            &mut self.above_50_ms,
        ]
    }

    fn ranges(&self) -> [&RangeStats; 9] {
        [
            &self.under_1_us,
            &self.under_10_us,
            &self.under_50_us,
            &self.under_100_us,
            &self.under_500_us,
            &self.under_1_ms,
            &self.under_5_ms,
            &self.under_50_ms,
            &self.above_50_ms,
        ]
    }

    /// Executions across all ranges.
    pub fn calls(&self) -> u64 {
        self.ranges().iter().map(|r| r.total_calls).sum()
    }

    /// Their total duration.
    pub fn total_duration(&self) -> u64 {
        self.ranges().iter().map(|r| r.total_duration).sum()
    }

    /// The longest of them.
    pub fn max_duration(&self) -> u64 {
        self.ranges()
            .iter()
            .map(|r| r.max_duration)
            .max()
            .unwrap_or(0)
    }
}
