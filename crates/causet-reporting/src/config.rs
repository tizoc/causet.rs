//! What the pipeline and the file dump read. Delivery (HTTP, signing, queues)
//! and application checks (healthchecks, snapshots) are the application's,
//! with its own configuration.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::types::{ReportKind, Source};

const DEFAULT_DEDUPE_WINDOW_MS: u64 = 5_000;
const DEFAULT_DEDUPE_MAX_ENTRIES: usize = 1_024;
const DEFAULT_RATE_LIMIT_WINDOW_MS: u64 = 60_000;
const DEFAULT_RATE_LIMIT_MAX_PER_WINDOW: u32 = 300;
const DEFAULT_DUMP_MAX_BYTES: u64 = 10 * 1024 * 1024;

/// Version of the envelope an application puts delivered records in, for it
/// to send along with them. Nothing in this crate reads it.
pub const ENVELOPE_VERSION: u8 = 1;

/// Everything `Pipeline` reads, and the dump settings `Filesystem` takes.
/// Every section has defaults, so an empty JSON object is a valid config.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    /// When false, the pipeline drops every report (counted as
    /// `dropped_disabled`) and flushes no aggregates. Default true.
    #[serde(default = "enabled")]
    pub enabled: bool,
    /// Where `Filesystem` writes records.
    #[serde(default)]
    pub dump: DumpConfig,
    /// The per-source rate limit, applied to every report.
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    /// The share of reports kept per kind, unless a metric policy sets its own.
    #[serde(default)]
    pub sampling: SamplingConfig,
    /// Per-event sampling rates and aggregation.
    #[serde(default)]
    pub metrics: MetricConfig,
    /// Which kinds drop repeated reports, and for how long.
    #[serde(default)]
    pub dedupe: DedupeConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: enabled(),
            dump: DumpConfig::default(),
            rate_limit: RateLimitConfig::default(),
            sampling: SamplingConfig::default(),
            metrics: MetricConfig::default(),
            dedupe: DedupeConfig::default(),
        }
    }
}

fn enabled() -> bool {
    true
}

/// JSON lines written by `Filesystem`, rotated by size. Nothing is written
/// while `dir` is empty.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DumpConfig {
    /// When false, `Filesystem::dump` writes nothing. Default true.
    #[serde(default = "enabled")]
    pub enabled: bool,
    /// Directory of the dump and its rotated files, created on the first
    /// write. Empty by default, which turns the dump off.
    #[serde(default)]
    pub dir: PathBuf,
    /// Name of the file in `dir` that records are appended to; a rotated file
    /// gets `-<timestamp_ms>` before its extension. Default `reports.jsonl`.
    #[serde(default = "DumpConfig::default_filename")]
    pub filename: String,
    /// Size at which the file is rotated before the next write; 0 never
    /// rotates. Default 10 MiB.
    #[serde(default = "DumpConfig::default_max_file_size_bytes")]
    pub max_file_size_bytes: u64,
}

impl DumpConfig {
    fn default_filename() -> String {
        "reports.jsonl".to_owned()
    }

    fn default_max_file_size_bytes() -> u64 {
        DEFAULT_DUMP_MAX_BYTES
    }
}

impl Default for DumpConfig {
    fn default() -> Self {
        Self {
            enabled: enabled(),
            dir: PathBuf::new(),
            filename: Self::default_filename(),
            max_file_size_bytes: Self::default_max_file_size_bytes(),
        }
    }
}

/// At most `max_per_window` reports per source in any `window_ms`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Length of the sliding window, in milliseconds. Default 60 seconds.
    #[serde(default = "RateLimitConfig::default_window_ms")]
    pub window_ms: u64,
    /// Reports per source let through in a window, counting those sampling
    /// later drops; the rest count as `dropped_rate_limited`. Default 300.
    #[serde(default = "RateLimitConfig::default_max_per_window")]
    pub max_per_window: u32,
}

impl RateLimitConfig {
    fn default_window_ms() -> u64 {
        DEFAULT_RATE_LIMIT_WINDOW_MS
    }

    fn default_max_per_window() -> u32 {
        DEFAULT_RATE_LIMIT_MAX_PER_WINDOW
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            window_ms: DEFAULT_RATE_LIMIT_WINDOW_MS,
            max_per_window: DEFAULT_RATE_LIMIT_MAX_PER_WINDOW,
        }
    }
}

/// The share of reports kept, per kind, decided by the report's hash so the
/// same report is always kept or always dropped.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SamplingConfig {
    /// Share of error reports kept, clamped to 0.0..=1.0. Default 1.0.
    #[serde(default = "SamplingConfig::default_error")]
    pub error: f32,
    /// Share of metric reports kept, clamped to 0.0..=1.0. Default 0.1.
    #[serde(default = "SamplingConfig::default_metric")]
    pub metric: f32,
    /// Share of event reports kept, clamped to 0.0..=1.0. Default 1.0.
    #[serde(default = "SamplingConfig::default_event")]
    pub event: f32,
    /// Share of healthcheck reports kept, clamped to 0.0..=1.0. Default 1.0.
    #[serde(default = "SamplingConfig::default_healthcheck")]
    pub healthcheck: f32,
}

impl SamplingConfig {
    fn default_error() -> f32 {
        1.0
    }

    fn default_metric() -> f32 {
        0.1
    }

    fn default_event() -> f32 {
        1.0
    }

    fn default_healthcheck() -> f32 {
        1.0
    }

    /// The rate for `kind`, clamped to 0.0..=1.0.
    pub fn rate_for(&self, kind: &ReportKind) -> f32 {
        match kind {
            ReportKind::Error => self.error,
            ReportKind::Metric => self.metric,
            ReportKind::Event => self.event,
            ReportKind::Healthcheck => self.healthcheck,
        }
        .clamp(0.0, 1.0)
    }
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            error: Self::default_error(),
            metric: Self::default_metric(),
            event: Self::default_event(),
            healthcheck: Self::default_healthcheck(),
        }
    }
}

/// Drops a report seen within `window_ms`, for the kinds switched on.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DedupeConfig {
    /// Switches dedupe on for the kinds flagged below. `default()` turns it
    /// on; a deserialized `dedupe` section that omits it leaves it off.
    #[serde(default)]
    pub enabled: bool,
    /// How long after its last sighting, in milliseconds, a report counts as
    /// a repeat; every sighting, dropped or not, starts it again. Default
    /// 5 seconds.
    #[serde(default = "DedupeConfig::default_window_ms")]
    pub window_ms: u64,
    /// Most sightings remembered; past it the oldest are forgotten first.
    /// Default 1024.
    #[serde(default = "DedupeConfig::default_max_entries")]
    pub max_entries: usize,
    /// Dedupe error reports. Default true.
    #[serde(default = "DedupeConfig::default_dedupe_error")]
    pub error: bool,
    /// Dedupe metric reports. Default false.
    #[serde(default = "DedupeConfig::default_dedupe_metric")]
    pub metric: bool,
    /// Dedupe event reports. Default false.
    #[serde(default = "DedupeConfig::default_dedupe_event")]
    pub event: bool,
    /// Dedupe healthcheck reports. Default false.
    #[serde(default = "DedupeConfig::default_dedupe_healthcheck")]
    pub healthcheck: bool,
}

impl DedupeConfig {
    fn default_window_ms() -> u64 {
        DEFAULT_DEDUPE_WINDOW_MS
    }

    fn default_max_entries() -> usize {
        DEFAULT_DEDUPE_MAX_ENTRIES
    }

    fn default_dedupe_error() -> bool {
        true
    }

    fn default_dedupe_metric() -> bool {
        false
    }

    fn default_dedupe_event() -> bool {
        false
    }

    fn default_dedupe_healthcheck() -> bool {
        false
    }
}

impl Default for DedupeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window_ms: DEFAULT_DEDUPE_WINDOW_MS,
            max_entries: DEFAULT_DEDUPE_MAX_ENTRIES,
            error: true,
            metric: false,
            event: false,
            healthcheck: false,
        }
    }
}

/// Per-event policies for metrics and events: a sampling rate of their own,
/// and windowed aggregation. None by default: they name the application's
/// sources and events.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MetricConfig {
    /// At most one per source and event: a later policy for the same pair
    /// replaces an earlier one.
    #[serde(default)]
    pub policies: Vec<MetricPolicy>,
}

/// Sampling and aggregation for the metric and event reports of one source
/// whose payload's `event` field names `event`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricPolicy {
    /// Source whose reports the policy covers.
    pub source: Source,
    /// Value of the payload's `event` field, as a string, that the policy covers.
    pub event: String,
    /// Share of reports kept, replacing the kind's rate in `SamplingConfig`;
    /// clamped to 0.0..=1.0. `None` keeps the kind's rate.
    #[serde(default)]
    pub sample_rate: Option<f32>,
    /// Aggregation over windows. It sees each report before dedupe, rate
    /// limits and sampling, which still decide whether the report itself is kept.
    #[serde(default)]
    pub aggregate: Option<MetricAggregateConfig>,
}

/// Aggregates the numeric `fields` (count, min, max, average, p50, p95 and a
/// reservoir of samples) and counts the values of the `group_by` fields over
/// `window_ms`, reported as one `metric_aggregate` report from `Source::PIPELINE`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricAggregateConfig {
    /// Length of a window, in milliseconds, from the first report it holds.
    /// Default 30 seconds.
    #[serde(default = "MetricAggregateConfig::default_window_ms")]
    pub window_ms: u64,
    /// Top-level payload fields aggregated as numbers; a report without a
    /// numeric value for one adds nothing to it.
    #[serde(default)]
    pub fields: Vec<String>,
    /// Top-level payload fields whose values are counted, each as a string; a
    /// value with no string form counts as `unknown`.
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Most distinct values counted per `group_by` field in a window; later
    /// new values are not counted. Default 64.
    #[serde(default = "MetricAggregateConfig::default_max_groups")]
    pub max_groups: usize,
    /// Size of each field's reservoir of samples, which p50 and p95 are read
    /// from by nearest rank (the ⌈p·n⌉-th smallest of the n samples); 0 keeps
    /// none. Default 128.
    #[serde(default = "MetricAggregateConfig::default_samples")]
    pub samples: usize,
}

impl MetricAggregateConfig {
    fn default_window_ms() -> u64 {
        30_000
    }

    /// The default for `samples`.
    pub fn default_samples() -> usize {
        128
    }

    /// The default for `max_groups`.
    pub fn default_max_groups() -> usize {
        64
    }
}
