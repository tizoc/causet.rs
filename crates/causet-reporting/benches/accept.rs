//! What one report costs: building it, and building and accepting it.
//!
//! Run with `cargo bench -p causet-reporting --bench accept`. Each scenario
//! runs several rounds and reports the fastest round's time and allocations
//! per report. A counting global allocator measures the allocations.
//!
//! - `build`: an error report with a dedupe key, dropped at once.
//! - `kept`: the same report accepted by a default pipeline with dedupe off;
//!   the rate limit's window is 0, so every report is kept.
//! - `deduplicated`: the same report, repeated, accepted by a default
//!   pipeline: all but the first are dropped as repeats.
//! - `aggregated`: a metric report whose policy samples none of them
//!   (`sample_rate` 0) and aggregates `took_ms` grouped by `ok`, with the
//!   default window (30 s) and reservoir (128 samples); the rate limit's
//!   window is 0, so every report reaches sampling.
//!
//! Time advances 1 ms per report. `CAUSET_BENCH_ROUNDS=<n>` sets the rounds
//! and `CAUSET_BENCH_REPORTS=<n>` the reports per round.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use causet_reporting::{
    Config, MetricAggregateConfig, MetricPolicy, PayloadBuilder, Pipeline, Report, ReportKind,
    Source,
};

struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const SOURCE: Source = Source::new("p2p_rpc");

fn error_report() -> Report {
    let (payload, key) = PayloadBuilder::new("rpc_failed")
        .with_field("peer", "12D3KooWQk")
        .with_field("method", "get_block")
        .with_field("code", 503u64)
        .with_field("took_ms", 120u64)
        .finish_with_dedupe_key("rpc_failed:get_block");
    Report::new(ReportKind::Error, SOURCE, payload).with_dedupe_key(Some(key))
}

fn metric_report(i: u64) -> Report {
    let payload = PayloadBuilder::new("rpc")
        .with_field("method", "get_block")
        .with_field("took_ms", 80 + (i * 37) % 400)
        .with_field("ok", !i.is_multiple_of(7))
        .finish();
    Report::new(ReportKind::Metric, SOURCE, payload)
}

fn kept_config() -> Config {
    let mut config = Config::default();
    config.dedupe.enabled = false;
    config.rate_limit.window_ms = 0;
    config
}

fn aggregated_config() -> Config {
    let mut config = Config::default();
    config.rate_limit.window_ms = 0;
    config.metrics.policies.push(MetricPolicy {
        source: SOURCE,
        event: "rpc".to_owned(),
        sample_rate: Some(0.0),
        aggregate: Some(MetricAggregateConfig {
            window_ms: 30_000,
            fields: vec!["took_ms".to_owned()],
            group_by: vec!["ok".to_owned()],
            max_groups: MetricAggregateConfig::default_max_groups(),
            samples: MetricAggregateConfig::default_samples(),
        }),
    });
    config
}

/// The fastest of `rounds` rounds of `reports` calls of `step`, as
/// nanoseconds and allocations per call.
fn measure(rounds: usize, reports: u64, mut step: impl FnMut(u64)) -> (f64, f64) {
    for i in 0..reports.min(10_000) {
        step(i);
    }
    let mut best = (f64::INFINITY, 0.0);
    let mut next = reports.min(10_000);
    for _ in 0..rounds {
        let allocs = ALLOCS.load(Ordering::Relaxed);
        let start = Instant::now();
        for i in next..next + reports {
            step(i);
        }
        let ns = start.elapsed().as_nanos() as f64 / reports as f64;
        let allocs = (ALLOCS.load(Ordering::Relaxed) - allocs) as f64 / reports as f64;
        next += reports;
        if ns < best.0 {
            best = (ns, allocs);
        }
    }
    best
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let rounds = env("CAUSET_BENCH_ROUNDS", 7) as usize;
    let reports = env("CAUSET_BENCH_REPORTS", 1_000_000);
    let filter = std::env::var("CAUSET_BENCH").unwrap_or_default();
    let run = |name: &str, result: &dyn Fn() -> (f64, f64)| {
        if name.contains(filter.as_str()) {
            let (ns, allocs) = result();
            println!("{name:<14} {ns:>8.1} ns/report {allocs:>8.3} allocations/report");
        }
    };

    run("build", &|| {
        measure(rounds, reports, |_| drop(black_box(error_report())))
    });
    run("kept", &|| {
        let mut pipeline = Pipeline::new(kept_config());
        let result = measure(rounds, reports, |i| {
            black_box(pipeline.accept_report(i, error_report()));
        });
        assert_eq!(pipeline.stats.accepted, pipeline.stats_total());
        result
    });
    run("deduplicated", &|| {
        let mut pipeline = Pipeline::new(Config::default());
        let result = measure(rounds, reports, |i| {
            black_box(pipeline.accept_report(i, error_report()));
        });
        assert_eq!(pipeline.stats.accepted, 1);
        assert_eq!(pipeline.stats.dropped_deduped + 1, pipeline.stats_total());
        result
    });
    run("aggregated", &|| {
        let mut pipeline = Pipeline::new(aggregated_config());
        let result = measure(rounds, reports, |i| {
            black_box(pipeline.accept_report(i, metric_report(i)));
        });
        assert_eq!(
            pipeline.stats.dropped_sampled + pipeline.stats.accepted,
            pipeline.stats_total()
        );
        result
    });
}

trait Total {
    fn stats_total(&self) -> u64;
}

impl Total for Pipeline {
    /// Every report counted, kept or dropped.
    fn stats_total(&self) -> u64 {
        let stats = &self.stats;
        stats.accepted
            + stats.dropped_disabled
            + stats.dropped_rate_limited
            + stats.dropped_deduped
            + stats.dropped_sampled
            + stats.dropped_invalid
    }
}
