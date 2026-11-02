use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::filesystem::Filesystem;
use crate::types::{Report, ReportKind, Source};
use crate::{
    Config, DedupeConfig, DumpConfig, MetricAggregateConfig, MetricConfig, MetricPolicy, Pipeline,
    RateLimitConfig, SamplingConfig,
};

fn ts_millis(ms: u64) -> u64 {
    ms
}

fn mk_report(kind: ReportKind, source: Source) -> Report {
    Report::new(
        kind,
        source,
        crate::payload!({
            msg: "test",
        }),
    )
}

fn mk_metric_report(source: Source, event: &str, value: u64) -> Report {
    Report::new(
        ReportKind::Metric,
        source,
        crate::payload!({
            event: event.to_string(),
            value: value,
        }),
    )
}

fn config_enabled() -> Config {
    Config {
        enabled: true,
        dedupe: DedupeConfig {
            enabled: true,
            window_ms: 0,
            max_entries: 1024,
            error: true,
            metric: true,
            event: true,
            healthcheck: true,
        },
        sampling: SamplingConfig {
            error: 1.0,
            metric: 1.0,
            event: 1.0,
            healthcheck: 1.0,
        },
        ..Config::default()
    }
}

#[derive(Debug)]
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let mut path = std::env::temp_dir();
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("causet-reporting-{prefix}-{}-{id}", std::process::id());
        path.push(name);
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[test]
fn dedupe_drops_duplicate_within_window() {
    let mut config = config_enabled();
    config.dedupe.window_ms = 10;
    config.dedupe.max_entries = 16;

    let mut state = Pipeline::new(config);

    let first = state.accept_report(
        ts_millis(0),
        mk_report(ReportKind::Metric, Source::new("test")),
    );
    assert!(first.record.is_some(), "first report should be accepted");

    let second = state.accept_report(
        ts_millis(5),
        mk_report(ReportKind::Metric, Source::new("test")),
    );
    assert!(
        second.record.is_none(),
        "duplicate report within window should be dropped"
    );

    let third = state.accept_report(
        ts_millis(20),
        mk_report(ReportKind::Metric, Source::new("test")),
    );
    assert!(
        third.record.is_some(),
        "report after window should be accepted"
    );
}

#[test]
fn rate_limit_drops_after_max_per_window() {
    let mut config = config_enabled();
    config.rate_limit = RateLimitConfig {
        window_ms: 100,
        max_per_window: 1,
    };
    config.dedupe.window_ms = 0;

    let mut state = Pipeline::new(config);

    let first = state.accept_report(
        ts_millis(0),
        mk_report(ReportKind::Metric, Source::new("test")),
    );
    assert!(first.record.is_some(), "first report should be accepted");

    let second = state.accept_report(
        ts_millis(10),
        mk_report(ReportKind::Metric, Source::new("test")),
    );
    assert!(
        second.record.is_none(),
        "second report within window should be dropped"
    );

    let third = state.accept_report(
        ts_millis(200),
        mk_report(ReportKind::Metric, Source::new("test")),
    );
    assert!(
        third.record.is_some(),
        "report after window should be accepted"
    );
}

#[test]
fn sampling_respects_rate() {
    let mut config = config_enabled();
    config.dedupe.window_ms = 0;
    config.dedupe.error = false;
    config.rate_limit = RateLimitConfig {
        window_ms: 1_000,
        max_per_window: 10,
    };
    config.sampling = SamplingConfig {
        error: 0.0,
        metric: 1.0,
        event: 1.0,
        healthcheck: 1.0,
    };

    let mut state = Pipeline::new(config);
    let res = state.accept_report(
        ts_millis(0),
        mk_report(ReportKind::Error, Source::new("test")),
    );
    assert!(res.record.is_none(), "sampling rate 0 should drop");

    let mut config = config_enabled();
    config.dedupe.window_ms = 0;
    config.dedupe.error = false;
    config.rate_limit = RateLimitConfig {
        window_ms: 1_000,
        max_per_window: 10,
    };
    let mut state = Pipeline::new(config);
    let res = state.accept_report(
        ts_millis(0),
        mk_report(ReportKind::Error, Source::new("test")),
    );
    assert!(res.record.is_some(), "sampling rate 1 should accept");
}

#[test]
fn metric_policy_overrides_sampling() {
    let mut config = config_enabled();
    config.dedupe.window_ms = 0;
    config.rate_limit = RateLimitConfig {
        window_ms: 1_000,
        max_per_window: 10,
    };
    config.sampling.metric = 0.0;
    config.metrics = MetricConfig {
        policies: vec![MetricPolicy {
            source: Source::new("test"),
            event: "latency".to_owned(),
            sample_rate: Some(1.0),
            aggregate: None,
        }],
    };

    let mut state = Pipeline::new(config);
    let res = state.accept_report(
        ts_millis(0),
        mk_metric_report(Source::new("test"), "latency", 10),
    );
    assert!(
        res.record.is_some(),
        "metric policy sample_rate should override defaults"
    );
}

#[test]
fn metric_aggregate_emits_summary() {
    let mut config = config_enabled();
    config.dedupe.window_ms = 0;
    config.rate_limit = RateLimitConfig {
        window_ms: 1_000,
        max_per_window: 10,
    };
    config.sampling.metric = 0.0;
    config.metrics = MetricConfig {
        policies: vec![MetricPolicy {
            source: Source::new("test"),
            event: "latency".to_owned(),
            sample_rate: Some(0.0),
            aggregate: Some(MetricAggregateConfig {
                window_ms: 10,
                fields: vec!["value".to_owned()],
                group_by: Vec::new(),
                max_groups: 64,
                samples: 4,
            }),
        }],
    };

    let mut state = Pipeline::new(config);
    let res = state.accept_report(
        ts_millis(0),
        mk_metric_report(Source::new("test"), "latency", 10),
    );
    assert!(res.record.is_none());
    assert!(res.aggregate.is_none());

    let res = state.accept_report(
        ts_millis(5),
        mk_metric_report(Source::new("test"), "latency", 10),
    );
    assert!(res.record.is_none());
    assert!(res.aggregate.is_none());

    let res = state.accept_report(
        ts_millis(12),
        mk_metric_report(Source::new("test"), "latency", 10),
    );
    assert!(res.record.is_none());
    assert!(res.aggregate.is_some(), "expected aggregate report");
    let payload = &res.aggregate.as_ref().unwrap().report.payload;
    assert_eq!(payload.event_str(), Some("metric_aggregate"));
    let metric = payload.section("metric").expect("metric section");
    assert_eq!(
        metric
            .field_value("source")
            .and_then(|value| payload.value_as_string(value))
            .as_deref(),
        Some("test")
    );
    assert_eq!(
        metric
            .field_value("event")
            .and_then(|value| payload.value_as_string(value))
            .as_deref(),
        Some("latency")
    );
    let fields = payload.section("fields").expect("fields section");
    let value = fields.section("value").expect("value aggregate");
    assert!(matches!(
        value.field_value("count"),
        Some(crate::types::Value::U64(2))
    ));
}

#[test]
fn metric_aggregate_flushes_on_timeout() {
    let mut config = config_enabled();
    config.dedupe.window_ms = 0;
    config.rate_limit = RateLimitConfig {
        window_ms: 1_000,
        max_per_window: 10,
    };
    config.sampling.metric = 0.0;
    config.metrics = MetricConfig {
        policies: vec![MetricPolicy {
            source: Source::new("test"),
            event: "latency".to_owned(),
            sample_rate: Some(0.0),
            aggregate: Some(MetricAggregateConfig {
                window_ms: 10,
                fields: vec!["value".to_owned()],
                group_by: Vec::new(),
                max_groups: 64,
                samples: 4,
            }),
        }],
    };

    let mut state = Pipeline::new(config);
    let res = state.accept_report(
        ts_millis(0),
        mk_metric_report(Source::new("test"), "latency", 10),
    );
    assert!(res.aggregate.is_none());
    let res = state.accept_report(
        ts_millis(5),
        mk_metric_report(Source::new("test"), "latency", 10),
    );
    assert!(res.aggregate.is_none());

    let mut flushed = Vec::new();
    state.flush_aggregates_into(ts_millis(12), &mut flushed);
    assert_eq!(flushed.len(), 1, "expected flushed aggregate");
    let payload = &flushed[0].payload;
    let fields = payload.section("fields").expect("fields section");
    let value = fields.section("value").expect("value aggregate");
    assert!(matches!(
        value.field_value("count"),
        Some(crate::types::Value::U64(2))
    ));

    flushed.clear();
    state.flush_aggregates_into(ts_millis(25), &mut flushed);
    assert!(flushed.is_empty());
}

#[test]
fn metric_aggregate_groups_by_reason() {
    let mut config = config_enabled();
    config.dedupe.window_ms = 0;
    config.rate_limit = RateLimitConfig {
        window_ms: 1_000,
        max_per_window: 10,
    };
    config.sampling.metric = 0.0;
    config.metrics = MetricConfig {
        policies: vec![MetricPolicy {
            source: Source::new("block_producer"),
            event: "won_slot_discarded".to_owned(),
            sample_rate: Some(0.0),
            aggregate: Some(MetricAggregateConfig {
                window_ms: 10,
                fields: Vec::new(),
                group_by: vec!["reason".to_owned()],
                max_groups: 8,
                samples: 0,
            }),
        }],
    };

    let mut state = Pipeline::new(config);
    let report_a = Report::new(
        ReportKind::Metric,
        Source::new("block_producer"),
        crate::payload!({
            event: "won_slot_discarded",
            reason: "BestTipSuperior",
        }),
    );
    let report_b = Report::new(
        ReportKind::Metric,
        Source::new("block_producer"),
        crate::payload!({
            event: "won_slot_discarded",
            reason: "BestTipSuperior",
        }),
    );
    let report_c = Report::new(
        ReportKind::Metric,
        Source::new("block_producer"),
        crate::payload!({
            event: "won_slot_discarded",
            reason: "Timeout",
        }),
    );

    let res = state.accept_report(ts_millis(0), report_a);
    assert!(res.aggregate.is_none());
    let res = state.accept_report(ts_millis(5), report_b);
    assert!(res.aggregate.is_none());
    let res = state.accept_report(ts_millis(12), report_c);
    assert!(res.aggregate.is_some());
    let payload = &res.aggregate.as_ref().unwrap().report.payload;
    let groups = payload.section("groups").expect("groups section");
    let reason = groups.section("reason").expect("reason group");
    assert!(matches!(
        reason.field_value("BestTipSuperior"),
        Some(crate::types::Value::U64(2))
    ));
    assert!(reason.field_value("Timeout").is_none());
}

/// The aggregate's percentiles are nearest-rank: the ⌈p·n⌉-th smallest
/// sample. Of 120, 80, 95 and 300 that is 95 for p50 and 300 for p95; the
/// lower sample (120) was reported for p95 before.
#[test]
fn metric_aggregate_percentiles_are_nearest_rank() {
    let mut config = config_enabled();
    config.dedupe.enabled = false;
    config.metrics = MetricConfig {
        policies: vec![MetricPolicy {
            source: Source::new("rpc"),
            event: "call".to_owned(),
            sample_rate: Some(0.0),
            aggregate: Some(MetricAggregateConfig {
                window_ms: 10,
                fields: vec!["took_ms".to_owned()],
                group_by: vec!["ok".to_owned()],
                max_groups: 8,
                samples: 128,
            }),
        }],
    };

    let mut state = Pipeline::new(config);
    for (i, took_ms) in [120u64, 80, 95, 300].into_iter().enumerate() {
        let ok = i != 1;
        let report = Report::new(
            ReportKind::Metric,
            Source::new("rpc"),
            crate::payload!({
                event: "call",
                took_ms: took_ms,
                ok: ok,
            }),
        );
        let res = state.accept_report(ts_millis(i as u64), report);
        assert!(res.record.is_none() && res.aggregate.is_none());
    }

    let mut flushed = Vec::new();
    state.flush_aggregates_into(ts_millis(10), &mut flushed);
    assert_eq!(flushed.len(), 1);
    let payload = &flushed[0].payload;
    let took_ms = payload
        .section("fields")
        .and_then(|fields| fields.section("took_ms"))
        .expect("took_ms aggregate");
    let number = |key: &str| match took_ms.field_value(key) {
        Some(crate::types::Value::F64(value)) => *value,
        other => panic!("{key}: {other:?}"),
    };
    assert_eq!(number("min"), 80.0);
    assert_eq!(number("max"), 300.0);
    assert_eq!(number("p50"), 95.0);
    assert_eq!(number("p95"), 300.0);
    let ok = payload
        .section("groups")
        .and_then(|groups| groups.section("ok"))
        .expect("ok group");
    assert!(matches!(
        ok.field_value("true"),
        Some(crate::types::Value::U64(3))
    ));
    assert!(matches!(
        ok.field_value("false"),
        Some(crate::types::Value::U64(1))
    ));
}

/// A NaN sample sorts after every number, as in the other ports: of 120,
/// NaN, 80 and 300, p50 is 120 and p95 the NaN. Min and max skip it.
#[test]
fn metric_aggregate_sorts_nan_last() {
    let mut config = config_enabled();
    config.dedupe.enabled = false;
    config.metrics = MetricConfig {
        policies: vec![MetricPolicy {
            source: Source::new("rpc"),
            event: "call".to_owned(),
            sample_rate: Some(0.0),
            aggregate: Some(MetricAggregateConfig {
                window_ms: 10,
                fields: vec!["took_ms".to_owned()],
                group_by: vec![],
                max_groups: 8,
                samples: 128,
            }),
        }],
    };

    let mut state = Pipeline::new(config);
    for (i, took_ms) in [120.0, f64::NAN, 80.0, 300.0].into_iter().enumerate() {
        let report = Report::new(
            ReportKind::Metric,
            Source::new("rpc"),
            crate::payload!({ event: "call", took_ms: took_ms }),
        );
        state.accept_report(ts_millis(i as u64), report);
    }

    let mut flushed = Vec::new();
    state.flush_aggregates_into(ts_millis(10), &mut flushed);
    let took_ms = flushed[0]
        .payload
        .section("fields")
        .and_then(|fields| fields.section("took_ms"))
        .expect("took_ms aggregate");
    let number = |key: &str| match took_ms.field_value(key) {
        Some(crate::types::Value::F64(value)) => *value,
        other => panic!("{key}: {other:?}"),
    };
    assert_eq!(number("min"), 80.0);
    assert_eq!(number("max"), 300.0);
    assert_eq!(number("p50"), 120.0);
    assert!(number("p95").is_nan());
}

#[test]
fn percentiles_of_few_samples() {
    use crate::pipeline::percentile;
    assert_eq!(percentile(&[], 50), None);
    assert_eq!(percentile(&[7.0], 50), Some(7.0));
    assert_eq!(percentile(&[7.0], 95), Some(7.0));
    assert_eq!(percentile(&[1.0, 2.0], 50), Some(1.0));
    assert_eq!(percentile(&[1.0, 2.0], 95), Some(2.0));
    assert_eq!(percentile(&[80.0, 95.0, 120.0, 300.0], 50), Some(95.0));
    assert_eq!(percentile(&[80.0, 95.0, 120.0, 300.0], 95), Some(300.0));
    let twenty: Vec<f64> = (1..=20).map(f64::from).collect();
    assert_eq!(percentile(&twenty, 95), Some(19.0));
    assert_eq!(percentile(&twenty, 0), Some(1.0));
    assert_eq!(percentile(&twenty, 100), Some(20.0));
}

#[test]
fn filesystem_dump_writes_jsonl() {
    let temp = TempDir::new("reports");
    let config = DumpConfig {
        enabled: true,
        dir: temp.path.clone(),
        filename: "reports.jsonl".to_owned(),
        max_file_size_bytes: 0,
    };
    let fs = Filesystem::new(config);
    let record = crate::Record {
        id: 1,
        timestamp_ms: 123,
        report: Report::new(
            ReportKind::Metric,
            Source::new("test"),
            crate::payload!({
                value: 1u64,
            }),
        ),
        run_id: None,
    };

    fs.dump(&record).expect("dump succeeds");

    let path = temp.path.join("reports.jsonl");
    let contents = std::fs::read_to_string(path).expect("read dump");
    assert!(
        contents.ends_with('\n'),
        "jsonl dump should end with newline"
    );
    let line = contents.trim_end();
    let parsed: serde_json::Value = serde_json::from_str(line).expect("valid json line");
    assert_eq!(parsed["source"], "test");
    assert_eq!(parsed["payload"]["value"], 1);
}

#[test]
fn filesystem_dump_rotates_on_size() {
    let temp = TempDir::new("reports-rotate");
    let config = DumpConfig {
        enabled: true,
        dir: temp.path.clone(),
        filename: "reports.jsonl".to_owned(),
        max_file_size_bytes: 1,
    };
    let fs = Filesystem::new(config);
    let record1 = crate::Record {
        id: 1,
        timestamp_ms: 100,
        report: Report::new(
            ReportKind::Metric,
            Source::new("test"),
            crate::payload!({
                value: 1u64,
            }),
        ),
        run_id: None,
    };
    let record2 = crate::Record {
        id: 2,
        timestamp_ms: 200,
        report: Report::new(
            ReportKind::Metric,
            Source::new("test"),
            crate::payload!({
                value: 2u64,
            }),
        ),
        run_id: None,
    };

    fs.dump(&record1).expect("first dump succeeds");
    fs.dump(&record2).expect("second dump succeeds");

    let mut files = std::fs::read_dir(&temp.path)
        .expect("read dir")
        .filter_map(|entry| {
            entry
                .ok()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
        })
        .collect::<Vec<_>>();
    files.sort();

    assert_eq!(files.len(), 2, "expected rotated + active file");
    assert!(files.iter().any(|name| name == "reports.jsonl"));
    let rotated_name = format!("reports-{}.jsonl", record2.timestamp_ms);
    assert!(files.iter().any(|name| name == &rotated_name));
}
