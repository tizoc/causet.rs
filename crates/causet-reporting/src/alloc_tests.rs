use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{
    Config, MetricAggregateConfig, MetricPolicy, PayloadBuilder, Pipeline, Report, ReportKind,
    Source,
};

// Test allocator that counts allocations while a thread-local flag is enabled.
pub(crate) struct TrackingAlloc;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static TRACK_ALLOC: Cell<bool> = const { Cell::new(false) };
}

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if TRACK_ALLOC.with(|flag| flag.get()) {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if TRACK_ALLOC.with(|flag| flag.get()) {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }
}

fn assert_no_alloc(f: impl FnOnce()) {
    let before = ALLOC_COUNT.load(Ordering::Relaxed);
    TRACK_ALLOC.with(|flag| {
        let prev = flag.replace(true);
        f();
        flag.set(prev);
    });
    let after = ALLOC_COUNT.load(Ordering::Relaxed);
    assert_eq!(after, before, "expected no allocations during call");
}

#[test]
fn report_construction_is_alloc_free() {
    let _warmup = PayloadBuilder::new("warmup")
        .with_field("value", 1u64)
        .finish();

    assert_no_alloc(|| {
        for _ in 0..8 {
            let payload = PayloadBuilder::new("event")
                .with_field("value", 42u64)
                .with_field("label", "ok")
                .finish();
            let _report = Report::new(ReportKind::Metric, Source::new("test"), payload);
        }
    });
}

#[test]
fn accept_report_is_alloc_free_after_warmup() {
    let mut config = Config {
        enabled: true,
        ..Config::default()
    };
    config.dedupe.enabled = false;
    config.rate_limit.window_ms = 0;
    config.rate_limit.max_per_window = 10;
    config.sampling.error = 1.0;
    config.sampling.metric = 1.0;
    config.sampling.healthcheck = 1.0;

    let mut pipeline = Pipeline::new(config);

    let warmup = Report::new(
        ReportKind::Error,
        Source::new("test"),
        crate::payload!({
            event: "warmup",
        }),
    );
    let _ = pipeline.accept_report(0, warmup);

    assert_no_alloc(|| {
        let report = Report::new(
            ReportKind::Error,
            Source::new("test"),
            crate::payload!({
                event: "test",
            }),
        );
        let _ = pipeline.accept_report(1, report);
    });
}

#[test]
fn deduplicating_a_report_is_alloc_free_after_warmup() {
    let mut config = Config::default();
    config.dedupe.max_entries = 4;
    let mut pipeline = Pipeline::new(config);
    let report = || {
        let (payload, key) = PayloadBuilder::new("rpc_failed")
            .with_field("peer", "a")
            .finish_with_dedupe_key("rpc_failed:a");
        Report::new(ReportKind::Error, Source::new("test"), payload).with_dedupe_key(Some(key))
    };
    for now in 0..8 {
        let _ = pipeline.accept_report(now, report());
    }

    assert_no_alloc(|| {
        for now in 8..16 {
            let outcome = pipeline.accept_report(now, report());
            assert!(outcome.record.is_none());
        }
    });
    assert_eq!(pipeline.stats.dropped_deduped, 15);
}

/// A metric report a policy aggregates, once its window holds the field, the
/// group's value and a full reservoir: only a report that closes the window
/// allocates, for the aggregate it reports.
#[test]
fn aggregating_a_metric_is_alloc_free_after_warmup() {
    let mut config = Config::default();
    config.rate_limit.window_ms = 0;
    config.metrics.policies.push(MetricPolicy {
        source: Source::new("rpc"),
        event: "call".to_owned(),
        sample_rate: Some(0.0),
        aggregate: Some(MetricAggregateConfig {
            window_ms: 1_000,
            fields: vec!["took_ms".to_owned()],
            group_by: vec!["ok".to_owned(), "took_ms".to_owned()],
            max_groups: 4,
            samples: 4,
        }),
    });
    let mut pipeline = Pipeline::new(config);
    let report = |i: u64| {
        Report::new(
            ReportKind::Metric,
            Source::new("rpc"),
            PayloadBuilder::new("call")
                .with_field("took_ms", 80 + i * 7)
                .with_field("ok", !i.is_multiple_of(3))
                .finish(),
        )
    };
    for now in 0..16 {
        let _ = pipeline.accept_report(now, report(now));
    }

    assert_no_alloc(|| {
        for now in 16..64 {
            let outcome = pipeline.accept_report(now, report(now));
            assert!(outcome.record.is_none() && outcome.aggregate.is_none());
        }
    });
    assert_eq!(pipeline.stats.dropped_sampled, 64);
}

#[test]
fn section_combinators_build_expected_payload() {
    let payload = PayloadBuilder::new("metric")
        .with_section_fields("counts", [("ok", 3u64), ("err", 1u64)])
        .with_section_objects(
            "stats",
            [("latency", (10u64, 42u64)), ("proofs", (1u64, 2u64))],
            |object, (min, max)| object.with_field("min", min).with_field("max", max),
        )
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["event"], "metric");
    assert_eq!(parsed["counts"]["ok"], 3);
    assert_eq!(parsed["counts"]["err"], 1);
    assert_eq!(parsed["stats"]["latency"]["min"], 10);
    assert_eq!(parsed["stats"]["latency"]["max"], 42);
    assert_eq!(parsed["stats"]["proofs"]["min"], 1);
    assert_eq!(parsed["stats"]["proofs"]["max"], 2);
}

#[test]
fn array_combinators_build_expected_payload() {
    let payload = PayloadBuilder::new("metric")
        .with_array_values("counts", [1u64, 2u64, 3u64])
        .with_array_objects(
            "peers",
            [("peer-a", Some(12u64)), ("peer-b", None)],
            |object, (peer_id, rtt_ms)| {
                crate::payload_object!(object, {
                    peer_id: peer_id,
                    rtt_ms?: rtt_ms,
                })
            },
        )
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["event"], "metric");
    assert_eq!(parsed["counts"][0], 1);
    assert_eq!(parsed["counts"][1], 2);
    assert_eq!(parsed["counts"][2], 3);
    assert_eq!(parsed["peers"][0]["peer_id"], "peer-a");
    assert_eq!(parsed["peers"][0]["rtt_ms"], 12);
    assert_eq!(parsed["peers"][1]["peer_id"], "peer-b");
    assert!(parsed["peers"][1].get("rtt_ms").is_none());
}

#[test]
fn payload_object_macro_supports_builder_closures() {
    let dynamic_key = "label";
    let payload = crate::payload_builder!({
        event: "metric",
    })
    .with_section("meta", |section| {
        crate::payload_object!(section, {
            [dynamic_key]: "ready",
            present?: Some(1u64),
            missing?: None::<u64>,
        })
    })
    .with_array_objects(
        "trail",
        [("accept", Some(5u64)), ("compact", None)],
        |object, entry| {
            let (kind, count) = entry;
            crate::payload_object!(object, {
                kind: kind,
                count?: count,
            })
        },
    )
    .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["meta"]["label"], "ready");
    assert_eq!(parsed["meta"]["present"], 1);
    assert!(parsed["meta"].get("missing").is_none());
    assert_eq!(parsed["trail"][0]["kind"], "accept");
    assert_eq!(parsed["trail"][0]["count"], 5);
    assert_eq!(parsed["trail"][1]["kind"], "compact");
    assert!(parsed["trail"][1].get("count").is_none());
}

#[test]
fn optional_field_helpers_include_only_present_values() {
    let mut payload = PayloadBuilder::new("metric")
        .with_field_opt("present_with", Some(7u64))
        .with_field_opt::<u64>("missing_with", None);
    payload.insert_field_opt("present_insert", Some(9u64));
    payload.insert_field_opt::<u64>("missing_insert", None);
    let payload = payload.finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["present_with"], 7);
    assert_eq!(parsed["present_insert"], 9);
    assert!(parsed.get("missing_with").is_none());
    assert!(parsed.get("missing_insert").is_none());
}

#[test]
fn payload_builder_macro_supports_optional_fields_and_chaining() {
    let payload = crate::payload_builder!({
        event: "metric",
        tx_seq?: Some(11u64),
        dep_seq?: None::<u64>,
        meta: {
            present?: Some(1u64),
            missing?: None::<u64>,
        },
    })
    .with_field("tail", true)
    .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["event"], "metric");
    assert_eq!(parsed["tx_seq"], 11);
    assert!(parsed.get("dep_seq").is_none());
    assert_eq!(parsed["meta"]["present"], 1);
    assert!(parsed["meta"].get("missing").is_none());
    assert_eq!(parsed["tail"], true);
}

#[test]
fn payload_macro_supports_optional_fields() {
    let payload = crate::payload!({
        event: "metric",
        present?: Some(3u64),
        missing?: None::<u64>,
    });

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["event"], "metric");
    assert_eq!(parsed["present"], 3);
    assert!(parsed.get("missing").is_none());
}

#[test]
fn payload_macro_supports_dynamic_optional_keys() {
    let key_present = "tx_seq";
    let key_missing = "tx_added_at";
    let payload = crate::payload!({
        event: "metric",
        [key_present]?: Some(5u64),
        [key_missing]?: None::<u64>,
    });

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["event"], "metric");
    assert_eq!(parsed["tx_seq"], 5);
    assert!(parsed.get("tx_added_at").is_none());
}

#[test]
fn payload_macro_supports_field_shorthand() {
    let ready = 4usize;
    let connecting = 2usize;
    let payload = crate::payload!({
        event: "p2p_snapshot",
        peers: {
            ready,
            connecting,
        },
        ready,
    });

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["event"], "p2p_snapshot");
    assert_eq!(parsed["peers"]["ready"], 4);
    assert_eq!(parsed["peers"]["connecting"], 2);
    assert_eq!(parsed["ready"], 4);
}

#[test]
fn payload_macro_supports_optional_field_shorthand() {
    let tx_seq = Some(11u64);
    let tx_added_at: Option<u64> = None;
    let payload = crate::payload!({
        event: "metric",
        tx_seq?,
        tx_added_at?,
    });

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["event"], "metric");
    assert_eq!(parsed["tx_seq"], 11);
    assert!(parsed.get("tx_added_at").is_none());
}

#[test]
fn finish_with_dedupe_key_helpers_attach_readable_key() {
    let (payload, dedupe_key) = crate::payload_builder!({
        event: "metric",
        value: 7u64,
    })
    .finish_with_dedupe_key("metric:7");
    assert_eq!(dedupe_key.as_str(&payload), Some("metric:7"));

    let value = 9u64;
    let (payload, dedupe_key) = crate::payload_builder!({
        event: "metric",
        value: value,
    })
    .finish_with_dedupe_key_fmt(format_args!("metric:{}", value));
    assert_eq!(dedupe_key.as_str(&payload), Some("metric:9"));
}
