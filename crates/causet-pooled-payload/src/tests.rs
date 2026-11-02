#![allow(clippy::indexing_slicing)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::Serialize;

use crate::{Payload, PoolConfig, PoolSet, display, json, pool_record, pool_record_object};

const MAX_POOL_RECORD_PAYLOAD_BYTES: usize = 128;

const _: () = {
    assert!(size_of::<Payload>() <= MAX_POOL_RECORD_PAYLOAD_BYTES);
};

// Global allocator wrapper that counts allocations while a thread-local flag is enabled.
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

#[derive(Debug)]
struct DisplayThing(u32);

impl std::fmt::Display for DisplayThing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "thing-{}", self.0)
    }
}

#[test]
fn write_json_embeds_raw_json() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_field("raw", json(&serde_json::json!({ "a": 1 })))
        .with_field("note", "ok")
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let text = String::from_utf8(buf).expect("utf8");

    let parsed: serde_json::Value = serde_json::from_str(&text).expect("valid json");
    assert_eq!(parsed["raw"]["a"], 1);
    assert_eq!(parsed["note"], "ok");
}

#[test]
fn display_values_render_to_pool() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_field("name", display(&DisplayThing(7)))
        .with_field("plain", "value")
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["name"], "thing-7");
    assert_eq!(parsed["plain"], "value");
}

#[test]
fn arrays_and_sections_are_serialized() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_section("section", |section| section.with_field("inner", "value"))
        .with_array("items", |array| {
            array.push_value(1u64);
            array.push_object(|object| object.with_field("k", "v"));
        })
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["section"]["inner"], "value");
    assert_eq!(parsed["items"][0], 1);
    assert_eq!(parsed["items"][1]["k"], "v");
}

#[test]
fn nested_arrays_are_serialized() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_array("matrix", |array| {
            array.push_array(|inner| {
                inner.push_value(1u64);
                inner.push_value(2u64);
            });
            array.push_array(|inner| {
                inner.push_value(3u64);
            });
        })
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["matrix"][0][0], 1);
    assert_eq!(parsed["matrix"][0][1], 2);
    assert_eq!(parsed["matrix"][1][0], 3);
}

#[derive(Serialize)]
struct JsonThing {
    value: u64,
}

struct FailingSerialize;

impl Serialize for FailingSerialize {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("boom"))
    }
}

#[test]
fn json_adapter_serializes_structs() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_field("payload", json(&JsonThing { value: 7 }))
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["payload"]["value"], 7);
}

#[test]
fn json_adapter_failure_is_marked() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_field("payload", json(&FailingSerialize))
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["payload"], "serialize_failed");
}

#[test]
fn json_adapter_failure_is_marked_in_serde_output() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_field("payload", json(&FailingSerialize))
        .finish();

    let serialized = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(serialized["payload"], "serialize_failed");
}

#[test]
fn pooled_clone_is_allocation_free_and_releases_on_last_drop() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool.payload().with_field("k", "v").finish();
    assert_eq!(pool.stats().free, 0);

    let clone = {
        let mut cloned = None;
        assert_no_alloc(|| {
            cloned = Some(payload.clone());
        });
        cloned.expect("clone captured")
    };

    assert_eq!(pool.stats().free, 0);
    drop(payload);
    assert_eq!(pool.stats().free, 0);
    drop(clone);
    assert_eq!(pool.stats().free, 1);
}

#[test]
fn with_field_display_does_not_allocate() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool.payload();
    assert_no_alloc(|| {
        let _payload = payload.with_field("name", display(&DisplayThing(42)));
    });
}

#[test]
fn with_array_values_does_not_allocate() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool.payload();
    assert_no_alloc(move || {
        let _payload = payload.with_array("values", |array| {
            array.push_value(1u64);
            array.push_value(2u64);
        });
    });
}

#[test]
fn write_json_does_not_allocate() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_field("name", display(&DisplayThing(1)))
        .with_field("count", 2u64)
        .finish();

    let mut buf = Vec::with_capacity(1024);
    assert_no_alloc(|| {
        payload.write_json(&mut buf).expect("write json");
    });
}

#[test]
fn repeated_payloads_reuse_pool_without_allocations() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let warmup = pool
        .payload()
        .with_field("name", "value")
        .with_field("count", 1u64)
        .finish();
    drop(warmup);

    assert_no_alloc(|| {
        for _ in 0..100 {
            let payload = pool
                .payload()
                .with_field("name", "value")
                .with_field("count", 1u64)
                .finish();
            drop(payload);
        }
    });
}

#[test]
fn pool_reuse_after_drop_returns_buffer() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });

    let payload = pool.payload().with_field("name", "value");
    drop(payload);

    let stats = pool.stats();
    assert_eq!(stats.total, 1);
    assert_eq!(stats.free, 1);
}

#[test]
fn pool_grows_then_falls_back() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 2,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });

    let _payload_a = pool.payload().with_field("name", "a");
    let _payload_b = pool.payload().with_field("name", "b");
    let _payload_c = pool.payload().with_field("name", "c");

    let stats = pool.stats();
    assert_eq!(stats.total, 2);
    assert_eq!(stats.grows, 1);
    assert_eq!(stats.fallbacks, 1);
}

#[test]
fn pool_reclaims_all_buffers_after_saturation() {
    let pool = PoolSet::new(PoolConfig {
        initial: 2,
        max: 4,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });

    let payloads: Vec<_> = (0..4)
        .map(|idx| pool.payload().with_field("name", format!("v{idx}")))
        .collect();
    drop(payloads);

    let stats = pool.stats();
    assert_eq!(stats.total, 4);
    assert_eq!(stats.free, 4);
}

#[test]
fn pool_lease_drop_across_threads_returns_buffer() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });

    let payload = pool.payload().with_field("name", "value");
    std::thread::spawn(move || drop(payload))
        .join()
        .expect("thread");

    let stats = pool.stats();
    assert_eq!(stats.free, 1);
}

#[test]
fn payload_size_budget() {
    assert!(
        size_of::<Payload>() <= MAX_POOL_RECORD_PAYLOAD_BYTES,
        "Payload is too large"
    );
}

#[test]
fn serde_round_trip_preserves_shape() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_field("name", "value")
        .with_section("meta", |section| section.with_field("count", 2u64))
        .with_array("items", |array| {
            array.push_value(1u64);
            array.push_object(|object| object.with_field("k", "v"));
        })
        .with_section("flags", |flags| flags.with_field("extra_a", true))
        .finish();

    let serialized = serde_json::to_value(&payload).expect("serialize");
    let deserialized: Payload = serde_json::from_value(serialized.clone()).expect("deserialize");
    let reserialized = serde_json::to_value(&deserialized).expect("serialize");
    assert_eq!(serialized, reserialized);
}

#[test]
fn write_json_escapes_control_characters() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_field("text", "line1\nline2\t\"quote\"\u{0000}")
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["text"], "line1\nline2\t\"quote\"\u{0000}");
}

#[test]
fn macro_builds_nested_payload() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });

    let payload = pool_record!(&pool, {
        event: "node_start",
        version: "1.2.3",
        meta: { env: "test", count: 2u64 },
        items: [1u64, { key: "value" }],
        matrix: [[1u64, 2u64], [3u64]],
    });

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["event"], "node_start");
    assert_eq!(parsed["version"], "1.2.3");
    assert_eq!(parsed["meta"]["env"], "test");
    assert_eq!(parsed["meta"]["count"], 2);
    assert_eq!(parsed["items"][0], 1);
    assert_eq!(parsed["items"][1]["key"], "value");
    assert_eq!(parsed["matrix"][0][0], 1);
    assert_eq!(parsed["matrix"][0][1], 2);
    assert_eq!(parsed["matrix"][1][0], 3);
}

#[test]
fn macro_optional_fields_include_only_present_values() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });

    let payload = pool_record!(&pool, {
        event: "node_start",
        tx_seq?: Some(42u64),
        tx_added_at?: None::<u64>,
        meta: {
            count?: Some(2u64),
            dropped?: None::<u64>,
        },
        items: [
            {
                present?: Some(1u64),
                missing?: None::<u64>,
            }
        ],
    });

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["tx_seq"], 42);
    assert!(parsed.get("tx_added_at").is_none());
    assert_eq!(parsed["meta"]["count"], 2);
    assert!(parsed["meta"].get("dropped").is_none());
    assert_eq!(parsed["items"][0]["present"], 1);
    assert!(parsed["items"][0].get("missing").is_none());
}

#[test]
fn macro_optional_fields_support_dynamic_keys() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let key_present = "tx_seq";
    let key_missing = "tx_added_at";
    let section_name = "meta";
    let nested_present = "count";
    let nested_missing = "dropped";

    let payload = pool_record!(&pool, {
        event: "node_start",
        [key_present]?: Some(7u64),
        [key_missing]?: None::<u64>,
        [section_name]: {
            [nested_present]?: Some(2u64),
            [nested_missing]?: None::<u64>,
        },
    });

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["tx_seq"], 7);
    assert!(parsed.get("tx_added_at").is_none());
    assert_eq!(parsed["meta"]["count"], 2);
    assert!(parsed["meta"].get("dropped").is_none());
}

#[test]
fn macro_field_shorthand_uses_ident_value() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let ready = 3usize;
    let connecting = 1usize;
    let payload = pool_record!(&pool, {
        event: "snapshot",
        peers: {
            ready,
            connecting,
        },
        ready,
    });

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["event"], "snapshot");
    assert_eq!(parsed["peers"]["ready"], 3);
    assert_eq!(parsed["peers"]["connecting"], 1);
    assert_eq!(parsed["ready"], 3);
}

#[test]
fn macro_optional_field_shorthand_uses_ident_option() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let tx_seq = Some(7u64);
    let tx_added_at: Option<u64> = None;
    let payload = pool_record!(&pool, {
        event: "snapshot",
        tx_seq?,
        tx_added_at?,
    });

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["event"], "snapshot");
    assert_eq!(parsed["tx_seq"], 7);
    assert!(parsed.get("tx_added_at").is_none());
}

#[test]
fn pool_record_object_macro_matches_fluent_builder() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let section_name = "meta";
    let nested_key = "count";
    let array_name = "items";
    let nested_present = "present";
    let nested_missing = "missing";

    let macro_payload = pool_record_object!(pool.payload(), {
        event: "node_start",
        [section_name]: {
            [nested_key]: 2u64,
            dropped?: None::<u64>,
        },
        [array_name]: [
            {
                [nested_present]?: Some(1u64),
                [nested_missing]?: None::<u64>,
            },
            3u64,
        ],
    })
    .finish();
    let fluent_payload = pool
        .payload()
        .with_field("event", "node_start")
        .with_section("meta", |section| {
            section
                .with_field("count", 2u64)
                .with_field_opt::<u64>("dropped", None)
        })
        .with_array("items", |array| {
            array.push_object(|object| {
                object
                    .with_field_opt("present", Some(1u64))
                    .with_field_opt::<u64>("missing", None)
            });
            array.push_value(3u64);
        })
        .finish();

    let macro_value = serde_json::to_value(&macro_payload).expect("macro payload");
    let fluent_value = serde_json::to_value(&fluent_payload).expect("fluent payload");
    assert_eq!(macro_value, fluent_value);
}

#[test]
fn large_string_value_grows_pool() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 16,
        on_exhausted: None,
    });
    let long_value = "x".repeat(1024);
    let payload = pool
        .payload()
        .with_field("value", long_value.as_str())
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["value"].as_str().unwrap().len(), 1024);
}

#[test]
fn object_builder_with_field_opt_skips_none() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_section("section", |section| {
            section
                .with_field_opt("present", Some(1u64))
                .with_field_opt::<u64>("missing", None)
        })
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["section"]["present"], 1);
    assert!(parsed["section"].get("missing").is_none());
}

#[test]
fn object_builder_combinators_build_expected_shape() {
    let pool = PoolSet::new(PoolConfig {
        initial: 1,
        max: 1,
        buf_capacity: 4 * 1024,
        on_exhausted: None,
    });
    let payload = pool
        .payload()
        .with_section("section", |section| {
            section
                .with_section_fields("counts", [("ok", 3u64), ("err", 1u64)])
                .with_section_objects(
                    "stats",
                    [("latency", (10u64, 42u64))],
                    |object, (min, max)| object.with_field("min", min).with_field("max", max),
                )
                .with_array_values("values", [1u64, 2u64, 3u64])
                .with_array_objects(
                    "items",
                    [("peer-a", Some(12u64)), ("peer-b", None)],
                    |object, (peer, rtt_ms)| {
                        object
                            .with_field("peer", peer)
                            .with_field_opt("rtt_ms", rtt_ms)
                    },
                )
        })
        .finish();

    let mut buf = Vec::new();
    payload.write_json(&mut buf).expect("write json");
    let parsed: serde_json::Value = serde_json::from_slice(&buf).expect("valid json");
    assert_eq!(parsed["section"]["counts"]["ok"], 3);
    assert_eq!(parsed["section"]["counts"]["err"], 1);
    assert_eq!(parsed["section"]["stats"]["latency"]["min"], 10);
    assert_eq!(parsed["section"]["stats"]["latency"]["max"], 42);
    assert_eq!(parsed["section"]["values"][0], 1);
    assert_eq!(parsed["section"]["values"][1], 2);
    assert_eq!(parsed["section"]["values"][2], 3);
    assert_eq!(parsed["section"]["items"][0]["peer"], "peer-a");
    assert_eq!(parsed["section"]["items"][0]["rtt_ms"], 12);
    assert_eq!(parsed["section"]["items"][1]["peer"], "peer-b");
    assert!(parsed["section"]["items"][1].get("rtt_ms").is_none());
}
