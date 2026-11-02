//! Reporting pipeline edges found in review: dedupe hashes of nested and
//! sibling objects, repeats within one millisecond, whole JSON lines from
//! cloned writers, and sources containing the key separator.

use std::fs;
use std::sync::{Arc, Barrier};
use std::thread;

use causet_reporting::{
    Config, DedupeConfig, DedupeKey, DumpConfig, Filesystem, Payload, PayloadBuilder, Pipeline,
    Record, Report, ReportKind, Source,
};

const SOURCE: Source = Source::new("review");

fn error(payload: Payload) -> Report {
    Report::new(ReportKind::Error, SOURCE, payload)
}

#[test]
fn nested_and_sibling_objects_must_not_have_the_same_dedupe_hash() {
    let siblings = PayloadBuilder::new("shape")
        .with_section("x", |x| x.with_field("value", 1u64))
        .with_section("y", |y| y.with_field("value", 2u64))
        .finish();
    let nested = PayloadBuilder::new("shape")
        .with_section("x", |x| {
            x.with_field("value", 1u64)
                .with_section("y", |y| y.with_field("value", 2u64))
        })
        .finish();
    assert_ne!(
        serde_json::to_value(&siblings).unwrap(),
        serde_json::to_value(&nested).unwrap()
    );
    let mut pipeline = Pipeline::default();
    assert!(
        pipeline
            .accept_report(100, error(siblings))
            .record
            .is_some()
    );
    let second = pipeline.accept_report(101, error(nested));
    assert!(
        second.record.is_some(),
        "distinct nested content was deduplicated: {:?}",
        pipeline.stats
    );
}

#[test]
fn repeats_in_one_millisecond_do_not_evict_the_current_dedupe_entry() {
    let config = Config {
        dedupe: DedupeConfig {
            max_entries: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut pipeline = Pipeline::new(config);
    let accepted: Vec<_> = (0..4)
        .map(|_| {
            pipeline
                .accept_report(100, error(PayloadBuilder::new("same").finish()))
                .record
                .is_some()
        })
        .collect();
    assert_eq!(
        accepted,
        [true, false, false, false],
        "a recent duplicate escaped dedupe"
    );
}

#[test]
fn cloned_filesystem_writers_keep_each_json_line_whole() {
    const WRITERS: usize = 8;
    const EACH: usize = 4;
    let dir = std::env::temp_dir().join(format!(
        "causet-concurrent-dump-review-{}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).unwrap();
    let filesystem = Arc::new(Filesystem::new(DumpConfig {
        dir: dir.clone(),
        max_file_size_bytes: 0,
        ..Default::default()
    }));
    let record = Record {
        id: 1,
        timestamp_ms: 100,
        report: error(
            PayloadBuilder::new("concurrent")
                .with_field("body", "x".repeat(1024))
                .finish(),
        ),
        run_id: None,
    };
    let barrier = Arc::new(Barrier::new(WRITERS));
    let handles: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let (filesystem, barrier, mut record) = (
                Arc::clone(&filesystem),
                Arc::clone(&barrier),
                record.clone(),
            );
            thread::spawn(move || {
                barrier.wait();
                for index in 0..EACH {
                    record.id = (writer * EACH + index + 1) as u64;
                    filesystem.dump(&record).unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let text = fs::read_to_string(dir.join("reports.jsonl")).unwrap();
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(lines.len(), WRITERS * EACH);
    let invalid = lines
        .iter()
        .filter(|line| serde_json::from_str::<serde_json::Value>(line).is_err())
        .count();
    assert_eq!(
        invalid, 0,
        "concurrent writes produced malformed JSON lines"
    );
}

#[test]
fn a_source_containing_the_separator_does_not_collide() {
    // Joined with "|", both read "error|a|e|x|e|y".
    let report = |source: &'static str, key: &'static str| {
        Report::new(
            ReportKind::Error,
            Source::new(source),
            PayloadBuilder::new("e").finish(),
        )
        .with_dedupe_key(Some(DedupeKey::Static(key.into())))
    };
    let mut pipeline = Pipeline::default();
    assert!(
        pipeline
            .accept_report(100, report("a", "x|e|y"))
            .record
            .is_some()
    );
    assert!(
        pipeline
            .accept_report(101, report("a|e|x", "y"))
            .record
            .is_some(),
        "a different source and key were deduplicated"
    );
}
