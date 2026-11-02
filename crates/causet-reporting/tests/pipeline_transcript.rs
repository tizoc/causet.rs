//! A 3,000-report run, pinned record for record: the records, aggregates,
//! flushed aggregates and counts the pipeline makes of reports from a fixed
//! generator. The OCaml port reproduces the same transcript
//! (`ocaml/test/reporting/pipeline_transcript.txt` there, a copy of
//! `tests/pipeline_transcript.txt` here, made by the same generator).
//!
//! To write the transcript again after a deliberate change to the output:
//!
//! ```sh
//! CAUSET_WRITE_TRANSCRIPT=1 cargo test -p causet-reporting --test pipeline_transcript
//! ```
//!
//! then copy the file to the OCaml port's tests.

use std::path::PathBuf;

use causet_reporting::{
    AcceptStats, Config, DedupeConfig, MetricAggregateConfig, MetricConfig, MetricPolicy,
    PayloadBuilder, Pipeline, RateLimitConfig, Record, Report, ReportKind, SamplingConfig, Source,
};

/// The generator's random numbers: a 64-bit LCG, read from its top 31 bits.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize]
    }
}

const SOURCES: [Source; 3] = [
    Source::new("net"),
    Source::new("disk"),
    Source::new("pipeline-ish"),
];
const PEERS: [&str; 5] = ["a", "b", "c", "d", "e\"q\\\n"];
const FLOATS: [f64; 10] = [
    0.1,
    2.0,
    -3.5,
    1e21,
    1e-7,
    123.456,
    0.0,
    -0.0,
    1.0 / 3.0,
    f64::INFINITY,
];

/// One report. Every draw is made in the order the OCaml generator makes it.
fn generated(rng: &mut Lcg) -> Report {
    let kind = match rng.below(4) {
        0 => ReportKind::Error,
        1 => ReportKind::Metric,
        2 => ReportKind::Event,
        _ => ReportKind::Healthcheck,
    };
    let source = rng.pick(&SOURCES);
    let event = rng.pick(&["latency", "drop", "boot", "tick"]);
    let mut b = PayloadBuilder::new(event);
    if rng.below(2) == 0 {
        b = b.with_field("value", rng.below(20));
    }
    if rng.below(3) == 0 {
        b = b.with_field("size", -(rng.below(5) as i64));
    }
    if rng.below(3) == 0 {
        b = b.with_field("ratio", rng.pick(&FLOATS));
    }
    if rng.below(2) == 0 {
        b = b.with_field("peer", rng.pick(&PEERS));
    }
    if rng.below(3) == 0 {
        b = b.with_field("ok", rng.below(2) == 0);
    }
    if rng.below(4) == 0 {
        b = b.with_field("reason", rng.pick(&["full", "slow"]));
    }
    if rng.below(4) == 0 {
        let n = rng.below(3);
        b = b.with_section("detail", |s| {
            let s = s.with_field("n", n);
            if n == 2 {
                s.with_section("inner", |i| i.with_field("deep", true))
            } else {
                s
            }
        });
    }
    if rng.below(5) == 0 {
        let k = rng.below(4);
        b = b.with_array_values("list", (0..k).map(|i| i * 3));
    }
    if rng.below(6) == 0 {
        let k = rng.below(3);
        b = b.with_array_objects("items", 0..k, |o, i| o.with_field("i", i));
    }
    let (payload, dedupe_key) = if rng.below(5) == 0 {
        let (payload, key) = b.finish_with_dedupe_key(rng.pick(&["k1", "k2"]));
        (payload, Some(key))
    } else {
        (b.finish(), None)
    };
    Report::new(kind, source, payload).with_dedupe_key(dedupe_key)
}

fn line(tag: &str, record: Option<Record>) -> String {
    match record {
        None => format!("{tag} -"),
        Some(mut record) => {
            record.run_id = None;
            let mut json = Vec::new();
            record.write_json(&mut json).expect("write record");
            format!("{tag} {}", String::from_utf8(json).expect("utf-8"))
        }
    }
}

fn config() -> Config {
    Config {
        dedupe: DedupeConfig {
            enabled: true,
            window_ms: 50,
            max_entries: 8,
            error: true,
            metric: true,
            event: false,
            healthcheck: true,
        },
        rate_limit: RateLimitConfig {
            window_ms: 100,
            max_per_window: 12,
        },
        sampling: SamplingConfig {
            error: 0.7,
            metric: 0.3,
            event: 0.5,
            healthcheck: 1.0,
        },
        metrics: MetricConfig {
            policies: vec![
                MetricPolicy {
                    source: Source::new("net"),
                    event: "latency".to_owned(),
                    sample_rate: Some(0.25),
                    aggregate: Some(MetricAggregateConfig {
                        window_ms: 200,
                        fields: vec!["value".to_owned(), "size".to_owned(), "ratio".to_owned()],
                        group_by: vec!["peer".to_owned(), "ok".to_owned()],
                        max_groups: 3,
                        samples: 4,
                    }),
                },
                MetricPolicy {
                    source: Source::new("disk"),
                    event: "drop".to_owned(),
                    sample_rate: None,
                    aggregate: Some(MetricAggregateConfig {
                        window_ms: 100,
                        fields: Vec::new(),
                        group_by: vec!["reason".to_owned(), "detail".to_owned()],
                        max_groups: 8,
                        samples: 0,
                    }),
                },
                MetricPolicy {
                    source: Source::new("disk"),
                    event: "tick".to_owned(),
                    sample_rate: Some(0.9),
                    aggregate: None,
                },
            ],
        },
        ..Config::default()
    }
}

fn transcript() -> String {
    let mut rng = Lcg(7);
    let mut pipeline = Pipeline::new(config());
    let mut out = Vec::new();
    let mut flushed = Vec::new();
    let mut now = 0;
    for step in 0..3000 {
        now += rng.below(8);
        let outcome = pipeline.accept_report(now, generated(&mut rng));
        out.push(line("R", outcome.record));
        if outcome.aggregate.is_some() {
            out.push(line("A", outcome.aggregate));
        }
        if step % 97 == 0 {
            pipeline.flush_aggregates_into(now, &mut flushed);
            for report in flushed.drain(..) {
                out.push(line("F", pipeline.accept_aggregate_report(now, report)));
            }
        }
    }
    let AcceptStats {
        accepted,
        dropped_disabled,
        dropped_rate_limited,
        dropped_deduped,
        dropped_sampled,
        dropped_invalid,
    } = pipeline.stats;
    out.push(format!(
        "S {accepted} {dropped_disabled} {dropped_rate_limited} {dropped_deduped} \
         {dropped_sampled} {dropped_invalid}"
    ));
    let mut text = out.join("\n");
    text.push('\n');
    text
}

#[test]
fn the_transcript_is_reproduced() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/pipeline_transcript.txt");
    let ours = transcript();
    if std::env::var_os("CAUSET_WRITE_TRANSCRIPT").is_some() {
        std::fs::write(&path, &ours).expect("write transcript");
        return;
    }
    let pinned = std::fs::read_to_string(&path).expect("read transcript");
    let pinned: Vec<&str> = pinned.lines().collect();
    let ours: Vec<&str> = ours.lines().collect();
    assert_eq!(pinned.len(), ours.len(), "line count");
    for (i, (pinned, ours)) in pinned.iter().zip(&ours).enumerate() {
        assert_eq!(pinned, ours, "line {}", i + 1);
    }
}
