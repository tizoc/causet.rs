//! The reporting pipeline wired into a Store.
//!
//! - Reducers emit compact report Effects: plain data, never a built report.
//! - A reporting Service owns the pipeline (dedupe, rate limits, sampling, metric
//!   aggregation), a context trail of recent steps, and the application's
//!   delivery: here, a JSON-lines dump and an in-memory outbox.
//! - Every report is built in one place, the `reports` module: one module
//!   per subsystem.
//! - Report times come from the Effect's scheduler time, never the system
//!   clock, and the execution stats from `causet::observers::Stats` go out as
//!   one more report.
//!
//! `cargo run -p causet-reporting --example reporting`

// An example program: its public items are not an API.
#![allow(missing_docs)]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use causet::observers::Stats;
use causet::{
    Action, ActionMeta, Component, ComponentContext, Effect, EffectContext, EffectMeta,
    ManualClock, Never, Store, StoreOptions, Timestamp, root,
};
use causet_reporting::{
    Config, ContextTrail, DumpConfig, MetricAggregateConfig, MetricPolicy, Pipeline, Record,
    Report, Source,
};

// ---------------------------------------------------------------------------
// Orders: the state machine, which reports through Effects.

pub struct Orders;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OrdersState {
    pub open: BTreeSet<u64>,
    pub shipped: u32,
    pub failed: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailReason {
    OutOfStock,
    PaymentDeclined,
}

#[derive(Debug, Action)]
#[causet(type = "orders.action")]
pub enum OrdersAction {
    Place { id: u64 },
    Ship { id: u64, took_ms: u64 },
    Fail { id: u64, reason: FailReason },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    Placed,
    Shipped,
}

/// What happened, as data. The reporting turns it into reports.
#[derive(Debug, Effect)]
#[causet(type = "orders.report", prefix = "orders.report")]
pub enum ReportEffect {
    /// A step worth keeping in the context trail.
    Step {
        order: u64,
        step: Step,
    },
    Checkout {
        took_ms: u64,
        ok: bool,
    },
    Failed {
        order: u64,
        reason: FailReason,
    },
}

impl Component for Orders {
    type State = OrdersState;
    type Service = Reporting;
    type Action = OrdersAction;
    type Effect = ReportEffect;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: OrdersAction, _: &ActionMeta) {
        match action {
            OrdersAction::Place { id } => {
                ctx.state_mut().open.insert(id);
                ctx.into_emitter().emit_effect(ReportEffect::Step {
                    order: id,
                    step: Step::Placed,
                });
            }
            OrdersAction::Ship { id, took_ms } => {
                ctx.state_mut().open.remove(&id);
                ctx.state_mut().shipped += 1;
                let mut out = ctx.into_emitter();
                out.emit_effect(ReportEffect::Step {
                    order: id,
                    step: Step::Shipped,
                });
                out.emit_effect(ReportEffect::Checkout { took_ms, ok: true });
            }
            OrdersAction::Fail { id, reason } => {
                ctx.state_mut().open.remove(&id);
                ctx.state_mut().failed += 1;
                let mut out = ctx.into_emitter();
                out.emit_effect(ReportEffect::Checkout {
                    took_ms: 0,
                    ok: false,
                });
                out.emit_effect(ReportEffect::Failed { order: id, reason });
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: ReportEffect, meta: &EffectMeta) {
        ctx.service().report(effect, meta.time);
    }
}

// ---------------------------------------------------------------------------
// The reporting Service: the pipeline, the trail and the application's delivery.

const ORDERS: Source = Source::new("orders");

#[derive(Clone, Copy, Debug)]
pub struct TrailEntry {
    order: u64,
    step: &'static str,
}

pub struct ReportingInner {
    pipeline: Pipeline,
    trail: ContextTrail<TrailEntry>,
    dump: causet_reporting::Filesystem,
    /// Delivered records as JSON lines: where an HTTP sender would post them.
    pub outbox: Vec<String>,
}

/// Shared between the Store's Service and the driver, which reports the
/// execution stats from outside.
#[derive(Clone)]
pub struct Reporting(Arc<Mutex<ReportingInner>>);

impl Reporting {
    fn new(dump_dir: &Path) -> Self {
        let config = Config {
            dump: DumpConfig {
                dir: dump_dir.to_path_buf(),
                ..DumpConfig::default()
            },
            metrics: causet_reporting::MetricConfig {
                policies: vec![MetricPolicy {
                    source: ORDERS,
                    event: "checkout".into(),
                    // Aggregated only: no checkout report goes out on its own.
                    sample_rate: Some(0.0),
                    aggregate: Some(MetricAggregateConfig {
                        window_ms: 60_000,
                        fields: vec!["took_ms".into()],
                        group_by: vec!["ok".into()],
                        max_groups: 4,
                        samples: 16,
                    }),
                }],
            },
            ..Config::default()
        };
        let dump = causet_reporting::Filesystem::new(config.dump.clone());
        Reporting(Arc::new(Mutex::new(ReportingInner {
            pipeline: Pipeline::new(config),
            trail: ContextTrail::new(8),
            dump,
            outbox: Vec::new(),
        })))
    }

    fn inner(&self) -> std::sync::MutexGuard<'_, ReportingInner> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn report(&mut self, effect: ReportEffect, time: Timestamp) {
        let now_ms = time / 1_000_000;
        let mut reporting = self.inner();
        let report = match effect {
            ReportEffect::Step { order, step } => {
                let step = match step {
                    Step::Placed => "placed",
                    Step::Shipped => "shipped",
                };
                reporting.trail.push(TrailEntry { order, step });
                return;
            }
            ReportEffect::Checkout { took_ms, ok } => reports::checkout(took_ms, ok),
            ReportEffect::Failed { order, reason } => {
                reports::failed(order, reason, &reporting.trail)
            }
        };
        reporting.accept(now_ms, report);
    }

    /// The execution stats, as one metric report.
    pub fn report_stats(&self, now_ms: u64, stats: &causet::observers::StatsSnapshot) {
        self.inner().accept(now_ms, reports::execution_stats(stats));
    }

    /// Flushes the metric aggregates whose window has passed by `now_ms`.
    pub fn flush(&self, now_ms: u64) {
        let mut reporting = self.inner();
        let mut aggregates = Vec::new();
        reporting
            .pipeline
            .flush_aggregates_into(now_ms, &mut aggregates);
        for report in aggregates {
            if let Some(record) = reporting.pipeline.accept_aggregate_report(now_ms, report) {
                reporting.deliver(&record);
            }
        }
    }

    pub fn snapshot(&self) -> (causet_reporting::AcceptStats, Vec<String>) {
        let reporting = self.inner();
        (reporting.pipeline.stats.clone(), reporting.outbox.clone())
    }
}

impl ReportingInner {
    fn accept(&mut self, now_ms: u64, report: Report) {
        let outcome = self.pipeline.accept_report(now_ms, report);
        for record in outcome.aggregate.iter().chain(outcome.record.iter()) {
            self.deliver(record);
        }
    }

    fn deliver(&mut self, record: &Record) {
        // Best effort: a failed dump never touches the state machine.
        let _ = self.dump.dump(record);
        let mut line = Vec::new();
        if record.write_json(&mut line).is_ok() {
            self.outbox
                .push(String::from_utf8_lossy(&line).into_owned());
        }
    }
}

/// Every report this application makes.
mod reports {
    use causet_reporting::{ContextTrail, Report, ReportKind, json, payload_builder};

    use super::{FailReason, ORDERS, TrailEntry};

    pub fn checkout(took_ms: u64, ok: bool) -> Report {
        let payload = payload_builder!({ event: "checkout", took_ms, ok }).finish();
        Report::new(ReportKind::Metric, ORDERS, payload)
    }

    /// An error, deduplicated by reason, with the last steps for context.
    pub fn failed(order: u64, reason: FailReason, trail: &ContextTrail<TrailEntry>) -> Report {
        let reason = match reason {
            FailReason::OutOfStock => "out_of_stock",
            FailReason::PaymentDeclined => "payment_declined",
        };
        let (payload, dedupe) = payload_builder!({ event: "order_failed", order, reason })
            .with_array_objects("trail", trail.iter_recent(4), |object, entry| {
                object
                    .with_field("order", entry.order)
                    .with_field("step", entry.step)
            })
            .finish_with_dedupe_key_fmt(format_args!("order_failed:{reason}"));
        Report::new(ReportKind::Error, ORDERS, payload).with_dedupe_key(Some(dedupe))
    }

    pub fn execution_stats(stats: &causet::observers::StatsSnapshot) -> Report {
        let payload =
            payload_builder!({ event: "execution_stats", actions: json(&stats.actions) }).finish();
        Report::new(ReportKind::Event, ORDERS, payload)
    }
}

// ---------------------------------------------------------------------------
// The run.

pub struct Run {
    pub live: OrdersState,
    pub accepted: causet_reporting::AcceptStats,
    pub outbox: Vec<String>,
    pub dumped_lines: usize,
}

const START_MS: u64 = 1_000_000;

pub fn run(dir: &Path) -> Result<Run, String> {
    let _ = std::fs::remove_dir_all(dir);
    let dumps = dir.join("reports");
    let reporting = Reporting::new(&dumps);
    // A standing clock keeps the report times in this run exact: every
    // execution is in the millisecond `START_MS`.
    let options = StoreOptions::with_clock(ManualClock::new(START_MS * 1_000_000));
    let (mut store, _sink) =
        Store::start::<Orders>(OrdersState::default(), reporting.clone(), options)
            .map_err(|e| format!("{e}"))?;
    store.add_observer(Stats::new());
    let orders = store
        .mount(&root::<Orders>())
        .map_err(|e| format!("{e:?}"))?;

    let mut send = |action| {
        store
            .dispatch_to(&orders, action)
            .map(|_| ())
            .map_err(|e| format!("{e:?}"))
    };
    for id in 1..=6 {
        send(OrdersAction::Place { id })?;
    }
    send(OrdersAction::Ship {
        id: 1,
        took_ms: 120,
    })?;
    send(OrdersAction::Ship { id: 2, took_ms: 80 })?;
    send(OrdersAction::Fail {
        id: 3,
        reason: FailReason::OutOfStock,
    })?;
    send(OrdersAction::Ship {
        id: 4,
        took_ms: 200,
    })?;
    // The same reason again, within the dedupe window: dropped.
    send(OrdersAction::Fail {
        id: 5,
        reason: FailReason::OutOfStock,
    })?;
    send(OrdersAction::Fail {
        id: 6,
        reason: FailReason::PaymentDeclined,
    })?;

    let live = store.state().clone();
    // Stats and aggregates go out once the aggregation window has passed.
    let later_ms = START_MS + 61_000;
    reporting.report_stats(
        later_ms,
        &store.observer::<Stats>().expect("installed").snapshot(),
    );
    reporting.flush(later_ms);
    let (accepted, outbox) = reporting.snapshot();
    let dumped = std::fs::read_to_string(dumps.join("reports.jsonl")).map_err(|e| e.to_string())?;
    Ok(Run {
        live,
        accepted,
        outbox,
        dumped_lines: dumped.lines().count(),
    })
}

fn main() {
    let dir = std::env::temp_dir().join(format!("causet-example-reporting-{}", std::process::id()));
    let run = run(&dir).unwrap_or_else(|e| panic!("{e}"));
    println!("orders: {:?}", run.live);
    println!("reporting: {:?}", run.accepted);
    for line in &run.outbox {
        println!("  {line}");
    }
    println!("{} records dumped", run.dumped_lines);
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reporting_run_reports_what_it_should() {
        let dir = std::env::temp_dir().join(format!(
            "causet-example-reporting-test-{}",
            std::process::id()
        ));
        let run = run(&dir).unwrap();

        assert_eq!((run.live.shipped, run.live.failed), (3, 3));

        // Two distinct failures, the stats and one checkout aggregate went
        // out; the second out-of-stock failure was deduplicated, and every
        // checkout report was kept for aggregation only.
        assert_eq!(run.accepted.accepted, 4, "{:?}", run.accepted);
        assert_eq!(run.accepted.dropped_deduped, 1);
        assert_eq!(run.accepted.dropped_sampled, 6);
        assert_eq!(run.outbox.len(), 4);
        assert_eq!(run.dumped_lines, 4);

        let failed: Vec<_> = run
            .outbox
            .iter()
            .filter(|l| l.contains(r#""event":"order_failed""#))
            .collect();
        assert_eq!(failed.len(), 2);
        // The first failure carries the last four steps, oldest first.
        let trail = r#""trail":[{"order":5,"step":"placed"},{"order":6,"step":"placed"},{"order":1,"step":"shipped"},{"order":2,"step":"shipped"}]"#;
        assert!(failed[0].contains(trail), "{}", failed[0]);

        let aggregate = run
            .outbox
            .iter()
            .find(|l| l.contains("metric_aggregate"))
            .expect("an aggregate");
        assert!(aggregate.contains(r#""took_ms":{"count":6"#), "{aggregate}");
        assert!(
            aggregate.contains(r#""ok":{"false":3,"true":3}"#),
            "{aggregate}"
        );
        assert!(
            run.outbox
                .iter()
                .any(|l| l.contains(r#""event":"execution_stats""#))
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
