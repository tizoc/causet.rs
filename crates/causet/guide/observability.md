The causet guide's Rust examples for its observability page, run as doc
tests (`cargo test -p causet --doc`). The guide's pages are in its own
repository and show each block here by the name above it.

<!-- snippet: custom-observer -->
```
use causet::guide::shop::*;
use causet::observers::{Execution, MessageView, Observer};

/// One line per execution: its ID, the execution that caused it, its kind
/// and where it ran.
#[derive(Default)]
struct Causes(Vec<String>);

impl Causes {
    fn line(&mut self, exec: &Execution<'_>) {
        let (id, cause) = (exec.meta.id.0, exec.meta.causal_execution_id().0);
        self.0.push(format!("{id} <- {cause} {} {}", exec.kind, exec.path));
    }
}

impl Observer for Causes {
    fn action_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) { self.line(exec) }
    fn effect_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) { self.line(exec) }
    fn event_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) { self.line(exec) }
}

let mut shop = Shop::open(StoreOptions::default());
shop.store.add_observer(Causes::default());
shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
shop.store.drain_events(16).unwrap();
assert_eq!(shop.store.observer::<Causes>().unwrap().0, [
    "1 <- 1 cart.add /cart",
    "2 <- 2 cart.checkout /cart",
    "3 <- 2 shop.pay /",
    "4 <- 3 payments.charge /payments",
    "5 <- 4 payments.charge /payments",
    "6 <- 6 payments.charged /payments",
    "7 <- 6 payments.settled /payments",
]);
```

<!-- snippet: why-chain -->
```
use causet::guide::shop::*;
use causet::observers::TraceLog;

let mut shop = Shop::open(StoreOptions::default());
shop.store.add_observer(TraceLog::new());
shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
shop.store.drain_events(16).unwrap();

// Why was the charge settled? Walk back from the last execution: an Event
// to the Effect it answers, anything else to its cause, up to a root input.
let entries = shop.store.observer::<TraceLog>().unwrap().entries();
let find = |id| entries.iter().find(|e| e.id == id).unwrap();
let mut why = Vec::new();
let mut step = entries.last().unwrap();
loop {
    why.push(step.kind);
    let back = step.answers.unwrap_or(step.causal);
    if back == step.id { break } // a root input
    step = find(back);
}
assert_eq!(why, [
    "payments.settled", "payments.charged", "payments.charge", "payments.charge",
    "shop.pay", "cart.checkout",
]);
```

<!-- snippet: log-attributes -->
```
use causet::{Action, Message};

#[derive(Debug, Action)]
#[causet(type = "withdrawal.action", log(level = "debug"))]
enum WithdrawalAction {
    /// Logged at info, with its amount only: the account stays out of the logs.
    #[causet(log(level = "info", fields(amount)))]
    Withdraw { account: String, amount: u64 },
    Cancel,
}

assert_eq!(WithdrawalAction::Cancel.kind(), "withdrawal.cancel");
```

<!-- snippet: logger-and-stats -->
```
use causet::guide::shop::*;
use causet::observers::{Logger, Stats};

let mut shop = Shop::open(StoreOptions::default());
shop.store.add_observer(Logger::new()); // one `tracing` event per execution
shop.store.add_observer(Stats::new());  // counts and durations, by kind
shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
shop.store.drain_events(16).unwrap();

let stats = shop.store.observer::<Stats>().unwrap().snapshot();
assert_eq!(stats.actions["payments.action"]["payments.charge"].calls(), 1);
assert_eq!(stats.events["payments.charged"]["payments.charged"].calls(), 1);
```

<!-- snippet: invariant-orders -->
```
use causet::guide::shop::*;
use causet::ExecutionId;
use causet::observers::{BugLog, Phase};

let mut shop = Shop::open(StoreOptions::default());
shop.store.add_observer(BugLog::new());
// Every order the shop took is paid, declined or still pending.
shop.store.add_invariant("every order is accounted for", |state: &ShopState, _| {
    let p = &state.payments;
    let charges = (p.paid.len() + p.declined.len() + p.pending.len()) as u64;
    if charges == state.orders { return Ok(()) }
    Err(format!("orders {}, charges {charges}", state.orders))
});
shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
shop.store.drain_events(16).unwrap();
shop.store.observer::<BugLog>().unwrap().assert_none();

// A charge sent straight to payments, with no order behind it.
shop.store.dispatch_to(&shop.payments, PaymentsAction::Charge { amount: 9 }).unwrap();
let report = &shop.store.observer::<BugLog>().unwrap().reports()[0];
assert_eq!(report.message, "orders 1, charges 2");
let place = report.place.as_ref().unwrap();
assert_eq!(place.phase, Phase::Invariant("every order is accounted for"));
assert_eq!(place.at, Some(ExecutionId(8))); // the root input that broke it

// The Store goes on, and the invariant is checked again after each root input.
shop.store.drain_events(16).unwrap();
assert_eq!(shop.store.observer::<BugLog>().unwrap().reports().len(), 2);
```

<!-- snippet: invariant-memory -->
```
use causet::guide::shop::*;
use causet::invariants::InvariantStep;
use causet::observers::BugLog;

/// No paid charge is ever forgotten. The checker remembers how many it has
/// seen, and reports each loss once, saying when it saw it.
fn never_forgets() -> impl FnMut(&ShopState, &InvariantStep<'_>) -> Result<(), String> + Send {
    let mut seen = 0;
    move |state, step| {
        let paid = state.payments.paid.len();
        let before = std::mem::replace(&mut seen, paid);
        if paid >= before { return Ok(()) }
        let when = match step {
            InvariantStep::Added => "when added".to_string(),
            InvariantStep::Restored => "on restore".to_string(),
            InvariantStep::Settled { kind, .. } => format!("after {kind}"),
        };
        Err(format!("{before} paid, then {paid} {when}"))
    }
}

let mut shop = Shop::open(StoreOptions::default());
shop.store.add_observer(BugLog::new());
shop.store.add_invariant("paid charges are kept", never_forgets());
for price in [4, 6] {
    shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price }).unwrap();
    shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
    shop.store.drain_events(16).unwrap();
}
shop.store.observer::<BugLog>().unwrap().assert_none();
```

<!-- snippet: invariant-restart -->
```
# use causet::guide::shop::*;
# use causet::hosting::EventBus;
# use causet::invariants::InvariantStep;
# fn never_forgets() -> impl FnMut(&ShopState, &InvariantStep<'_>) -> Result<(), String> + Send {
#     let mut seen = 0;
#     move |state, step| {
#         let paid = state.payments.paid.len();
#         let before = std::mem::replace(&mut seen, paid);
#         if paid >= before { return Ok(()) }
#         let when = match step {
#             InvariantStep::Added => "when added".to_string(),
#             InvariantStep::Restored => "on restore".to_string(),
#             InvariantStep::Settled { kind, .. } => format!("after {kind}"),
#         };
#         Err(format!("{before} paid, then {paid} {when}"))
#     }
# }
use causet::observers::BugLog;

let mut shop = Shop::open(StoreOptions::default());
shop.store.add_invariant("paid charges are kept", never_forgets());
let mut snapshots = Vec::new();
for price in [4, 6] {
    shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price }).unwrap();
    shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
    shop.store.drain_events(16).unwrap();
    snapshots.push(shop.store.state().clone());
}

// The Store is replaced, and its successor starts from a snapshot: here a
// stale one, taken before the second charge was paid. The invariants move
// across with what they remember.
let invariants = shop.store.take_invariants();
let services = ShopServices { gateway: Box::new(ApproveUpTo(100)) };
let state = snapshots.remove(0);
let (mut store, _sink) = Store::open(shop.store.protocol(), state, services, StoreOptions::default());
store.add_observer(BugLog::new());
store.restore_invariants(invariants);
store.observer::<BugLog>().unwrap().assert_messages(&["2 paid, then 1 on restore"]);
```

<!-- snippet: report-dedupe -->
```
use causet_reporting::{Config, Pipeline, Report, ReportKind, Source, payload_builder};

const PAYMENTS: Source = Source::new("payments");

/// A declined charge, as an error report deduplicated by its reason.
fn declined(amount: u64, reason: &str) -> Report {
    let (payload, dedupe) = payload_builder!({ event: "charge_declined", amount, reason })
        .finish_with_dedupe_key_fmt(format_args!("charge_declined:{reason}"));
    Report::new(ReportKind::Error, PAYMENTS, payload).with_dedupe_key(Some(dedupe))
}

let mut pipeline = Pipeline::new(Config::default());
// Times are milliseconds of the Store's clock, so a test chooses them.
let first = pipeline.accept_report(1_000, declined(30, "insufficient_funds"));
let again = pipeline.accept_report(1_200, declined(45, "insufficient_funds"));
let other = pipeline.accept_report(1_300, declined(10, "card_expired"));
assert!(first.record.is_some() && other.record.is_some());
assert!(again.record.is_none()); // the same error, within the dedupe window
assert_eq!((pipeline.stats.accepted, pipeline.stats.dropped_deduped), (2, 1));

// One JSON line, ready to dump or send.
let mut line = Vec::new();
first.record.unwrap().write_json(&mut line).unwrap();
let line = String::from_utf8(line).unwrap();
assert!(line.contains(r#""kind":"error","source":"payments""#));
assert!(line.contains(r#""event":"charge_declined","amount":30,"reason":"insufficient_funds""#));
```

<!-- snippet: report-aggregate -->
```
use causet_reporting::{
    Config, MetricAggregateConfig, MetricConfig, MetricPolicy, Pipeline, Report, ReportKind,
    Source, payload_builder,
};

const SHOP: Source = Source::new("shop");

let mut pipeline = Pipeline::new(Config {
    metrics: MetricConfig {
        policies: vec![MetricPolicy {
            source: SHOP,
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
});
for (at, (took_ms, ok)) in [(120u64, true), (80, true), (95, true), (300, false)].into_iter().enumerate() {
    let report = payload_builder!({ event: "checkout", took_ms, ok }).finish();
    let outcome = pipeline.accept_report(1_000 + at as u64, Report::new(ReportKind::Metric, SHOP, report));
    assert!(outcome.record.is_none());
}

// When the minute is up, one report sums it up.
let mut aggregates = Vec::new();
pipeline.flush_aggregates_into(61_000, &mut aggregates);
assert_eq!(aggregates.len(), 1);
let record = pipeline.accept_aggregate_report(61_000, aggregates.remove(0)).unwrap();
let mut line = Vec::new();
record.write_json(&mut line).unwrap();
let line = String::from_utf8(line).unwrap();
assert!(line.contains(r#""event":"metric_aggregate""#));
assert!(line.contains(r#""count":4,"min":80,"max":300,"avg":148.75"#));
assert!(line.contains(r#""groups":{"ok":{"false":1,"true":3}}"#));
```

<!-- snippet: report-service -->
```
use causet::*;
use causet_reporting::{Config, Pipeline, Record, Report, ReportKind, Source, payload_builder};

/// The reporting Service: the pipeline, and where its records go.
struct Reporting {
    pipeline: Pipeline,
    delivered: Vec<Record>,
}

#[derive(Debug, Action)]
#[causet(type = "orders.action", kind = "orders.fail")]
struct Fail {
    order: u64,
}

/// What went wrong, as data: the reducer describes it, the Service builds
/// the report.
#[derive(Debug, Effect)]
#[causet(type = "orders.report", kind = "orders.failed")]
struct Failed {
    order: u64,
}

struct Orders;

impl Component for Orders {
    type State = u32;
    type Service = Reporting;
    type Action = Fail;
    type Effect = Failed;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, Fail { order }: Fail, _: &ActionMeta) {
        *ctx.state_mut() += 1;
        ctx.into_emitter().emit_effect(Failed { order });
    }

    fn handle(mut ctx: EffectContext<'_, Self>, Failed { order }: Failed, meta: &EffectMeta) {
        let payload = payload_builder!({ event: "order_failed", order }).finish();
        let report = Report::new(ReportKind::Error, Source::new("orders"), payload);
        // Stamped with the execution's time, in milliseconds, never the system clock.
        let reporting = ctx.service();
        let outcome = reporting.pipeline.accept_report(meta.time / 1_000_000, report);
        reporting.delivered.extend(outcome.record);
    }
}

let reporting = Reporting { pipeline: Pipeline::new(Config::default()), delivered: Vec::new() };
let options = StoreOptions::with_clock(ManualClock::new(5_000_000_000));
let (mut store, _sink) = Store::start::<Orders>(0u32, reporting, options).unwrap();
let orders = store.mount(&root::<Orders>()).unwrap();
store.dispatch_to(&orders, Fail { order: 7 }).unwrap();

let delivered = &store.services().delivered;
assert_eq!(delivered.len(), 1);
assert_eq!(delivered[0].timestamp_ms, 5_000);
```
