//! `causet-observers`: the `tracing` logger, the scheduler-time log macros,
//! and execution statistics on one combined timeline.

mod common;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use causet::ExecutionId;
use causet::observers::{Category, Execution, InlineCall, Observer, Refusal, RefusedBy, SentFrom};
use causet::observers::{DurationRanges, Logger, Stats, TARGET};
use causet::{Action, ManualClock, StoreOptions};
use causet::{ExecMeta, InstanceId};
use common::*;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

// ---------------------------------------------------------------------------
// A subscriber that keeps every event.

#[derive(Debug)]
struct Captured {
    target: String,
    level: Level,
    fields: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Captured>>>);

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
}

impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        let metadata = event.metadata();
        self.0.lock().unwrap().push(Captured {
            target: metadata.target().into(),
            level: *metadata.level(),
            fields,
        });
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

fn captured(f: impl FnOnce()) -> Vec<Captured> {
    let capture = Capture::default();
    tracing::subscriber::with_default(capture.clone(), f);
    std::mem::take(&mut *capture.0.lock().unwrap())
}

fn field<'a>(event: &'a Captured, name: &str) -> &'a str {
    event.fields.get(name).map_or("<missing>", String::as_str)
}

// ---------------------------------------------------------------------------
// Logger

#[test]
fn every_execution_is_one_event_with_its_metadata() {
    let clock = ManualClock::new(1000);
    let options = StoreOptions::with_clock(clock);
    let mut f = fixture_with(Box::new(Inline), 4, options);
    f.store.add_observer(Logger::with_label("node-1"));

    let events = captured(|| {
        f.store
            .dispatch_to(&f.a, DownloaderAction::Fetch("alpha".into()))
            .unwrap();
        f.store.drain_events(16).unwrap();
    });

    let kinds: Vec<_> = events
        .iter()
        .map(|e| (field(e, "category"), field(e, "kind")))
        .collect();
    assert_eq!(
        kinds,
        [
            ("action", "downloader.fetch"),
            ("effect", "downloader.effect.fetch"),
            ("event", "download.done"),
            ("action", "downloader.completed"),
            ("action", "downloader.finished"),
        ]
    );
    assert!(
        events
            .iter()
            .all(|e| e.target == TARGET && e.level == Level::DEBUG)
    );
    assert!(
        events
            .iter()
            .all(|e| field(e, "label") == "node-1" && !e.fields.contains_key("fields"))
    );

    // The clock stands at 1000: every execution is at 1000, numbered from 1.
    let fetch = &events[0];
    assert_eq!(
        [
            field(fetch, "time"),
            field(fetch, "id"),
            field(fetch, "causal")
        ],
        ["1000", "1", "1"]
    );
    assert_eq!(
        [
            field(fetch, "type"),
            field(fetch, "path"),
            field(fetch, "depth")
        ],
        ["downloader.action", "/a", "1"]
    );
    assert!(!fetch.fields.contains_key("answers"));
    let done = &events[2];
    assert_eq!(field(done, "answers"), field(&events[1], "id"));
}

#[derive(Debug, Action)]
#[causet(type = "peer.action")]
enum PeerAction {
    #[causet(log(level = "warn", fields(peer, display(addr))))]
    Dropped { peer: u32, addr: String },
}

#[test]
fn a_message_chooses_its_level_and_fields() {
    let meta = ExecMeta {
        id: ExecutionId(7),
        depth: 1,
        time: 7,
        origin: None,
    };
    let path: Arc<str> = Arc::from("/peers");
    let exec = Execution {
        category: Category::Action,
        path: &path,
        instance: InstanceId(1),
        message: "peer.action",
        kind: "peer.dropped",
        meta: &meta,
        answers: None,
    };

    let events = captured(|| {
        Logger::new().action_start(
            &exec,
            &PeerAction::Dropped {
                peer: 3,
                addr: "10.0.0.1".into(),
            },
        );
    });

    let [event] = events.as_slice() else {
        panic!("one event: {events:?}")
    };
    assert_eq!(event.level, Level::WARN);
    assert_eq!(field(event, "fields"), "peer=3 addr=10.0.0.1");
    assert!(!event.fields.contains_key("label"));
}

/// A refused Action and an inline one are logged at the message's level,
/// with what refused it and where it came from, or the execution it ran
/// within.
#[test]
fn refused_and_inline_actions_are_logged() {
    let path: Arc<str> = Arc::from("/peers");
    let action = PeerAction::Dropped {
        peer: 3,
        addr: "10.0.0.1".into(),
    };
    let refusal = Refusal {
        path: &path,
        instance: InstanceId(1),
        message: "peer.action",
        kind: "peer.dropped",
        sent: SentFrom::Inline(ExecutionId(7)),
        by: RefusedBy::Guard,
        time: 7,
    };
    let inline = InlineCall {
        path: &path,
        instance: InstanceId(1),
        message: "peer.action",
        kind: "peer.dropped",
        within: ExecutionId(9),
        time: 4,
        root_guard_skipped: true,
    };
    let events = captured(|| {
        let mut logger = Logger::new();
        logger.action_refused(&refusal, &action);
        logger.inline_action(&inline, &action);
    });

    let [refused, inline] = events.as_slice() else {
        panic!("two events: {events:?}")
    };
    assert_eq!((refused.level, inline.level), (Level::WARN, Level::WARN));
    assert_eq!(
        ["refused", "sent", "causal", "kind", "fields"].map(|f| field(refused, f)),
        [
            "guard",
            "inline",
            "7",
            "peer.dropped",
            "peer=3 addr=10.0.0.1"
        ]
    );
    assert!(
        !refused.fields.contains_key("id"),
        "a refused Action has no execution"
    );
    // An inline call is stamped with its parent's time, not its ID.
    assert_eq!(
        ["time", "within", "path", "kind", "root_guard_skipped"].map(|f| field(inline, f)),
        ["4", "9", "/peers", "peer.dropped", "true"]
    );
}

#[test]
fn application_logs_carry_the_scheduler_time() {
    let events = captured(|| causet::observers::info!(42; job = 7, "stale job"));

    let [event] = events.as_slice() else {
        panic!("one event: {events:?}")
    };
    assert_eq!(event.level, Level::INFO);
    assert_eq!(
        [
            field(event, "time"),
            field(event, "job"),
            field(event, "message")
        ],
        ["42", "7", "stale job"]
    );
}

// ---------------------------------------------------------------------------
// Stats

fn ranges(calls: &[u64]) -> DurationRanges {
    let mut ranges = DurationRanges::default();
    calls.iter().for_each(|&d| ranges.add(d));
    ranges
}

#[test]
fn each_execution_is_charged_until_the_next_one_starts() {
    let clock = ManualClock::new(1000);
    let options = StoreOptions::with_clock(clock.clone());
    let mut f = fixture_with(Box::new(Inline), 4, options);
    f.store.add_observer(Stats::new());

    clock.set(2000);
    f.store.dispatch_to(&f.counter, Inc).unwrap(); // 2000
    clock.set(2500);
    f.store.dispatch_to(&f.counter, Inc).unwrap(); // 2500: the first Inc took 500
    clock.set(10_000);
    // 10000, and its Effect at 10000 too (the clock stands still).
    f.store
        .dispatch_to(&f.a, DownloaderAction::Fetch("alpha".into()))
        .unwrap();
    clock.set(20_000);
    // The Event, Completed and Finished at 20000; Finished is still open.
    f.store.drain_events(16).unwrap();

    let snapshot = f.store.observer::<Stats>().unwrap().snapshot();
    let inc = &snapshot.actions["counter.action"]["counter.inc"];
    assert_eq!(*inc, ranges(&[500, 7500]));
    assert_eq!(
        (inc.calls(), inc.total_duration(), inc.max_duration()),
        (2, 8000, 7500)
    );
    assert_eq!(
        (inc.under_1_us.total_calls, inc.under_10_us.total_calls),
        (1, 1)
    );

    let downloader = &snapshot.actions["downloader.action"];
    assert_eq!(downloader["downloader.fetch"], ranges(&[0]));
    assert_eq!(downloader["downloader.completed"], ranges(&[0]));
    assert!(
        !downloader.contains_key("downloader.finished"),
        "the latest execution stays open"
    );
    assert_eq!(
        snapshot.effects["downloader.effect"]["downloader.effect.fetch"],
        ranges(&[10_000])
    );
    assert_eq!(
        snapshot.events["download.event"]["download.done"],
        ranges(&[0])
    );
}

#[test]
fn durations_fall_into_their_buckets() {
    let all = ranges(&[
        1_000, 1_001, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 50_000_000, 50_000_001,
    ]);
    let calls = |r: &causet::observers::RangeStats| r.total_calls;
    assert_eq!(
        [
            calls(&all.under_1_us),
            calls(&all.under_10_us),
            calls(&all.under_50_us),
            calls(&all.under_100_us),
            calls(&all.under_500_us),
            calls(&all.under_1_ms),
            calls(&all.under_5_ms),
            calls(&all.under_50_ms),
            calls(&all.above_50_ms),
        ],
        [1, 1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(all.max_duration(), 50_000_001);
}

#[test]
fn equal_names_at_different_addresses_are_one_entry() {
    // The same names in two statics, so at two addresses: Stats slots them
    // apart and merges them.
    static ACTION: [u8; 8] = *b"x.action";
    static GO: [u8; 4] = *b"x.go";
    static ACTION_AGAIN: [u8; 8] = *b"x.action";
    static GO_AGAIN: [u8; 4] = *b"x.go";
    let name = |bytes: &'static [u8]| std::str::from_utf8(bytes).unwrap();
    let (first, second) = (
        (name(&ACTION), name(&GO)),
        (name(&ACTION_AGAIN), name(&GO_AGAIN)),
    );
    assert_ne!(first.1.as_ptr(), second.1.as_ptr());

    let path: Arc<str> = Arc::from("/");
    let mut stats = Stats::new();
    for (time, (message, kind)) in [(10, first), (20, second), (50, first)] {
        let meta = ExecMeta {
            id: ExecutionId(time),
            depth: 1,
            time,
            origin: None,
        };
        let exec = Execution {
            category: Category::Action,
            path: &path,
            instance: InstanceId(1),
            message,
            kind,
            meta: &meta,
            answers: None,
        };
        stats.action_start(
            &exec,
            &PeerAction::Dropped {
                peer: 0,
                addr: String::new(),
            },
        );
    }

    assert_eq!(
        stats.snapshot().actions["x.action"]["x.go"],
        ranges(&[10, 30])
    );
}
