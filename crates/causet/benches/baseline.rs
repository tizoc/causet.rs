//! Baseline benchmarks.
//!
//! Run with `cargo bench -p causet --bench baseline`. Each scenario runs
//! several rounds on a fresh Store and reports the median time per execution
//! (every Action, Effect and Event the Store runs counts once), with the
//! allocations, allocated bytes and peak retained bytes per execution of one
//! round. A hand-written static-dispatch scheduler runs the queued-Action
//! scenario as the reference point for the cost of dynamic dispatch.
//!
//! No dependency beyond the workspace: a counting global allocator measures
//! memory.
//!
//! For profiling, `CAUSET_BENCH=<text>` runs only the scenarios whose name
//! contains it (and the reference), and `CAUSET_BENCH_ROUNDS=<n>` sets the
//! rounds.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use causet::hosting::EventBus;
use causet::{
    Action, ActionMeta, ActionPort, Adapts, Child, Component, ComponentContext, ComponentRegistry,
    DispatchOutcome, Effect, EffectContext, EffectMeta, Event, EventSink, Mount, Never, Output,
    Protocol, Registry, Store, StoreOptions, Timestamp, lens, no_effects, root,
};

// ---------------------------------------------------------------------------
// Counting allocator

struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        PEAK.fetch_max(now, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(Clone, Copy)]
struct MemMark {
    allocs: usize,
    bytes: usize,
    current: usize,
}

fn mark() -> MemMark {
    PEAK.store(CURRENT.load(Ordering::Relaxed), Ordering::Relaxed);
    MemMark {
        allocs: ALLOCS.load(Ordering::Relaxed),
        bytes: BYTES.load(Ordering::Relaxed),
        current: CURRENT.load(Ordering::Relaxed),
    }
}

struct MemUse {
    allocs: usize,
    bytes: usize,
    peak_retained: usize,
}

fn since(start: MemMark) -> MemUse {
    MemUse {
        allocs: ALLOCS.load(Ordering::Relaxed) - start.allocs,
        bytes: BYTES.load(Ordering::Relaxed) - start.bytes,
        peak_retained: PEAK.load(Ordering::Relaxed).saturating_sub(start.current),
    }
}

// ---------------------------------------------------------------------------
// Messages

#[derive(Debug, Action)]
#[causet(type = "bench.action")]
enum BenchAction {
    /// n queued Actions to self.
    Fan(u32),
    /// A small payload: its size is part of what is measured.
    Noop(#[allow(dead_code)] u64),
    /// n queued Actions its enabling check refuses.
    RefusedFan(u32),
    Refused,
    /// n pairs of a queued Effect and a queued Action.
    Mixed(u32),
    /// n inline child calls.
    InlineChildren(u32),
    /// n queued child sends.
    QueuedChildren(u32),
    /// n inline child calls, each reporting an Output lowered to 4 Actions.
    Fanout(u32),
    /// One Effect dispatching n synchronous children.
    SyncChildren(u32),
    /// n queued Actions carrying 4 KiB each.
    LargeFan(u32),
    Large(Vec<u8>),
}

#[derive(Debug, Effect)]
#[causet(type = "bench.effect")]
enum BenchEffect {
    Noop,
    Sync(u32),
}

#[derive(Debug, Event)]
#[causet(type = "bench.tick")]
struct Tick(u64);

#[derive(Debug, Action)]
#[causet(type = "kid.action")]
enum KidAction {
    Ping,
    Emit,
}

#[derive(Debug, Output)]
#[causet(type = "kid.done")]
struct Done;

// ---------------------------------------------------------------------------
// Components

struct Kid;

#[derive(Clone, Default)]
struct KidState {
    pings: u64,
}

impl Component for Kid {
    type State = KidState;
    type Action = KidAction;
    type Output = Done;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: KidAction, _: &ActionMeta) {
        ctx.state_mut().pings += 1;
        if matches!(action, KidAction::Emit) {
            ctx.into_emitter().emit_output(Done);
        }
    }
}

struct Bench;

const A: Child<Bench, Kid> = Child::new("a");
const B: Child<Bench, Kid> = Child::new("b");

#[derive(Clone, Default)]
struct BenchState {
    count: u64,
    bytes: usize,
    a: KidState,
    b: KidState,
}

#[derive(Default)]
struct BenchServices {
    effects: u64,
}

fn lower_done(_: Child<Bench, Kid>, _: Done, port: &mut ActionPort<'_, Bench>) {
    for i in 0..4 {
        port.emit_action(BenchAction::Noop(i));
    }
}

impl Component for Bench {
    type State = BenchState;
    type Service = BenchServices;
    type Action = BenchAction;
    type Effect = BenchEffect;
    type Output = Never;

    fn is_enabled(_: &BenchState, action: &BenchAction, _: Timestamp) -> bool {
        !matches!(action, BenchAction::Refused)
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: BenchAction, _: &ActionMeta) {
        ctx.state_mut().count += 1;
        match action {
            BenchAction::Fan(n) => {
                let mut out = ctx.into_emitter();
                for i in 0..n {
                    out.emit_action(BenchAction::Noop(u64::from(i)));
                }
            }
            BenchAction::RefusedFan(n) => {
                let mut out = ctx.into_emitter();
                for _ in 0..n {
                    out.emit_action(BenchAction::Refused);
                }
            }
            BenchAction::Mixed(n) => {
                let mut out = ctx.into_emitter();
                for i in 0..n {
                    out.emit_effect(BenchEffect::Noop);
                    out.emit_action(BenchAction::Noop(u64::from(i)));
                }
            }
            BenchAction::InlineChildren(n) => {
                for _ in 0..n {
                    ctx.reduce_child(A, KidAction::Ping);
                }
            }
            BenchAction::QueuedChildren(n) => {
                let mut out = ctx.into_emitter();
                for _ in 0..n {
                    out.to_child(A, KidAction::Ping);
                }
            }
            BenchAction::Fanout(n) => {
                for _ in 0..n {
                    ctx.reduce_child(A, KidAction::Emit);
                }
            }
            BenchAction::SyncChildren(n) => ctx.into_emitter().emit_effect(BenchEffect::Sync(n)),
            BenchAction::LargeFan(n) => {
                let mut out = ctx.into_emitter();
                for _ in 0..n {
                    out.emit_action(BenchAction::Large(vec![7; 4096]));
                }
            }
            BenchAction::Large(payload) => ctx.state_mut().bytes += black_box(payload).len(),
            BenchAction::Noop(_) | BenchAction::Refused => {}
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: BenchEffect, _: &EffectMeta) {
        ctx.service().effects += 1;
        if let BenchEffect::Sync(n) = effect {
            for i in 0..n {
                ctx.dispatch(BenchAction::Noop(u64::from(i)));
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Tick>();
        r.child(A, lens!(BenchState => a))
            .on_output(lower_done)
            .bind();
        r.child(B, lens!(BenchState => b))
            .on_output(lower_done)
            .bind();
    }
}

impl Adapts<Tick> for Bench {
    fn adapt(event: Tick, port: &mut ActionPort<'_, Self>) {
        port.emit_action(BenchAction::Noop(event.0));
    }
}

// ---------------------------------------------------------------------------
// Fixture

struct Fixture {
    store: Store<BenchState, BenchServices>,
    sink: EventSink,
    root: Mount<Bench>,
    a: Mount<Kid>,
    b: Mount<Kid>,
}

fn protocol() -> Arc<Protocol<BenchState, BenchServices>> {
    let mut registry = Registry::<BenchState, BenchServices>::new();
    registry.root::<Bench>();
    registry.freeze().expect("valid protocol")
}

fn fixture(
    p: &Arc<Protocol<BenchState, BenchServices>>,
    options: StoreOptions,
    worker_capacity: usize,
) -> Fixture {
    let (sink, inbox) = EventBus::new(p, worker_capacity);
    let store = Store::new(
        p.clone(),
        BenchState::default(),
        BenchServices::default(),
        inbox,
        options,
    )
    .unwrap();
    Fixture {
        store,
        sink,
        root: p.resolve(&root::<Bench>()).unwrap(),
        a: p.resolve(&root::<Bench>().child(A)).unwrap(),
        b: p.resolve(&root::<Bench>().child(B)).unwrap(),
    }
}

// ---------------------------------------------------------------------------
// Harness

fn rounds() -> usize {
    std::env::var("CAUSET_BENCH_ROUNDS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(7)
}

fn selected(name: &str) -> bool {
    name.starts_with("reference")
        || std::env::var("CAUSET_BENCH").is_ok_and(|only| name.contains(&only))
        || std::env::var("CAUSET_BENCH").is_err()
}
/// Root inputs per round; each fans out to `FAN` items.
const ROOTS: u32 = 1_000;
const FAN: u32 = 100;

struct Sample {
    elapsed: Duration,
    executions: usize,
    mem: MemUse,
}

struct Row {
    name: &'static str,
    ns_per_exec: f64,
    allocs_per_exec: f64,
    bytes_per_exec: f64,
    peak_retained_per_exec: f64,
    executions: usize,
}

/// Runs `round` several times; the median time, the memory of the median
/// round. `None` for a scenario `CAUSET_BENCH` leaves out.
fn measure(name: &'static str, mut round: impl FnMut(usize) -> Sample) -> Option<Row> {
    if !selected(name) {
        return None;
    }
    let rounds = rounds();
    let _warm_up = round(usize::MAX);
    let mut samples: Vec<Sample> = (0..rounds).map(&mut round).collect();
    samples.sort_by(|a, b| {
        let a = a.elapsed.as_secs_f64() / a.executions as f64;
        let b = b.elapsed.as_secs_f64() / b.executions as f64;
        a.total_cmp(&b)
    });
    let median = &samples[rounds / 2];
    let per = |x: usize| x as f64 / median.executions as f64;
    Some(Row {
        name,
        ns_per_exec: median.elapsed.as_nanos() as f64 / median.executions as f64,
        allocs_per_exec: per(median.mem.allocs),
        bytes_per_exec: per(median.mem.bytes),
        peak_retained_per_exec: per(median.mem.peak_retained),
        executions: median.executions,
    })
}

/// Times `run` on a fresh fixture; executions come from the Store's trace.
fn store_round(
    p: &Arc<Protocol<BenchState, BenchServices>>,
    options: StoreOptions,
    run: impl FnOnce(&mut Fixture),
) -> Sample {
    let mut f = fixture(p, options, 1 << 16);
    let start = mark();
    let t = Instant::now();
    run(&mut f);
    let elapsed = t.elapsed();
    let mem = since(start);
    let executions = f.store.executions();
    black_box(f.store.state().count);
    drop(f);
    Sample {
        elapsed,
        executions,
        mem,
    }
}

fn dispatch_roots(f: &mut Fixture, action: impl Fn() -> BenchAction) {
    for _ in 0..ROOTS {
        assert_eq!(
            f.store.dispatch_to(&f.root, action()),
            Ok(DispatchOutcome::Executed)
        );
    }
}

// ---------------------------------------------------------------------------
// Static-dispatch reference: one enum, one match, one FIFO per execution, the
// same depth-first order and the same causal metadata, no erasure. Like causet
// with no observers, it counts executions and keeps no metadata.

mod reference {
    use super::black_box;

    enum Act {
        Fan(u32),
        Noop(u64),
    }

    #[derive(Clone, Copy)]
    struct Meta {
        id: u64,
        causal: u64,
        ordinal: u32,
    }

    #[derive(Default)]
    pub struct Store {
        last: u64,
        count: u64,
        pub executions: usize,
    }

    impl Store {
        fn execute(&mut self, action: Act, causal: Option<(u64, u32)>) {
            self.last += 1;
            let id = self.last;
            let meta = Meta {
                id,
                causal: causal.map_or(id, |c| c.0),
                ordinal: causal.map_or(0, |c| c.1),
            };
            black_box((meta.id, meta.causal, meta.ordinal));
            self.executions += 1;
            self.count += 1;
            let mut fifo = Vec::new();
            match action {
                Act::Fan(n) => {
                    for i in 0..n {
                        fifo.push(Act::Noop(u64::from(i)));
                    }
                }
                Act::Noop(x) => {
                    black_box(x);
                }
            }
            for (ordinal, child) in fifo.into_iter().enumerate() {
                self.execute(child, Some((id, ordinal as u32)));
            }
        }

        pub fn dispatch_fan(&mut self, n: u32) {
            self.execute(Act::Fan(n), None);
        }

        pub fn count(&self) -> u64 {
            self.count
        }
    }
}

// ---------------------------------------------------------------------------

fn main() {
    let p = protocol();
    let mut rows = Vec::new();

    rows.extend(measure("reference: static enum, queued Actions", |_| {
        let mut s = reference::Store::default();
        let start = mark();
        let t = Instant::now();
        for _ in 0..ROOTS {
            s.dispatch_fan(FAN);
        }
        let elapsed = t.elapsed();
        let mem = since(start);
        black_box(s.count());
        Sample {
            elapsed,
            executions: s.executions,
            mem,
        }
    }));

    rows.extend(measure("queued Actions (small payload)", |_| {
        store_round(&p, StoreOptions::default(), |f| {
            dispatch_roots(f, || BenchAction::Fan(FAN))
        })
    }));
    rows.extend(measure("queued Actions, Stats observer", |_| {
        store_round(&p, StoreOptions::default(), |f| {
            f.store.add_observer(causet::observers::Stats::new());
            dispatch_roots(f, || BenchAction::Fan(FAN))
        })
    }));
    // No `tracing` subscriber is installed: this is the price of the level
    // check alone.
    rows.extend(measure("queued Actions, Logger, logging off", |_| {
        store_round(&p, StoreOptions::default(), |f| {
            f.store.add_observer(causet::observers::Logger::new());
            dispatch_roots(f, || BenchAction::Fan(FAN))
        })
    }));
    rows.extend(measure("queued Actions (4 KiB payload)", |_| {
        store_round(&p, StoreOptions::default(), |f| {
            dispatch_roots(f, || BenchAction::LargeFan(FAN))
        })
    }));
    rows.extend(measure("refused Actions (per refusal)", |_| {
        let mut s = store_round(&p, StoreOptions::default(), |f| {
            dispatch_roots(f, || BenchAction::RefusedFan(FAN))
        });
        // Refusals leave no execution; count each attempt.
        s.executions = (ROOTS * (FAN + 1)) as usize;
        s
    }));
    // An observer that ignores refusals, and one that logs them with logging
    // off.
    rows.extend(measure("refused Actions, Stats observer", |_| {
        let mut s = store_round(&p, StoreOptions::default(), |f| {
            f.store.add_observer(causet::observers::Stats::new());
            dispatch_roots(f, || BenchAction::RefusedFan(FAN))
        });
        s.executions = (ROOTS * (FAN + 1)) as usize;
        s
    }));
    rows.extend(measure("refused Actions, Logger, logging off", |_| {
        let mut s = store_round(&p, StoreOptions::default(), |f| {
            f.store.add_observer(causet::observers::Logger::new());
            dispatch_roots(f, || BenchAction::RefusedFan(FAN))
        });
        s.executions = (ROOTS * (FAN + 1)) as usize;
        s
    }));
    rows.extend(measure("Effects and Actions, interleaved", |_| {
        store_round(&p, StoreOptions::default(), |f| {
            dispatch_roots(f, || BenchAction::Mixed(FAN / 2))
        })
    }));
    rows.extend(measure("inline child calls (per call)", |_| {
        let mut s = store_round(&p, StoreOptions::default(), |f| {
            dispatch_roots(f, || BenchAction::InlineChildren(FAN))
        });
        // Inline calls add no execution; count each call.
        s.executions = (ROOTS * (FAN + 1)) as usize;
        s
    }));
    rows.extend(measure("inline child calls, Logger, logging off", |_| {
        let mut s = store_round(&p, StoreOptions::default(), |f| {
            f.store.add_observer(causet::observers::Logger::new());
            dispatch_roots(f, || BenchAction::InlineChildren(FAN))
        });
        s.executions = (ROOTS * (FAN + 1)) as usize;
        s
    }));
    rows.extend(measure("queued child sends", |_| {
        store_round(&p, StoreOptions::default(), |f| {
            dispatch_roots(f, || BenchAction::QueuedChildren(FAN))
        })
    }));
    rows.extend(measure("Output fan-out (1 Output -> 4 Actions)", |_| {
        store_round(&p, StoreOptions::default(), |f| {
            dispatch_roots(f, || BenchAction::Fanout(FAN / 4))
        })
    }));
    rows.extend(measure("synchronous Effect children", |_| {
        store_round(&p, StoreOptions::default(), |f| {
            dispatch_roots(f, || BenchAction::SyncChildren(FAN))
        })
    }));
    rows.extend(measure("two mounts, alternating root inputs", |_| {
        store_round(&p, StoreOptions::default(), |f| {
            for i in 0..ROOTS * FAN {
                let mount = if i % 2 == 0 { f.a } else { f.b };
                f.store.dispatch_to(&mount, KidAction::Ping).unwrap();
            }
        })
    }));
    rows.extend(measure("Event drain, Store lane", |_| {
        store_round(&p, StoreOptions::default(), |f| {
            f.store.drain_events(0).unwrap(); // this thread owns the Store lane
            for i in 0..u64::from(ROOTS * FAN / 2) {
                f.sink.try_send_to(&f.root, Tick(i)).unwrap();
            }
            while f.store.drain_events(1024).unwrap() > 0 {}
        })
    }));
    rows.extend(measure("Event drain, worker lane", |_| {
        store_round(&p, StoreOptions::default(), |f| {
            let (sink, root) = (f.sink.clone(), f.root);
            let n = u64::from(ROOTS * FAN / 2);
            std::thread::spawn(move || {
                for i in 0..n {
                    sink.try_send_to(&root, Tick(i)).unwrap();
                }
            })
            .join()
            .unwrap();
            while f.store.drain_events(1024).unwrap() > 0 {}
        })
    }));

    print_table(&rows);
}

fn print_table(rows: &[Row]) {
    let reference = rows[0].ns_per_exec;
    println!(
        "| Scenario | ns / exec | x reference | allocs / exec | bytes / exec | peak retained bytes / exec | executions |"
    );
    println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: |");
    for row in rows {
        println!(
            "| {} | {:.1} | {:.1} | {:.2} | {:.0} | {:.0} | {} |",
            row.name,
            row.ns_per_exec,
            row.ns_per_exec / reference,
            row.allocs_per_exec,
            row.bytes_per_exec,
            row.peak_retained_per_exec,
            row.executions
        );
    }
}
