//! Generated runs and shrinking: runs made from a stream of choices, and a
//! failing run's choices cut down to a small list that fails the same way.

use std::cell::{Cell, RefCell};
use std::panic::{self, AssertUnwindSafe};
use std::sync::Once;

use crate::{Rejected, Rng};

/// How [`check`] runs. `CAUSET_SEED` and `CAUSET_RUNS` override `seed` and
/// `runs`.
#[derive(Clone, Debug)]
pub struct Config {
    /// Runs that must pass. Run `i` draws from seed `seed + i`.
    pub runs: u64,
    /// The first run's seed.
    pub seed: u64,
    /// Rejected runs allowed before the test fails; `None` is ten times
    /// `runs`.
    pub max_rejects: Option<u64>,
    /// Root inputs a run may take, counted over every `Node`.
    pub max_steps: u64,
    /// Runs shrinking may make.
    pub max_shrinks: u64,
    /// Makes every passing run a second time from its own choices, and fails
    /// the first whose second making rejects, or differs in its executions,
    /// refusals or inline calls on any node, or in their messages' `Debug`
    /// forms: something in it reads what its choices do not decide. A
    /// difference only in State goes unseen; an invariant catches that.
    /// Doubles the time passing runs that use a `Node` take.
    pub determinism: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            runs: 100,
            seed: 0,
            max_rejects: None,
            max_steps: 10_000,
            max_shrinks: 2_000,
            determinism: true,
        }
    }
}

/// A failing run, found and shrunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    /// The seed of the run that failed first.
    pub seed: u64,
    /// What failed, the same for the original and the shrunk run.
    pub key: String,
    /// The shrunk run's choices: `replay(&choices, f)` repeats it.
    pub choices: Vec<u64>,
    /// How many choices the original run made.
    pub original: usize,
    /// How many runs shrinking made.
    pub shrinks: u64,
    /// The step limit it was found under, which a replay applies too.
    pub max_steps: u64,
    /// For a run that was not deterministic (`key` "not deterministic"), the
    /// first execution its two makings differ at.
    pub detail: Option<String>,
}

impl Found {
    /// Runs `f` on the shrunk choices under the step limit they were found
    /// with: the failure, kept as a regression test.
    pub fn replay(&self, f: impl FnOnce(&mut Rng)) {
        replay_with(
            &Config {
                max_steps: self.max_steps,
                ..Config::default()
            },
            &self.choices,
            f,
        );
    }
}

/// Runs `f` on generated choices until a run fails or `config.runs` runs
/// passed. A failure is shrunk; then the seed and the shrunk choices are
/// printed, and the shrunk run is made once more, without catching it, so
/// its own panic fails the test. Panics, too, when `f` rejects more runs
/// than allowed, or when the shrunk run passes on that last replay (the run
/// is not repeatable: are all its Services simulated?).
pub fn check(config: Config, f: impl Fn(&mut Rng)) {
    let config = from_env(config);
    let Some(found) = search(config.clone(), &f) else {
        return;
    };
    if let Some(detail) = &found.detail {
        panic!(
            "causet_test::check: the run with seed {} is not deterministic: made twice from the same \
             {} choices, {detail}. Something in it reads what its choices do not decide (the system \
             clock, a HashMap's order, a thread's timing). Repeat it with CAUSET_SEED={}",
            found.seed, found.original, found.seed
        );
    }
    let call = if found.max_steps == Config::default().max_steps {
        format!("causet_test::replay(&{:?}, ...)", found.choices)
    } else {
        format!(
            "causet_test::replay_with(&Config {{ max_steps: {}, ..Config::default() }}, &{:?}, ...)",
            found.max_steps, found.choices
        )
    };
    eprintln!(
        "causet_test::check: the run with seed {} failed ({}); shrunk from {} choices to {} in {} runs. \
         Repeat it with CAUSET_SEED={} or\n    {call}",
        found.seed,
        found.key,
        found.original,
        found.choices.len(),
        found.shrinks,
        found.seed,
    );
    found.replay(&f);
    panic!(
        "the shrunk run of seed {} ({}) passed when replayed: the run is not repeatable \
         (are all its Services simulated?)",
        found.seed, found.key
    );
}

/// What [`check`] does before reporting: the first failing run, shrunk, or
/// `None` when `config.runs` runs passed. It applies no environment
/// overrides. Panics when `f` rejects more runs than allowed.
pub fn search(config: Config, f: impl Fn(&mut Rng)) -> Option<Found> {
    install_hook();
    let _limit = StepLimit::set(config.max_steps);
    let max_rejects = config.max_rejects.unwrap_or(config.runs.saturating_mul(10));
    let (mut passed, mut rejected) = (0, 0);
    let mut seed = config.seed;
    while passed < config.runs {
        let mut rng = Rng::recording(seed);
        let (outcome, first) = attempt_traced(&f, &mut rng, config.determinism);
        match outcome {
            Outcome::Passed => {
                // A run that made no Store, such as a projection's, has
                // nothing to compare.
                if config.determinism && !first.is_empty() {
                    let choices = rng.choices().expect("a recording Rng");
                    let (again, second) = attempt_traced(&f, &mut Rng::replaying(&choices), true);
                    if let Some(detail) = difference(&first, &second, &again) {
                        return Some(Found {
                            seed,
                            key: "not deterministic".to_owned(),
                            original: choices.len(),
                            choices,
                            shrinks: 0,
                            max_steps: config.max_steps,
                            detail: Some(detail),
                        });
                    }
                }
                passed += 1
            }
            Outcome::Rejected => {
                rejected += 1;
                assert!(
                    rejected <= max_rejects,
                    "{rejected} runs rejected and {passed} passed: the generator rejects almost everything, \
                     so it tests almost nothing"
                );
            }
            Outcome::Failed { key, choices } => {
                let original = choices.len();
                let (choices, shrinks) = shrink(&f, &key, choices, config.max_shrinks);
                return Some(Found {
                    seed,
                    key,
                    choices,
                    original,
                    shrinks,
                    max_steps: config.max_steps,
                    detail: None,
                });
            }
        }
        seed = seed.wrapping_add(1);
    }
    None
}

/// Runs `f` on exactly `choices` (each clamped to its draw's bound, then 0
/// past the end), under the default step limit: a failure [`check`] found,
/// kept as a regression test. `Found::replay` and [`replay_with`] apply the
/// limit a failure was found under.
pub fn replay(choices: &[u64], f: impl FnOnce(&mut Rng)) {
    replay_with(&Config::default(), choices, f);
}

/// [`replay`] under `config`'s step limit.
pub fn replay_with(config: &Config, choices: &[u64], f: impl FnOnce(&mut Rng)) {
    install_hook();
    let _limit = StepLimit::set(config.max_steps);
    begin_run(false);
    f(&mut Rng::replaying(choices));
}

fn from_env(mut config: Config) -> Config {
    let read = |name: &str| {
        std::env::var(name).ok().map(|value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name}={value:?} is not a number"))
        })
    };
    if let Some(seed) = read("CAUSET_SEED") {
        config.seed = seed;
    }
    if let Some(runs) = read("CAUSET_RUNS") {
        config.runs = runs;
    }
    config
}

// ---------------------------------------------------------------------------
// Runs

enum Outcome {
    Passed,
    Rejected,
    Failed { key: String, choices: Vec<u64> },
}

/// A fresh run context: no steps taken, no failure named, no panic seen.
fn begin_run(quiet: bool) {
    RUN.with(|run| {
        run.quiet.set(quiet);
        run.steps.set(0);
        run.pending_key.take();
        run.key.take();
        run.last_panic.take();
        run.fault_candidates.take();
    });
}

/// [`attempt`], keeping the run's executions when `traced`.
fn attempt_traced(f: &impl Fn(&mut Rng), rng: &mut Rng, traced: bool) -> (Outcome, Vec<String>) {
    if traced {
        RUN.with(|run| *run.trace.borrow_mut() = Some(Vec::new()));
    }
    let outcome = attempt_with(f, rng);
    let trace = RUN.with(|run| run.trace.take()).unwrap_or_default();
    (outcome, trace)
}

/// Where a run's second making first differs from its first, which passed.
fn difference(first: &[String], second: &[String], outcome: &Outcome) -> Option<String> {
    match outcome {
        Outcome::Failed { key, .. } => {
            return Some(format!("the first passed and the second failed ({key})"));
        }
        // The same choices led one run to pass and the other to reject:
        // whatever the traces show, the two runs did not agree.
        Outcome::Rejected => {
            return Some("the first passed and the second was rejected".to_owned());
        }
        Outcome::Passed => {}
    }
    let at = first.iter().zip(second).position(|(a, b)| a != b);
    match at {
        Some(i) => Some(format!(
            "at step {}: first {}, then {}",
            i + 1,
            first[i],
            second[i]
        )),
        None if first.len() != second.len() => Some(format!(
            "the first made {} executions, refusals and inline calls, the second {}",
            first.len(),
            second.len()
        )),
        None => None,
    }
}

/// Records what a node's Store did, while a run's executions are kept.
pub(crate) fn record(line: impl FnOnce() -> String) {
    RUN.with(|run| {
        if let Some(trace) = run.trace.borrow_mut().as_mut() {
            trace.push(line());
        }
    });
}

fn attempt(f: &impl Fn(&mut Rng), mut rng: Rng) -> Outcome {
    attempt_with(f, &mut rng)
}

fn attempt_with(f: &impl Fn(&mut Rng), rng: &mut Rng) -> Outcome {
    begin_run(true);
    let result = panic::catch_unwind(AssertUnwindSafe(|| f(rng)));
    RUN.with(|run| run.quiet.set(false));
    match result {
        Ok(()) => Outcome::Passed,
        Err(payload) if payload.is::<Rejected>() => Outcome::Rejected,
        Err(_) => {
            let key = RUN.with(|run| {
                run.key
                    .take()
                    .or_else(|| run.last_panic.take().map(|at| format!("panic at {at}")))
            });
            let choices = rng.choices().expect("a recording Rng");
            Outcome::Failed {
                key: key.unwrap_or_else(|| "panic".to_owned()),
                choices,
            }
        }
    }
}

/// Section 19.4: passes over the choices until none makes progress or the
/// budget is spent. An edit is kept only if the run fails with the same key
/// and makes a smaller list of choices: shorter, or as long and smaller at
/// the first difference.
fn shrink(f: &impl Fn(&mut Rng), key: &str, mut best: Vec<u64>, budget: u64) -> (Vec<u64>, u64) {
    let runs = Cell::new(0);
    let try_edit = |best: &mut Vec<u64>, candidate: Vec<u64>| -> bool {
        if runs.get() >= budget {
            return false;
        }
        runs.set(runs.get() + 1);
        match attempt(f, Rng::replaying(&candidate)) {
            Outcome::Failed { key: k, choices } if k == key && smaller(&choices, best) => {
                *best = choices;
                true
            }
            _ => false,
        }
    };
    'shrinking: loop {
        let before = best.clone();
        // 1. Delete a span: removes steps. Every length up to 8, since a
        // step is as many choices as it draws.
        for len in (1..=8).rev() {
            let mut i = 0;
            while i + len <= best.len() {
                let mut candidate = best.clone();
                candidate.drain(i..i + len);
                if !try_edit(&mut best, candidate) {
                    i += 1;
                }
            }
        }
        // 2. Set a span to 0.
        for len in [8, 4, 2, 1] {
            let mut i = 0;
            while i + len <= best.len() {
                if best[i..i + len].iter().any(|&c| c != 0) {
                    let mut candidate = best.clone();
                    candidate[i..i + len].fill(0);
                    try_edit(&mut best, candidate);
                }
                i += 1;
            }
        }
        // 3. Lower one choice: the smallest value that still fails, by
        // halving the distance toward 0.
        let mut i = 0;
        while i < best.len() {
            let (mut low, mut high) = (0, best[i]);
            while low < high && i < best.len() {
                let mid = low + (high - low) / 2;
                let mut candidate = best.clone();
                candidate[i] = mid;
                if try_edit(&mut best, candidate) {
                    high = mid;
                } else {
                    low = mid + 1;
                }
            }
            i += 1;
        }
        // 4. Two choices at once, only when no single edit helps: lower a
        // choice (to 0, else to half) and set one of the next few to a small
        // value. A list smaller at its first difference can need both, as
        // when one draw stood in for what a later one now provides.
        if best == before {
            for i in 0..best.len() {
                let current = best[i];
                for lowered in [0, current / 2] {
                    if lowered >= current {
                        continue;
                    }
                    for j in i + 1..best.len().min(i + 1 + PAIR_WINDOW) {
                        for value in 0..=PAIR_VALUES {
                            if value == best[j] {
                                continue;
                            }
                            let mut candidate = best.clone();
                            candidate[i] = lowered;
                            candidate[j] = value;
                            if try_edit(&mut best, candidate) {
                                continue 'shrinking;
                            }
                        }
                    }
                }
            }
        }
        if best == before || runs.get() >= budget {
            return (best, runs.get());
        }
    }
}

/// How far after a lowered choice the two-choice pass looks for one to set,
/// and the values it tries there.
const PAIR_WINDOW: usize = 4;
const PAIR_VALUES: u64 = 4;

/// Shorter, or as long and smaller at the first difference.
fn smaller(a: &[u64], b: &[u64]) -> bool {
    (a.len(), a) < (b.len(), b)
}

// ---------------------------------------------------------------------------
// What a run's thread knows about it

struct Run {
    /// A generated run is going: panics are not printed.
    quiet: Cell<bool>,
    /// Root inputs so far, and the limit, while `check` or `search` runs.
    steps: Cell<u64>,
    max_steps: Cell<Option<u64>>,
    /// Set by the harness just before it panics; the hook moves it to `key`.
    pending_key: RefCell<Option<String>>,
    /// The key of the last panic, if the harness named it.
    key: RefCell<Option<String>>,
    /// Where the last panic was raised.
    last_panic: RefCell<Option<String>>,
    /// While a run is checked for determinism: every node's executions,
    /// refusals and inline calls, with their messages, in order.
    trace: RefCell<Option<Vec<String>>>,
    /// The current root input's panics that the Store does not ignore, as
    /// (message, place), in order: the faulting one among them, with panics
    /// the application caught itself before the fault, or in a destructor
    /// while it unwinds.
    fault_candidates: RefCell<Vec<(String, String)>>,
}

thread_local! {
    static RUN: Run = const {
        Run {
            quiet: Cell::new(false),
            steps: Cell::new(0),
            max_steps: Cell::new(None),
            pending_key: RefCell::new(None),
            key: RefCell::new(None),
            last_panic: RefCell::new(None),
            trace: RefCell::new(None),
            fault_candidates: RefCell::new(Vec::new()),
        }
    };
}

/// Sets the step limit for as long as it lives.
struct StepLimit(Option<u64>);

impl StepLimit {
    fn set(max: u64) -> Self {
        Self(RUN.with(|run| run.max_steps.replace(Some(max))))
    }
}

impl Drop for StepLimit {
    fn drop(&mut self) {
        RUN.with(|run| run.max_steps.set(self.0));
    }
}

/// The failure named last did not happen: its code returned.
pub(crate) fn not_failing() {
    RUN.with(|run| run.pending_key.take());
}

/// Whether a generated run is going on this thread, whose failures are not
/// printed: only the shrunk run's is.
pub(crate) fn quiet() -> bool {
    RUN.with(|run| run.quiet.get())
}

/// Names the failure the harness is about to raise.
pub(crate) fn failing(key: String) {
    RUN.with(|run| *run.pending_key.borrow_mut() = Some(key));
}

/// Where the current root input's faulting panic was raised, while a
/// generated run goes on: the last panic the Store did not ignore whose
/// message is the fault's. The Store says which panics it ignores (those
/// after the fault in its root input among them), and its fault carries the
/// faulting panic's message; a panic the application caught before the
/// fault, or in a destructor while it unwinds, is told apart by its
/// message.
pub(crate) fn fault_panic(message: &str) -> Option<String> {
    RUN.with(|run| {
        run.fault_candidates
            .borrow()
            .iter()
            .rev()
            .find(|(m, _)| m == message)
            .map(|(_, at)| at.clone())
    })
}

/// A root input may start now: forgets the previous one's panics.
pub(crate) fn input_starts() {
    RUN.with(|run| run.fault_candidates.take());
}

/// Counts a root input; past the run's step limit, fails the run.
pub(crate) fn count_step(node: &str) {
    let over = RUN.with(|run| {
        let steps = run.steps.get() + 1;
        run.steps.set(steps);
        run.max_steps.get().filter(|&max| steps > max)
    });
    if let Some(max) = over {
        failing("step budget".to_owned());
        panic!("node {node}: the run took more than {max} root inputs (Config::max_steps)");
    }
}

/// Wraps the panic hook once: on a thread running a generated run it records
/// where each panic was raised and the key the harness named, and prints
/// nothing; everywhere else it leaves the previous hook to do its work.
fn install_hook() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            let quiet = RUN.with(|run| {
                if run.max_steps.get().is_some() {
                    let at = info
                        .location()
                        .map(|at| format!("{}:{}", at.file(), at.line()));
                    if !causet_core::panic_is_ignored() {
                        // As the Store words a payload in its fault.
                        let payload = info.payload();
                        let message = payload
                            .downcast_ref::<&str>()
                            .copied()
                            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                            .unwrap_or("panic")
                            .to_owned();
                        run.fault_candidates
                            .borrow_mut()
                            .push((message, at.clone().unwrap_or_default()));
                    }
                    *run.last_panic.borrow_mut() = at;
                    *run.key.borrow_mut() = run.pending_key.take();
                }
                run.quiet.get()
            });
            if !quiet {
                previous(info);
            }
        }));
    });
}
