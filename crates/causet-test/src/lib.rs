//! Automatic bug finding through generated simulations on the real Store.
//!
//! Supply the possible operations and application invariants; `check`
//! generates seeded runs, checks the invariants and shrinks failures into
//! choices that `replay` repeats.
//!
//! - [`Node`]: a Store on a held bus, with a chosen session and a shared
//!   clock, from supplied State and Services. Any component can be its root,
//!   so a composite runs its real children. Every Event a Service or a test
//!   sends is held until the test delivers it, and the application's
//!   invariants are checked after every root input.
//! - [`Scenario`]: drives several nodes on one virtual clock. It delivers held
//!   Events (by default in the order the bus's lanes would run them, see
//!   [`next_in_queue_order`]), advances the clock to the next timer deadline or to the next
//!   time the application's [`World`] names, and stops at a predicate, a
//!   time or a step budget, saying what is left when progress stops.
//! - [`Rng`]: a seeded generator with no dependencies, for the choices a
//!   scenario or a simulated network makes; [`Rng::split`] gives the world
//!   a stream of its own.
//! - [`Swarm`]: the step kinds a run uses, drawn once per run, so runs
//!   differ in what they stress.
//! - [`InFlight`]: a simulated world's messages in flight, due in order of
//!   time and of sending, kept in order per channel.
//! - [`check`]: generated runs: the application's own steps,
//!   drawn from one stream of choices, checked after every root input, and a
//!   failing run shrunk to a small list of choices that [`replay`] repeats.
//!
//! A scenario repeats from its seed, its starting fixture and any explicit
//! choices only when every Service in it is simulated.
//!
//! The guide's testing page (<https://tizoc.github.io/causet/testing.html>) shows them together: one
//! node, generated runs, and several nodes on a simulated world.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

mod generate;
mod group;

pub use generate::{Config, Found, check, replay, replay_with, search};
pub use group::{Group, GroupStep, Views};

use causet_core::{
    BugLog, BugReport, Component, DispatchOutcome, EventBus, EventSink, Execution, HeldEvent,
    HeldEvents, HeldId, InlineCall, InvariantStep, Lane, ManualClock, MessageView, Mount, Observer,
    Path, Phase, Protocol, Refusal, Registry, ResolveError, SessionId, Store, StoreOptions,
    TimerControl, Timestamp,
};

// ---------------------------------------------------------------------------
// The determinism check's record

/// Records a node's executions, refusals and inline calls while a generated
/// run is checked for determinism; nothing otherwise. Each line carries the
/// message's `Debug` form, so two runs that send the same kinds with other
/// values differ.
struct RunTrace {
    node: String,
}

impl RunTrace {
    fn execution(&self, e: &Execution<'_>, payload: &dyn MessageView) {
        generate::record(|| {
            format!(
                "{} {:?} {} {} id {} time {} causal {} depth {}: {:?}",
                self.node,
                e.category,
                e.path,
                e.kind,
                e.meta.id.0,
                e.meta.time,
                e.meta.causal_execution_id().0,
                e.meta.depth,
                payload
            )
        });
    }
}

impl Observer for RunTrace {
    fn action_start(&mut self, exec: &Execution<'_>, payload: &dyn MessageView) {
        self.execution(exec, payload);
    }

    fn effect_start(&mut self, exec: &Execution<'_>, payload: &dyn MessageView) {
        self.execution(exec, payload);
    }

    fn event_start(&mut self, exec: &Execution<'_>, payload: &dyn MessageView) {
        self.execution(exec, payload);
    }

    fn action_refused(&mut self, r: &Refusal<'_>, payload: &dyn MessageView) {
        generate::record(|| {
            format!(
                "{} refused {} {} by {:?} at {}: {:?}",
                self.node, r.path, r.kind, r.by, r.time, payload
            )
        });
    }

    fn inline_action(&mut self, i: &InlineCall<'_>, payload: &dyn MessageView) {
        generate::record(|| {
            format!(
                "{} inline {} {} within {} at {}: {:?}",
                self.node, i.path, i.kind, i.within.0, i.time, payload
            )
        });
    }
}

// ---------------------------------------------------------------------------
// Rng

/// The choices of a run. Every draw is a bounded integer, "3 out of 0..10",
/// and 0 is always the simplest answer.
///
/// A generator made by `Rng::new` draws from a seed (SplitMix64: the same
/// seed, the same sequence, on every platform). One that [`check`] or
/// [`replay`] makes also keeps every choice it makes, or replays a list of
/// them: then a run is described completely by its list. Clones share one
/// stream, so a scenario, a simulated network and a test's own steps can
/// draw from it together, in the order the run draws.
#[derive(Clone)]
pub struct Rng(Arc<Mutex<Stream>>);

struct Stream {
    source: Source,
    /// Every choice made, when they are kept.
    kept: Option<Vec<u64>>,
}

enum Source {
    Seeded {
        seed: u64,
        state: u64,
    },
    /// A list of choices; past its end every choice is 0.
    Replay {
        choices: Vec<u64>,
        at: usize,
    },
}

/// The bound of [`Rng::next_u64`]: 2^53 - 1, TypeScript's
/// `Number.MAX_SAFE_INTEGER`.
const NEXT_BOUND: u64 = (1 << 53) - 1;

/// The payload `Rng::reject` unwinds with.
pub(crate) struct Rejected;

impl Rng {
    /// A generator starting from `seed`.
    pub fn new(seed: u64) -> Self {
        Self::from_stream(Source::Seeded { seed, state: seed }, false)
    }

    /// A generator from `seed` that keeps its choices.
    pub(crate) fn recording(seed: u64) -> Self {
        Self::from_stream(Source::Seeded { seed, state: seed }, true)
    }

    /// A generator replaying `choices`, each clamped to its draw's bound,
    /// then 0; it keeps the choices it actually makes.
    pub(crate) fn replaying(choices: &[u64]) -> Self {
        Self::from_stream(
            Source::Replay {
                choices: choices.to_vec(),
                at: 0,
            },
            true,
        )
    }

    fn from_stream(source: Source, keep: bool) -> Self {
        Self(Arc::new(Mutex::new(Stream {
            source,
            kept: keep.then(Vec::new),
        })))
    }

    fn stream(&self) -> std::sync::MutexGuard<'_, Stream> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The seed, for a generator drawing from one.
    pub fn seed(&self) -> Option<u64> {
        match self.stream().source {
            Source::Seeded { seed, .. } => Some(seed),
            Source::Replay { .. } => None,
        }
    }

    /// The choices made so far, if this generator keeps them.
    pub fn choices(&self) -> Option<Vec<u64>> {
        self.stream().kept.clone()
    }

    /// A number in `0..n`; 0, and no choice made, when `n` is 0 or 1.
    pub fn below(&mut self, n: u64) -> u64 {
        if n <= 1 {
            return 0;
        }
        let mut stream = self.stream();
        let choice = match &mut stream.source {
            Source::Seeded { state, .. } => split_mix(state) % n,
            Source::Replay { choices, at } => {
                let choice = choices.get(*at).map_or(0, |&c| c.min(n - 1));
                *at += 1;
                choice
            }
        };
        if let Some(kept) = &mut stream.kept {
            kept.push(choice);
        }
        choice
    }

    /// A generator for the run's world: one
    /// choice, a seed, and a generator seeded from it that records nothing.
    /// The world (a simulated network, Services' delays) draws from it, the
    /// run's steps from this one: neither then shifts the other's draws,
    /// and a run's choices are its steps' and the seed. Replaying them
    /// replays the world; shrinking tries the seed as one choice.
    pub fn split(&mut self) -> Rng {
        Rng::new(self.next_u64())
    }

    /// A number below 2^53 - 1, as one choice. Every port draws it below
    /// the same bound, the largest TypeScript holds exactly, so a seed's
    /// stream, and every world [`split`](Self::split) seeds from it, is the
    /// same in every port.
    pub fn next_u64(&mut self) -> u64 {
        self.below(NEXT_BOUND)
    }

    /// A number in `low..=high`.
    pub fn between(&mut self, low: u64, high: u64) -> u64 {
        low + self.below(high.saturating_sub(low).saturating_add(1))
    }

    /// True with probability `numerator / denominator`; a choice of 0 says
    /// no.
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.below(denominator) >= denominator.saturating_sub(numerator)
    }

    /// Whether a run takes another step: 15 times in 16, so a run takes 16
    /// steps on average. One choice per step, so deleting a choice while
    /// shrinking deletes a step, and a run whose choices ran out stops.
    pub fn more(&mut self) -> bool {
        self.chance(15, 16)
    }

    /// One of `items`, the first being the simplest.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        assert!(!items.is_empty(), "pick from no items");
        &items[self.below(items.len() as u64) as usize]
    }

    /// Ends this run as rejected (an invalid fixture, say): [`check`] does
    /// not count it as a run or a failure, and neither does shrinking.
    pub fn reject(&self) -> ! {
        std::panic::resume_unwind(Box::new(Rejected))
    }
}

impl std::fmt::Debug for Rng {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stream = self.stream();
        match &stream.source {
            Source::Seeded { seed, .. } => write!(f, "Rng(seed {seed})"),
            Source::Replay { choices, at } => write!(f, "Rng(replay {at} of {})", choices.len()),
        }
    }
}

// ---------------------------------------------------------------------------
// Swarm

/// The step kinds a run uses, drawn once at its start: each is on with chance
/// 1/2, a choice of 0 meaning on, so that shrinking turns kinds back on
/// toward the plain mix; if none is on, all are. Runs then differ in what
/// they stress, which reaches rare combinations in fewer runs than one mix
/// for every run ("swarm testing").
#[derive(Clone, Debug)]
pub struct Swarm<K> {
    on: Vec<(K, u64)>,
    /// The weights of the kinds on, summed.
    total: u64,
}

impl<K: Copy> Swarm<K> {
    /// Draws which of `kinds` this run uses; each comes with its weight, the
    /// first being the simplest. The weights must be positive, and their
    /// total must fit in a `u64`, so that a pick among any of them does.
    pub fn new(rng: &mut Rng, kinds: &[(K, u64)]) -> Self {
        assert!(!kinds.is_empty(), "a swarm of no kinds");
        assert!(
            kinds.iter().all(|(_, weight)| *weight > 0),
            "a kind of weight 0 is never picked"
        );
        assert!(
            kinds
                .iter()
                .try_fold(0u64, |total, (_, weight)| total.checked_add(*weight))
                .is_some(),
            "a swarm's weights must total at most u64::MAX"
        );
        let mut on = Vec::with_capacity(kinds.len());
        for kind in kinds {
            if rng.below(2) == 0 {
                on.push(*kind);
            }
        }
        if on.is_empty() {
            on = kinds.to_vec();
        }
        let total = on.iter().map(|(_, weight)| weight).sum();
        Self { on, total }
    }

    /// One of the kinds on, by weight.
    pub fn pick(&self, rng: &mut Rng) -> K {
        let mut at = rng.below(self.total);
        for (kind, weight) in &self.on {
            if at < *weight {
                return *kind;
            }
            at -= weight;
        }
        unreachable!("a draw below the total weight")
    }

    /// The kinds on.
    pub fn kinds(&self) -> impl Iterator<Item = K> + '_ {
        self.on.iter().map(|(kind, _)| *kind)
    }
}

/// SplitMix64.
fn split_mix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

// ---------------------------------------------------------------------------
// Node

/// Makes a node's view for a group from its State.
type MakeView<S> = Box<dyn Fn(&S) -> Box<dyn std::any::Any>>;

/// A view a node publishes into a group, and how it is made from State.
struct Publication<S> {
    membership: group::Membership,
    view: MakeView<S>,
}

/// How many Events a node's worker lane holds once released.
const WORKER_CAPACITY: usize = 1024;

/// A real Store under test: a held bus with a chosen session, the shared
/// clock, and invariants checked after every root input. Its methods panic,
/// naming the node and the step, when the Store faults, an invariant fails or
/// a bug is reported unexpectedly: a test fails at the step that broke.
pub struct Node<S, Svc> {
    name: String,
    store: Store<S, Svc>,
    sink: EventSink,
    held: HeldEvents,
    protocol: Arc<Protocol<S, Svc>>,
    clock: ManualClock,
    timers: Option<TimerControl>,
    publications: Vec<Publication<S>>,
    /// Root inputs run so far, to name the step that failed.
    steps: usize,
    /// Bug reports already checked, and whether the test expects them.
    bugs_seen: usize,
    allow_bugs: bool,
}

impl<S: 'static, Svc: 'static> Node<S, Svc> {
    /// A node whose root component is `R`, starting from `state`.
    pub fn new<R>(
        name: &str,
        state: S,
        services: Svc,
        session: SessionId,
        clock: &ManualClock,
    ) -> Self
    where
        R: Component<State = S, Service = Svc>,
    {
        let mut registry = Registry::<S, Svc>::new();
        registry.root::<R>();
        let protocol = registry
            .freeze()
            .unwrap_or_else(|e| panic!("node {name}: {e}"));
        Self::from_protocol(name, protocol, state, services, session, clock)
    }

    /// A node running `protocol`, for an application that builds its own,
    /// or shares one Protocol between nodes.
    pub fn from_protocol(
        name: &str,
        protocol: Arc<Protocol<S, Svc>>,
        state: S,
        services: Svc,
        session: SessionId,
        clock: &ManualClock,
    ) -> Self {
        let (sink, inbox, held) = EventBus::held(&protocol, WORKER_CAPACITY, session);
        let options = StoreOptions::with_clock(clock.clone());
        let mut store = Store::new(Arc::clone(&protocol), state, services, inbox, options)
            .unwrap_or_else(|e| panic!("node {name}: {e:?}"));
        store.add_observer(BugLog::new());
        store.add_observer(RunTrace {
            node: name.to_owned(),
        });
        Self {
            name: name.to_owned(),
            store,
            sink,
            held,
            protocol,
            clock: clock.clone(),
            timers: None,
            publications: Vec::new(),
            steps: 0,
            bugs_seen: 0,
            allow_bugs: false,
        }
    }

    /// Bug reports (`bug_condition!`) are expected: they no longer fail the
    /// node, and the test checks them with `bug_reports`.
    pub fn allow_bugs(mut self) -> Self {
        self.allow_bugs = true;
        self
    }

    /// The bug reports this Store made, in order.
    pub fn bug_reports(&self) -> &[BugReport] {
        self.store.observer::<BugLog>().map_or(&[], BugLog::reports)
    }

    /// The node's simulated timer, so a scenario fires it as time passes.
    pub fn with_timer(mut self, timers: TimerControl) -> Self {
        self.timers = Some(timers);
        self
    }

    /// An invariant: a read-only function of State that says what is wrong,
    /// if anything, registered on the Store. It is
    /// checked at once on the current State, which validates a fixture, then
    /// after every root input; a violation, or a panic of the check, fails
    /// the node whatever `allow_bugs` says. The Store's other bug reports
    /// fail it too, unless `allow_bugs`.
    #[track_caller]
    pub fn check(
        self,
        name: &'static str,
        check: impl Fn(&S) -> Result<(), String> + Send + 'static,
    ) -> Self {
        self.check_with(name, move |state, _| check(state))
    }

    /// `check`, with memory and the step it runs at: `Added`, `Restored`
    /// after `restart`, or the root input that `Settled`.
    #[track_caller]
    pub fn check_with(
        mut self,
        name: &'static str,
        check: impl FnMut(&S, &InvariantStep<'_>) -> Result<(), String> + Send + 'static,
    ) -> Self {
        self.store.add_invariant(name, check);
        let when = if self.steps == 0 {
            "before any root input".to_owned()
        } else {
            "when the check was added".to_owned()
        };
        self.assert_reports(&when);
        self
    }

    /// Publishes a view of this node's State into `group` under the node's
    /// name: made now, then after each of the
    /// node's root inputs, for the group's checks to read.
    pub fn publish<V: 'static>(mut self, group: &Group, view: impl Fn(&S) -> V + 'static) -> Self {
        let first = Box::new(view(self.store.state()));
        let membership = group.join(&self.name, first);
        let view: MakeView<S> = Box::new(move |state| Box::new(view(state)));
        self.publications.push(Publication { membership, view });
        self
    }

    /// The node's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The Store's State.
    pub fn state(&self) -> &S {
        self.store.state()
    }

    /// The Store, for what this harness does not wrap.
    pub fn store(&self) -> &Store<S, Svc> {
        &self.store
    }

    /// The Store, to add observers and the like.
    pub fn store_mut(&mut self) -> &mut Store<S, Svc> {
        &mut self.store
    }

    /// The bus's sending side, for the application's world to send through.
    pub fn sink(&self) -> &EventSink {
        &self.sink
    }

    /// The Protocol the node runs.
    pub fn protocol(&self) -> &Arc<Protocol<S, Svc>> {
        &self.protocol
    }

    /// This run's session.
    pub fn session(&self) -> SessionId {
        self.store.session()
    }

    /// The mount at `path`.
    pub fn mount<C: Component>(&self, path: &Path<C>) -> Mount<C> {
        self.try_mount(path)
            .unwrap_or_else(|e| panic!("node {}: {e}", self.name))
    }

    /// The mount at `path`, or why there is none.
    pub fn try_mount<C: Component>(&self, path: &Path<C>) -> Result<Mount<C>, ResolveError> {
        self.protocol.resolve(path)
    }

    /// Dispatches an Action as a root input, then checks the invariants.
    pub fn dispatch<C: Component>(
        &mut self,
        mount: &Mount<C>,
        action: C::Action,
    ) -> DispatchOutcome {
        generate::count_step(&self.name);
        generate::input_starts();
        let outcome = self.store.dispatch_to(mount, action);
        self.steps += 1;
        let outcome = outcome.unwrap_or_else(|e| self.fault(e));
        match outcome {
            DispatchOutcome::Executed => self.settled(),
            // Nothing ran, and nothing settled: the reports of its admission
            // still count, but its groups are not told.
            DispatchOutcome::Disabled => {
                self.assert_reports(&format!("after root input {}", self.steps))
            }
            DispatchOutcome::Faulted => unreachable!("a root input returns its fault as an error"),
        }
        outcome
    }

    /// The Events held for this node, in send order.
    pub fn held(&self) -> Vec<HeldEvent> {
        self.held.list()
    }

    /// Calls `f` with the held Event `id`, if it is held and an `Ev`.
    pub fn inspect<Ev: causet_core::Event, R>(
        &self,
        id: HeldId,
        f: impl FnOnce(&Ev) -> R,
    ) -> Option<R> {
        self.held.inspect(id, f)
    }

    /// Delivers the held Event `id`: releases it, then runs every released
    /// Event as a root input, checking the invariants after each.
    pub fn deliver(&mut self, id: HeldId) {
        self.held
            .release(id)
            .unwrap_or_else(|e| panic!("node {}: cannot release {id:?}: {e:?}", self.name));
        self.drain();
    }

    /// Delivers the held Event the bus would run next if nothing were held
    /// ([`next_in_queue_order`]), and returns it.
    pub fn deliver_next(&mut self) -> Option<HeldEvent> {
        let held = self.held();
        let event = held[next_in_queue_order(&held)?].clone();
        self.deliver(event.id);
        Some(event)
    }

    /// Delivers the first held `Ev` for which `select` is true. Returns
    /// whether there was one.
    pub fn deliver_where<Ev: causet_core::Event>(&mut self, select: impl Fn(&Ev) -> bool) -> bool {
        let found = self.held().into_iter().find(|e| {
            self.held
                .inspect::<Ev, bool>(e.id, &select)
                .unwrap_or(false)
        });
        match found {
            Some(event) => {
                self.deliver(event.id);
                true
            }
            None => false,
        }
    }

    /// Drops the held Event `id`, as a lost message.
    pub fn discard(&mut self, id: HeldId) -> bool {
        self.held.discard(id)
    }

    /// Fires the node's simulated timers that are due on the shared clock;
    /// their Events are held like any other.
    pub fn fire_timers(&mut self) -> usize {
        self.timers.as_ref().map_or(0, TimerControl::fire_due)
    }

    /// The node's earliest timer deadline, if it has a simulated timer.
    pub fn next_deadline(&self) -> Option<Timestamp> {
        self.timers.as_ref().and_then(TimerControl::next_deadline)
    }

    /// The node restarted: its Store, bus, held Events and timers are gone
    /// (anything addressed to them is refused), and a new Store starts from
    /// `state`, the State the application chose to keep, with a new
    /// `session`. The protocol, the clock, the invariants with their memory
    /// and the node's place in its groups stay: the invariants run first on
    /// the restored State (`InvariantStep::Restored`), then the groups'
    /// checks (`GroupStep::Restarted`). Give the new Services' simulated
    /// timer with `with_timer`. What survives a restart is the application's
    /// policy.
    pub fn restart(self, state: S, services: Svc, session: SessionId) -> Self {
        let Self {
            name,
            mut store,
            protocol,
            clock,
            publications,
            steps,
            allow_bugs,
            ..
        } = self;
        let invariants = store.take_invariants();
        drop(store);
        let mut node = Self::from_protocol(&name, protocol, state, services, session, &clock);
        node.steps = steps;
        node.allow_bugs = allow_bugs;
        node.publications = publications;
        node.store.restore_invariants(invariants);
        node.assert_reports("on restart");
        node.refresh_groups(|node| GroupStep::Restarted { node });
        node
    }

    fn drain(&mut self) {
        loop {
            generate::input_starts();
            match self.store.drain_events(1) {
                Ok(0) => return,
                Ok(_) => {
                    generate::count_step(&self.name);
                    self.steps += 1;
                    self.settled();
                }
                Err(e) => {
                    self.steps += 1;
                    self.fault(e)
                }
            }
        }
    }

    /// Fails on a Store fault, keyed by where the faulting code panicked.
    fn fault(&self, e: causet_core::DispatchError) -> ! {
        let message = match &e {
            causet_core::DispatchError::Faulted(message) => message.as_str(),
            _ => "",
        };
        let at = generate::fault_panic(message).unwrap_or_else(|| "an unknown place".to_owned());
        generate::failing(format!("node {}: fault at {at}", self.name));
        panic!("node {}, root input {}: {e:?}", self.name, self.steps)
    }

    /// A root input settled: the node's reports, then its groups.
    fn settled(&mut self) {
        self.assert_reports(&format!("after root input {}", self.steps));
        self.refresh_groups(|node| GroupStep::Settled { node });
    }

    /// Fails on the first report not seen yet that fails the node: any
    /// invariant's, and any other unless `allow_bugs`.
    fn assert_reports(&mut self, when: &str) {
        let reports = self.bug_reports();
        let new: Vec<BugReport> = reports[self.bugs_seen.min(reports.len())..].to_vec();
        self.bugs_seen = reports.len();
        for report in new {
            let at = format!("{}:{}", report.file, report.line);
            match report.place.as_ref().map(|place| place.phase) {
                Some(Phase::Invariant(name)) => {
                    generate::failing(format!("node {}: invariant {name:?}", self.name));
                    panic!(
                        "node {}, {when}: invariant {name:?}: {}",
                        self.name, report.message
                    );
                }
                Some(Phase::InvariantPanic(name)) => {
                    generate::failing(format!("node {}: invariant {name:?} panicked", self.name));
                    panic!(
                        "node {}, {when}: invariant {name:?}: {}",
                        self.name, report.message
                    );
                }
                _ if self.allow_bugs => {}
                _ => {
                    generate::failing(format!("node {}: bug report at {at}", self.name));
                    panic!(
                        "node {}, {when}: unexpected bug report at {at}: {}",
                        self.name, report.message
                    );
                }
            }
        }
    }

    /// Refreshes every view this node publishes, then runs each of its
    /// groups' checks once.
    fn refresh_groups(&mut self, step: impl Fn(&str) -> GroupStep<'_>) {
        if self.publications.is_empty() {
            return;
        }
        let state = self.store.state();
        for publication in &self.publications {
            publication
                .membership
                .group
                .update(&self.name, (publication.view)(state));
        }
        let mut groups: Vec<&Group> = Vec::new();
        for publication in &self.publications {
            let group = &publication.membership.group;
            if !groups.iter().any(|g| g.same(group)) {
                groups.push(group);
            }
        }
        for group in groups {
            group.run(step(&self.name));
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario

/// What a scenario can do with a node, whatever its State and Services.
pub trait Driven {
    /// The node's name.
    fn name(&self) -> &str;
    /// The Events held for it, in send order.
    fn held(&self) -> Vec<HeldEvent>;
    /// Delivers one held Event.
    fn deliver(&mut self, id: HeldId);
    /// Its earliest timer deadline.
    fn next_deadline(&self) -> Option<Timestamp>;
    /// Fires its due timers.
    fn fire_timers(&mut self) -> usize;
    /// The node, to read its State in a stop condition.
    fn as_any(&self) -> &dyn Any;
}

impl<S: 'static, Svc: 'static> Driven for Node<S, Svc> {
    fn name(&self) -> &str {
        Node::name(self)
    }
    fn held(&self) -> Vec<HeldEvent> {
        Node::held(self)
    }
    fn deliver(&mut self, id: HeldId) {
        Node::deliver(self, id);
    }
    fn next_deadline(&self) -> Option<Timestamp> {
        Node::next_deadline(self)
    }
    fn fire_timers(&mut self) -> usize {
        Node::fire_timers(self)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// The application's world outside the Stores, such as a simulated network:
/// what it will do next, and doing it once the clock gets there. It sends
/// into nodes through their sinks, so what it delivers is held like any
/// Event. It runs on the driver's thread, which is the Stores' own: what
/// stands in for a worker thread sends with `try_send_to_as_worker` or
/// `Reply::try_send_as_worker`, so its Events take the worker lane.
pub trait World {
    /// The earliest time it has something to do.
    fn next_time(&self) -> Option<Timestamp> {
        None
    }
    /// Does what is due at `now`.
    fn advance(&mut self, _now: Timestamp) {}
}

/// A world with nothing in it.
pub struct NoWorld;

impl World for NoWorld {}

/// What a simulated world has in flight: messages due at a time, delivered
/// in that order, and in the order they were sent when due together. On a
/// channel (one direction of a link, say), a message can also be kept after
/// the one before it, whatever delay it drew. What a message is, and what
/// delivering it means, are the world's: its [`World`] reports
/// [`next_time`], and delivers what [`due`] returns.
///
/// [`next_time`]: InFlight::next_time
/// [`due`]: InFlight::due
#[derive(Debug)]
pub struct InFlight<M, C = ()> {
    queue: BTreeMap<(Timestamp, u64), M>,
    sent: u64,
    /// By channel, when its last message is due.
    last: BTreeMap<C, Timestamp>,
}

impl<M, C: Ord> Default for InFlight<M, C> {
    fn default() -> Self {
        Self {
            queue: BTreeMap::new(),
            sent: 0,
            last: BTreeMap::new(),
        }
    }
}

impl<M, C: Ord> InFlight<M, C> {
    /// Nothing in flight.
    pub fn new() -> Self {
        Self::default()
    }

    /// `message`, due at `at`.
    pub fn send_at(&mut self, at: Timestamp, message: M) {
        self.queue.insert((at, self.sent), message);
        self.sent += 1;
    }

    /// `message` on `channel`, due at `at` or just after the channel's last
    /// message, whichever is later: a channel's first message is due at
    /// `at`. Returns when it is due. After a message due at the end of time
    /// every later one on its channel is due then too, after it by sending.
    pub fn send_in_order(&mut self, channel: C, at: Timestamp, message: M) -> Timestamp {
        let at = match self.last.get(&channel) {
            Some(last) => at.max(last.saturating_add(1)),
            None => at,
        };
        self.last.insert(channel, at);
        self.send_at(at, message);
        at
    }

    /// What is in flight, in the order it will be delivered, with when it
    /// is due.
    pub fn iter(&self) -> impl Iterator<Item = (Timestamp, &M)> {
        self.queue.iter().map(|((at, _), message)| (*at, message))
    }

    /// When the next message is due.
    pub fn next_time(&self) -> Option<Timestamp> {
        self.queue.first_key_value().map(|((at, _), _)| *at)
    }

    /// Every message due at `now` or before, in order, taken out.
    pub fn due(&mut self, now: Timestamp) -> Vec<M> {
        let later = match now.checked_add(1) {
            Some(after) => self.queue.split_off(&(after, 0)),
            None => BTreeMap::new(),
        };
        std::mem::replace(&mut self.queue, later)
            .into_values()
            .collect()
    }

    /// How many messages are in flight.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Whether nothing is in flight.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

/// One step of a scenario.
#[derive(Clone, Debug)]
pub enum Step {
    /// A held Event was delivered to the node at this index.
    Delivered {
        /// The node's index among the scenario's nodes.
        node: usize,
        /// The Event delivered.
        event: HeldEvent,
    },
    /// The clock moved to this time; the world, then the timers, did what
    /// was due.
    Advanced {
        /// The new time.
        to: Timestamp,
        /// How many timers fired.
        fired: usize,
    },
}

/// Why a scenario stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stop {
    /// The stop condition held.
    Reached,
    /// Nothing is held, and no timer or world event is ahead.
    Quiet,
    /// The step budget ran out.
    Budget,
    /// The next thing to do lies past the time limit.
    TimeLimit,
}

/// Picks the next Event to deliver among those held: `(node, event)` pairs in
/// node order, each node's in send order. Returns an index into them.
pub type Chooser = dyn FnMut(&[(usize, HeldEvent)], &mut Rng) -> usize;

/// Which of one bus's held Events (as `HeldEvents::list` gives them, in send
/// order) its lanes would run next had none been held: the drain takes the
/// Store lane before the worker lane, and each lane holds the Events sent to
/// its head newest first, then the others oldest first. Delivering Events
/// one at a time in this order, each with the lanes empty, runs them as
/// production would.
pub fn next_in_queue_order(held: &[HeldEvent]) -> Option<usize> {
    let indices = 0..held.len();
    let next_in = |lane: Lane| {
        let on = |i: &usize| held[*i].lane == lane;
        indices
            .clone()
            .rfind(|i| on(i) && held[*i].front)
            .or_else(|| indices.clone().find(|i| on(i) && !held[*i].front))
    };
    next_in(Lane::Store).or_else(|| next_in(Lane::Worker))
}

/// Drives several nodes on one virtual clock.
///
/// If the test panics while the scenario is alive (an invariant, a fault, a
/// failed assertion), the scenario prints its seed, how many steps it took
/// and the last of them on drop, so the failure can be repeated from the
/// seed without reading the test first.
pub struct Scenario {
    /// The clock every node and simulated timer of the scenario shares.
    pub clock: ManualClock,
    /// The scenario's generator, also lent to the application's choices.
    pub rng: Rng,
    /// Every step taken, in words.
    pub log: Vec<String>,
}

impl Scenario {
    /// A scenario starting at `start` on its clock, with choices from `seed`.
    pub fn new(seed: u64, start: Timestamp) -> Self {
        Self::with_rng(Rng::new(seed), start)
    }

    /// A scenario drawing from `rng`, a generated run's.
    pub fn with_rng(rng: Rng, start: Timestamp) -> Self {
        Self {
            clock: ManualClock::new(start),
            rng,
            log: Vec::new(),
        }
    }

    /// The seed its choices come from; `None` when they are replayed.
    pub fn seed(&self) -> Option<u64> {
        self.rng.seed()
    }

    /// The log, kept after the scenario is done (it cannot be moved out of a
    /// `Scenario`, which reports on drop).
    pub fn into_log(mut self) -> Vec<String> {
        std::mem::take(&mut self.log)
    }

    /// The failure report: the seed, the step count and the last `last`
    /// steps.
    pub fn report(&self, last: usize) -> String {
        let mut out = match self.seed() {
            Some(seed) => format!("scenario seed {seed} failed after {} steps", self.log.len()),
            None => format!(
                "scenario on replayed choices failed after {} steps",
                self.log.len()
            ),
        };
        let skipped = self.log.len().saturating_sub(last);
        if skipped > 0 {
            let _ = write!(out, "; the last {last}:");
        } else if !self.log.is_empty() {
            out.push(':');
        }
        for (n, step) in self.log.iter().enumerate().skip(skipped) {
            let _ = write!(out, "\n  {:>4}. {step}", n + 1);
        }
        out
    }

    /// One step: deliver a held Event if any node has one (of the first node
    /// that has one, the Event its lanes would run next: production's order),
    /// else advance the clock to the
    /// next timer deadline or world time, let the world act, then fire the
    /// timers due. `None` when nothing is held or ahead.
    pub fn step(&mut self, nodes: &mut [&mut dyn Driven], world: &mut dyn World) -> Option<Step> {
        self.step_with(
            nodes,
            world,
            &mut |candidates: &[(usize, HeldEvent)], _: &mut Rng| {
                let node = candidates[0].0;
                let first: Vec<HeldEvent> = candidates
                    .iter()
                    .take_while(|(n, _)| *n == node)
                    .map(|(_, e)| e.clone())
                    .collect();
                next_in_queue_order(&first).expect("a held Event")
            },
        )
    }

    /// `step`, with `choose` picking which held Event to deliver among every
    /// node's, listed node by node in send order: a seeded choice explores
    /// orders the bus alone would not make. A chooser that keeps
    /// production's order within a node uses [`next_in_queue_order`].
    pub fn step_with(
        &mut self,
        nodes: &mut [&mut dyn Driven],
        world: &mut dyn World,
        choose: &mut Chooser,
    ) -> Option<Step> {
        let candidates: Vec<(usize, HeldEvent)> = nodes
            .iter()
            .enumerate()
            .flat_map(|(i, n)| n.held().into_iter().map(move |e| (i, e)))
            .collect();
        if !candidates.is_empty() {
            let pick = choose(&candidates, &mut self.rng).min(candidates.len() - 1);
            let (node, event) = candidates[pick].clone();
            self.log.push(format!(
                "{}: deliver {} to {}",
                nodes[node].name(),
                event.kind,
                event.path
            ));
            nodes[node].deliver(event.id);
            return Some(Step::Delivered { node, event });
        }
        let next = self.next_time(nodes, world)?;
        let to = next.max(self.clock.get());
        self.clock.set(to);
        world.advance(to);
        let fired = nodes.iter_mut().map(|n| n.fire_timers()).sum();
        self.log
            .push(format!("advance to {to}: {fired} timers fired"));
        Some(Step::Advanced { to, fired })
    }

    /// Steps until `until` holds, nothing is left, `steps` steps were taken,
    /// or the next thing to do lies past `time_limit`.
    pub fn run_until(
        &mut self,
        nodes: &mut [&mut dyn Driven],
        world: &mut dyn World,
        steps: usize,
        time_limit: Option<Timestamp>,
        mut until: impl FnMut(&[&mut dyn Driven]) -> bool,
    ) -> Stop {
        for _ in 0..steps {
            if until(nodes) {
                return Stop::Reached;
            }
            let held = nodes.iter().any(|n| !n.held().is_empty());
            if !held
                && let (Some(limit), Some(next)) = (time_limit, self.next_time(nodes, world))
                && next > limit
            {
                return Stop::TimeLimit;
            }
            if self.step(nodes, world).is_none() {
                return Stop::Quiet;
            }
        }
        if until(nodes) {
            Stop::Reached
        } else {
            Stop::Budget
        }
    }

    /// What is left: each node's held Events and next timer, and the world's
    /// next time. For a stop that was not `Reached`.
    pub fn pending(&self, nodes: &[&mut dyn Driven], world: &dyn World) -> String {
        let mut out = format!("at {}:\n", self.clock.get());
        for node in nodes {
            let held = node.held();
            let _ = writeln!(
                out,
                "  {}: {} held, next timer {:?}",
                node.name(),
                held.len(),
                node.next_deadline()
            );
            for e in held {
                let _ = writeln!(
                    out,
                    "    {:?} {} to {} ({:?} lane, answers {:?})",
                    e.id, e.kind, e.path, e.lane, e.answers
                );
            }
        }
        let _ = writeln!(out, "  world: next {:?}", world.next_time());
        out
    }

    fn next_time(&self, nodes: &[&mut dyn Driven], world: &dyn World) -> Option<Timestamp> {
        nodes
            .iter()
            .filter_map(|n| n.next_deadline())
            .chain(world.next_time())
            .min()
    }
}

/// How many steps a failure report shows.
const REPORTED_STEPS: usize = 20;

impl Drop for Scenario {
    fn drop(&mut self) {
        if !std::thread::panicking() || generate::quiet() {
            return;
        }
        // A panic now, during unwinding, would abort the process, and
        // `eprintln!` panics when stderr is closed. A thread of its own keeps
        // that panic there, and still inherits the test's output capture,
        // which a direct write to stderr would bypass.
        let report = self.report(REPORTED_STEPS);
        if let Ok(printer) = std::thread::Builder::new().spawn(move || eprintln!("{report}")) {
            let _ = printer.join();
        }
    }
}
