<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/tizoc/causet.rs/main/assets/logo-dark.svg">
  <img src="https://raw.githubusercontent.com/tizoc/causet.rs/main/assets/logo.svg" alt="" width="120" height="36">
</picture>

# Causet

Causet is a framework for building event-driven systems as explicit,
composable state machines, with automatic bug finding through deterministic
simulation.

You define the operations your system can perform and the rules it must
obey. Causet generates simulated test runs that vary inputs, event delivery,
time and modeled failures. It checks your invariants after every input,
shrinks failing runs and gives you the choices needed to reproduce them.

> **Unreleased.** No Causet packages have been published to crates.io yet.
> Use a commit-pinned Git dependency as shown below.

The [changelog](https://github.com/tizoc/causet.rs/blob/main/CHANGELOG.md) tracks notable changes.

It addresses a common problem in asynchronous software: the behavior of a
system becomes scattered across callbacks, tasks, timers and shared state.
Understanding a transition requires reconstructing what ran, in which
order, and which request a delayed result belonged to. Testing those
interactions reliably becomes difficult.

Causet gives that behavior a clear structure:

- **Actions** request state transitions. Reducers decide how state
  changes, and no other code changes it.
- **Effects** request external work. Services perform networking, storage
  and other I/O.
- **Events** bring results back. Request identities and reply routes tie
  each result to the request and the component that made it, so a reducer
  can tell a stale result from the one it is waiting for.
- **Outputs** connect components. A parent decides how a child's facts
  affect the wider application.

```text
          In production                     In a test

   ┌─────────────────────────┐     ┌─────────────────────────┐
   │ network, disk, clock    │     │ simulated network, disk │
   │                         │     │ and a virtual clock     │
   └──────▲───────────┬──────┘     └──────▲───────────┬──────┘
          │ Effects   │ Events            │ Effects   │ Events
   ┌──────┴───────────▼──────┐     ┌──────┴───────────▼──────┐
   │ your components:        │     │ the same components;    │
   │ one input at a time,    │     │ the test decides what   │
   │ in a fixed order        │     │ arrives, and when       │
   └─────────────────────────┘     └─────────────────────────┘
```

The components never touch the world directly: they ask for work with
Effects and hear back through Events. So the same components run against
the real world or a simulated one. They handle one input at a time, in a
fixed order, with time from the Store's clock, so a test can repeat a run
exactly (Guarantees below says on what conditions).

Its main advantages are:

- **Automatic bug discovery.** Generated simulations explore operation
  sequences and event timing while checking application invariants.
- **Reproducible failures.** Shrinking reduces a failing run to a small
  sequence of choices that reproduces it. Individual transitions can also
  be tested from deliberately constructed states.
- **Predictable execution.** Explicit scheduling and admission checks make
  ordering and transition rules easier to reason about.
- **Reusable components.** Components own their state and protocols, while
  their parent owns integration and policy. They can live in independent
  crates.
- **Causal observability.** Execution metadata connects actions, effects
  and component emissions, helping explain how the system reached a
  particular outcome.

The cost is more explicit protocol and lifecycle code. That investment is
especially useful when the hard parts are ordering, timeouts, retries,
cancellation and stale asynchronous results, as in network protocols,
wallets and synchronization engines.

## Getting started

Use both packages from the same full commit of this repository. Replace
`COMMIT_SHA` with the revision you want to use:

```toml
[dependencies]
causet = { git = "https://github.com/tizoc/causet.rs.git", rev = "COMMIT_SHA" }

[dev-dependencies]
causet-test = { git = "https://github.com/tizoc/causet.rs.git", rev = "COMMIT_SHA" }
```

Commit your application's `Cargo.lock`. Registry installation will be
available after the first package release.

In a clone of this repository, start with the shop, then explore further:

```sh
cargo run -p causet --example shop        # one checkout, then reordered replies in simulation
cargo run -p causet --example downloads   # worker threads, timeouts and retries
cargo test -p p2p-node                    # up to four nodes on a simulated network, in generated runs
```

The [examples index](examples/README.md) gives a reading order, entry points and commands
for the larger reference applications.

## Find bugs automatically

A simulated payment gateway has a total budget of 100. Its bug is forgetting
to deduct earlier approvals. This test generates charges and decides when
to deliver replies, checking that total payments stay within the budget.
It checks the fixed gateway, then finds and shrinks a failure in the faulty
one and verifies that replay fails the same way.

```rust
use causet::{ManualClock, Reply, RequestId, SessionId, root};
use causet::guide::shop::{Charged, Gateway, Payments, PaymentsAction, PaymentsState};
use causet_test::{Config, Node, Rng, check, replay, search};

struct Budget { remaining: u64, forgets: bool }
impl Gateway for Budget {
    fn charge(&mut self, request: RequestId, amount: u64, reply: Reply<Charged>) {
        let approved = amount <= self.remaining;
        // The planted bug: earlier approvals are forgotten.
        if approved && !self.forgets { self.remaining -= amount; }
        reply.try_send(Charged { request, approved }).unwrap();
    }
}

fn spending(rng: &mut Rng, forgets: bool) {
    let gateway: Box<dyn Gateway> = Box::new(Budget { remaining: 100, forgets });
    let mut node = Node::new::<Payments>("shop", PaymentsState::default(), gateway, SessionId(1), &ManualClock::new(0))
        .check("paid within budget", |s: &PaymentsState| {
            if s.paid.iter().sum::<u64>() <= 100 { Ok(()) } else { Err("overspent".into()) }
        });
    let mount = node.mount(&root::<Payments>());
    while rng.more() {
        if rng.chance(1, 2) {
            node.dispatch(&mount, PaymentsAction::Charge { amount: rng.between(1, 100) });
        } else {
            node.deliver_next();
        }
    }
    while node.deliver_next().is_some() {}
}

check(Config::default(), |rng| spending(rng, false)); // The fixed gateway passes.
let found = search(Config::default(), |rng| spending(rng, true)).unwrap();
assert_eq!(found.key, r#"node shop: invariant "paid within budget""#);
assert!(found.choices.len() < found.original);
println!("seed {}: {}\nshrunk {} choices to {}\nchoices {:?}",
    found.seed, found.key, found.original, found.choices.len(), found.choices);
// Replay must reproduce the planted failure.
assert!(std::panic::catch_unwind(|| replay(&found.choices, |rng| spending(rng, true))).is_err());
```

```text
seed 1: node shop: invariant "paid within budget"
shrunk 13 choices to 7
choices [1, 1, 51, 1, 1, 48, 0]
```

The shrunk run charges 52 and 49, then delivers both replies: 101 paid from
a budget of 100. `search` lets this example assert that a known bug is found;
use `check` in ordinary tests to fail on a discovered violation. Keeping the
shrunk choices gives you a reproducible regression test.


Here is the payment component, with a gateway that applies a per-request
limit, also available in [the guide's shop](crates/causet/src/guide/shop.rs):

```rust
use causet::*;

struct Payments;

#[derive(Debug, Default)]
struct PaymentsState {
    pending: Pending<u64>,
    paid: Vec<u64>,
    declined: Vec<u64>,
}

#[derive(Debug, Action)]
#[causet(type = "payments.action")]
enum PaymentsAction {
    Charge { amount: u64 },
    Settled { request: RequestId, approved: bool },
}

#[derive(Debug, Effect)]
#[causet(type = "payments.effect")]
enum PaymentsEffect {
    Charge { request: RequestId, amount: u64 },
}

/// The gateway's answer.
#[derive(Debug, Event)]
#[causet(type = "payments.charged")]
struct Charged {
    request: RequestId,
    approved: bool,
}

/// The Service: anything that charges, and answers through `reply`.
trait Gateway: Send {
    fn charge(&mut self, request: RequestId, amount: u64, reply: Reply<Charged>);
}

impl Component for Payments {
    type State = PaymentsState;
    type Service = Box<dyn Gateway>;
    type Action = PaymentsAction;
    type Effect = PaymentsEffect;
    type Output = Never;

    // The only code that changes State.
    fn reduce(mut ctx: ComponentContext<'_, Self>, action: PaymentsAction, _: &ActionMeta) {
        match action {
            PaymentsAction::Charge { amount } => {
                let request = ctx.new_request();
                ctx.state_mut().pending.insert(request, amount);
                ctx.into_emitter().emit_effect(PaymentsEffect::Charge { request, amount });
            }
            PaymentsAction::Settled { request, approved } => {
                let charges = ctx.state_mut();
                // An answer to no pending charge is stale, and ignored.
                let Some(amount) = charges.pending.take(request) else { return };
                if approved { charges.paid.push(amount) } else { charges.declined.push(amount) }
            }
        }
    }

    // The only code that reaches the outside world.
    fn handle(mut ctx: EffectContext<'_, Self>, effect: PaymentsEffect, _: &EffectMeta) {
        let PaymentsEffect::Charge { request, amount } = effect;
        let reply = ctx.reply::<Charged>();
        ctx.service().charge(request, amount, reply);
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Charged>();
    }
}

// The answer becomes an Action of the component that asked.
impl Adapts<Charged> for Payments {
    fn adapt(event: Charged, port: &mut ActionPort<'_, Self>) {
        port.emit_action(PaymentsAction::Settled { request: event.request, approved: event.approved });
    }
}

// In a test: a gateway that approves up to 100. The node holds its answers
// until the test delivers them.
struct ApproveUpTo(u64);

impl Gateway for ApproveUpTo {
    fn charge(&mut self, request: RequestId, amount: u64, reply: Reply<Charged>) {
        reply.try_send(Charged { request, approved: amount <= self.0 }).unwrap();
    }
}
```

The guide's shop (`causet::guide::shop`) puts the same component under a
cart, with child components and Outputs. Beside `downloads`, the runnable
programs in `crates/causet/examples/` are:

```sh
cargo run -p causet --example composite   # one component mounted twice, inline and queued calls
cargo run -p causet-reporting --example reporting   # reports through a reporting Service
```

Larger applications, each with transition tests and generated runs on a
simulated world (`cargo test -p <name>`):

- `examples/wallet-send`: a wallet send with paged input discovery,
  stale-result pins, retries and asynchronous proving;
- `examples/p2p` and `examples/p2p-node`: a reusable P2P component, generic
  over its messages, with several connections per peer, mounted in a node
  that fetches and serves over it; up to four nodes on a simulated network;
- `examples/chain-placement`: projections of a chain's stored reports into
  each block's placement, checked across restarts and against recomputing
  from scratch.

## Hosting

The [hosting guide](https://github.com/tizoc/causet.rs/blob/main/docs/hosting.md)
covers moving a Store to its driver thread, Event readiness through a
persistent notification or a blocking `wait_timeout` on a native driver
thread, host-driven timers and WebAssembly support.

## Guarantees

- **Authority is typed.** Only reducers change State and start Effects;
  Effect handlers use Services and read State; Event adapters and Output
  lowerers emit Actions only.
- **Order is fixed.** Each input's consequences run depth first, in the
  order emitted, each Action admitted against the State of that moment.
- **Addresses are stable.** A mount's identity is a hash of its path,
  identical in every run; replies return to the mount that asked.
- **Runs repeat, on conditions.** From the same starting State, the same
  inputs at the same clock readings make the same run, provided the
  application's own code is deterministic (the guide's determinism page
  says what that takes). The one difference is each run's session
  identity, which request IDs carry.
- **Correlation, not policy.** Causet ties every answer to its request and
  returns it to the component that asked; whether an answer is still
  wanted is that component's reducer's decision.
- **Messages need no serialization.** Serialization is the application's
  choice, for its own State; causet's `serde` feature covers the identity
  types State holds.
- **Panics fault the Store** at the root input, with nothing resumed, even
  one an Effect handler catches from its `dispatch`; its Event bus closes,
  as if it had been dropped.
- **Bugs are reported, not fatal.** `bug_condition!` reports a state the
  code believes impossible and lets the caller's fallback run, in every
  build; reports carry their place and reach observers.

## Testing

`causet-test` runs the real Store:

- a `Node` holds every Event its Services send until the test delivers it,
  and checks the application's invariants after every input;
- `check` generates runs from a stream of choices, makes every passing run
  twice to check it is deterministic, and shrinks a failing run to a short
  list of choices that `replay` repeats;
- a `Scenario` runs several nodes on one virtual clock and a simulated
  world, and `causet-projection` checks projections across restarts and
  against recomputing from scratch.

```rust
use causet::guide::shop::*;
use causet::{ManualClock, SessionId};
use causet_test::{Config, Node, check};

check(Config::default(), |rng| {
    let services = ShopServices { gateway: Box::new(ApproveUpTo(100)) };
    let clock = ManualClock::new(1_000);
    let mut shop = Node::from_protocol("shop", protocol(), ShopState::default(), services, SessionId(1), &clock)
        .check("every order is paid, declined or pending", |s: &ShopState| {
            let p = &s.payments;
            let charges = (p.paid.len() + p.declined.len() + p.pending.len()) as u64;
            if charges == s.orders { Ok(()) } else { Err(format!("{} orders, {charges} charges", s.orders)) }
        });
    let cart = shop.mount(&root::<ShopRoot>().child(CART));
    while rng.more() {
        match rng.below(3) {
            0 => drop(shop.deliver_next()),
            1 => drop(shop.dispatch(&cart, CartAction::Add { item: "tea".into(), price: rng.between(1, 60) })),
            _ => drop(shop.dispatch(&cart, CartAction::Checkout)),
        }
    }
});
```

The guide's [testing page](https://tizoc.github.io/causet/testing.html) has the rest: a
simulated world shared by several shops, and a planted bug found and
shrunk.

## Documentation

[The guide](https://tizoc.github.io/causet/), shared with the OCaml and TypeScript ports, explains causet one page
at a time, each example in all three languages:

- **overview**: the problem, the model, and one checkout's journey
  through the example shop; start here;
- **authority**: what each kind of code may read, change and start;
- **scheduling**: the order in which work runs, admission, and faults;
- **addressing**: mounts, paths, child slots, Outputs and replies, and
  where context and Effects belong;
- **determinism**: what application code must do so a run can be repeated;
- **testing**: one node, generated runs, several nodes on a simulated
  world, and the checks for projections.

Every example in the guide, and in this file, runs as a test: the guide's
Rust examples are in `crates/causet/guide/`. The API reference is
`cargo doc -p causet --open`.

## Crates

| Crate | Contents |
| --- | --- |
| `causet` | The facade: re-exports the crates below, and hosts the guide, the examples, the integration tests and the benchmarks. |
| `causet-core` | Components, the Registry and Protocol, the Store and its scheduler, contexts, mounts, the Event bus, the clock, the timer Service, observers, bug reports and invariants. |
| `causet-derive` | `#[derive(Action, Effect, Event, Output)]`: type names, kinds, log fields. |
| `causet-observers` | Logging through `tracing`, and execution statistics, as observers. |
| `causet-projection` | Projections of an ordered log of records into derived facts: warm-up after a restart, a check that restarts change nothing a processor outputs, and a check that incremental recomputation stores, for every key output, what recomputing from scratch would. |
| `causet-test` | Testing and simulation on the real Store: held Event delivery, simulated timers on a shared clock, invariants per node and over groups of nodes, seeded scenarios over several Stores, a simulated world's messages in flight, generated runs with a separate stream for the world and swarm steps, shrunk to small repeatable cases. |

Beside them, in this repository as separate packages:

| Crate | Contents |
| --- | --- |
| `causet-reporting` | A reporting pipeline: deduplication, rate limits, sampling, metric aggregation, JSON-lines dumps. It knows nothing of the Store; an application wires it in as a Service, as its `reporting` example shows. |
| `causet-pooled-payload` | Report payloads built in pooled buffers, for `causet-reporting`. |

## Development

```sh
cargo test --workspace --all-targets   # unit, integration and example tests
cargo test --workspace --doc           # the guide and every doc example
cargo bench -p causet --bench baseline # prints ns, allocations and bytes per execution
MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test -p causet
cargo fmt --all -- --check             # rustfmt's defaults
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Rust 2024, tested with rustc 1.90.
