//! Causet is a framework for building event-driven systems as explicit,
//! composable state machines, with automatic bug finding through deterministic
//! simulation.
//!
//! You define the operations your system can perform and the rules it must
//! obey. Causet generates simulated test runs that vary inputs, event delivery,
//! time and modeled failures. It checks your invariants after every input,
//! shrinks failing runs and gives you the choices needed to reproduce them.
//!
//! It addresses a common problem in asynchronous software: the behavior of
//! a system becomes scattered across callbacks, tasks, timers and shared
//! state. Understanding a transition means reconstructing what ran, in
//! which order, and which request a late result belonged to, and testing
//! those interactions reliably is hard.
//!
//! Causet gives that behavior one structure. Each component owns a piece of
//! State and changes it only in its reducer, in answer to an Action. It
//! never touches the world directly: it asks for work with Effects, which
//! Services carry out, and hears back through Events. So the same
//! components run against the real world or a simulated one:
//!
//! ```text
//!           In production                     In a test
//!
//!    ┌─────────────────────────┐     ┌─────────────────────────┐
//!    │ network, disk, clock    │     │ simulated network, disk │
//!    │                         │     │ and a virtual clock     │
//!    └──────▲───────────┬──────┘     └──────▲───────────┬──────┘
//!           │ Effects   │ Events            │ Effects   │ Events
//!    ┌──────┴───────────▼──────┐     ┌──────┴───────────▼──────┐
//!    │ your components:        │     │ the same components;    │
//!    │ one input at a time,    │     │ the test decides what   │
//!    │ in a fixed order        │     │ arrives, and when       │
//!    └─────────────────────────┘     └─────────────────────────┘
//! ```
//!
//! A [`Store`] runs the components one input at a time, in a fixed order,
//! with time from its own clock. So a test can deliver answers in any order
//! it likes, generate thousands of runs, and repeat a failing one exactly.
//!
//! ```
//! use causet::guide::shop::*;
//!
//! let mut shop = Shop::open(StoreOptions::default());
//! shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
//! shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap(); // charges through a Service
//! shop.store.drain_events(16).unwrap();                               // delivers its answer
//! assert_eq!(shop.store.state().payments.paid, [4]);
//! ```
//!
//! The cost is more explicit code: every asynchronous step is a named
//! message, and every lifecycle is State. It pays off when the hard parts
//! are ordering, timeouts, retries, cancellation and stale results, as in
//! network protocols, wallets and synchronization engines.
//!
//! # Where to start
//!
//! Read [the guide](https://tizoc.github.io/causet/), shared with the OCaml and TypeScript ports. Its overview covers
//! the problem, the model, and one checkout's journey through the guide's
//! example shop, message by message, then how a test takes control of it.
//! The pages after it give the rules: authority, scheduling, addressing,
//! determinism and testing. Every example in it runs here as a doc test.
//! The example application, [`guide::shop`], is ordinary code: read its
//! source.
//!
//! # Reference
//!
//! The pieces, in the order an application meets them:
//!
//! - messages: [`Action`], [`Effect`], [`Event`] and [`Output`], declared with
//!   the derives below;
//! - [`Component`]: State, Service, messages, and the functions that handle
//!   them, with child slots ([`Child`]) bound in
//!   [`register`](Component::register);
//! - [`Store`], which runs it: [`Store::start`] registers the root
//!   component and starts a Store with an Event bus of its own, and
//!   [`Store::mount`] resolves a [`Path`] into the [`Mount`] to address;
//!   [`Harness`] runs one reducer alone, for tests;
//! - [`Clock`], [`Timer`], and bug reports ([`bug_condition!`]);
//! - [`Registry`] and [`Protocol`], for root guards and several Stores on
//!   one Protocol ([`Store::open`]);
//! - [`observers`]: what an observer sees, logging and statistics;
//!   [`hosting`]: driving a Store from a host and building its bus by hand;
//!   [`invariants`]: checks of the whole State after every root input;
//!   [`projection`]: deriving facts from a stored log of records.
//!
//! The reporting pipeline (dedupe, rate limits, sampling, metric
//! aggregation, dumps) is a crate of its own, `causet-reporting`: it knows
//! nothing of the Store, and an application wires it in as a Service.
//!
//! # Declaring messages
//!
//! The derives implement [`Message`] (the type's name, `TYPE`, and each
//! value's `kind()`) and the category's marker trait. Messages need no
//! serialization:
//!
//! ```
//! use causet::{Action, Message};
//!
//! #[derive(Debug, Action)]
//! #[causet(type = "cart.action")]
//! enum CartAction {
//!     AddItem(String),
//!     #[causet(kind = "cart.emptied")]
//!     Clear,
//! }
//!
//! assert_eq!(CartAction::AddItem("tea".into()).kind(), "cart.add_item");
//! assert_eq!(CartAction::Clear.kind(), "cart.emptied");
//! ```
//!
//! The type's name is never guessed:
//!
//! ```compile_fail
//! #[derive(Debug, causet::Action)]
//! enum CartAction { Clear }
//! ```
//!
//! A `nested` variant holds exactly the inner message:
//!
//! ```compile_fail
//! #[derive(Debug, causet::Action)]
//! #[causet(type = "cart.action")]
//! enum CartAction { #[causet(nested)] Clear }
//! ```

// The derives name `::causet`; the guide's example uses them here.
extern crate self as causet;

/// The README's examples, run as doc tests.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

pub mod guide;

/// The causet guide's examples, run as doc tests. The guide's pages are in
/// its own repository, shared with the OCaml and TypeScript ports; each shows the blocks of
/// one file here by name.
#[cfg(doctest)]
mod guide_examples {
    #[doc = include_str!("../guide/overview.md")]
    struct Overview;
    #[doc = include_str!("../guide/messages.md")]
    struct Messages;
    #[doc = include_str!("../guide/authority.md")]
    struct Authority;
    #[doc = include_str!("../guide/scheduling.md")]
    struct Scheduling;
    #[doc = include_str!("../guide/addressing.md")]
    struct Addressing;
    #[doc = include_str!("../guide/running.md")]
    struct Running;
    #[doc = include_str!("../guide/timeouts.md")]
    struct Timeouts;
    #[doc = include_str!("../guide/determinism.md")]
    struct Determinism;
    #[doc = include_str!("../guide/observability.md")]
    struct Observability;
    #[doc = include_str!("../guide/projections.md")]
    struct Projections;
    #[doc = include_str!("../guide/testing.md")]
    struct Testing;
}

// What an application writes: components and their messages, contexts,
// registration, a Store and its inputs, time, one transition under test, and
// bug reports. The rest is grouped by what it is for: `observers`, `hosting`,
// `invariants`.
pub use causet_core::{
    Action, ActionMeta, ActionPort, Adapts, Child, ChildCall, ChildOutcome, Clock, ClockOffset,
    Component, ComponentContext, ComponentEmitter, ComponentRegistry, Disabled, DispatchError,
    DispatchOutcome, Effect, EffectContext, EffectMeta, Emission, Event, EventMeta, EventSink,
    ExecMeta, ExecutionId, Harness, InstanceId, Lens, LogLevel, LowerFn, ManualClock, Message,
    Mount, Never, Origin, Output, Path, Pending, Problem, Protocol, RegistrationError, Registry,
    Reply, RequestId, ResolveError, SendError, SessionId, SlotBinding, Store, StoreOptions,
    SystemClock, Timer, TimerFired, Timestamp, bug_condition, lens, no_effects, root,
};
/// The derives share their traits' names; they live in the macro namespace.
pub use causet_derive::{Action, Effect, Event, Output};

#[doc(hidden)]
pub use causet_core::__derive;

/// Observing a Store: the `Observer` trait and what it is told, the trace
/// and bug logs, the identities in them, and the logging and stats
/// observers (`Logger`, `Stats`).
pub mod observers {
    pub use causet_core::{
        Boundary, BugLog, BugPlace, BugReport, BusDepths, Category, Execution, FaultInfo,
        FieldVisitor, InlineCall, InlineEntry, MessageView, Observer, Phase, Refusal, RefusalEntry,
        RefusedBy, SentFrom, TraceEntry, TraceLog, set_bug_hook,
    };
    pub use causet_observers::*;
}

/// Driving a Store from a host and building its Event bus by hand:
/// readiness, the bus's lanes and held Events, a driven timer's control.
/// `Store::start` and `Store::open` make a bus of their own.
pub mod hosting {
    pub use causet_core::{
        DEFAULT_WORKER_CAPACITY, Envelope, EventAvailability, EventBus, EventInbox, EventReadiness,
        HeldEvent, HeldEvents, HeldId, Lane, ReadinessInUse, ReadinessSubscription, ReleaseError,
        ReplyRoute, TimerControl, panic_is_ignored,
    };
}

/// Invariants a Store checks after every root input
/// (`Store::add_invariant`).
pub mod invariants {
    pub use causet_core::{Checker, InvariantStep, Invariants};
}
/// Projections of an ordered log of records: `causet::projection::{Projector, Projection}`.
pub use causet_projection as projection;
