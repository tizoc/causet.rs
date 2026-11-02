//! The causet kernel: components, typed child slots, inline (`reduce_child`)
//! and queued (`to_child`) child calls, Output lowering, parent admission
//! guards, mount paths, per-execution request counters, the root-boundary
//! fault policy, Effects with scoped Services, synchronous Effect children,
//! Events delivered by `drain_events` through typed reply routes and a
//! two-lane bus, bug reports, and the component test harness.
//!
//! Observers (`observe`) are called at every lifecycle boundary; logging and
//! stats built on them are in `causet-observers`.

mod bug;
mod clock;
mod observe;
mod payload;
mod pending;
mod readiness;
mod timer;

pub use bug::{BugPlace, BugReport, report_bug, set_bug_hook};
pub use clock::{Clock, ClockOffset, ManualClock, SystemClock};
pub use observe::{
    Boundary, BugLog, BusDepths, Execution, FaultInfo, FieldVisitor, InlineCall, InlineEntry,
    LogLevel, MessageView, Observer, Phase, Refusal, RefusalEntry, RefusedBy, SentFrom, TraceEntry,
    TraceLog,
};
use observe::{ViewFn, view_as};
use payload::Payload;
pub use pending::Pending;
pub use readiness::{EventAvailability, EventReadiness, ReadinessInUse, ReadinessSubscription};
pub use timer::{Timer, TimerControl, TimerFired};

mod ids;
pub use ids::{ExecutionId, InstanceId, RequestId, SessionId, Timestamp};

mod messages;
pub use messages::{Action, Effect, Event, Message, Never, Output};

mod components;
pub use components::{Adapts, Child, Component, Lens};

mod metadata;
pub use metadata::{ActionMeta, Category, ChildOutcome, EffectMeta, EventMeta, ExecMeta, Origin};

mod execution;
use execution::{Exec, Queued};

mod context;
pub use context::{ActionPort, ComponentContext, ComponentEmitter};
use context::{Backend, ObserverPanic, PortMode, Watch};

mod harness;
pub use harness::{ChildCall, Disabled, Emission, Harness};
use harness::{Collected, RawEmission};

mod effects;
use effects::IGNORING;
pub use effects::{EffectContext, panic_is_ignored};
use effects::{FaultUnwind, fault_unwind, ignoring_panics, notify, panic_message};

mod bus;
use bus::{Bus, Lanes};
pub use bus::{
    Envelope, EventBus, EventInbox, EventSink, HeldEvent, HeldEvents, HeldId, Lane, ReleaseError,
    Reply, ReplyRoute, SendError,
};

mod protocol;
use protocol::ProtocolDefs;
pub use protocol::{
    ComponentRegistry, LowerFn, Mount, Path, Problem, Protocol, RegistrationError, Registry,
    ResolveError, SlotBinding, root,
};

mod store;
use store::Core;
pub use store::{
    Checker, DEFAULT_WORKER_CAPACITY, DispatchError, DispatchOutcome, InvariantStep, Invariants,
    Store, StoreOptions,
};

/// What the derives name, wherever the names are documented.
#[doc(hidden)]
pub mod __derive {
    pub use crate::{FieldVisitor, LogLevel};
}
