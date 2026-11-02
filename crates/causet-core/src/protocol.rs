//! Type erasure, component registration and frozen mount paths.

use crate::{
    ActionMeta, ActionPort, Adapts, Backend, Child, Component, ComponentContext, Core,
    EffectContext, EffectMeta, Event, Exec, InstanceId, Lens, Message, Never, Origin, Payload,
    PortMode, Timestamp, ViewFn, Watch, view_as,
};
use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::panic;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

// ---------------------------------------------------------------------------
// Type-erased bindings

pub(super) trait ErasedLens: Send + Sync {
    fn get<'a>(&self, parent: &'a dyn Any) -> Option<&'a dyn Any>;
    fn get_mut<'a>(&self, parent: &'a mut dyn Any) -> Option<&'a mut dyn Any>;
}

impl<P: 'static, T: 'static> ErasedLens for Lens<P, T> {
    fn get<'a>(&self, parent: &'a dyn Any) -> Option<&'a dyn Any> {
        let parent = parent.downcast_ref::<P>().expect("parent type");
        (self.get)(parent).map(|t| t as &dyn Any)
    }

    fn get_mut<'a>(&self, parent: &'a mut dyn Any) -> Option<&'a mut dyn Any> {
        let parent = parent.downcast_mut::<P>().expect("parent type");
        (self.get_mut)(parent).map(|t| t as &mut dyn Any)
    }
}

pub(super) trait ErasedGuard: Send + Sync {
    fn admit(&self, parent: &dyn Any, action: &dyn Any, now: Timestamp) -> bool;
}

pub(super) struct Guard<PS, A>(fn(&PS, &A, Timestamp) -> bool);

impl<PS: 'static, A: 'static> ErasedGuard for Guard<PS, A> {
    fn admit(&self, parent: &dyn Any, action: &dyn Any, now: Timestamp) -> bool {
        let parent = parent.downcast_ref::<PS>().expect("parent State type");
        let action = action.downcast_ref::<A>().expect("child Action type");
        (self.0)(parent, action, now)
    }
}

pub(super) trait ErasedRootGuard: Send + Sync {
    fn admit(&self, root: &dyn Any, own: &dyn Any, action: &dyn Any, now: Timestamp) -> bool;
}

pub(super) struct RootGuard<S, DS, A>(fn(&S, &DS, &A, Timestamp) -> bool);

impl<S: 'static, DS: 'static, A: 'static> ErasedRootGuard for RootGuard<S, DS, A> {
    fn admit(&self, root: &dyn Any, own: &dyn Any, action: &dyn Any, now: Timestamp) -> bool {
        let root = root.downcast_ref::<S>().expect("root State type");
        let own = own.downcast_ref::<DS>().expect("mount State type");
        let action = action.downcast_ref::<A>().expect("mount Action type");
        (self.0)(root, own, action, now)
    }
}

pub(super) trait ErasedLower: Send + Sync {
    fn lower(&self, output: Payload, exec: &mut Exec, parent: usize, origin: Origin);
}

/// A parent's lowerer for one slot: turns the child's Output into parent
/// Actions. It is told which slot, so one function can serve several.
pub type LowerFn<P, D> = fn(Child<P, D>, <D as Component>::Output, &mut ActionPort<'_, P>);

pub(super) struct Lower<P: Component, D: Component> {
    pub(super) slot: Child<P, D>,
    pub(super) f: LowerFn<P, D>,
}

impl<P: Component, D: Component> ErasedLower for Lower<P, D> {
    fn lower(&self, output: Payload, exec: &mut Exec, parent: usize, origin: Origin) {
        let output = output.take::<D::Output>().expect("child Output type");
        let mut port = ActionPort::<P> {
            exec,
            target: parent,
            mode: PortMode::Lowering { origin, next: 0 },
            _p: PhantomData,
        };
        (self.f)(self.slot, output, &mut port);
    }
}

pub(super) type RunFn =
    fn(&ProtocolDefs, usize, &mut dyn Any, &mut Exec, Watch<'_>, Payload, &ActionMeta);
pub(super) type HandleFn = fn(&mut Core, usize, Payload, &EffectMeta);
pub(super) type AdaptFn = fn(InstanceId, usize, &mut Exec, Payload);
pub(super) type AdmitFn = fn(&dyn Any, &dyn Any, Timestamp) -> bool;
pub(super) type KindFn = fn(&dyn Any) -> &'static str;

pub(super) fn run_reduce<C: Component>(
    proto: &ProtocolDefs,
    mount: usize,
    state: &mut dyn Any,
    exec: &mut Exec,
    watch: Watch<'_>,
    payload: Payload,
    meta: &ActionMeta,
) {
    let state = state.downcast_mut::<C::State>().expect("State type");
    let action = payload.take::<C::Action>().expect("Action type");
    C::reduce(
        ComponentContext {
            backend: Backend::Store {
                proto,
                mount,
                exec,
                watch,
            },
            state,
            meta,
            _c: PhantomData,
        },
        action,
        meta,
    );
}

pub(super) fn run_handle<C: Component>(
    core: &mut Core,
    mount: usize,
    payload: Payload,
    meta: &EffectMeta,
) {
    let effect = payload.take::<C::Effect>().expect("Effect type");
    C::handle(
        EffectContext {
            core,
            mount,
            meta,
            next_emission: 0,
            _c: PhantomData,
        },
        effect,
        meta,
    );
}

pub(super) fn run_adapt<C: Adapts<Ev>, Ev: Event>(
    instance: InstanceId,
    mount: usize,
    exec: &mut Exec,
    payload: Payload,
) {
    let event = payload.take::<Ev>().expect("Event type");
    let mut port = ActionPort::<C> {
        exec,
        target: mount,
        mode: PortMode::Adapting {
            component: instance,
        },
        _p: PhantomData,
    };
    C::adapt(event, &mut port);
}

pub(super) fn admit_own<C: Component>(state: &dyn Any, action: &dyn Any, now: Timestamp) -> bool {
    let state = state.downcast_ref::<C::State>().expect("State type");
    let action = action.downcast_ref::<C::Action>().expect("Action type");
    C::is_enabled(state, action, now)
}

pub(super) fn action_kind<C: Component>(action: &dyn Any) -> &'static str {
    action
        .downcast_ref::<C::Action>()
        .expect("Action type")
        .kind()
}

pub(super) fn effect_kind<C: Component>(effect: &dyn Any) -> &'static str {
    effect
        .downcast_ref::<C::Effect>()
        .expect("Effect type")
        .kind()
}

pub(super) fn event_kind<Ev: Event>(event: &dyn Any) -> &'static str {
    event.downcast_ref::<Ev>().expect("Event type").kind()
}

// ---------------------------------------------------------------------------
// Registry and Protocol

pub(super) struct EventBinding {
    pub(super) event: TypeId,
    pub(super) name: &'static str,
    pub(super) adapt: AdaptFn,
    pub(super) kind: KindFn,
    pub(super) view: ViewFn,
}

pub(super) struct MountDef {
    pub(super) path: Arc<str>,
    pub(super) instance: InstanceId,
    pub(super) parent: Option<usize>,
    /// The mounts from the root's child down to this one, computed at freeze:
    /// the lenses to walk from the root State to this mount's.
    pub(super) chain: Box<[usize]>,
    pub(super) component: TypeId,
    pub(super) has_effects: bool,
    pub(super) children: Vec<(&'static str, usize)>,
    pub(super) lens: Option<Box<dyn ErasedLens>>,
    pub(super) service_lens: Option<Box<dyn ErasedLens>>,
    pub(super) guard: Option<Box<dyn ErasedGuard>>,
    /// The application's guard on this mount, reading the whole State.
    pub(super) root_guard: Option<Box<dyn ErasedRootGuard>>,
    pub(super) lower: Option<Box<dyn ErasedLower>>,
    pub(super) events: Vec<EventBinding>,
    pub(super) run: RunFn,
    pub(super) handle: HandleFn,
    pub(super) admit: AdmitFn,
    pub(super) action_kind: KindFn,
    pub(super) effect_kind: KindFn,
    /// The Action's and Effect's `Message::TYPE`.
    pub(super) action_type: &'static str,
    pub(super) effect_type: &'static str,
    pub(super) view_action: ViewFn,
    pub(super) view_effect: ViewFn,
}

impl MountDef {
    fn new<C: Component>(path: String, parent: Option<usize>) -> Self {
        Self {
            instance: instance_id(&path),
            path: Arc::from(path),
            parent,
            chain: Box::new([]),
            component: TypeId::of::<C>(),
            has_effects: TypeId::of::<C::Effect>() != TypeId::of::<Never>(),
            children: Vec::new(),
            lens: None,
            service_lens: None,
            guard: None,
            root_guard: None,
            lower: None,
            events: Vec::new(),
            run: run_reduce::<C>,
            handle: run_handle::<C>,
            admit: admit_own::<C>,
            action_kind: action_kind::<C>,
            effect_kind: effect_kind::<C>,
            action_type: C::Action::TYPE,
            effect_type: C::Effect::TYPE,
            view_action: view_as::<C::Action>,
            view_effect: view_as::<C::Effect>,
        }
    }
}

/// xxh3-64 with seed 0 of the UTF-8 path: a named, fixed algorithm, so the
/// same in every run and every Rust version.
pub(super) fn instance_id(path: &str) -> InstanceId {
    InstanceId(xxhash_rust::xxh3::xxh3_64(path.as_bytes()))
}

#[derive(Default)]
pub(super) struct Builder {
    pub(super) mounts: Vec<MountDef>,
    pub(super) errors: Vec<Problem>,
    /// `r.child(...)` calls whose `.bind()` has not run yet.
    pub(super) unfinished: Vec<String>,
    /// `Registry::admit` calls, resolved by `freeze`.
    pub(super) root_guards: Vec<PendingRootGuard>,
}

pub(super) struct PendingRootGuard {
    root: TypeId,
    names: Vec<&'static str>,
    component: TypeId,
    guard: Box<dyn ErasedRootGuard>,
}

/// One thing wrong with a registration. Paths are mount paths.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Problem {
    /// `Registry::root` was called twice.
    RootRegisteredTwice,
    /// `Registry::root` was never called.
    NoRoot,
    /// The root reports Outputs, but it has no parent to lower them.
    RootHasOutputs,
    /// `r.child(...)` without `.bind()`.
    ChildNeverBound {
        /// The unbound slot's mount path.
        path: String,
    },
    /// Two slots of one parent share a name.
    SlotBoundTwice {
        /// The mount path both slots would have.
        path: String,
    },
    /// The child reports Outputs and the slot binds no lowerer.
    NoLowerer {
        /// The child's mount path.
        path: String,
    },
    /// `r.event::<Ev>()` was called twice for one mount.
    EventRegisteredTwice {
        /// The mount's path.
        path: String,
        /// The Event's type name.
        event: &'static str,
    },
    /// Two mount paths hash to the same `InstanceId`: rename a slot.
    InstanceCollision {
        /// The path registered first.
        first: String,
        /// The path that collides with it.
        second: String,
    },
    /// A component with Effects whose Service no lens reaches: `at` is the
    /// mount on the way that binds no Service lens.
    NoServiceProjection {
        /// The component's mount path.
        path: String,
        /// The mount that binds no Service lens.
        at: String,
    },
    /// A root guard's path names no mount.
    RootGuardNoMount {
        /// The path.
        path: String,
    },
    /// A root guard's path starts at another root, or reaches a mount of
    /// another component than its type says.
    RootGuardOtherComponent {
        /// The path, as far as it goes.
        path: String,
    },
    /// A root guard on the root itself, whose own `is_enabled` already reads
    /// the whole State.
    RootGuardOnRoot,
    /// Two root guards on one mount.
    RootGuardTwice {
        /// The mount's path.
        path: String,
    },
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootRegisteredTwice => write!(f, "root registered twice"),
            Self::NoRoot => write!(f, "no root component"),
            Self::RootHasOutputs => write!(f, "the root component's Output must be Never"),
            Self::ChildNeverBound { path } => write!(
                f,
                "child {path} was registered but never bound (missing .bind())"
            ),
            Self::SlotBoundTwice { path } => write!(f, "slot {path} bound twice"),
            Self::NoLowerer { path } => write!(f, "{path} reports Outputs but has no lowerer"),
            Self::EventRegisteredTwice { path, event } => {
                write!(f, "{path} registered Event {event} twice")
            }
            Self::InstanceCollision { first, second } => {
                write!(f, "InstanceId collision: {first} and {second}")
            }
            Self::NoServiceProjection { path, at } => {
                write!(
                    f,
                    "{path} has Effects but no Service projection reaches it at {at}"
                )
            }
            Self::RootGuardNoMount { path } => write!(f, "root guard on {path}: no such mount"),
            Self::RootGuardOtherComponent { path } => write!(
                f,
                "root guard on {path}: the mount holds another component type"
            ),
            Self::RootGuardOnRoot => {
                write!(f, "root guard on /: use the root's own is_enabled")
            }
            Self::RootGuardTwice { path } => write!(f, "two root guards on {path}"),
        }
    }
}

/// Everything wrong with a registration, as `freeze()` found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationError(pub Vec<Problem>);

impl fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, problem) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{problem}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RegistrationError {}

/// Startup configuration for an application whose root State is `S` and
/// root Services `Svc`. `freeze` validates it into a `Protocol`.
pub struct Registry<S, Svc> {
    pub(super) builder: Builder,
    pub(super) _types: PhantomData<fn() -> (S, Svc)>,
}

impl<S: 'static, Svc: 'static> Default for Registry<S, Svc> {
    fn default() -> Self {
        Self {
            builder: Builder::default(),
            _types: PhantomData,
        }
    }
}

impl<S: 'static, Svc: 'static> Registry<S, Svc> {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mounts the root component at `/`, which registers the whole tree
    /// through the components' `register`.
    pub fn root<R: Component<State = S, Service = Svc>>(&mut self) {
        if !self.builder.mounts.is_empty() {
            self.builder.errors.push(Problem::RootRegisteredTwice);
            return;
        }
        if TypeId::of::<R::Output>() != TypeId::of::<Never>() {
            self.builder.errors.push(Problem::RootHasOutputs);
        }
        self.builder
            .mounts
            .push(MountDef::new::<R>("/".into(), None));
        R::register(&mut ComponentRegistry {
            builder: &mut self.builder,
            mount: 0,
            _c: PhantomData,
        });
    }

    /// Sets a root guard on the mount at `path`, anywhere below the root: a
    /// check of the whole State, the mount's own State, its Action and the
    /// time, as pure as `Component::is_enabled`.
    ///
    /// It checks every Action that enters the mount, that is, that the Store
    /// admits there to start an execution: a root input, a queued Action, an
    /// Action adapted from an Event or lowered from an Output, a dispatch
    /// from an Effect handler, a `dispatch_to`. It also checks an inline call
    /// (`reduce_child`) the root makes, the one caller that holds the whole
    /// State. An inline call made below the root runs inside its caller's
    /// execution without it: the caller takes that on, and observers are
    /// told (`InlineCall::root_guard`). So an Action forwarded inline below
    /// the root is checked where it entered, by that mount's root guard.
    ///
    /// The checks run in this order: the mount's State is there, its root
    /// guard, its parent's slot guard, its own `is_enabled`. A panic in the
    /// guard faults the Store, as one anywhere in a root input does; on the
    /// root's inline call it unwinds through `reduce_child` first, like any
    /// panic of an inline call. `freeze`
    /// reports a path that names no mount or another component, a root
    /// guard on the root itself, and a second root guard on a mount.
    pub fn admit<D: Component>(
        &mut self,
        path: Path<D>,
        guard: fn(&S, &D::State, &D::Action, Timestamp) -> bool,
    ) {
        self.builder.root_guards.push(PendingRootGuard {
            root: path.root,
            names: path.names,
            component: TypeId::of::<D>(),
            guard: Box::new(RootGuard(guard)),
        });
    }

    /// Validates the registration into an immutable `Protocol`, or reports
    /// everything wrong with it.
    pub fn freeze(self) -> Result<Arc<Protocol<S, Svc>>, RegistrationError> {
        let Builder {
            mut mounts,
            mut errors,
            unfinished,
            root_guards,
        } = self.builder;
        for path in unfinished {
            errors.push(Problem::ChildNeverBound { path });
        }
        if mounts.is_empty() {
            errors.push(Problem::NoRoot);
        }
        for (i, a) in mounts.iter().enumerate() {
            for b in &mounts[i + 1..] {
                if a.instance == b.instance {
                    errors.push(Problem::InstanceCollision {
                        first: a.path.to_string(),
                        second: b.path.to_string(),
                    });
                }
            }
        }
        if !mounts.is_empty() {
            for pending in root_guards {
                if let Err(problem) = place_root_guard(&mut mounts, pending) {
                    errors.push(problem);
                }
            }
        }
        // An Effect handler's Service is projected from the root's through
        // every mount on the way, so each of them needs a Service lens.
        for (index, def) in mounts.iter().enumerate() {
            if !def.has_effects {
                continue;
            }
            let mut current = index;
            while let Some(parent) = mounts[current].parent {
                if mounts[current].service_lens.is_none() {
                    errors.push(Problem::NoServiceProjection {
                        path: def.path.to_string(),
                        at: mounts[current].path.to_string(),
                    });
                    break;
                }
                current = parent;
            }
        }
        if !errors.is_empty() {
            return Err(RegistrationError(errors));
        }
        for index in 0..mounts.len() {
            let mut chain = Vec::new();
            let mut current = index;
            while let Some(parent) = mounts[current].parent {
                chain.push(current);
                current = parent;
            }
            chain.reverse();
            mounts[index].chain = chain.into_boxed_slice();
        }
        let by_instance = mounts
            .iter()
            .enumerate()
            .map(|(i, m)| (m.instance, i))
            .collect();
        static NEXT_PROTOCOL: AtomicU64 = AtomicU64::new(1);
        Ok(Arc::new(Protocol {
            defs: Arc::new(ProtocolDefs {
                id: NEXT_PROTOCOL.fetch_add(1, Ordering::Relaxed),
                mounts,
                by_instance,
            }),
            _types: PhantomData,
        }))
    }
}

/// Resolves a root guard's path and sets the guard on its mount.
fn place_root_guard(mounts: &mut [MountDef], pending: PendingRootGuard) -> Result<(), Problem> {
    let path = format!("/{}", pending.names.join("/"));
    if mounts[0].component != pending.root {
        return Err(Problem::RootGuardOtherComponent { path: "/".into() });
    }
    let mut index = 0;
    for name in &pending.names {
        index = mounts[index]
            .children
            .iter()
            .find(|(n, _)| n == name)
            .map(|&(_, i)| i)
            .ok_or_else(|| Problem::RootGuardNoMount { path: path.clone() })?;
    }
    if mounts[index].component != pending.component {
        return Err(Problem::RootGuardOtherComponent { path });
    }
    if pending.names.is_empty() {
        return Err(Problem::RootGuardOnRoot);
    }
    if mounts[index].root_guard.is_some() {
        return Err(Problem::RootGuardTwice { path });
    }
    mounts[index].root_guard = Some(pending.guard);
    Ok(())
}

/// What a component's `register` declares through.
pub struct ComponentRegistry<'r, C> {
    pub(super) builder: &'r mut Builder,
    pub(super) mount: usize,
    pub(super) _c: PhantomData<fn() -> C>,
}

impl<C: Component> ComponentRegistry<'_, C> {
    /// Declares the child at `slot`, its State projected by `lens`. The
    /// binding takes effect at `.bind()`.
    pub fn child<D: Component>(
        &mut self,
        slot: Child<C, D>,
        lens: Lens<C::State, D::State>,
    ) -> SlotBinding<'_, C, D> {
        let parent_path: &str = &self.builder.mounts[self.mount].path;
        let path = if parent_path == "/" {
            format!("/{}", slot.name)
        } else {
            format!("{parent_path}/{}", slot.name)
        };
        self.builder.unfinished.push(path.clone());
        SlotBinding {
            builder: &mut *self.builder,
            parent: self.mount,
            path,
            slot,
            lens,
            service: None,
            guard: None,
            lower: None,
        }
    }

    /// Accepts Events of type `Ev`, adapted by the component's `Adapts<Ev>`.
    pub fn event<Ev: Event>(&mut self)
    where
        C: Adapts<Ev>,
    {
        let def = &mut self.builder.mounts[self.mount];
        if def.events.iter().any(|b| b.event == TypeId::of::<Ev>()) {
            let problem = Problem::EventRegisteredTwice {
                path: def.path.to_string(),
                event: Ev::TYPE,
            };
            self.builder.errors.push(problem);
            return;
        }
        def.events.push(EventBinding {
            event: TypeId::of::<Ev>(),
            name: Ev::TYPE,
            adapt: run_adapt::<C, Ev>,
            kind: event_kind::<Ev>,
            view: view_as::<Ev>,
        });
    }
}

/// A child slot being bound: add the Service lens, a guard and a lowerer as
/// needed, then `bind`.
#[must_use = "a child registration takes effect only after `.bind()`"]
pub struct SlotBinding<'b, C: Component, D: Component> {
    pub(super) builder: &'b mut Builder,
    pub(super) parent: usize,
    pub(super) path: String,
    pub(super) slot: Child<C, D>,
    pub(super) lens: Lens<C::State, D::State>,
    pub(super) service: Option<Box<dyn ErasedLens>>,
    pub(super) guard: Option<Box<dyn ErasedGuard>>,
    pub(super) lower: Option<Box<dyn ErasedLower>>,
}

impl<C: Component, D: Component> SlotBinding<'_, C, D> {
    /// The child's Service, projected from this component's Service.
    pub fn service(mut self, lens: Lens<C::Service, D::Service>) -> Self {
        self.service = Some(Box::new(lens));
        self
    }

    /// A guard on every route into the child, reading this component's State:
    /// an Action it refuses never runs.
    pub fn admit(mut self, guard: fn(&C::State, &D::Action, Timestamp) -> bool) -> Self {
        self.guard = Some(Box::new(Guard(guard)));
        self
    }

    /// The lowerer for the child's Outputs; required when it has any.
    pub fn on_output(mut self, f: LowerFn<C, D>) -> Self {
        self.lower = Some(Box::new(Lower { slot: self.slot, f }));
        self
    }

    /// Mounts the child and registers its own subtree.
    pub fn bind(self) {
        let SlotBinding {
            builder,
            parent,
            path,
            slot,
            lens,
            service,
            guard,
            lower,
        } = self;
        if let Some(i) = builder.unfinished.iter().position(|p| *p == path) {
            builder.unfinished.remove(i);
        }
        if builder.mounts[parent]
            .children
            .iter()
            .any(|(name, _)| *name == slot.name)
        {
            builder.errors.push(Problem::SlotBoundTwice { path });
            return;
        }
        if lower.is_none() && TypeId::of::<D::Output>() != TypeId::of::<Never>() {
            builder
                .errors
                .push(Problem::NoLowerer { path: path.clone() });
        }
        let index = builder.mounts.len();
        let mut def = MountDef::new::<D>(path, Some(parent));
        def.lens = Some(Box::new(lens));
        def.service_lens = service;
        def.guard = guard;
        def.lower = lower;
        builder.mounts.push(def);
        builder.mounts[parent].children.push((slot.name, index));
        D::register(&mut ComponentRegistry {
            builder,
            mount: index,
            _c: PhantomData,
        });
    }
}

/// A frozen application definition, typed by its root State `S` and root
/// Services `Svc`: mounts, bindings, lenses and lowerers. Immutable and
/// shareable: every Store of the application (tests, simulations) runs from
/// one, and only a Store of `S` and `Svc` can (`Store::new`).
pub struct Protocol<S, Svc> {
    pub(super) defs: Arc<ProtocolDefs>,
    pub(super) _types: PhantomData<fn() -> (S, Svc)>,
}

/// What a Protocol holds, apart from its root types, so that the Store's
/// non-generic core and the Event bus can share it.
pub(super) struct ProtocolDefs {
    pub(super) id: u64,
    pub(super) mounts: Vec<MountDef>,
    pub(super) by_instance: HashMap<InstanceId, usize>,
}

impl<S, Svc> Protocol<S, Svc> {
    /// The mount at `path`, as a handle for Stores of this Protocol.
    pub fn resolve<C: Component>(&self, path: &Path<C>) -> Result<Mount<C>, ResolveError> {
        self.defs.resolve(path)
    }

    /// A mount's path, such as `/cart`.
    pub fn path_of<C>(&self, mount: &Mount<C>) -> &str {
        &self.defs.mounts[mount.index].path
    }

    /// A mount's instance identity.
    pub fn instance_of<C>(&self, mount: &Mount<C>) -> InstanceId {
        mount.instance
    }
}

impl ProtocolDefs {
    pub(super) fn resolve<C: Component>(&self, path: &Path<C>) -> Result<Mount<C>, ResolveError> {
        let defs = self;
        if defs.mounts[0].component != path.root {
            return Err(ResolveError::OtherComponent { path: "/".into() });
        }
        let mut index = 0;
        for name in &path.names {
            index = defs.mounts[index]
                .children
                .iter()
                .find(|(n, _)| n == name)
                .map(|&(_, i)| i)
                .ok_or_else(|| ResolveError::NoMount {
                    name,
                    under: defs.mounts[index].path.to_string(),
                })?;
        }
        if defs.mounts[index].component != TypeId::of::<C>() {
            return Err(ResolveError::OtherComponent {
                path: defs.mounts[index].path.to_string(),
            });
        }
        Ok(Mount {
            index,
            protocol: defs.id,
            instance: defs.mounts[index].instance,
            _c: PhantomData,
        })
    }

    /// A slot used but never bound is a configuration fault: it panics, and the
    /// root-input boundary turns the panic into a Store fault.
    pub(super) fn resolve_slot<D: Component>(&self, parent: usize, name: &'static str) -> usize {
        let def = &self.mounts[parent];
        let Some(&(_, child)) = def.children.iter().find(|(n, _)| *n == name) else {
            panic!("slot {name} is not bound under {}", def.path);
        };
        assert!(
            self.mounts[child].component == TypeId::of::<D>(),
            "slot {name} holds another type"
        );
        child
    }

    pub(super) fn mount_by_instance(&self, instance: InstanceId) -> Option<usize> {
        self.by_instance.get(&instance).copied()
    }

    pub(super) fn state_ref<'s>(&self, root: &'s dyn Any, mount: usize) -> Option<&'s dyn Any> {
        let mut current = root;
        for &m in &*self.mounts[mount].chain {
            current = self.mounts[m].lens.as_ref()?.get(current)?;
        }
        Some(current)
    }

    pub(super) fn state_mut<'s>(
        &self,
        root: &'s mut dyn Any,
        mount: usize,
    ) -> Option<&'s mut dyn Any> {
        let mut current = root;
        for &m in &*self.mounts[mount].chain {
            current = self.mounts[m].lens.as_ref()?.get_mut(current)?;
        }
        Some(current)
    }

    pub(super) fn service_mut<'s>(
        &self,
        root: &'s mut dyn Any,
        mount: usize,
    ) -> Option<&'s mut dyn Any> {
        let mut current = root;
        for &m in &*self.mounts[mount].chain {
            current = self.mounts[m].service_lens.as_ref()?.get_mut(current)?;
        }
        Some(current)
    }
}

/// Why a path did not resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResolveError {
    /// No slot `name` under the mount at `under`.
    NoMount {
        /// The slot's name.
        name: &'static str,
        /// The path of the mount it was looked for under.
        under: String,
    },
    /// The mount at `path` holds another component type than the path says,
    /// or the path starts at another root (`path` is then `/`).
    OtherComponent {
        /// The mount's path.
        path: String,
    },
}

impl fmt::Display for ResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoMount { name, under } => write!(f, "no mount {name} under {under}"),
            Self::OtherComponent { path } => write!(f, "{path} holds another component type"),
        }
    }
}

impl std::error::Error for ResolveError {}

/// A typed path built from slots, resolved once against a Protocol.
pub struct Path<C> {
    pub(super) root: TypeId,
    pub(super) names: Vec<&'static str>,
    pub(super) _c: PhantomData<fn() -> C>,
}

impl<C> Clone for Path<C> {
    fn clone(&self) -> Self {
        Self {
            root: self.root,
            names: self.names.clone(),
            _c: PhantomData,
        }
    }
}

/// The path of the root mount, `/`; extend it with `.child(SLOT)`.
pub fn root<R: Component>() -> Path<R> {
    Path {
        root: TypeId::of::<R>(),
        names: Vec::new(),
        _c: PhantomData,
    }
}

impl<C: Component> Path<C> {
    /// The path of the child at `slot`.
    pub fn child<D: Component>(mut self, slot: Child<C, D>) -> Path<D> {
        self.names.push(slot.name);
        Path {
            root: self.root,
            names: self.names,
            _c: PhantomData,
        }
    }
}

/// The only external handle type; valid on every Store of its Protocol.
pub struct Mount<C> {
    pub(super) index: usize,
    pub(super) protocol: u64,
    pub(super) instance: InstanceId,
    pub(super) _c: PhantomData<fn() -> C>,
}

impl<C> Clone for Mount<C> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<C> Copy for Mount<C> {}
