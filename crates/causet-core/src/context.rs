//! Reducer mutation and emission capabilities; Action-only adapters.

use crate::{
    ActionMeta, Boundary, Category, Child, ChildCall, ChildOutcome, Collected, Component, Exec,
    ExecutionId, InlineCall, InstanceId, Message, Observer, Origin, Payload, Phase, ProtocolDefs,
    Queued, RawEmission, Refusal, RefusedBy, RequestId, SentFrom, Timestamp, bug,
};
use std::any::Any;
use std::marker::PhantomData;
use std::panic;
use std::panic::AssertUnwindSafe;

// ---------------------------------------------------------------------------
// Reducer contexts

/// Where a reducer's emissions go: the Store's scheduler, or a list when
/// a component is tested on its own (`Harness`).
pub(super) enum Backend<'a> {
    Store {
        proto: &'a ProtocolDefs,
        mount: usize,
        exec: &'a mut Exec,
        watch: Watch<'a>,
    },
    Harness(&'a mut Collected),
}

/// What a reducer needs of the Core to tell observers of the children it
/// calls inline.
pub(super) struct Watch<'a> {
    pub(super) observers: &'a mut Vec<Box<dyn Observer>>,
}

/// A panic in an observer called from inside a reducer, unwinding with its
/// place. A reducer may catch it, so no phase is left set for it: the fault
/// is placed here only if this very payload ends the root input, or leaves
/// an Effect's `dispatch`.
pub(super) struct ObserverPanic {
    pub(super) at: ExecutionId,
    pub(super) boundary: Boundary,
    pub(super) mount: usize,
    pub(super) payload: Box<dyn Any + Send>,
}

impl Watch<'_> {
    fn reborrow(&mut self) -> Watch<'_> {
        Watch {
            observers: &mut *self.observers,
        }
    }

    /// Calls the observers at `boundary`, inside a reducer, if there are
    /// any: a panic in one (or in what `call` reads for it) faults the Store
    /// as that boundary's fault, and its bug reports are placed there.
    fn tell(
        &mut self,
        at: ExecutionId,
        boundary: Boundary,
        mount: usize,
        mut call: impl FnMut(&mut dyn Observer),
    ) {
        if self.observers.is_empty() {
            return;
        }
        let _observer = bug::Attribute(Phase::Observer(boundary), mount);
        for observer in self.observers.iter_mut() {
            if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| call(&mut **observer))) {
                panic::resume_unwind(Box::new(ObserverPanic {
                    at,
                    boundary,
                    mount,
                    payload,
                }));
            }
        }
    }

    /// Tells the observers that the Action `action` for `child`, called
    /// inline within `at`, was refused.
    #[cold]
    #[inline(never)]
    fn refused<A: Message>(
        &mut self,
        proto: &ProtocolDefs,
        child: usize,
        action: &A,
        at: ExecutionId,
        by: RefusedBy,
        now: Timestamp,
    ) {
        let def = &proto.mounts[child];
        // The kind is read only to tell an observer, in its phase.
        self.tell(at, Boundary::Refused, child, |o| {
            let refusal = Refusal {
                path: &def.path,
                instance: def.instance,
                message: A::TYPE,
                kind: action.kind(),
                sent: SentFrom::Inline(at),
                by,
                time: now,
            };
            o.action_refused(&refusal, action)
        });
    }
}

/// A reducer's context while it changes State: read and change the
/// component's State, make request IDs, call children inline. Then trade it
/// for a `ComponentEmitter` to emit; no API gives both at once.
pub struct ComponentContext<'a, C: Component> {
    pub(super) backend: Backend<'a>,
    pub(super) state: &'a mut C::State,
    pub(super) meta: &'a ActionMeta,
    pub(super) _c: PhantomData<fn() -> C>,
}

impl<'a, C: Component> ComponentContext<'a, C> {
    /// The component's State.
    pub fn state(&self) -> &C::State {
        &*self.state
    }

    /// The component's State, to change.
    pub fn state_mut(&mut self) -> &mut C::State {
        &mut *self.state
    }

    /// One counter per execution, shared with children called inline.
    pub fn new_request(&mut self) -> RequestId {
        let (session, execution, counter) = match &mut self.backend {
            Backend::Store { exec, .. } => (exec.session, exec.execution, &mut exec.next_request),
            Backend::Harness(collected) => (
                collected.session,
                collected.execution,
                &mut collected.next_request,
            ),
        };
        let index = *counter;
        *counter = counter.checked_add(1).expect("request overflow");
        RequestId {
            session,
            execution,
            index,
        }
    }

    /// Runs the child's reducer inside this execution. The child's State is a
    /// reborrow of this component's State, so this needs no `unsafe`, and the
    /// parent's context is usable again once the call returns. It makes no
    /// execution of its own: observers are told of it within this one
    /// (`Observer::inline_action`), or of its refusal. Under a `Harness` the
    /// call is recorded instead, and reported as `Reduced`.
    ///
    /// The child's checks run first, in a Store's order: its State is there,
    /// its root guard (only when this is the root's context, the one that
    /// holds the whole State; see `Registry::admit`), the slot guard, its own
    /// `is_enabled`. A panic in them, or in the child's reducer, unwinds
    /// through this call like any panic in this reducer; uncaught, it faults
    /// the Store.
    pub fn reduce_child<D: Component>(
        &mut self,
        slot: Child<C, D>,
        action: D::Action,
    ) -> ChildOutcome {
        let (proto, mount, exec, mut watch) = match &mut self.backend {
            Backend::Harness(collected) => {
                collected
                    .emissions
                    .push(RawEmission::InlineChild(ChildCall::new(slot.name, action)));
                return ChildOutcome::Reduced;
            }
            Backend::Store {
                proto,
                mount,
                exec,
                watch,
            } => (*proto, *mount, &mut **exec, watch.reborrow()),
        };
        let at = exec.execution;
        let child = proto.resolve_slot::<D>(mount, slot.name);
        let def = &proto.mounts[child];
        let now = self.meta.time;
        bug::tag(Phase::Reducer, mount);

        // The checks, in order: the child's State is there, its root guard,
        // the slot guard, the child's own `is_enabled`. The root guard runs
        // only when the root is the caller, the one context that holds the
        // whole State; below it, the caller takes on what the guard checks
        // (`Registry::admit`).
        let lens = def.lens.as_ref().expect("a child mount always has a lens");
        let root_guard = def.root_guard.as_ref().filter(|_| mount == 0);
        if def.guard.is_some() || root_guard.is_some() {
            let Some(own) = lens.get(&*self.state) else {
                watch.refused(proto, child, &action, at, RefusedBy::Absent, now);
                return ChildOutcome::Refused;
            };
            if let Some(guard) = root_guard {
                let admitted = {
                    let _root = bug::Attribute(Phase::Admission, 0);
                    guard.admit(&*self.state, own, &action, now)
                };
                if !admitted {
                    watch.refused(proto, child, &action, at, RefusedBy::RootGuard, now);
                    return ChildOutcome::Refused;
                }
            }
            if let Some(guard) = &def.guard {
                let admitted = {
                    let _parent = bug::Attribute(Phase::Admission, mount);
                    guard.admit(&*self.state, &action, now)
                };
                if !admitted {
                    watch.refused(proto, child, &action, at, RefusedBy::Guard, now);
                    return ChildOutcome::Refused;
                }
            }
        }
        let Some(child_state) = lens.get_mut(&mut *self.state) else {
            watch.refused(proto, child, &action, at, RefusedBy::Absent, now);
            return ChildOutcome::Refused;
        };
        let child_state = child_state
            .downcast_mut::<D::State>()
            .expect("child State has the slot's type");
        let enabled = {
            let _child = bug::Attribute(Phase::Admission, child);
            D::is_enabled(child_state, &action, now)
        };
        if !enabled {
            watch.refused(proto, child, &action, at, RefusedBy::Component, now);
            return ChildOutcome::Refused;
        }
        watch.tell(at, Boundary::Inline, child, |o| {
            let inline = InlineCall {
                path: &def.path,
                instance: def.instance,
                message: <D::Action as Message>::TYPE,
                kind: action.kind(),
                within: at,
                time: now,
                root_guard_skipped: def.root_guard.is_some() && mount != 0,
            };
            o.inline_action(&inline, &action)
        });

        let ctx = ComponentContext::<D> {
            backend: Backend::Store {
                proto,
                mount: child,
                exec,
                watch,
            },
            state: child_state,
            meta: self.meta,
            _c: PhantomData,
        };
        {
            let _child = bug::Attribute(Phase::Reducer, child);
            D::reduce(ctx, action, self.meta);
        }
        ChildOutcome::Reduced
    }

    /// Ends the mutation phase: from here on the reducer emits.
    pub fn into_emitter(self) -> ComponentEmitter<'a, C> {
        ComponentEmitter {
            backend: self.backend,
            _c: PhantomData,
        }
    }

    /// Ends the mutation phase, keeping a read-only view of State for
    /// planning what to emit.
    pub fn into_emitter_with_view(self) -> (ComponentEmitter<'a, C>, &'a C::State) {
        let view: &'a C::State = self.state;
        (
            ComponentEmitter {
                backend: self.backend,
                _c: PhantomData,
            },
            view,
        )
    }
}

/// A reducer's emitter: everything emitted joins the execution's FIFO in
/// order, and runs after the reducer returns.
pub struct ComponentEmitter<'a, C: Component> {
    pub(super) backend: Backend<'a>,
    pub(super) _c: PhantomData<fn() -> C>,
}

impl<C: Component> ComponentEmitter<'_, C> {
    /// Queues an Action for this component, checked for admission when its
    /// turn comes.
    pub fn emit_action(&mut self, action: C::Action) {
        match &mut self.backend {
            Backend::Store { mount, .. } => {
                let mount = *mount;
                self.push(Category::Action, mount, Payload::new(action));
            }
            Backend::Harness(collected) => collected
                .emissions
                .push(RawEmission::Action(Payload::new(action))),
        }
    }

    /// Queues an Effect for this component's handler.
    pub fn emit_effect(&mut self, effect: C::Effect) {
        match &mut self.backend {
            Backend::Store { mount, .. } => {
                let mount = *mount;
                self.push(Category::Effect, mount, Payload::new(effect));
            }
            Backend::Harness(collected) => collected
                .emissions
                .push(RawEmission::Effect(Payload::new(effect))),
        }
    }

    /// Queues an Action for the child at `slot`, run as its own execution
    /// when its turn comes. `reduce_child` calls it inline instead, and is the
    /// way for a parent to learn of a refusal: a queued Action is checked after
    /// this reducer returns, so only observers hear of its refusal.
    pub fn to_child<D: Component>(&mut self, slot: Child<C, D>, action: D::Action) {
        match &mut self.backend {
            Backend::Store { proto, mount, .. } => {
                let child = proto.resolve_slot::<D>(*mount, slot.name);
                self.push(Category::Action, child, Payload::new(action));
            }
            Backend::Harness(collected) => {
                collected
                    .emissions
                    .push(RawEmission::ToChild(ChildCall::new(slot.name, action)));
            }
        }
    }

    /// Lowered in place by the parent's lowerer for this mount.
    pub fn emit_output(&mut self, output: C::Output) {
        let (proto, mount, exec) = match &mut self.backend {
            Backend::Harness(collected) => {
                collected
                    .emissions
                    .push(RawEmission::Output(Payload::new(output)));
                return;
            }
            Backend::Store {
                proto, mount, exec, ..
            } => (*proto, *mount, &mut **exec),
        };
        let ordinal = exec.claim_emission();
        let def = &proto.mounts[mount];
        let parent = def
            .parent
            .expect("the root has no parent; its Output must be Never");
        let lower = def
            .lower
            .as_ref()
            .unwrap_or_else(|| panic!("no Output lowerer bound for {}", def.path));
        let origin = Origin {
            causal: exec.execution,
            component: def.instance,
            emission_ordinal: ordinal,
            lowering_ordinal: 0,
        };
        bug::tag(Phase::Reducer, mount);
        let _parent = bug::Attribute(Phase::Reducer, parent);
        lower.lower(Payload::new(output), exec, parent, origin);
    }

    fn push(&mut self, category: Category, target: usize, payload: Payload) {
        let Backend::Store {
            proto, mount, exec, ..
        } = &mut self.backend
        else {
            unreachable!("only the Store backend queues");
        };
        let ordinal = exec.claim_emission();
        let origin = Origin {
            causal: exec.execution,
            component: proto.mounts[*mount].instance,
            emission_ordinal: ordinal,
            lowering_ordinal: 0,
        };
        exec.fifo.push(Queued {
            category,
            target,
            payload,
            origin,
        });
    }
}

/// Action-only port given to an Output lowerer (scoped to the parent) or an
/// Event adapter (scoped to its own instance).
pub struct ActionPort<'a, P: Component> {
    pub(super) exec: &'a mut Exec,
    pub(super) target: usize,
    pub(super) mode: PortMode,
    pub(super) _p: PhantomData<fn() -> P>,
}

pub(super) enum PortMode {
    /// Every Action shares the Output's emission ordinal.
    Lowering { origin: Origin, next: u32 },
    /// Every Action is an emission of the Event's execution.
    Adapting { component: InstanceId },
}

impl<P: Component> ActionPort<'_, P> {
    /// Queues an Action for the port's component.
    pub fn emit_action(&mut self, action: P::Action) {
        let origin = match &mut self.mode {
            PortMode::Lowering { origin, next } => {
                let o = Origin {
                    lowering_ordinal: *next,
                    ..*origin
                };
                *next += 1;
                o
            }
            PortMode::Adapting { component } => Origin {
                causal: self.exec.execution,
                component: *component,
                emission_ordinal: self.exec.claim_emission(),
                lowering_ordinal: 0,
            },
        };
        self.exec.fifo.push(Queued {
            category: Category::Action,
            target: self.target,
            payload: Payload::new(action),
            origin,
        });
    }
}
