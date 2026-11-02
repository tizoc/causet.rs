//! Effect capabilities and fault-boundary helpers.

use crate::{
    Adapts, Component, Core, DispatchOutcome, EffectMeta, Event, Observer, Origin, Payload, Phase,
    Reply, ReplyRoute, SentFrom,
};
use std::any::Any;
use std::any::TypeId;
use std::marker::PhantomData;
use std::panic;
use std::panic::AssertUnwindSafe;

// ---------------------------------------------------------------------------
// Effect context

/// What an Effect handler gets: read-only State, its Service, synchronous
/// Action dispatch and reply routes. Every borrow is tied to one method call,
/// so it has ended before `dispatch` runs a child that may mutate State or use
/// Services.
pub struct EffectContext<'a, C: Component> {
    pub(super) core: &'a mut Core,
    pub(super) mount: usize,
    pub(super) meta: &'a EffectMeta,
    pub(super) next_emission: u32,
    pub(super) _c: PhantomData<fn() -> C>,
}

impl<C: Component> EffectContext<'_, C> {
    /// This Effect's metadata.
    pub fn meta(&self) -> &EffectMeta {
        self.meta
    }

    /// The component's State, read-only; `None` if it was mounted through a
    /// fallible lens and is absent now.
    pub fn state(&self) -> Option<&C::State> {
        self.core
            .proto
            .state_ref(&*self.core.state, self.mount)?
            .downcast_ref()
    }

    /// The component's Service.
    pub fn service(&mut self) -> &mut C::Service {
        let Core {
            proto, services, ..
        } = &mut *self.core;
        proto
            .service_mut(&mut **services, self.mount)
            .unwrap_or_else(|| {
                panic!(
                    "no Service projection reaches {}",
                    proto.mounts[self.mount].path
                )
            })
            .downcast_mut()
            .expect("Service type")
    }

    /// State and Service together, to read one while using the other.
    pub fn parts(&mut self) -> (Option<&C::State>, &mut C::Service) {
        let Core {
            proto,
            state,
            services,
            ..
        } = &mut *self.core;
        let state = proto
            .state_ref(&**state, self.mount)
            .and_then(|s| s.downcast_ref());
        let service = proto
            .service_mut(&mut **services, self.mount)
            .unwrap_or_else(|| {
                panic!(
                    "no Service projection reaches {}",
                    proto.mounts[self.mount].path
                )
            })
            .downcast_mut()
            .expect("Service type");
        (state, service)
    }

    /// Admission first; an ordinal is claimed only for an admitted child, so a
    /// refused attempt leaves no gap.
    ///
    /// A panic in that work faults the Store as it leaves this call, whether
    /// or not the handler catches it: the fault is the panic's, with its
    /// place. A handler that catches it finds the Store faulted: a later
    /// `dispatch` runs nothing and returns `DispatchOutcome::Faulted`, the
    /// bus is closed, and as the handler returns, the rest of the root input
    /// is dropped and the Store call returns the fault.
    pub fn dispatch(&mut self, action: C::Action) -> DispatchOutcome {
        if self.core.fault.is_some() {
            return DispatchOutcome::Faulted;
        }
        let (open, depth) = (self.core.open.len(), self.core.depth);
        match panic::catch_unwind(AssertUnwindSafe(|| self.run_dispatch(action))) {
            Ok(outcome) => outcome,
            Err(payload) => self.core.unwound(payload, open, depth),
        }
    }

    fn run_dispatch(&mut self, action: C::Action) -> DispatchOutcome {
        self.core
            .flush_bugs(Phase::Handler, Some(self.meta.id), self.mount);
        let now = self.core.last;
        if !self.core.admit(
            self.mount,
            &action,
            SentFrom::Effect(self.meta.id),
            Some(self.meta.id),
            now,
        ) {
            return DispatchOutcome::Disabled;
        }
        let ordinal = self.next_emission;
        self.next_emission += 1;
        let origin = Origin {
            causal: self.meta.id,
            component: self.core.proto.mounts[self.mount].instance,
            emission_ordinal: ordinal,
            lowering_ordinal: 0,
        };
        self.core
            .run_action(self.mount, Payload::new(action), Some(origin), None);
        DispatchOutcome::Executed
    }

    /// A reply handle for the work this Effect starts. `C: Adapts<Ev>` is
    /// checked by the compiler; the binding (`r.event::<Ev>()`) at run time.
    pub fn reply<Ev: Event>(&self) -> Reply<Ev>
    where
        C: Adapts<Ev>,
    {
        let def = &self.core.proto.mounts[self.mount];
        assert!(
            def.events.iter().any(|b| b.event == TypeId::of::<Ev>()),
            "{} never registered Event {}",
            def.path,
            Ev::TYPE
        );
        Reply {
            sink: self.core.inbox.sink.clone(),
            route: ReplyRoute {
                session: self.core.session,
                instance: def.instance,
                effect: self.meta.id,
                _event: PhantomData,
            },
        }
    }
}

/// Tells observers about something already decided (a fault, a bug
/// report). A panic here is ignored: it must not change or unwind past
/// what is being reported.
pub(super) fn notify(observers: &mut [Box<dyn Observer>], mut call: impl FnMut(&mut dyn Observer)) {
    for observer in observers {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            ignoring_panics(|| call(&mut **observer))
        }));
    }
}

thread_local! {
    /// How deep this thread is in code whose panics the Store ignores.
    pub(super) static IGNORING: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Whether this thread is running code whose panic a Store catches and
/// ignores: an observer told of a bug report or of a fault, or anything
/// that runs in a root input after its fault (a handler that caught the
/// panic of its dispatch, say): the first panic is the fault. Every other
/// panic inside a Store's root input faults it. A test harness reads this
/// from a panic hook to tell the panic that faulted a Store from ignored
/// ones.
pub fn panic_is_ignored() -> bool {
    IGNORING.with(|depth| depth.get() > 0)
}

/// Runs `f` with its panics marked as ignored (`panic_is_ignored`); the
/// caller catches and drops them.
pub(super) fn ignoring_panics<R>(f: impl FnOnce() -> R) -> R {
    struct Scope;
    impl Drop for Scope {
        fn drop(&mut self) {
            IGNORING.with(|depth| depth.set(depth.get() - 1));
        }
    }
    IGNORING.with(|depth| depth.set(depth.get() + 1));
    let _scope = Scope;
    f()
}

/// What a root input unwinds with once its fault is recorded and a handler
/// that caught the panic has returned: the rest of its work does not run.
/// It never reaches a panic hook (`resume_unwind`).
pub(super) struct FaultUnwind;

#[cold]
#[inline(never)]
pub(super) fn fault_unwind() -> ! {
    panic::resume_unwind(Box::new(FaultUnwind))
}

pub(super) fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "panic".into())
}
