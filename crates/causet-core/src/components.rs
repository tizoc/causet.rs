//! Component contracts, typed child slots and lenses.

use crate::{
    Action, ActionMeta, ActionPort, ComponentContext, ComponentRegistry, Effect, EffectContext,
    EffectMeta, Event, Output, Timestamp,
};
use std::marker::PhantomData;

// ---------------------------------------------------------------------------
// Components

/// A reusable state machine: its State, its Service, the messages it handles,
/// and the functions that handle them. Written once and mounted wherever a
/// parent declares a slot for it; each mount has its own State and Service,
/// projected from its parent's.
///
/// Each handler runs with exactly the authority its job needs: `is_enabled`
/// only reads, `reduce` changes State and then emits, `handle` uses the
/// Service. The causet guide (`causet::guide`) explains the rules.
pub trait Component: Sized + 'static {
    /// The component's slice of State: the whole application State for the
    /// root.
    type State: 'static;
    /// The capability surface the component's Effect handler sees. Sized, so
    /// it projects like State; a trait object is held as `Box<dyn Trait>`.
    type Service: 'static;
    /// The Actions it reduces; its own enum, not the application's.
    type Action: Action;
    /// The Effects it emits and handles; `Never` when it has none
    /// (`no_effects!`).
    type Effect: Effect;
    /// What it reports to its parent; `Never` for the root and for components
    /// that report nothing.
    type Output: Output;

    /// Whether `action` may run against `state` at `now`. Checked on every
    /// route into the component, before anything else happens; a refused Action
    /// runs nothing and gets no execution, and observers are told of it
    /// (`Observer::action_refused`). Admits by default. Pure: it can neither
    /// change State nor emit.
    fn is_enabled(_state: &Self::State, _action: &Self::Action, _now: Timestamp) -> bool {
        true
    }

    /// Handles an admitted Action: change State through `ctx`, then trade it
    /// for an emitter and emit. Takes the Action by value.
    fn reduce(ctx: ComponentContext<'_, Self>, action: Self::Action, meta: &ActionMeta);

    /// Carries out an Effect through the Service. Reads State, never changes
    /// it; Actions that must run now go through `ctx.dispatch`, answers that
    /// come later through a `Reply`.
    fn handle(ctx: EffectContext<'_, Self>, effect: Self::Effect, meta: &EffectMeta);

    /// Declares the component's child slots and the Events it accepts. Called
    /// once per mount when the Registry is built; mistakes are reported by
    /// `freeze`.
    fn register(_r: &mut ComponentRegistry<'_, Self>) {}
}

/// Implemented once per Event type a component accepts.
pub trait Adapts<Ev: Event>: Component {
    /// Turns the Event into this component's Actions. Pure: no State, no
    /// Service.
    fn adapt(event: Ev, port: &mut ActionPort<'_, Self>);
}

/// Writes `Effect = Never` and the empty handler for a component without
/// Effects. `no_effects!()` also sets `Service = ()`; `no_effects!(T)` sets
/// `Service = T`, for a composite with no Effects whose children have some
/// and so need their Services carried through it.
#[macro_export]
macro_rules! no_effects {
    () => {
        $crate::no_effects!(());
    };
    ($service:ty) => {
        type Service = $service;
        type Effect = $crate::Never;
        fn handle(
            _: $crate::EffectContext<'_, Self>,
            effect: $crate::Never,
            _: &$crate::EffectMeta,
        ) {
            match effect {}
        }
    };
}

/// A child slot declared by `P`, holding a `C`. Only `P` can use it:
///
/// ```compile_fail
/// use causet_core::*;
/// #[derive(Debug)] struct A; impl Message for A { const TYPE: &'static str = "a"; fn kind(&self) -> &'static str { "a" } } impl Action for A {}
/// struct Parent; struct Other; struct Kid;
/// impl Component for Kid { type State = (); type Action = A; type Output = Never; no_effects!();
///     fn reduce(_: ComponentContext<'_, Self>, _: A, _: &ActionMeta) {} }
/// impl Component for Other { type State = (); type Action = A; type Output = Never; no_effects!();
///     fn reduce(_: ComponentContext<'_, Self>, _: A, _: &ActionMeta) {} }
/// const SLOT: Child<Parent, Kid> = Child::new("kid");
/// impl Component for Parent { type State = (); type Action = A; type Output = Never; no_effects!();
///     fn reduce(_: ComponentContext<'_, Self>, _: A, _: &ActionMeta) {} }
/// fn misuse(mut ctx: ComponentContext<'_, Other>) {
///     ctx.reduce_child(SLOT, A); // SLOT belongs to Parent, not Other
/// }
/// ```
///
/// Mutable State does not survive the switch to emission:
///
/// ```compile_fail
/// use causet_core::*;
/// #[derive(Debug)] struct A; impl Message for A { const TYPE: &'static str = "a"; fn kind(&self) -> &'static str { "a" } } impl Action for A {}
/// struct C;
/// impl Component for C { type State = u64; type Action = A; type Output = Never; no_effects!();
///     fn reduce(mut ctx: ComponentContext<'_, Self>, a: A, _: &ActionMeta) {
///         let state = ctx.state_mut();
///         let mut out = ctx.into_emitter();
///         *state += 1;
///         out.emit_action(a);
///     } }
/// ```
///
/// A component can ask for replies only for Event types it adapts:
///
/// ```compile_fail
/// use causet_core::*;
/// #[derive(Debug)] struct A; impl Message for A { const TYPE: &'static str = "a"; fn kind(&self) -> &'static str { "a" } } impl Action for A {}
/// #[derive(Debug)] struct Ping; impl Message for Ping { const TYPE: &'static str = "p"; fn kind(&self) -> &'static str { "p" } } impl Event for Ping {}
/// #[derive(Debug)] struct E; impl Message for E { const TYPE: &'static str = "e"; fn kind(&self) -> &'static str { "e" } } impl Effect for E {}
/// struct C;
/// impl Component for C { type State = (); type Service = (); type Action = A; type Effect = E; type Output = Never;
///     fn reduce(_: ComponentContext<'_, Self>, _: A, _: &ActionMeta) {}
///     fn handle(mut ctx: EffectContext<'_, Self>, _: E, _: &EffectMeta) {
///         let _ = ctx.reply::<Ping>(); // C does not implement Adapts<Ping>
///     } }
/// ```
pub struct Child<P, C> {
    pub(super) name: &'static str,
    pub(super) _types: PhantomData<fn() -> (P, C)>,
}

impl<P, C> Child<P, C> {
    /// A slot named `name`: the child's path is its parent's path plus `/name`.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _types: PhantomData,
        }
    }

    /// The slot's name, as it appears in mount paths.
    pub fn name(&self) -> &'static str {
        self.name
    }
}

impl<P, C> Clone for Child<P, C> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<P, C> Copy for Child<P, C> {}

/// A pair of accessors from a parent value to a child value. Used for State
/// and for Services alike.
pub struct Lens<P, T> {
    pub(super) get: for<'a> fn(&'a P) -> Option<&'a T>,
    pub(super) get_mut: for<'a> fn(&'a mut P) -> Option<&'a mut T>,
}

impl<T> Lens<T, T> {
    /// The whole value: a child that shares its parent's Service, or State.
    pub fn identity() -> Self {
        Self::new(|t| Some(t), |t| Some(t))
    }
}

impl<P, T> Lens<P, T> {
    /// A lens from two accessors; `lens!` writes them for a field.
    pub fn new(
        get: for<'a> fn(&'a P) -> Option<&'a T>,
        get_mut: for<'a> fn(&'a mut P) -> Option<&'a mut T>,
    ) -> Self {
        Self { get, get_mut }
    }
}

/// `lens!(Parent => field)` for a plain field, `lens!(Parent => field?)` for an
/// `Option` field that may be absent.
#[macro_export]
macro_rules! lens {
    ($parent:ty => $field:ident ?) => {
        $crate::Lens::<$parent, _>::new(|s| s.$field.as_ref(), |s| s.$field.as_mut())
    };
    ($parent:ty => $field:ident) => {
        $crate::Lens::<$parent, _>::new(|s| Some(&s.$field), |s| Some(&mut s.$field))
    };
}
