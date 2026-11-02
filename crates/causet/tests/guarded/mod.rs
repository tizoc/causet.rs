//! The tree the root-guard tests and the cross-port conformance trace
//! (`examples/conformance_guards.rs`) share: / -> mid -> leaf -> tip, and a
//! second mount of Mid at /other. The leaf under /mid has a root guard,
//! `may_add`, and the one under /other another, `other_may_add`.

#![allow(dead_code)]

use causet::*;

// ---------------------------------------------------------------------------
// Tip: reports what it is asked, as an Output its parent lowers.

pub struct Tip;

#[derive(Debug, Action)]
#[causet(type = "tip.action")]
pub enum TipAction {
    Report(u32),
}

#[derive(Debug, Output)]
#[causet(type = "tip.output")]
pub enum TipOutput {
    Reported(u32),
}

impl Component for Tip {
    type State = ();
    type Action = TipAction;
    type Output = TipOutput;
    no_effects!();

    fn reduce(ctx: ComponentContext<'_, Self>, action: TipAction, _: &ActionMeta) {
        let TipAction::Report(n) = action;
        ctx.into_emitter().emit_output(TipOutput::Reported(n));
    }
}

// ---------------------------------------------------------------------------
// Leaf: adds numbers, and reaches `Add` by every route that enters a mount.
// Its own check refuses 100 and more.

pub struct Leaf;

pub const TIP: Child<Leaf, Tip> = Child::new("tip");

#[derive(Debug, Default)]
pub struct LeafState {
    pub total: u32,
    pub tip: (),
}

#[derive(Debug, Action)]
#[causet(type = "leaf.action")]
pub enum LeafAction {
    Add(u32),
    /// Queues `Add` for itself.
    Again(u32),
    /// `Add` from its Effect handler.
    Echo(u32),
    /// `Add` lowered from its child's Output.
    Ask(u32),
}

#[derive(Debug, Effect)]
#[causet(type = "leaf.effect")]
pub enum LeafEffect {
    Echo(u32),
}

#[derive(Debug, Event)]
#[causet(type = "leaf.bumped")]
pub struct Bumped(pub u32);

impl Component for Leaf {
    type State = LeafState;
    type Service = ();
    type Action = LeafAction;
    type Effect = LeafEffect;
    type Output = Never;

    fn is_enabled(_: &LeafState, action: &LeafAction, _: Timestamp) -> bool {
        !matches!(action, LeafAction::Add(n) if *n >= 100)
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: LeafAction, _: &ActionMeta) {
        match action {
            LeafAction::Add(n) => ctx.state_mut().total += n,
            LeafAction::Again(n) => ctx.into_emitter().emit_action(LeafAction::Add(n)),
            LeafAction::Echo(n) => ctx.into_emitter().emit_effect(LeafEffect::Echo(n)),
            LeafAction::Ask(n) => ctx.into_emitter().to_child(TIP, TipAction::Report(n)),
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: LeafEffect, _: &EffectMeta) {
        let LeafEffect::Echo(n) = effect;
        let _ = ctx.dispatch(LeafAction::Add(n));
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(TIP, lens!(LeafState => tip))
            .on_output(|_, TipOutput::Reported(n), port| port.emit_action(LeafAction::Add(n)))
            .bind();
        r.event::<Bumped>();
    }
}

impl Adapts<Bumped> for Leaf {
    fn adapt(event: Bumped, port: &mut ActionPort<'_, Self>) {
        port.emit_action(LeafAction::Add(event.0));
    }
}

// ---------------------------------------------------------------------------
// Mid: forwards to its leaf inline or queued, and can drop it. Its slot guard
// refuses an addition ending in 13.

pub struct Mid;

pub const LEAF: Child<Mid, Leaf> = Child::new("leaf");

#[derive(Debug)]
pub struct MidState {
    pub leaf: Option<LeafState>,
    pub outcomes: Vec<ChildOutcome>,
}

impl Default for MidState {
    fn default() -> Self {
        Self {
            leaf: Some(LeafState::default()),
            outcomes: Vec::new(),
        }
    }
}

#[derive(Default)]
pub struct MidServices {
    pub leaf: (),
}

#[derive(Debug, Action)]
#[causet(type = "mid.action")]
pub enum MidAction {
    Forward(LeafAction),
    Send(LeafAction),
    DropLeaf,
}

impl Component for Mid {
    type State = MidState;
    type Action = MidAction;
    type Output = Never;
    no_effects!(MidServices);

    fn is_enabled(_: &MidState, action: &MidAction, _: Timestamp) -> bool {
        !matches!(action, MidAction::Forward(LeafAction::Add(n)) if *n >= 1000)
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: MidAction, _: &ActionMeta) {
        match action {
            MidAction::Forward(action) => {
                let outcome = ctx.reduce_child(LEAF, action);
                ctx.state_mut().outcomes.push(outcome);
            }
            MidAction::Send(action) => ctx.into_emitter().to_child(LEAF, action),
            MidAction::DropLeaf => ctx.state_mut().leaf = None,
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(LEAF, lens!(MidState => leaf?))
            .service(lens!(MidServices => leaf))
            .admit(|_, action, _| !matches!(action, LeafAction::Add(n) if n % 100 == 13))
            .bind();
    }
}

// ---------------------------------------------------------------------------
// App: three mounts of Mid, and inline calls into /mid and /spare. The leaf
// under /mid is gated by `blocked` and a limit on its total, the one under
// /other by `other_blocked`. The root guard on /mid checks an addition
// forwarded through it (the forwarding rule), and App's slot guard on /mid
// refuses one ending in 31; /other has neither. /spare is absent unless set,
// and its root guard refuses everything. Mid refuses forwarding 1000 or
// more.

pub struct App;

pub const MID: Child<App, Mid> = Child::new("mid");
pub const OTHER: Child<App, Mid> = Child::new("other");
pub const SPARE: Child<App, Mid> = Child::new("spare");

#[derive(Debug, Default)]
pub struct AppState {
    pub blocked: Vec<u32>,
    pub other_blocked: Vec<u32>,
    pub limit: u32,
    pub mid: MidState,
    pub other: MidState,
    pub spare: Option<MidState>,
}

#[derive(Default)]
pub struct AppServices {
    pub mid: MidServices,
    pub other: MidServices,
    pub spare: MidServices,
}

#[derive(Debug, Action)]
#[causet(type = "app.action")]
pub enum AppAction {
    Block(u32),
    Unblock(u32),
    /// Calls /mid inline.
    Mid(MidAction),
    /// Calls /spare inline.
    Spare(MidAction),
    /// Blocks `n`, then forwards an addition of it through /mid inline: the
    /// root guard on /mid sees the block.
    BlockThenForward(u32),
}

impl Component for App {
    type State = AppState;
    type Action = AppAction;
    type Output = Never;
    no_effects!(AppServices);

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: AppAction, _: &ActionMeta) {
        match action {
            AppAction::Block(n) => ctx.state_mut().blocked.push(n),
            AppAction::Unblock(n) => ctx.state_mut().blocked.retain(|b| *b != n),
            AppAction::Mid(action) => {
                let _ = ctx.reduce_child(MID, action);
            }
            AppAction::Spare(action) => {
                let _ = ctx.reduce_child(SPARE, action);
            }
            AppAction::BlockThenForward(n) => {
                ctx.state_mut().blocked.push(n);
                let _ = ctx.reduce_child(MID, MidAction::Forward(LeafAction::Add(n)));
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(MID, lens!(AppState => mid))
            .service(lens!(AppServices => mid))
            .admit(|_, action, _| {
                !matches!(action, MidAction::Forward(LeafAction::Add(n)) if n % 100 == 31)
            })
            .bind();
        r.child(OTHER, lens!(AppState => other))
            .service(lens!(AppServices => other))
            .bind();
        r.child(SPARE, lens!(AppState => spare?))
            .service(lens!(AppServices => spare))
            .bind();
    }
}

/// The root guard on /mid: an addition forwarded to the leaf, checked where
/// it entered.
pub fn mid_may_forward(app: &AppState, _: &MidState, action: &MidAction, _: Timestamp) -> bool {
    !matches!(action, MidAction::Forward(LeafAction::Add(n)) if app.blocked.contains(n))
}

/// The root guard on /spare: it refuses everything, so a refusal as absent
/// shows that availability is checked first.
pub fn spare_never(_: &AppState, _: &MidState, _: &MidAction, _: Timestamp) -> bool {
    false
}

/// The root guard on /mid/leaf. It reads the whole State and the leaf's.
pub fn may_add(app: &AppState, leaf: &LeafState, action: &LeafAction, _: Timestamp) -> bool {
    match action {
        LeafAction::Add(n) => {
            assert_ne!(*n, 666, "a root guard that panics");
            if *n == 777 {
                bug_condition!("a root guard's report");
                return false;
            }
            !app.blocked.contains(n) && leaf.total + n <= app.limit
        }
        _ => true,
    }
}

pub fn other_may_add(app: &AppState, _: &LeafState, action: &LeafAction, _: Timestamp) -> bool {
    !matches!(action, LeafAction::Add(n) if app.other_blocked.contains(n))
}

pub fn mid() -> Path<Mid> {
    root::<App>().child(MID)
}

pub fn spare() -> Path<Mid> {
    root::<App>().child(SPARE)
}

pub fn mid_leaf() -> Path<Leaf> {
    root::<App>().child(MID).child(LEAF)
}

pub fn other_leaf() -> Path<Leaf> {
    root::<App>().child(OTHER).child(LEAF)
}
