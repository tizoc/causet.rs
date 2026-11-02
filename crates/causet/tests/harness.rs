//! One component tested on its own: `Harness` drives its reducer with local
//! State and records what it emits, with no Protocol or Store. The first
//! tests run a child component's steps (Action, Effect, Output) in order;
//! the later ones, child calls and requests.

mod common;

use causet::{
    Action, ActionMeta, Child, ChildOutcome, Component, ComponentContext, Effect, EffectContext,
    EffectMeta, Emission, Harness, Never, Output, RequestId, no_effects,
};
use common::{Downloader, DownloaderAction, DownloaderEffect, DownloaderState};

// ---------------------------------------------------------------------------
// A child component's protocol: ChildAction / ChildEffect / ChildOutput

#[derive(Debug, PartialEq, Eq, Action)]
#[causet(type = "child.action")]
enum ChildAction {
    First,
    Last,
    /// Emits First, Request, Completed, Last.
    EmitAll,
    /// Adds 1, checks the State, then emits First.
    MutateThenEmit,
    /// Adds 1, then emits Completed while reading the State.
    EmitWithView,
}

#[derive(Debug, PartialEq, Eq, Effect)]
#[causet(type = "child.effect")]
enum ChildEffect {
    Request,
}

#[derive(Debug, PartialEq, Eq, Output)]
#[causet(type = "child.output")]
enum ChildOutput {
    Completed,
}

struct ChildComponent;

impl Component for ChildComponent {
    type State = i32;
    type Service = ();
    type Action = ChildAction;
    type Effect = ChildEffect;
    type Output = ChildOutput;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: ChildAction, _: &ActionMeta) {
        match action {
            ChildAction::EmitAll => {
                let mut out = ctx.into_emitter();
                out.emit_action(ChildAction::First);
                out.emit_effect(ChildEffect::Request);
                out.emit_output(ChildOutput::Completed);
                out.emit_action(ChildAction::Last);
            }
            ChildAction::MutateThenEmit => {
                *ctx.state_mut() += 1;
                assert_eq!(*ctx.state(), 2);
                let mut out = ctx.into_emitter();
                out.emit_action(ChildAction::First);
            }
            ChildAction::EmitWithView => {
                *ctx.state_mut() += 1;
                let (mut out, state) = ctx.into_emitter_with_view();
                assert_eq!(*state, 2);
                out.emit_output(ChildOutput::Completed);
            }
            ChildAction::First | ChildAction::Last => {}
        }
    }

    fn handle(_: EffectContext<'_, Self>, _: ChildEffect, _: &EffectMeta) {}
}

type ChildEmission = Emission<ChildComponent>;

#[test]
fn recording_emitter_preserves_one_ordered_stream() {
    let mut harness = Harness::<ChildComponent>::new();
    let mut state = 0;

    let emissions = harness.reduce(&mut state, ChildAction::EmitAll);

    assert_eq!(emissions.len(), 4);
    assert_eq!(
        emissions,
        [
            ChildEmission::Action(ChildAction::First),
            ChildEmission::Effect(ChildEffect::Request),
            ChildEmission::Output(ChildOutput::Completed),
            ChildEmission::Action(ChildAction::Last),
        ]
    );
}

#[test]
fn reducer_context_transitions_from_mutation_to_emission() {
    let mut harness = Harness::<ChildComponent>::new();
    let mut state = 1;

    let emissions = harness.reduce(&mut state, ChildAction::MutateThenEmit);

    assert_eq!(state, 2);
    assert_eq!(emissions, [ChildEmission::Action(ChildAction::First)]);
}

#[test]
fn reducer_context_can_emit_with_immutable_state_access() {
    let mut harness = Harness::<ChildComponent>::new();
    let mut state = 1;

    let emissions = harness.reduce(&mut state, ChildAction::EmitWithView);

    assert_eq!(emissions, [ChildEmission::Output(ChildOutput::Completed)]);
}

// ---------------------------------------------------------------------------
// Child calls and requests under the harness

struct Parent;

const KID: Child<Parent, ChildComponent> = Child::new("kid");

#[derive(Default, Debug)]
struct ParentState {
    kid: i32,
    requests: Vec<RequestId>,
    outcomes: Vec<ChildOutcome>,
}

#[derive(Debug, PartialEq, Eq, Action)]
#[causet(type = "parent.action")]
enum ParentAction {
    Go,
    Done,
}

impl Component for Parent {
    type State = ParentState;
    type Action = ParentAction;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: ParentAction, _: &ActionMeta) {
        if action == ParentAction::Go {
            let first = ctx.new_request();
            ctx.state_mut().requests.push(first);
            let outcome = ctx.reduce_child(KID, ChildAction::First);
            ctx.state_mut().outcomes.push(outcome);
            let second = ctx.new_request();
            ctx.state_mut().requests.push(second);
            let mut out = ctx.into_emitter();
            out.to_child(KID, ChildAction::Last);
            out.emit_action(ParentAction::Done);
        }
    }
}

#[test]
fn child_calls_are_recorded_in_order_not_run() {
    let mut harness = Harness::<Parent>::new();
    let mut state = ParentState::default();

    let emissions = harness.reduce(&mut state, ParentAction::Go);

    assert_eq!(
        format!("{emissions:?}"),
        "[InlineChild(kid(child.first)), ToChild(kid(child.last)), Action(Done)]"
    );
    let Emission::InlineChild(call) = &emissions[0] else {
        panic!("an inline child call first");
    };
    assert_eq!(call.action::<ChildAction>(), Some(&ChildAction::First));
    assert_eq!(state.outcomes, [ChildOutcome::Reduced]);
    assert_eq!(state.kid, 0, "the child is recorded, not run");
}

#[test]
fn requests_count_per_execution() {
    let mut harness = Harness::<Parent>::new();
    let mut state = ParentState::default();

    harness.reduce(&mut state, ParentAction::Go);
    harness.reduce(&mut state, ParentAction::Go);

    let indexes: Vec<_> = state
        .requests
        .iter()
        .map(|r| (r.execution, r.index))
        .collect();
    assert_eq!(indexes[0].1, 0);
    assert_eq!(indexes[1].1, 1);
    assert_eq!(indexes[2].1, 0, "a new execution restarts the counter");
    assert_ne!(indexes[0].0, indexes[2].0);
}

#[test]
fn a_component_with_effects_and_a_service_needs_no_store() {
    let mut harness = Harness::<Downloader>::new();
    let mut state = DownloaderState::default();

    let emissions = harness.reduce(&mut state, DownloaderAction::Fetch("alpha".into()));

    let [Emission::Effect(DownloaderEffect::Fetch { request, url })] = emissions.as_slice() else {
        panic!("expected one fetch Effect, got {emissions:?}");
    };
    assert_eq!(url, "alpha");
    assert_eq!(
        state.pending.get(request).map(String::as_str),
        Some("alpha")
    );
    assert!(harness.is_enabled(&state, &DownloaderAction::Probe));
    assert!(!harness.is_enabled(&state, &DownloaderAction::Note(0)));
}

// ---------------------------------------------------------------------------
// One transition as a Store runs it: admission at a chosen time, the new
// State, the ordered emissions, refusals as easy to test as successes.

mod transitions {
    use causet::guide::shop::*;
    use causet::*;

    /// Pings at most once per 10 time units, each one a request.
    struct Pinger;

    #[derive(Debug, Default)]
    struct PingerState {
        last: Option<Timestamp>,
        sent: Vec<RequestId>,
    }

    #[derive(Debug, PartialEq, Action)]
    #[causet(type = "pinger.action", kind = "pinger.ping")]
    struct Ping;

    #[derive(Debug, PartialEq, Effect)]
    #[causet(type = "pinger.effect", kind = "pinger.send")]
    struct Send(RequestId);

    impl Component for Pinger {
        type State = PingerState;
        type Service = ();
        type Action = Ping;
        type Effect = Send;
        type Output = Never;

        fn is_enabled(state: &PingerState, _: &Ping, now: Timestamp) -> bool {
            state.last.is_none_or(|last| now >= last + 10)
        }

        fn reduce(mut ctx: ComponentContext<'_, Self>, _: Ping, meta: &ActionMeta) {
            let request = ctx.new_request();
            let state = ctx.state_mut();
            state.last = Some(meta.time);
            state.sent.push(request);
            ctx.into_emitter().emit_effect(Send(request));
        }

        fn handle(_: EffectContext<'_, Self>, _: Send, _: &EffectMeta) {}
    }

    #[test]
    fn a_transition_is_stamped_from_the_time_given_in_the_session_given() {
        let mut harness = Harness::<Pinger>::with_session(SessionId(7));
        let mut state = PingerState::default();
        harness.at(1_000);
        let emissions = harness.dispatch(&mut state, Ping).unwrap();
        let request = RequestId {
            session: SessionId(7),
            execution: ExecutionId(1),
            index: 0,
        };
        assert!(matches!(emissions.as_slice(), [Emission::Effect(Send(r))] if *r == request));
        assert_eq!(state.last, Some(1_000));
    }

    #[test]
    fn a_refusal_leaves_state_and_time_alone() {
        let mut harness = Harness::<Pinger>::new();
        let mut state = PingerState::default();
        harness.at(100).dispatch(&mut state, Ping).unwrap();

        // Too soon: refused, and the refused attempt uses no time.
        harness.at(105);
        assert_eq!(harness.dispatch(&mut state, Ping).err(), Some(Disabled));
        assert_eq!(
            (state.last, state.sent.len(), harness.next_time()),
            (Some(100), 1, 105)
        );

        harness.at(110);
        assert!(harness.dispatch(&mut state, Ping).is_ok());
        assert_eq!(state.last, Some(110));
    }

    #[test]
    fn times_never_go_back_when_the_clock_does() {
        let mut harness = Harness::<Cart>::new();
        let mut state = CartState::default();
        harness.at(50);
        assert_eq!(
            harness.dispatch(&mut state, CartAction::Checkout).err(),
            Some(Disabled),
            "an empty cart"
        );
        harness
            .dispatch(
                &mut state,
                CartAction::Add {
                    item: "tea".into(),
                    price: 4,
                },
            )
            .unwrap();
        assert_eq!(harness.next_time(), 50);
        harness.at(10);
        assert_eq!(harness.next_time(), 50);
        assert!(harness.dispatch(&mut state, CartAction::Checkout).is_ok());
    }
}
