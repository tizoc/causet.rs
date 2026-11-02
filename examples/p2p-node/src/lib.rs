//! A node mounting the reusable `p2p` component under its root, and owning
//! what the component must not know: its policy, the routes of its outbound
//! requests, the reply tokens of the inbound ones it answers later, and two
//! consumers standing in for its subsystems.
//!
//! - **Policy** applies to Intents only: a guard on P2P's binding refuses a
//!   fetch while fetching is off and an answer while serving is off, and
//!   lets every internal transition through (timeouts, transport callbacks,
//!   cleanup). The node sends Intents inline, and a refusal, by its policy
//!   or by P2P, answers the consumer at once.
//! - **Routes** are keyed by the correlation the node owns, registered from
//!   `RpcRequestStarted` and resolved only when all four identities match.
//! - **Reply tokens** carry an inbound request's exact identity through the
//!   asynchronous work of answering it.
//! - **Fetching** asks a peer for a key's value; **serving** answers a
//!   peer's fetch, at once for small keys, through the `Server` Service for
//!   the others, which may never answer.

use std::collections::{BTreeMap, BTreeSet};

use causet::*;
use p2p::{
    Correlation, InboundId, Intent, P2p, P2pAction, P2pOutput, P2pServices, P2pState, PeerId, RpcId,
};

/// A fetch: the value of `key`, under the requester's `tag`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Req {
    pub key: u64,
    pub tag: u64,
}

/// A served value, naming the request it answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resp {
    pub tag: u64,
    pub key: u64,
    pub value: u64,
}

impl Resp {
    /// What `server` answers to `request`.
    pub fn of(server: PeerId, request: &Req) -> Self {
        Self {
            tag: request.tag,
            key: request.key,
            value: request.key * 1_000 + u64::from(server.0),
        }
    }
}

pub type NodeP2p = P2p<Req, Resp>;
pub const P2P: Child<Node, NodeP2p> = Child::new("p2p");

/// Keys below this are answered at once; the others through the Server.
pub const SERVED_AT_ONCE_BELOW: u64 = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    pub fetch: bool,
    pub serve: bool,
}

/// How a fetch ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Answered(Option<Resp>),
    TimedOut,
    Aborted,
    Failed,
    Refused,
}

/// A fetch offered: what it asked, and how it ended (`None` while P2P has
/// its request out).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetch {
    pub peer: PeerId,
    pub key: u64,
    pub outcome: Option<Outcome>,
}

/// Where an outbound request's ending goes: all four identities must match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Route {
    pub peer: PeerId,
    pub id: RpcId,
    pub request: Req,
}

/// The exact address of an inbound request being answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReplyToken {
    pub peer: PeerId,
    pub inbound: InboundId,
}

#[derive(Clone, Debug)]
pub struct NodeState {
    pub id: PeerId,
    pub p2p: P2pState<Req>,
    pub policy: Policy,
    pub routes: BTreeMap<Correlation, Route>,
    /// Every fetch offered, by its correlation.
    pub fetches: BTreeMap<Correlation, Fetch>,
    pub tokens: BTreeSet<ReplyToken>,
    /// What this node sent, by requester and tag.
    pub served: BTreeMap<(PeerId, u64), Resp>,
    next_correlation: u64,
}

impl NodeState {
    pub fn new(id: PeerId) -> Self {
        Self {
            id,
            p2p: P2pState::default(),
            policy: Policy {
                fetch: true,
                serve: true,
            },
            routes: BTreeMap::new(),
            fetches: BTreeMap::new(),
            tokens: BTreeSet::new(),
            served: BTreeMap::new(),
            next_correlation: 0,
        }
    }

    /// P2P's invariants, and that the node's routes and reply tokens agree
    /// with what P2P holds.
    pub fn invariants(&self) -> Result<(), String> {
        self.p2p.invariants()?;
        let outstanding: BTreeMap<Correlation, Route> = self
            .p2p
            .outbound()
            .map(|(peer, o)| {
                let route = Route {
                    peer,
                    id: o.id,
                    request: o.request.clone(),
                };
                (o.correlation, route)
            })
            .collect();
        if outstanding != self.routes {
            return Err(format!(
                "routes {:?} do not match the requests out {:?}",
                self.routes, outstanding
            ));
        }
        for (correlation, fetch) in &self.fetches {
            if fetch.outcome.is_none() != self.routes.contains_key(correlation) {
                return Err(format!(
                    "{correlation:?} is {fetch:?}, its route says otherwise"
                ));
            }
        }
        if let Some(token) = self
            .tokens
            .iter()
            .find(|t| !self.p2p.holds(t.peer, t.inbound))
        {
            return Err(format!("{token:?} names no inbound request P2P holds"));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Services

#[derive(Debug, Event)]
#[causet(type = "node.served")]
pub struct Served {
    pub token: ReplyToken,
    pub response: Option<Resp>,
}

/// Answers a fetch asynchronously, or never.
pub trait Server: Send {
    fn serve(&mut self, token: ReplyToken, request: Req, reply: Reply<Served>);
}

pub struct NodeServices {
    pub p2p: P2pServices<Req, Resp>,
    pub server: Box<dyn Server>,
}

// ---------------------------------------------------------------------------
// The root

#[derive(Debug, Action)]
#[causet(type = "node.action")]
pub enum NodeAction {
    Connect {
        peer: PeerId,
    },
    Disconnect {
        peer: PeerId,
    },
    /// A consumer offers a fetch.
    Fetch {
        peer: PeerId,
        key: u64,
    },
    SetPolicy {
        policy: Policy,
    },
    /// Lowered from P2P.
    FromP2p(P2pOutput<Req, Resp>),
    Served {
        token: ReplyToken,
        response: Option<Resp>,
    },
}

#[derive(Debug, Effect)]
#[causet(type = "node.effect")]
pub enum NodeEffect {
    Serve { token: ReplyToken, request: Req },
}

pub struct Node;

/// The node's policy over what enters P2P: it refuses only the Intents it
/// forbids, and lets P2P's own transitions through.
fn policy(state: &NodeState, action: &P2pAction<Req, Resp>, _: Timestamp) -> bool {
    match action.intent() {
        Some(Intent::RpcRequest { .. }) => state.policy.fetch,
        Some(Intent::RpcResponsePending { .. } | Intent::RpcRespond { .. }) => state.policy.serve,
        Some(Intent::Dial { .. } | Intent::Disconnect { .. }) | None => true,
    }
}

impl Component for Node {
    type State = NodeState;
    type Service = NodeServices;
    type Action = NodeAction;
    type Effect = NodeEffect;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: NodeAction, _: &ActionMeta) {
        match action {
            NodeAction::Connect { peer } => {
                ctx.reduce_child(P2P, Intent::Dial { peer }.into());
            }
            NodeAction::Disconnect { peer } => {
                ctx.reduce_child(P2P, Intent::Disconnect { peer }.into());
            }
            NodeAction::SetPolicy { policy } => ctx.state_mut().policy = policy,
            NodeAction::Fetch { peer, key } => {
                let state = ctx.state_mut();
                let correlation = Correlation(state.next_correlation);
                state.next_correlation += 1;
                let fetch = Fetch {
                    peer,
                    key,
                    outcome: None,
                };
                state.fetches.insert(correlation, fetch);
                let request = Req {
                    key,
                    tag: correlation.0,
                };
                let intent = Intent::RpcRequest {
                    peer,
                    correlation,
                    request,
                };
                // Refused by the node's policy or by P2P: nothing ran, and
                // the consumer is answered now.
                if ctx.reduce_child(P2P, intent.into()) == ChildOutcome::Refused {
                    end(ctx.state_mut(), correlation, Outcome::Refused);
                }
            }
            NodeAction::FromP2p(output) => from_p2p(ctx, output),
            NodeAction::Served { token, response } => {
                if !ctx.state_mut().tokens.remove(&token) {
                    return;
                }
                respond(&mut ctx, token, response);
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: NodeEffect, _: &EffectMeta) {
        match effect {
            NodeEffect::Serve { token, request } => {
                let reply = ctx.reply::<Served>();
                ctx.service().server.serve(token, request, reply);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(P2P, lens!(NodeState => p2p))
            .service(lens!(NodeServices => p2p))
            .admit(policy)
            .on_output(|_, output, port| port.emit_action(NodeAction::FromP2p(output)))
            .bind();
        r.event::<Served>();
    }
}

impl Adapts<Served> for Node {
    fn adapt(event: Served, port: &mut ActionPort<'_, Self>) {
        port.emit_action(NodeAction::Served {
            token: event.token,
            response: event.response,
        });
    }
}

/// Answers an inbound request through P2P; what it sent is recorded.
fn respond(ctx: &mut ComponentContext<'_, Node>, token: ReplyToken, response: Option<Resp>) {
    let intent = Intent::RpcRespond {
        peer: token.peer,
        inbound: token.inbound,
        response: response.clone(),
    };
    if ctx.reduce_child(P2P, intent.into()) == ChildOutcome::Reduced
        && let Some(sent) = response
    {
        ctx.state_mut().served.insert((token.peer, sent.tag), sent);
    }
}

/// An outbound request ended: its route, matched on all four identities,
/// says which fetch.
fn ended(
    state: &mut NodeState,
    peer: PeerId,
    id: RpcId,
    correlation: Correlation,
    request: &Req,
    outcome: Outcome,
) {
    let route = state.routes.get(&correlation);
    if route.is_none_or(|r| r.peer != peer || r.id != id || r.request != *request) {
        bug_condition!(peer = peer.0; "an ending without its exact route");
        return;
    }
    state.routes.remove(&correlation);
    end(state, correlation, outcome);
}

fn end(state: &mut NodeState, correlation: Correlation, outcome: Outcome) {
    if let Some(fetch) = state.fetches.get_mut(&correlation) {
        fetch.outcome = Some(outcome);
    }
}

fn from_p2p(mut ctx: ComponentContext<'_, Node>, output: P2pOutput<Req, Resp>) {
    let state = ctx.state_mut();
    match output {
        P2pOutput::PeerReady { .. } => {}
        P2pOutput::PeerDisconnected { peer } => {
            state.tokens.retain(|token| token.peer != peer);
            // Its request was aborted first: no route is left for it.
            let left: Vec<Correlation> = state
                .routes
                .iter()
                .filter(|(_, route)| route.peer == peer)
                .map(|(correlation, _)| *correlation)
                .collect();
            for correlation in left {
                bug_condition!(peer = peer.0; "a route outlived its peer's disconnection");
                state.routes.remove(&correlation);
                end(state, correlation, Outcome::Aborted);
            }
        }
        P2pOutput::RpcRequestStarted {
            peer,
            id,
            correlation,
            request,
        } => {
            if state
                .fetches
                .get(&correlation)
                .is_none_or(|f| f.outcome.is_some())
            {
                bug_condition!(peer = peer.0; "a request started for no fetch");
                return;
            }
            state
                .routes
                .insert(correlation, Route { peer, id, request });
        }
        P2pOutput::RpcResponseReceived {
            peer,
            id,
            correlation,
            request,
            response,
        } => ended(
            state,
            peer,
            id,
            correlation,
            &request,
            Outcome::Answered(response),
        ),
        P2pOutput::RpcTimedOut {
            peer,
            id,
            correlation,
            request,
        } => ended(state, peer, id, correlation, &request, Outcome::TimedOut),
        P2pOutput::RpcRequestAborted {
            peer,
            id,
            correlation,
            request,
        } => ended(state, peer, id, correlation, &request, Outcome::Aborted),
        P2pOutput::RpcFailed {
            peer,
            id,
            correlation,
            request,
        } => ended(state, peer, id, correlation, &request, Outcome::Failed),
        P2pOutput::RpcRequestReceived {
            peer,
            inbound,
            request,
        } => {
            let token = ReplyToken { peer, inbound };
            let server = state.id;
            if request.key < SERVED_AT_ONCE_BELOW {
                respond(&mut ctx, token, Some(Resp::of(server, &request)));
                return;
            }
            let pending = Intent::RpcResponsePending { peer, inbound };
            if ctx.reduce_child(P2P, pending.into()) == ChildOutcome::Refused {
                return;
            }
            ctx.state_mut().tokens.insert(token);
            ctx.into_emitter()
                .emit_effect(NodeEffect::Serve { token, request });
        }
        // Expired, or its connection went: an answer to it is inert.
        P2pOutput::RpcRequestExpired { peer, inbound }
        | P2pOutput::RpcRequestDropped { peer, inbound } => {
            state.tokens.remove(&ReplyToken { peer, inbound });
        }
    }
}
