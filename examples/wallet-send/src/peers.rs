//! The network layer: which full peers are connected and which ledger root
//! each serves, and the requests sent to them. It gives each request the
//! network's own ID, routes the answer back by it, times requests out, and
//! reports a disconnect. What a request is for travels with it as an opaque
//! `Correlation`; the transport is a Service, and the link events (connected,
//! announced, disconnected) arrive from it as Events.

use std::collections::BTreeMap;
use std::time::Duration;

use causet::*;
use serde::{Deserialize, Serialize};

use crate::domain::Root;
use crate::ledger::{PeerAnswer, ProbeRequest};
use crate::node::NodeServices;

/// A full peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerId(pub u32);

/// What a request is for, as its sender knows it. Peers carries it back
/// with every report on the request, and never reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Correlation(pub RequestId);

/// How long a request waits for its answer.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Connection {
    /// The ledger root the peer last announced.
    pub root: Root,
    /// When it connected, in connection order: older first.
    pub since: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outstanding {
    pub peer: PeerId,
    pub correlation: Correlation,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeersState {
    pub connected: BTreeMap<PeerId, Connection>,
    /// The requests sent and not yet answered, by the network's request ID.
    pub outstanding: BTreeMap<RequestId, Outstanding>,
    next_since: u64,
}

impl PeersState {
    /// The connected peers serving `root`, longest connected first.
    pub fn serving(&self, root: Root) -> Vec<PeerId> {
        let mut peers: Vec<(u64, PeerId)> = self
            .connected
            .iter()
            .filter(|(_, connection)| connection.root == root)
            .map(|(peer, connection)| (connection.since, *peer))
            .collect();
        peers.sort();
        peers.into_iter().map(|(_, peer)| peer).collect()
    }

    /// Every outstanding request is to a connected peer.
    pub fn invariants(&self) -> Result<(), String> {
        for (request_id, outstanding) in &self.outstanding {
            if !self.connected.contains_key(&outstanding.peer) {
                return Err(format!(
                    "request {request_id:?} is outstanding to {:?}, which is not connected",
                    outstanding.peer
                ));
            }
        }
        Ok(())
    }
}

/// Why a request failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerError {
    Timeout,
    Disconnected,
}

// ---------------------------------------------------------------------------
// The transport

/// A link changed: from the transport, unasked.
#[derive(Debug, Event)]
#[causet(type = "peers.link")]
pub enum LinkEvent {
    Connected { peer: PeerId, root: Root },
    Announced { peer: PeerId, root: Root },
    Disconnected { peer: PeerId },
}

/// A peer's answer, through the Reply its request was sent with: the
/// connection it came on, and the request it says it answers.
#[derive(Debug, Event)]
#[causet(type = "peers.answer")]
pub struct WireAnswer {
    pub peer: PeerId,
    pub request_id: RequestId,
    pub answer: PeerAnswer,
}

pub trait Wire: Send {
    fn send(
        &mut self,
        peer: PeerId,
        request_id: RequestId,
        request: ProbeRequest,
        reply: Reply<WireAnswer>,
    );
}

// ---------------------------------------------------------------------------
// The component

#[derive(Debug, Action)]
#[causet(type = "peers.action")]
pub enum PeersAction {
    Connected {
        peer: PeerId,
        root: Root,
    },
    Announced {
        peer: PeerId,
        root: Root,
    },
    Disconnected {
        peer: PeerId,
    },
    Request {
        peer: PeerId,
        correlation: Correlation,
        request: ProbeRequest,
    },
    Answered {
        peer: PeerId,
        request_id: RequestId,
        answer: PeerAnswer,
    },
    TimedOut {
        request_id: RequestId,
    },
}

#[derive(Debug, Effect)]
#[causet(type = "peers.effect")]
pub enum PeersEffect {
    Send {
        peer: PeerId,
        request_id: RequestId,
        request: ProbeRequest,
    },
    Arm {
        request_id: RequestId,
    },
}

#[derive(Debug, Output)]
#[causet(type = "peers.output")]
pub enum PeersOutput {
    /// The request is on its way, under `request_id`.
    Started {
        correlation: Correlation,
        peer: PeerId,
        request_id: RequestId,
        request: ProbeRequest,
    },
    Answered {
        correlation: Correlation,
        peer: PeerId,
        request_id: RequestId,
        answer: PeerAnswer,
    },
    Failed {
        correlation: Correlation,
        peer: PeerId,
        request_id: RequestId,
        error: PeerError,
    },
    /// The peer cannot take a request: it is not connected.
    Refused {
        correlation: Correlation,
        peer: PeerId,
    },
    /// The peer disconnected; its outstanding requests have failed first.
    Gone { peer: PeerId },
}

pub struct Peers;

impl Component for Peers {
    type State = PeersState;
    type Service = NodeServices;
    type Action = PeersAction;
    type Effect = PeersEffect;
    type Output = PeersOutput;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: PeersAction, _: &ActionMeta) {
        match action {
            PeersAction::Connected { peer, root } => {
                let state = ctx.state_mut();
                let since = state.next_since;
                state.next_since += 1;
                state.connected.insert(peer, Connection { root, since });
            }
            PeersAction::Announced { peer, root } => {
                if let Some(connection) = ctx.state_mut().connected.get_mut(&peer) {
                    connection.root = root;
                }
            }
            PeersAction::Disconnected { peer } => {
                let state = ctx.state_mut();
                if state.connected.remove(&peer).is_none() {
                    return;
                }
                let failed: Vec<(RequestId, Outstanding)> = state
                    .outstanding
                    .iter()
                    .filter(|(_, outstanding)| outstanding.peer == peer)
                    .map(|(request_id, outstanding)| (*request_id, *outstanding))
                    .collect();
                for (request_id, _) in &failed {
                    state.outstanding.remove(request_id);
                }
                let mut out = ctx.into_emitter();
                for (request_id, outstanding) in failed {
                    out.emit_output(PeersOutput::Failed {
                        correlation: outstanding.correlation,
                        peer,
                        request_id,
                        error: PeerError::Disconnected,
                    });
                }
                out.emit_output(PeersOutput::Gone { peer });
            }
            PeersAction::Request {
                peer,
                correlation,
                request,
            } => {
                if !ctx.state().connected.contains_key(&peer) {
                    ctx.into_emitter()
                        .emit_output(PeersOutput::Refused { correlation, peer });
                    return;
                }
                let request_id = ctx.new_request();
                ctx.state_mut()
                    .outstanding
                    .insert(request_id, Outstanding { peer, correlation });
                let mut out = ctx.into_emitter();
                out.emit_output(PeersOutput::Started {
                    correlation,
                    peer,
                    request_id,
                    request: request.clone(),
                });
                out.emit_effect(PeersEffect::Send {
                    peer,
                    request_id,
                    request,
                });
                out.emit_effect(PeersEffect::Arm { request_id });
            }
            // An answer, or a timeout, for a request no longer outstanding
            // (answered, timed out, its peer gone) is dropped here, and so
            // is an answer from another peer than the one asked: what
            // reaches the sender is about its current request.
            PeersAction::Answered {
                peer,
                request_id,
                answer,
            } => {
                let outstanding = ctx.state().outstanding.get(&request_id);
                let Some(&Outstanding { correlation, .. }) =
                    outstanding.filter(|outstanding| outstanding.peer == peer)
                else {
                    return;
                };
                ctx.state_mut().outstanding.remove(&request_id);
                ctx.into_emitter().emit_output(PeersOutput::Answered {
                    correlation,
                    peer,
                    request_id,
                    answer,
                });
            }
            PeersAction::TimedOut { request_id } => {
                let Some(Outstanding { peer, correlation }) =
                    ctx.state_mut().outstanding.remove(&request_id)
                else {
                    return;
                };
                ctx.into_emitter().emit_output(PeersOutput::Failed {
                    correlation,
                    peer,
                    request_id,
                    error: PeerError::Timeout,
                });
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: PeersEffect, _: &EffectMeta) {
        match effect {
            PeersEffect::Send {
                peer,
                request_id,
                request,
            } => {
                let reply = ctx.reply::<WireAnswer>();
                ctx.service().wire.send(peer, request_id, request, reply);
            }
            PeersEffect::Arm { request_id } => {
                let reply = ctx.reply::<TimerFired>();
                ctx.service()
                    .timer
                    .after(REQUEST_TIMEOUT, reply, request_id);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<LinkEvent>();
        r.event::<WireAnswer>();
        r.event::<TimerFired>();
    }
}

impl Adapts<LinkEvent> for Peers {
    fn adapt(event: LinkEvent, port: &mut ActionPort<'_, Self>) {
        port.emit_action(match event {
            LinkEvent::Connected { peer, root } => PeersAction::Connected { peer, root },
            LinkEvent::Announced { peer, root } => PeersAction::Announced { peer, root },
            LinkEvent::Disconnected { peer } => PeersAction::Disconnected { peer },
        });
    }
}

impl Adapts<WireAnswer> for Peers {
    fn adapt(event: WireAnswer, port: &mut ActionPort<'_, Self>) {
        port.emit_action(PeersAction::Answered {
            peer: event.peer,
            request_id: event.request_id,
            answer: event.answer,
        });
    }
}

impl Adapts<TimerFired> for Peers {
    fn adapt(fired: TimerFired, port: &mut ActionPort<'_, Self>) {
        port.emit_action(PeersAction::TimedOut {
            request_id: fired.request,
        });
    }
}
