//! A reusable peer-to-peer RPC component on causet, in a crate of its own
//! and generic over its messages: it knows nothing of the node that mounts
//! it.
//!
//! - **Connections.** A peer has as many connections as are up, up to
//!   `MAX_CONNECTIONS`: the answer to the dial P2P waits for, and those the
//!   peer opened. P2P never picks one of them to close, so the two ends of a
//!   connection never have to agree on one. A connection goes when it closes,
//!   when a send on it fails, or when the parent disconnects the peer; the peer
//!   is ready while it holds one. Every transport callback carries the
//!   connection it belongs to, and a dial's answer the [`DialId`] P2P gave it:
//!   a callback of a connection P2P does not hold, or of a dial it no longer
//!   waits for, changes nothing, and such a dial's connection is closed.
//! - **Outbound requests.** At most one per peer, under a [`Correlation`]
//!   the parent owns, sent on the connection established last and bound to
//!   it. A request is started when P2P admits it: it allocates the
//!   [`RpcId`] (which the parent cannot construct) and reports
//!   [`P2pOutput::RpcRequestStarted`] before any I/O. Every started request
//!   ends exactly once: answered, timed out, aborted when its connection
//!   goes, or failed with its own send. The abort is reported before the
//!   peer gone.
//! - **Inbound requests.** At most `MAX_INBOUND` pending per peer, over its
//!   connections, each identified by a fresh [`InboundId`], whatever wire ID
//!   the peer reused, and answered on the connection it came on. One is
//!   answered at once or after asynchronous work, expires after
//!   `SERVING_TIMEOUT`, and is dropped when its connection goes; a late
//!   answer to it is inert.
//!
//! The parent sends [`Intent`]s only: [`P2pAction`] is opaque, built from an
//! Intent and read through [`P2pAction::intent`]; P2P's own transitions
//! (transport callbacks, timers) are variants the parent cannot name.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Duration;

use causet::*;

/// What a request or response can be: any value the parent chooses.
pub trait Payload: Clone + Debug + PartialEq + Send + 'static {}
impl<T: Clone + Debug + PartialEq + Send + 'static> Payload for T {}

/// How long an outbound request waits for its answer.
pub const RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// How long an inbound request waits for the node's answer.
pub const SERVING_TIMEOUT: Duration = Duration::from_secs(10);
/// At most this many inbound requests pending per peer.
pub const MAX_INBOUND: usize = 5;
/// At most this many connections per peer: a peer cannot open them without
/// bound.
pub const MAX_CONNECTIONS: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(pub u32);

/// A connection, as this end's transport numbers it: unique among one
/// peer's connections, and never reused. It has no order: each end numbers
/// its own, and may number a dial when it starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionId(pub u64);

/// A request's ID on the wire, numbered per connection by its sender: a
/// peer reuses them on its next connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WireId(pub u32);

/// The parent's token for one outbound operation, unique among its
/// outstanding ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Correlation(pub u64);

/// An outbound request, as P2P identifies it. Only P2P makes one:
///
/// ```compile_fail
/// let id = p2p::RpcId(causet::RequestId {
///     session: causet::SessionId(1),
///     execution: causet::ExecutionId(1),
///     index: 0,
/// });
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RpcId(RequestId);

/// A dial, as P2P identifies it: fresh for every attempt, and echoed by the
/// transport's answer, so the answer to a dial P2P gave up (cancelled by a
/// disconnect) cannot end a later one. Only P2P makes one:
///
/// ```compile_fail
/// let id = p2p::DialId(causet::RequestId {
///     session: causet::SessionId(1),
///     execution: causet::ExecutionId(1),
///     index: 0,
/// });
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DialId(RequestId);

/// An inbound request, as P2P identifies it: fresh for every request, so an
/// answer to one cannot reach another that reused its wire ID. Only P2P
/// makes one:
///
/// ```compile_fail
/// let id = p2p::InboundId(causet::RequestId {
///     session: causet::SessionId(1),
///     execution: causet::ExecutionId(1),
///     index: 0,
/// });
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InboundId(RequestId);

/// What crosses a connection.
#[derive(Clone, Debug, PartialEq)]
pub enum Wire<Req: Payload, Resp: Payload> {
    Request {
        wire: WireId,
        request: Req,
    },
    Response {
        wire: WireId,
        response: Option<Resp>,
    },
}

// ---------------------------------------------------------------------------
// The transport

/// Which message a failed send was.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sent {
    Request(WireId),
    Response(WireId),
}

impl Sent {
    /// The message `message` is, as a failed send names it.
    pub fn of<Req: Payload, Resp: Payload>(message: &Wire<Req, Resp>) -> Self {
        match message {
            Wire::Request { wire, .. } => Sent::Request(*wire),
            Wire::Response { wire, .. } => Sent::Response(*wire),
        }
    }
}

/// The transport's callbacks, each naming the connection it belongs to. A
/// connection's `Connected` comes once, before any other callback of it,
/// and none comes after its `Closed` or `SendFailed`.
#[derive(Clone, Debug, Event)]
#[causet(type = "p2p.transport")]
pub enum TransportEvent<Req: Payload, Resp: Payload> {
    /// A connection is up: the answer to `dial`, or, without one, a
    /// connection the peer opened.
    Connected {
        peer: PeerId,
        connection: ConnectionId,
        dial: Option<DialId>,
    },
    DialFailed {
        peer: PeerId,
        dial: DialId,
    },
    Received {
        peer: PeerId,
        connection: ConnectionId,
        message: Wire<Req, Resp>,
    },
    Closed {
        peer: PeerId,
        connection: ConnectionId,
    },
    /// The message `sent` could not be sent: the connection is unusable.
    SendFailed {
        peer: PeerId,
        connection: ConnectionId,
        sent: Sent,
    },
}

/// The transport: what P2P asks of it. Its answers arrive as
/// [`TransportEvent`]s, sent to P2P's mount.
pub trait Transport<Req: Payload, Resp: Payload>: Send {
    /// Answered by `Connected` or `DialFailed`, carrying `dial`.
    fn dial(&mut self, peer: PeerId, dial: DialId);
    fn send(&mut self, peer: PeerId, connection: ConnectionId, message: Wire<Req, Resp>);
    fn close(&mut self, peer: PeerId, connection: ConnectionId);
}

/// P2P's Services.
pub struct P2pServices<Req: Payload, Resp: Payload> {
    pub transport: Box<dyn Transport<Req, Resp>>,
    /// Request timeouts and serving expiry.
    pub timer: Timer,
}

// ---------------------------------------------------------------------------
// State

#[derive(Clone, Debug)]
pub struct Outbound<Req> {
    pub id: RpcId,
    pub wire: WireId,
    pub correlation: Correlation,
    pub request: Req,
    /// The connection it was sent on, and is answered on.
    pub connection: ConnectionId,
}

#[derive(Clone, Debug)]
pub struct Inbound<Req> {
    pub wire: WireId,
    pub request: Req,
    /// The node is working on its answer.
    pub pending: bool,
}

#[derive(Clone, Debug)]
pub struct Connection<Req> {
    /// When P2P saw it established, by its own count.
    established: u64,
    next_wire: u32,
    pub inbound: BTreeMap<InboundId, Inbound<Req>>,
}

/// A peer: ready while it holds a connection, dialing while it holds none
/// and waits for a dial, disconnected otherwise.
#[derive(Clone, Debug)]
pub struct Peer<Req> {
    pub connections: BTreeMap<ConnectionId, Connection<Req>>,
    /// The dial P2P waits for an answer to, kept while connections the
    /// peer opened come and go.
    dial: Option<DialId>,
    pub outbound: Option<Outbound<Req>>,
    /// The parent was told the peer gone, and nothing since: not ready
    /// again, and no `Dial`, whether it made a dial or kept the one out.
    told_gone: bool,
}

impl<Req> Default for Peer<Req> {
    fn default() -> Self {
        Self {
            connections: BTreeMap::new(),
            dial: None,
            outbound: None,
            told_gone: false,
        }
    }
}

impl<Req> Peer<Req> {
    /// Tells the parent the peer is gone, once: a peer whose last
    /// connection went while its dial is still out is gone already when
    /// that dial fails or is cancelled.
    fn report_gone<Resp: Payload>(&mut self, emits: &mut Emit<Req, Resp>, peer: PeerId)
    where
        Req: Payload,
    {
        if !self.told_gone {
            self.told_gone = true;
            emits.push(Emission::Output(P2pOutput::PeerDisconnected { peer }));
        }
    }

    /// The connection a new request goes on: the one established last.
    fn newest(&self) -> Option<ConnectionId> {
        self.connections
            .iter()
            .max_by_key(|(_, c)| c.established)
            .map(|(id, _)| *id)
    }

    /// The connection holding `inbound`.
    fn holding(&self, inbound: InboundId) -> Option<ConnectionId> {
        self.connections
            .iter()
            .find(|(_, c)| c.inbound.contains_key(&inbound))
            .map(|(id, _)| *id)
    }

    fn inbound_count(&self) -> usize {
        self.connections.values().map(|c| c.inbound.len()).sum()
    }
}

#[derive(Clone, Debug)]
pub struct P2pState<Req> {
    pub peers: BTreeMap<PeerId, Peer<Req>>,
    /// Connections established so far, for the order of `established`.
    established: u64,
}

impl<Req> Default for P2pState<Req> {
    fn default() -> Self {
        Self {
            peers: BTreeMap::new(),
            established: 0,
        }
    }
}

impl<Req: Payload> P2pState<Req> {
    /// Whether the peer holds a connection.
    pub fn is_ready(&self, peer: PeerId) -> bool {
        self.peers
            .get(&peer)
            .is_some_and(|p| !p.connections.is_empty())
    }

    /// Whether the peer holds no connection and waits for a dial.
    pub fn is_dialing(&self, peer: PeerId) -> bool {
        self.peers
            .get(&peer)
            .is_some_and(|p| p.connections.is_empty() && p.dial.is_some())
    }

    /// The dial P2P waits for an answer to, if any.
    pub fn dialing(&self, peer: PeerId) -> Option<DialId> {
        self.peers.get(&peer)?.dial
    }

    /// The peer's connections.
    pub fn connections(&self, peer: PeerId) -> BTreeSet<ConnectionId> {
        self.peers
            .get(&peer)
            .map(|p| p.connections.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Whether the peer's connection is one P2P holds.
    pub fn holds_connection(&self, peer: PeerId, connection: ConnectionId) -> bool {
        self.peers
            .get(&peer)
            .is_some_and(|p| p.connections.contains_key(&connection))
    }

    /// The outbound requests out, by peer.
    pub fn outbound(&self) -> impl Iterator<Item = (PeerId, &Outbound<Req>)> {
        self.peers
            .iter()
            .filter_map(|(peer, p)| p.outbound.as_ref().map(|o| (*peer, o)))
    }

    /// The peer's outbound request, if one is out.
    pub fn outbound_to(&self, peer: PeerId) -> Option<&Outbound<Req>> {
        self.peers.get(&peer)?.outbound.as_ref()
    }

    /// Whether one of the peer's connections holds this inbound request.
    pub fn holds(&self, peer: PeerId, inbound: InboundId) -> bool {
        self.peers
            .get(&peer)
            .is_some_and(|p| p.holding(inbound).is_some())
    }

    /// The inbound requests pending, over every peer.
    pub fn inbound_count(&self) -> usize {
        self.peers.values().map(Peer::inbound_count).sum()
    }

    /// P2P's own invariants: an outbound request's connection is held, at
    /// most `MAX_CONNECTIONS` connections and `MAX_INBOUND` inbound requests
    /// per peer, and no two inbound requests of a connection under one wire
    /// ID.
    pub fn invariants(&self) -> Result<(), String> {
        for (peer, p) in &self.peers {
            if let Some(outbound) = &p.outbound
                && !p.connections.contains_key(&outbound.connection)
            {
                return Err(format!(
                    "{peer:?}'s request is out on {:?}, which it does not hold",
                    outbound.connection
                ));
            }
            if p.connections.len() > MAX_CONNECTIONS {
                return Err(format!("{peer:?} has {} connections", p.connections.len()));
            }
            if p.inbound_count() > MAX_INBOUND {
                return Err(format!(
                    "{peer:?} has {} inbound requests",
                    p.inbound_count()
                ));
            }
            for (id, connection) in &p.connections {
                let wires: BTreeSet<WireId> = connection.inbound.values().map(|i| i.wire).collect();
                if wires.len() != connection.inbound.len() {
                    return Err(format!(
                        "{peer:?} holds two inbound requests under one wire ID on {id:?}"
                    ));
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Input: Intents from the parent, and P2P's own transitions

/// What the parent asks of P2P.
#[derive(Clone, Debug, PartialEq)]
pub enum Intent<Req: Payload, Resp: Payload> {
    /// Connect to the peer, once it holds no connection. A dial still out is
    /// kept, and its outcome reported; this is how a parent retries when
    /// told the peer gone.
    Dial { peer: PeerId },
    /// Close the peer's connections and cancel its dial. Followed by a
    /// `Dial`, it forces a fresh dial, but it is no way to retry: the
    /// cancelled dial's connection is closed when it comes, which the peer
    /// takes for a lost connection.
    Disconnect { peer: PeerId },
    RpcRequest {
        peer: PeerId,
        correlation: Correlation,
        request: Req,
    },
    /// The node is working on an inbound request's answer.
    RpcResponsePending { peer: PeerId, inbound: InboundId },
    RpcRespond {
        peer: PeerId,
        inbound: InboundId,
        response: Option<Resp>,
    },
}

#[derive(Debug)]
enum Inner<Req: Payload, Resp: Payload> {
    Intent(Intent<Req, Resp>),
    Transport(TransportEvent<Req, Resp>),
    TimerFired(RequestId),
}

/// P2P's input: an [`Intent`] from the parent, or one of P2P's own
/// transitions, which the parent can neither build nor name.
#[derive(Debug)]
pub struct P2pAction<Req: Payload, Resp: Payload>(Inner<Req, Resp>);

impl<Req: Payload, Resp: Payload> P2pAction<Req, Resp> {
    /// The Intent it carries, if it is one: what a parent's policy reads.
    pub fn intent(&self) -> Option<&Intent<Req, Resp>> {
        match &self.0 {
            Inner::Intent(intent) => Some(intent),
            _ => None,
        }
    }

    /// The transport callback it carries, if it is one: what an observer
    /// reads.
    pub fn transport(&self) -> Option<&TransportEvent<Req, Resp>> {
        match &self.0 {
            Inner::Transport(event) => Some(event),
            _ => None,
        }
    }
}

impl<Req: Payload, Resp: Payload> From<Intent<Req, Resp>> for P2pAction<Req, Resp> {
    fn from(intent: Intent<Req, Resp>) -> Self {
        Self(Inner::Intent(intent))
    }
}

impl<Req: Payload, Resp: Payload> Message for P2pAction<Req, Resp> {
    const TYPE: &'static str = "p2p.action";
    fn kind(&self) -> &'static str {
        match &self.0 {
            Inner::Intent(Intent::Dial { .. }) => "p2p.dial",
            Inner::Intent(Intent::Disconnect { .. }) => "p2p.disconnect",
            Inner::Intent(Intent::RpcRequest { .. }) => "p2p.rpc_request",
            Inner::Intent(Intent::RpcResponsePending { .. }) => "p2p.rpc_response_pending",
            Inner::Intent(Intent::RpcRespond { .. }) => "p2p.rpc_respond",
            Inner::Transport(event) => event.kind(),
            Inner::TimerFired(_) => "p2p.timer_fired",
        }
    }
}

impl<Req: Payload, Resp: Payload> Action for P2pAction<Req, Resp> {}

#[derive(Debug, Effect)]
#[causet(type = "p2p.effect")]
pub enum P2pEffect<Req: Payload, Resp: Payload> {
    Dial {
        peer: PeerId,
        dial: DialId,
    },
    Send {
        peer: PeerId,
        connection: ConnectionId,
        message: Wire<Req, Resp>,
    },
    Close {
        peer: PeerId,
        connection: ConnectionId,
    },
    Arm {
        timer: RequestId,
        after: Duration,
    },
}

/// The facts P2P reports to its parent. Every outbound one carries the
/// request's four identities.
#[derive(Debug, Output)]
#[causet(type = "p2p.output")]
pub enum P2pOutput<Req: Payload, Resp: Payload> {
    /// Its first connection is up.
    PeerReady { peer: PeerId },
    /// Its last connection went, or its dial failed, or the parent
    /// disconnected it: reported once, until it is ready again or the
    /// parent connects again. A dial may still be out (`P2pState::dialing`);
    /// a new `Dial` keeps it, and its outcome is then reported.
    PeerDisconnected { peer: PeerId },
    RpcRequestStarted {
        peer: PeerId,
        id: RpcId,
        correlation: Correlation,
        request: Req,
    },
    RpcResponseReceived {
        peer: PeerId,
        id: RpcId,
        correlation: Correlation,
        request: Req,
        response: Option<Resp>,
    },
    RpcTimedOut {
        peer: PeerId,
        id: RpcId,
        correlation: Correlation,
        request: Req,
    },
    /// Its connection went.
    RpcRequestAborted {
        peer: PeerId,
        id: RpcId,
        correlation: Correlation,
        request: Req,
    },
    /// Its own send failed: its connection is gone too.
    RpcFailed {
        peer: PeerId,
        id: RpcId,
        correlation: Correlation,
        request: Req,
    },
    RpcRequestReceived {
        peer: PeerId,
        inbound: InboundId,
        request: Req,
    },
    /// Not answered within `SERVING_TIMEOUT`: dropped.
    RpcRequestExpired { peer: PeerId, inbound: InboundId },
    /// Its connection went before it was answered.
    RpcRequestDropped { peer: PeerId, inbound: InboundId },
}

// ---------------------------------------------------------------------------
// The component

/// The P2P component, for requests `Req` and responses `Resp`.
pub struct P2p<Req, Resp>(PhantomData<fn() -> (Req, Resp)>);

type Ctx<'a, Req, Resp> = ComponentContext<'a, P2p<Req, Resp>>;
type Emit<Req, Resp> = Vec<Emission<Req, Resp>>;

enum Emission<Req: Payload, Resp: Payload> {
    Output(P2pOutput<Req, Resp>),
    Effect(P2pEffect<Req, Resp>),
}

impl<Req: Payload, Resp: Payload> Component for P2p<Req, Resp> {
    type State = P2pState<Req>;
    type Service = P2pServices<Req, Resp>;
    type Action = P2pAction<Req, Resp>;
    type Effect = P2pEffect<Req, Resp>;
    type Output = P2pOutput<Req, Resp>;

    fn is_enabled(state: &P2pState<Req>, action: &P2pAction<Req, Resp>, _: Timestamp) -> bool {
        let peer_of = |peer: &PeerId| state.peers.get(peer);
        match &action.0 {
            // Whenever the peer holds no connection: a dial still out is
            // kept and its outcome reported, so a parent told the peer gone
            // always hears again.
            Inner::Intent(Intent::Dial { peer }) => {
                peer_of(peer).is_none_or(|p| p.connections.is_empty())
            }
            Inner::Intent(Intent::Disconnect { peer }) => {
                peer_of(peer).is_some_and(|p| !p.connections.is_empty() || p.dial.is_some())
            }
            Inner::Intent(Intent::RpcRequest { peer, .. }) => {
                peer_of(peer).is_some_and(|p| !p.connections.is_empty() && p.outbound.is_none())
            }
            Inner::Intent(Intent::RpcResponsePending { peer, inbound }) => peer_of(peer)
                .and_then(|p| {
                    let connection = p.holding(*inbound)?;
                    p.connections[&connection].inbound.get(inbound)
                })
                .is_some_and(|i| !i.pending),
            Inner::Intent(Intent::RpcRespond { peer, inbound, .. }) => state.holds(*peer, *inbound),
            // A callback of a connection P2P does not hold is stale: it
            // cannot touch the connections P2P has.
            Inner::Transport(TransportEvent::Received {
                peer, connection, ..
            })
            | Inner::Transport(TransportEvent::Closed { peer, connection })
            | Inner::Transport(TransportEvent::SendFailed {
                peer, connection, ..
            }) => state.holds_connection(*peer, *connection),
            // Only the dial P2P waits for can fail.
            Inner::Transport(TransportEvent::DialFailed { peer, dial }) => {
                state.dialing(*peer) == Some(*dial)
            }
            // Admitted whatever its dial: a cancelled dial's connection is
            // closed.
            Inner::Transport(TransportEvent::Connected { .. }) | Inner::TimerFired(_) => true,
        }
    }

    fn reduce(mut ctx: Ctx<'_, Req, Resp>, action: P2pAction<Req, Resp>, _: &ActionMeta) {
        let mut emits = Vec::new();
        match action.0 {
            Inner::Intent(intent) => intent_reduce(&mut ctx, &mut emits, intent),
            Inner::Transport(event) => transport_reduce(&mut ctx, &mut emits, event),
            Inner::TimerFired(timer) => timer_reduce(&mut ctx, &mut emits, timer),
        }
        let mut out = ctx.into_emitter();
        for emit in emits {
            match emit {
                Emission::Output(output) => out.emit_output(output),
                Emission::Effect(effect) => out.emit_effect(effect),
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: P2pEffect<Req, Resp>, _: &EffectMeta) {
        match effect {
            P2pEffect::Dial { peer, dial } => ctx.service().transport.dial(peer, dial),
            P2pEffect::Send {
                peer,
                connection,
                message,
            } => ctx.service().transport.send(peer, connection, message),
            P2pEffect::Close { peer, connection } => {
                ctx.service().transport.close(peer, connection)
            }
            P2pEffect::Arm { timer, after } => {
                let reply = ctx.reply::<TimerFired>();
                ctx.service().timer.after(after, reply, timer);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<TransportEvent<Req, Resp>>();
        r.event::<TimerFired>();
    }
}

impl<Req: Payload, Resp: Payload> Adapts<TransportEvent<Req, Resp>> for P2p<Req, Resp> {
    fn adapt(event: TransportEvent<Req, Resp>, port: &mut ActionPort<'_, Self>) {
        port.emit_action(P2pAction(Inner::Transport(event)));
    }
}

impl<Req: Payload, Resp: Payload> Adapts<TimerFired> for P2p<Req, Resp> {
    fn adapt(fired: TimerFired, port: &mut ActionPort<'_, Self>) {
        port.emit_action(P2pAction(Inner::TimerFired(fired.request)));
    }
}

fn intent_reduce<Req: Payload, Resp: Payload>(
    ctx: &mut Ctx<'_, Req, Resp>,
    emits: &mut Emit<Req, Resp>,
    intent: Intent<Req, Resp>,
) {
    match intent {
        Intent::Dial { peer } => {
            // A dial still out is kept: replacing it would close its
            // connection when it comes, which the peer would take for a
            // lost connection and retry, replacing its own.
            // The transport answers every dial; Disconnect, then Dial,
            // forces a fresh one, which is no way to retry.
            let kept = ctx.state().dialing(peer);
            let dial = kept.unwrap_or_else(|| DialId(ctx.new_request()));
            let p = ctx.state_mut().peers.entry(peer).or_default();
            // Its outcome is reported, whether gone was told already or not.
            p.told_gone = false;
            if kept.is_none() {
                p.dial = Some(dial);
                emits.push(Emission::Effect(P2pEffect::Dial { peer, dial }));
            }
        }
        Intent::Disconnect { peer } => {
            let Some(p) = ctx.state_mut().peers.get_mut(&peer) else {
                return;
            };
            let was_dialing = p.connections.is_empty() && p.dial.is_some();
            p.dial = None;
            let connections: Vec<ConnectionId> = p.connections.keys().copied().collect();
            for connection in connections {
                drop_connection(p, emits, peer, connection, Ending::Closed(true));
            }
            if was_dialing {
                p.report_gone(emits, peer);
            }
        }
        Intent::RpcRequest {
            peer,
            correlation,
            request,
        } => {
            let id = RpcId(ctx.new_request());
            let Some(p) = ctx.state_mut().peers.get_mut(&peer) else {
                return;
            };
            let Some(connection) = p.newest() else {
                return;
            };
            let sent_on = p.connections.get_mut(&connection).expect("the newest");
            let wire = WireId(sent_on.next_wire);
            sent_on.next_wire += 1;
            p.outbound = Some(Outbound {
                id,
                wire,
                correlation,
                request: request.clone(),
                connection,
            });
            emits.push(Emission::Output(P2pOutput::RpcRequestStarted {
                peer,
                id,
                correlation,
                request: request.clone(),
            }));
            emits.push(Emission::Effect(P2pEffect::Send {
                peer,
                connection,
                message: Wire::Request { wire, request },
            }));
            emits.push(Emission::Effect(P2pEffect::Arm {
                timer: id.0,
                after: RPC_TIMEOUT,
            }));
        }
        Intent::RpcResponsePending { peer, inbound } => {
            let Some(p) = ctx.state_mut().peers.get_mut(&peer) else {
                return;
            };
            if let Some(connection) = p.holding(inbound)
                && let Some(pending) = p
                    .connections
                    .get_mut(&connection)
                    .and_then(|c| c.inbound.get_mut(&inbound))
            {
                pending.pending = true;
            }
        }
        Intent::RpcRespond {
            peer,
            inbound,
            response,
        } => {
            let Some(p) = ctx.state_mut().peers.get_mut(&peer) else {
                return;
            };
            // On the connection it came on.
            let Some(connection) = p.holding(inbound) else {
                return;
            };
            let Some(answered) = p
                .connections
                .get_mut(&connection)
                .and_then(|c| c.inbound.remove(&inbound))
            else {
                return;
            };
            let message = Wire::Response {
                wire: answered.wire,
                response,
            };
            emits.push(Emission::Effect(P2pEffect::Send {
                peer,
                connection,
                message,
            }));
        }
    }
}

fn transport_reduce<Req: Payload, Resp: Payload>(
    ctx: &mut Ctx<'_, Req, Resp>,
    emits: &mut Emit<Req, Resp>,
    event: TransportEvent<Req, Resp>,
) {
    match event {
        TransportEvent::Connected {
            peer,
            connection,
            dial,
        } => {
            let state = ctx.state_mut();
            let established = state.established;
            let p = state.peers.entry(peer).or_default();
            // Told again: it is held already.
            if p.connections.contains_key(&connection) {
                return;
            }
            let answers = dial.is_some() && dial == p.dial;
            if answers {
                p.dial = None;
            }
            // A cancelled dial's connection, or one too many: closed.
            if (dial.is_some() && !answers) || p.connections.len() >= MAX_CONNECTIONS {
                emits.push(Emission::Effect(P2pEffect::Close { peer, connection }));
                return;
            }
            let first = p.connections.is_empty();
            let added = Connection {
                established,
                next_wire: 0,
                inbound: BTreeMap::new(),
            };
            p.connections.insert(connection, added);
            state.established += 1;
            if first {
                p.told_gone = false;
                emits.push(Emission::Output(P2pOutput::PeerReady { peer }));
            }
        }
        TransportEvent::DialFailed { peer, .. } => {
            let Some(p) = ctx.state_mut().peers.get_mut(&peer) else {
                return;
            };
            p.dial = None;
            if p.connections.is_empty() {
                p.report_gone(emits, peer);
            }
        }
        TransportEvent::Received {
            peer,
            connection,
            message: Wire::Request { wire, request },
        } => {
            let inbound = InboundId(ctx.new_request());
            let Some(p) = ctx.state_mut().peers.get_mut(&peer) else {
                return;
            };
            // Past the peer's capacity, or a wire ID still pending on this
            // connection: dropped, as the peer's own request timeout will
            // tell it.
            let full = p.inbound_count() >= MAX_INBOUND;
            let Some(on) = p.connections.get_mut(&connection) else {
                return;
            };
            if full || on.inbound.values().any(|i| i.wire == wire) {
                return;
            }
            let pending = Inbound {
                wire,
                request: request.clone(),
                pending: false,
            };
            on.inbound.insert(inbound, pending);
            emits.push(Emission::Output(P2pOutput::RpcRequestReceived {
                peer,
                inbound,
                request,
            }));
            emits.push(Emission::Effect(P2pEffect::Arm {
                timer: inbound.0,
                after: SERVING_TIMEOUT,
            }));
        }
        TransportEvent::Received {
            peer,
            connection,
            message: Wire::Response { wire, response },
        } => {
            let Some(p) = ctx.state_mut().peers.get_mut(&peer) else {
                return;
            };
            // Only on the connection the request went on.
            if p.outbound
                .as_ref()
                .is_none_or(|o| o.connection != connection || o.wire != wire)
            {
                return;
            }
            let Some(answered) = p.outbound.take() else {
                return;
            };
            emits.push(Emission::Output(P2pOutput::RpcResponseReceived {
                peer,
                id: answered.id,
                correlation: answered.correlation,
                request: answered.request,
                response,
            }));
        }
        TransportEvent::Closed { peer, connection } => {
            if let Some(p) = ctx.state_mut().peers.get_mut(&peer) {
                drop_connection(p, emits, peer, connection, Ending::Closed(false));
            }
        }
        TransportEvent::SendFailed {
            peer,
            connection,
            sent,
        } => {
            if let Some(p) = ctx.state_mut().peers.get_mut(&peer) {
                drop_connection(p, emits, peer, connection, Ending::Failed(sent));
            }
        }
    }
}

fn timer_reduce<Req: Payload, Resp: Payload>(
    ctx: &mut Ctx<'_, Req, Resp>,
    emits: &mut Emit<Req, Resp>,
    timer: RequestId,
) {
    let state = ctx.state_mut();
    for (peer, p) in &mut state.peers {
        if p.outbound.as_ref().is_some_and(|o| o.id.0 == timer) {
            let Some(timed_out) = p.outbound.take() else {
                return;
            };
            emits.push(Emission::Output(P2pOutput::RpcTimedOut {
                peer: *peer,
                id: timed_out.id,
                correlation: timed_out.correlation,
                request: timed_out.request,
            }));
            return;
        }
        let inbound = InboundId(timer);
        for connection in p.connections.values_mut() {
            if connection.inbound.remove(&inbound).is_some() {
                emits.push(Emission::Output(P2pOutput::RpcRequestExpired {
                    peer: *peer,
                    inbound,
                }));
                return;
            }
        }
    }
}

/// How a connection went.
enum Ending {
    /// Closed: by P2P too, when `true` (the parent disconnected the peer).
    Closed(bool),
    /// A send on it failed, this one.
    Failed(Sent),
}

/// The peer's connection goes: its outbound request is aborted (or failed,
/// if the send that failed was its own) and its inbound requests dropped;
/// then, if it was the peer's last, the peer is reported gone.
fn drop_connection<Req: Payload, Resp: Payload>(
    p: &mut Peer<Req>,
    emits: &mut Emit<Req, Resp>,
    peer: PeerId,
    connection: ConnectionId,
    ending: Ending,
) {
    let Some(gone) = p.connections.remove(&connection) else {
        return;
    };
    if let Some(ended) = p.outbound.take_if(|o| o.connection == connection) {
        let failed = matches!(ending, Ending::Failed(Sent::Request(wire)) if wire == ended.wire);
        let (id, correlation, request) = (ended.id, ended.correlation, ended.request);
        emits.push(Emission::Output(if failed {
            P2pOutput::RpcFailed {
                peer,
                id,
                correlation,
                request,
            }
        } else {
            P2pOutput::RpcRequestAborted {
                peer,
                id,
                correlation,
                request,
            }
        }));
    }
    for inbound in gone.inbound.into_keys() {
        emits.push(Emission::Output(P2pOutput::RpcRequestDropped {
            peer,
            inbound,
        }));
    }
    if !matches!(ending, Ending::Closed(false)) {
        emits.push(Emission::Effect(P2pEffect::Close { peer, connection }));
    }
    if p.connections.is_empty() {
        p.report_gone(emits, peer);
    }
}
