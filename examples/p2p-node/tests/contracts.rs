//! Each of the P2P component's contracts, step by step, on one node whose
//! transport and server record what they are asked, and whose transport
//! callbacks the tests send by hand.

use std::sync::{Arc, Mutex};

use causet::SessionId;
use causet::observers::{Execution, MessageView, Observer};
use causet::{ManualClock, Mount, Reply, Timer, root};
use causet_test::Node as Harness;
use p2p::{
    ConnectionId, DialId, MAX_CONNECTIONS, P2pOutput, P2pServices, PeerId, RPC_TIMEOUT,
    SERVING_TIMEOUT, Sent, Transport, TransportEvent, Wire, WireId,
};
use p2p_node::{
    Node, NodeAction, NodeP2p, NodeServices, NodeState, Outcome, P2P, Policy, ReplyToken, Req,
    Resp, Served, Server,
};

#[derive(Debug, Clone, PartialEq)]
enum Asked {
    Dial(PeerId, DialId),
    Send(PeerId, ConnectionId, Wire<Req, Resp>),
    Close(PeerId, ConnectionId),
}

#[derive(Default)]
struct Record {
    asked: Vec<Asked>,
    serves: Vec<(ReplyToken, Req, Reply<Served>)>,
    /// P2P's Outputs, in the order the node lowered them.
    outputs: Vec<String>,
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Record>>);

impl Recorder {
    fn get(&self) -> std::sync::MutexGuard<'_, Record> {
        self.0.lock().unwrap()
    }
}

impl Transport<Req, Resp> for Recorder {
    fn dial(&mut self, peer: PeerId, dial: DialId) {
        self.get().asked.push(Asked::Dial(peer, dial));
    }
    fn send(&mut self, peer: PeerId, connection: ConnectionId, message: Wire<Req, Resp>) {
        self.get()
            .asked
            .push(Asked::Send(peer, connection, message));
    }
    fn close(&mut self, peer: PeerId, connection: ConnectionId) {
        self.get().asked.push(Asked::Close(peer, connection));
    }
}

impl Server for Recorder {
    fn serve(&mut self, token: ReplyToken, request: Req, reply: Reply<Served>) {
        self.get().serves.push((token, request, reply));
    }
}

/// Records the kind of every P2P Output the node lowers, with its peer.
struct Outputs(Recorder);

impl Observer for Outputs {
    fn action_start(&mut self, _: &Execution<'_>, payload: &dyn MessageView) {
        if let Some(NodeAction::FromP2p(output)) = payload.downcast_ref::<NodeAction>() {
            let line = match output {
                P2pOutput::PeerReady { peer } => format!("ready {}", peer.0),
                P2pOutput::PeerDisconnected { peer } => format!("gone {}", peer.0),
                P2pOutput::RpcRequestStarted { peer, .. } => format!("started {}", peer.0),
                P2pOutput::RpcResponseReceived { peer, .. } => format!("answered {}", peer.0),
                P2pOutput::RpcTimedOut { peer, .. } => format!("timed out {}", peer.0),
                P2pOutput::RpcRequestAborted { peer, .. } => format!("aborted {}", peer.0),
                P2pOutput::RpcFailed { peer, .. } => format!("failed {}", peer.0),
                P2pOutput::RpcRequestReceived { peer, .. } => format!("received {}", peer.0),
                P2pOutput::RpcRequestExpired { peer, .. } => format!("expired {}", peer.0),
                P2pOutput::RpcRequestDropped { peer, .. } => format!("dropped {}", peer.0),
            };
            self.0.get().outputs.push(line);
        }
    }
}

const PEER: PeerId = PeerId(7);
const ME: PeerId = PeerId(1);

struct Fixture {
    node: Harness<NodeState, NodeServices>,
    mount: Mount<Node>,
    p2p: Mount<NodeP2p>,
    rec: Recorder,
    clock: ManualClock,
}

fn fixture() -> Fixture {
    let rec = Recorder::default();
    let clock = ManualClock::new(1_000_000_000);
    let (timer, control) = Timer::simulated(&clock);
    let services = NodeServices {
        p2p: P2pServices {
            transport: Box::new(rec.clone()),
            timer,
        },
        server: Box::new(rec.clone()),
    };
    let mut node = Harness::new::<Node>("node", NodeState::new(ME), services, SessionId(1), &clock)
        .with_timer(control)
        .check("node", NodeState::invariants);
    node.store_mut().add_observer(Outputs(rec.clone()));
    let mount = node.mount(&root::<Node>());
    let p2p = node.mount(&root::<Node>().child(P2P));
    Fixture {
        node,
        mount,
        p2p,
        rec,
        clock,
    }
}

impl Fixture {
    fn act(&mut self, action: NodeAction) {
        self.node.dispatch(&self.mount, action);
    }

    fn callback(&mut self, event: TransportEvent<Req, Resp>) {
        self.node
            .sink()
            .try_send_to_as_worker(&self.p2p, event)
            .unwrap();
        while self.node.deliver_next().is_some() {}
    }

    /// The dials asked of the transport, in order.
    fn dials(&self) -> Vec<DialId> {
        let asked = &self.rec.get().asked;
        asked
            .iter()
            .filter_map(|asked| match asked {
                Asked::Dial(_, dial) => Some(*dial),
                _ => None,
            })
            .collect()
    }

    /// The last dial, answered on `connection`.
    fn dialed(&mut self, connection: u64) {
        let dial = *self.dials().last().expect("a dial");
        self.connected(connection, Some(dial));
    }

    /// A connection the peer opened.
    fn opened(&mut self, connection: u64) {
        self.connected(connection, None);
    }

    fn connected(&mut self, connection: u64, dial: Option<DialId>) {
        let connection = ConnectionId(connection);
        self.callback(TransportEvent::Connected {
            peer: PEER,
            connection,
            dial,
        });
    }

    fn closes(&self) -> Vec<ConnectionId> {
        let asked = &self.rec.get().asked;
        asked
            .iter()
            .filter_map(|asked| match asked {
                Asked::Close(_, connection) => Some(*connection),
                _ => None,
            })
            .collect()
    }

    fn connections(&self) -> Vec<ConnectionId> {
        let held = self.node.state().p2p.connections(PEER);
        held.into_iter().collect()
    }

    fn closed(&mut self, connection: u64) {
        let connection = ConnectionId(connection);
        self.callback(TransportEvent::Closed {
            peer: PEER,
            connection,
        });
    }

    fn after(&mut self, duration: std::time::Duration) {
        self.clock.advance(duration.as_nanos() as u64);
        self.node.fire_timers();
        while self.node.deliver_next().is_some() {}
    }

    fn outcome(&self, tag: u64) -> Option<Outcome> {
        self.node.state().fetches[&p2p::Correlation(tag)]
            .outcome
            .clone()
    }

    /// The last request sent to the peer, with its wire ID.
    fn last_request(&self) -> (ConnectionId, WireId, Req) {
        self.rec
            .get()
            .asked
            .iter()
            .rev()
            .find_map(|asked| match asked {
                Asked::Send(_, connection, Wire::Request { wire, request }) => {
                    Some((*connection, *wire, request.clone()))
                }
                _ => None,
            })
            .expect("a request sent")
    }

    fn responses_sent(&self) -> Vec<(ConnectionId, WireId, Option<Resp>)> {
        self.rec
            .get()
            .asked
            .iter()
            .filter_map(|asked| match asked {
                Asked::Send(_, connection, Wire::Response { wire, response }) => {
                    Some((*connection, *wire, response.clone()))
                }
                _ => None,
            })
            .collect()
    }

    fn outputs(&self) -> Vec<String> {
        self.rec.get().outputs.clone()
    }
}

fn request(key: u64, tag: u64) -> Req {
    Req { key, tag }
}

fn answer(key: u64, tag: u64) -> Resp {
    Resp::of(PEER, &request(key, tag))
}

#[test]
fn a_fetch_is_started_routed_and_answered() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    assert!(matches!(f.rec.get().asked[..], [Asked::Dial(PEER, _)]));
    f.dialed(1);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    let (connection, wire, sent) = f.last_request();
    assert_eq!(sent, request(3, 0));
    assert_eq!(f.node.state().routes.len(), 1);

    let response = Some(answer(3, 0));
    let message = Wire::Response {
        wire,
        response: response.clone(),
    };
    f.callback(TransportEvent::Received {
        peer: PEER,
        connection,
        message,
    });
    assert_eq!(f.outcome(0), Some(Outcome::Answered(response)));
    assert!(f.node.state().routes.is_empty());
    assert_eq!(f.outputs(), ["ready 7", "started 7", "answered 7"]);
}

#[test]
fn a_stale_callback_cannot_touch_the_connection_that_replaced_it() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    f.callback(TransportEvent::Closed {
        peer: PEER,
        connection: ConnectionId(1),
    });
    f.opened(2);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    let (connection, wire, _) = f.last_request();
    assert_eq!(connection, ConnectionId(2));

    // The old connection's answer, close and failure, delivered late.
    let stale = ConnectionId(1);
    let message = Wire::Response {
        wire,
        response: Some(answer(3, 99)),
    };
    f.callback(TransportEvent::Received {
        peer: PEER,
        connection: stale,
        message,
    });
    f.callback(TransportEvent::Closed {
        peer: PEER,
        connection: stale,
    });
    f.callback(TransportEvent::SendFailed {
        peer: PEER,
        connection: stale,
        sent: Sent::Request(wire),
    });
    assert!(f.outcome(0).is_none(), "{:?}", f.outcome(0));
    assert_eq!(f.connections(), [ConnectionId(2)]);
}

/// Section 26.10: a connection the peer opens is added beside the one in
/// use, which keeps its request; a new request goes on the one established
/// last. (Before, the newest number replaced the other, aborting the request
/// out on it.) A connection told again is inert.
#[test]
fn a_connection_the_peer_opens_is_added_beside_the_one_in_use() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(2);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    f.opened(1);
    assert_eq!(f.connections(), [ConnectionId(1), ConnectionId(2)]);
    assert!(f.outcome(0).is_none(), "{:?}", f.outcome(0));
    f.dialed(2);
    f.opened(1);
    assert!(f.closes().is_empty(), "{:?}", f.closes());
    assert_eq!(f.outputs(), ["ready 7", "started 7"]);

    let (connection, wire, _) = f.last_request();
    assert_eq!(connection, ConnectionId(2));
    let message = Wire::Response {
        wire,
        response: Some(answer(3, 0)),
    };
    f.callback(TransportEvent::Received {
        peer: PEER,
        connection,
        message,
    });
    f.act(NodeAction::Fetch { peer: PEER, key: 4 });
    assert_eq!(f.last_request().0, ConnectionId(1), "established last");
}

/// Regression: two peers dialing each other at once, each numbering
/// its own connections. Each kept the one its numbers called newest, a
/// different one, and closed the other: both died. Each now keeps both.
fn simultaneous_dial(inbound_first: bool) {
    let mut a = fixture();
    let mut b = fixture();
    a.act(NodeAction::Connect { peer: PEER });
    b.act(NodeAction::Connect { peer: PEER });
    // X is a's dial and b's inbound; Y is b's dial and a's inbound. a
    // numbers them X 1 and Y 2, b numbers Y 1 and X 2.
    if inbound_first {
        a.opened(2);
        b.opened(2);
        a.dialed(1);
        b.dialed(1);
    } else {
        a.dialed(1);
        b.dialed(1);
        a.opened(2);
        b.opened(2);
    }
    for end in [&a, &b] {
        assert_eq!(end.connections(), [ConnectionId(1), ConnectionId(2)]);
        assert!(end.closes().is_empty(), "{:?}", end.closes());
    }
}

#[test]
fn a_simultaneous_dial_keeps_both_connections_inbound_first() {
    simultaneous_dial(true);
}

#[test]
fn a_simultaneous_dial_keeps_both_connections_dial_first() {
    simultaneous_dial(false);
}

/// Regression: the dial waited for, numbered when it started, was
/// refused because a connection the peer opened meanwhile had a greater
/// number.
#[test]
fn the_dial_waited_for_is_used_whatever_its_number() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.opened(2);
    f.closed(2);
    f.dialed(1);
    assert_eq!(f.connections(), [ConnectionId(1)]);
    assert!(f.closes().is_empty(), "{:?}", f.closes());
    assert_eq!(f.outputs(), ["ready 7", "gone 7", "ready 7"]);
}

/// Regression: `DialFailed` named no dial, so a cancelled dial's
/// failure ended the dial that replaced it.
#[test]
fn a_cancelled_dial_s_failure_cannot_end_its_replacement() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.act(NodeAction::Disconnect { peer: PEER });
    f.act(NodeAction::Connect { peer: PEER });
    let [cancelled, current] = f.dials()[..] else {
        panic!("two dials: {:?}", f.dials());
    };
    assert_ne!(cancelled, current);
    f.callback(TransportEvent::DialFailed {
        peer: PEER,
        dial: cancelled,
    });
    assert!(f.node.state().p2p.is_dialing(PEER));
    assert_eq!(f.outputs(), ["gone 7"], "only the disconnection's");
    f.dialed(1);
    assert_eq!(f.connections(), [ConnectionId(1)]);

    // Its own failure, once answered, is stale too.
    f.callback(TransportEvent::DialFailed {
        peer: PEER,
        dial: current,
    });
    assert_eq!(f.connections(), [ConnectionId(1)]);
}

/// Regression: a cancelled dial's connection made the peer ready,
/// though the node had disconnected it.
#[test]
fn a_cancelled_dial_s_connection_is_closed_not_used() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.act(NodeAction::Disconnect { peer: PEER });
    let cancelled = f.dials()[0];
    f.connected(1, Some(cancelled));
    assert!(f.connections().is_empty());
    assert_eq!(f.closes(), [ConnectionId(1)]);

    f.act(NodeAction::Connect { peer: PEER });
    f.connected(2, Some(cancelled));
    assert!(f.node.state().p2p.is_dialing(PEER));
    assert_eq!(f.closes(), [ConnectionId(1), ConnectionId(2)]);
    f.dialed(3);
    assert_eq!(f.connections(), [ConnectionId(3)]);
    assert_eq!(f.outputs(), ["gone 7", "ready 7"]);
}

/// A connection the peer opens while a dial is out is used, the dial's
/// answer is added beside it, and when the first goes, its request is
/// aborted while the peer stays ready on the other.
#[test]
fn a_request_ends_with_its_own_connection_while_another_stays() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.opened(1);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    f.dialed(2);
    assert_eq!(f.connections(), [ConnectionId(1), ConnectionId(2)]);
    assert!(f.outcome(0).is_none());
    f.closed(1);
    assert_eq!(f.outcome(0), Some(Outcome::Aborted));
    assert_eq!(f.connections(), [ConnectionId(2)]);
    assert_eq!(f.outputs(), ["ready 7", "started 7", "aborted 7"]);
    f.act(NodeAction::Fetch { peer: PEER, key: 4 });
    assert_eq!(f.last_request().0, ConnectionId(2));
}

/// An answer comes on the connection its request went on: the same wire ID
/// on another connection answers nothing.
#[test]
fn an_answer_on_another_connection_is_inert() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    f.opened(2);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    let (connection, wire, _) = f.last_request();
    assert_eq!(connection, ConnectionId(2));
    let other = Wire::Response {
        wire,
        response: Some(answer(3, 99)),
    };
    f.callback(TransportEvent::Received {
        peer: PEER,
        connection: ConnectionId(1),
        message: other,
    });
    assert!(f.outcome(0).is_none(), "{:?}", f.outcome(0));
    let own = Wire::Response {
        wire,
        response: Some(answer(3, 0)),
    };
    f.callback(TransportEvent::Received {
        peer: PEER,
        connection,
        message: own,
    });
    assert_eq!(f.outcome(0), Some(Outcome::Answered(Some(answer(3, 0)))));
}

/// A peer cannot open connections without bound.
#[test]
fn a_connection_past_the_limit_is_closed() {
    let mut f = fixture();
    for connection in 1..=MAX_CONNECTIONS as u64 {
        f.opened(connection);
    }
    let past = MAX_CONNECTIONS as u64 + 1;
    f.opened(past);
    assert_eq!(f.connections().len(), MAX_CONNECTIONS);
    assert_eq!(f.closes(), [ConnectionId(past)]);
}

#[test]
fn a_send_that_fails_at_once_ends_its_request_once_after_it_started() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    let (_, wire, _) = f.last_request();
    f.callback(TransportEvent::SendFailed {
        peer: PEER,
        connection: ConnectionId(1),
        sent: Sent::Request(wire),
    });
    assert_eq!(f.outcome(0), Some(Outcome::Failed));
    assert_eq!(f.outputs(), ["ready 7", "started 7", "failed 7", "gone 7"]);
    // Its timeout, later, changes nothing.
    f.after(RPC_TIMEOUT);
    assert_eq!(f.outcome(0), Some(Outcome::Failed));
    assert_eq!(f.outputs().len(), 4);
}

/// Regression: a failed send of an answer to an inbound request was
/// reported as the outbound request's own send failing.
#[test]
fn a_failed_answer_is_not_the_request_s_failure() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    // Answered at once: a second send on the connection.
    inbound(&mut f, 1, 0, 3);
    f.callback(TransportEvent::SendFailed {
        peer: PEER,
        connection: ConnectionId(1),
        sent: Sent::Response(WireId(0)),
    });
    assert_eq!(f.outcome(0), Some(Outcome::Aborted));
}

#[test]
fn a_disconnect_reports_the_aborted_request_before_the_peer_gone() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    f.act(NodeAction::Disconnect { peer: PEER });
    assert_eq!(f.outcome(0), Some(Outcome::Aborted));
    assert_eq!(f.outputs(), ["ready 7", "started 7", "aborted 7", "gone 7"]);
    assert!(
        f.rec
            .get()
            .asked
            .contains(&Asked::Close(PEER, ConnectionId(1)))
    );
}

#[test]
fn a_request_times_out() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    f.after(RPC_TIMEOUT);
    assert_eq!(f.outcome(0), Some(Outcome::TimedOut));
}

#[test]
fn one_request_per_peer_and_none_to_a_peer_not_ready() {
    let mut f = fixture();
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    assert_eq!(f.outcome(0), Some(Outcome::Refused), "not ready");
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    f.act(NodeAction::Fetch { peer: PEER, key: 4 });
    assert!(f.outcome(1).is_none());
    assert_eq!(f.outcome(2), Some(Outcome::Refused), "one at a time");
}

fn inbound(f: &mut Fixture, connection: u64, wire: u32, key: u64) {
    let message = Wire::Request {
        wire: WireId(wire),
        request: request(key, wire.into()),
    };
    let connection = ConnectionId(connection);
    f.callback(TransportEvent::Received {
        peer: PEER,
        connection,
        message,
    });
}

#[test]
fn an_inbound_request_is_answered_at_once_or_later() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    inbound(&mut f, 1, 0, 3);
    let me = |key, tag| Some(Resp::of(ME, &request(key, tag)));
    assert_eq!(f.responses_sent(), [(ConnectionId(1), WireId(0), me(3, 0))]);

    inbound(&mut f, 1, 1, 30);
    assert_eq!(f.node.state().tokens.len(), 1);
    let (token, request, reply) = f.rec.get().serves.remove(0);
    let response = Some(Resp::of(ME, &request));
    reply
        .try_send_as_worker(Served { token, response })
        .unwrap();
    while f.node.deliver_next().is_some() {}
    assert_eq!(
        f.responses_sent()[1],
        (ConnectionId(1), WireId(1), me(30, 1))
    );
    assert!(f.node.state().tokens.is_empty());
}

#[test]
fn an_answer_that_never_comes_expires_and_frees_the_peer_s_capacity() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    for wire in 0..6 {
        inbound(&mut f, 1, wire, 30);
    }
    assert_eq!(f.rec.get().serves.len(), 5, "the sixth is past capacity");
    f.after(SERVING_TIMEOUT);
    assert!(f.node.state().tokens.is_empty());
    assert_eq!(f.node.state().p2p.inbound_count(), 0);

    // A late answer is inert, and the capacity is back.
    let (token, request, reply) = f.rec.get().serves.remove(0);
    let response = Some(Resp::of(ME, &request));
    reply
        .try_send_as_worker(Served { token, response })
        .unwrap();
    while f.node.deliver_next().is_some() {}
    assert!(f.responses_sent().is_empty());
    inbound(&mut f, 1, 6, 30);
    assert_eq!(f.node.state().tokens.len(), 1);
}

#[test]
fn an_answer_for_a_request_before_a_reconnect_cannot_answer_its_wire_id_reused() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    inbound(&mut f, 1, 0, 30);
    f.callback(TransportEvent::Closed {
        peer: PEER,
        connection: ConnectionId(1),
    });
    f.opened(2);
    inbound(&mut f, 2, 0, 31);

    let (token, request, reply) = f.rec.get().serves.remove(0);
    let response = Some(Resp::of(ME, &request));
    reply
        .try_send_as_worker(Served { token, response })
        .unwrap();
    while f.node.deliver_next().is_some() {}
    assert!(f.responses_sent().is_empty(), "{:?}", f.responses_sent());
    assert_eq!(f.node.state().tokens.len(), 1);
}

#[test]
fn the_node_s_policy_refuses_its_intents_and_lets_cleanup_through() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    let off = Policy {
        fetch: false,
        serve: false,
    };
    f.act(NodeAction::SetPolicy { policy: off });

    f.act(NodeAction::Fetch { peer: PEER, key: 4 });
    assert_eq!(f.outcome(1), Some(Outcome::Refused));
    inbound(&mut f, 1, 0, 3);
    assert!(f.responses_sent().is_empty(), "serving is off");

    // The request already out still times out, and the inbound expires.
    f.after(SERVING_TIMEOUT);
    assert_eq!(f.outcome(0), Some(Outcome::TimedOut));
    assert_eq!(f.node.state().p2p.inbound_count(), 0);
}

/// Provenance: an action lowered from a P2P Output names, in the trace, the
/// mount that emitted it and the execution that ran P2P's transition.
#[test]
fn a_lowered_output_names_the_p2p_execution_that_made_it() {
    let mut f = fixture();
    f.node
        .store_mut()
        .add_observer(causet::observers::TraceLog::new());
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    f.act(NodeAction::Fetch { peer: PEER, key: 3 });
    let (connection, wire, _) = f.last_request();
    let message = Wire::Response {
        wire,
        response: Some(answer(3, 0)),
    };
    f.callback(TransportEvent::Received {
        peer: PEER,
        connection,
        message,
    });

    let p2p = f.node.protocol().instance_of(&f.p2p);
    let trace = f
        .node
        .store()
        .observer::<causet::observers::TraceLog>()
        .unwrap();
    let entries = trace.entries();
    let lowered: Vec<_> = entries
        .iter()
        .filter(|entry| entry.kind.ends_with("from_p2p"))
        .collect();
    assert_eq!(lowered.len(), 3, "ready, started, answered");
    for entry in &lowered {
        let origin = entry.origin.expect("a lowered action has an origin");
        assert_eq!(origin.component, p2p, "{entry:?}");
        assert_eq!(origin.lowering_ordinal, 0);
    }
    // The answer was lowered from P2P's own execution of the transport's
    // callback, at its path.
    let answered = lowered[2].origin.unwrap().causal;
    let cause = entries.iter().find(|entry| entry.id == answered).unwrap();
    assert_eq!(&*cause.path, "/p2p");
    assert_eq!(cause.kind, "p2p.received");
}

/// `MAX_INBOUND` is the peer's, over all its connections.
#[test]
fn inbound_capacity_is_the_peer_s_over_its_connections() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    f.opened(2);
    for wire in 0..3 {
        inbound(&mut f, 1, wire, 30);
        inbound(&mut f, 2, wire, 30);
    }
    assert_eq!(f.rec.get().serves.len(), 5, "the sixth is past capacity");
    assert_eq!(f.node.state().p2p.inbound_count(), 5);
}

/// An inbound request whose connection goes while another stays is
/// dropped, and its late answer is inert; the peer stays ready.
#[test]
fn an_inbound_request_is_dropped_with_its_connection() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.dialed(1);
    f.opened(2);
    inbound(&mut f, 1, 0, 30);
    assert_eq!(f.node.state().tokens.len(), 1);
    f.closed(1);
    assert!(f.node.state().tokens.is_empty());
    assert_eq!(f.outputs(), ["ready 7", "received 7", "dropped 7"]);

    let (token, request, reply) = f.rec.get().serves.remove(0);
    let response = Some(Resp::of(ME, &request));
    reply
        .try_send_as_worker(Served { token, response })
        .unwrap();
    while f.node.deliver_next().is_some() {}
    assert!(f.responses_sent().is_empty(), "{:?}", f.responses_sent());
    assert_eq!(f.connections(), [ConnectionId(2)]);
}

/// Regression: a peer whose last connection went while its dial was
/// still out was reported gone again when that dial failed.
#[test]
fn a_peer_is_reported_gone_once_when_its_dial_then_fails() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.opened(2);
    f.closed(2);
    assert!(f.node.state().p2p.is_dialing(PEER), "its dial is still out");
    let dial = f.dials()[0];
    f.callback(TransportEvent::DialFailed { peer: PEER, dial });
    assert_eq!(f.outputs(), ["ready 7", "gone 7"]);
}

/// Regression: the same, when the node then disconnects the peer.
/// And a new dial's failure is a report of its own.
#[test]
fn a_peer_is_reported_gone_once_when_then_disconnected() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.opened(2);
    f.closed(2);
    f.act(NodeAction::Disconnect { peer: PEER });
    assert_eq!(f.outputs(), ["ready 7", "gone 7"]);
    f.act(NodeAction::Connect { peer: PEER });
    let dial = f.dials()[1];
    f.callback(TransportEvent::DialFailed { peer: PEER, dial });
    assert_eq!(f.outputs(), ["ready 7", "gone 7", "gone 7"]);
}

/// Regression: told the peer gone while P2P's own dial was still
/// out, the node dialed again and was refused; when that dial failed, gone
/// had been told already, so the node never heard how its connect ended.
/// The connect now keeps the dial still out and renews its report: the
/// dial's failure is told, and so is its connection.
#[test]
fn a_connect_after_gone_keeps_the_dial_still_out_and_reports_it() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.opened(2);
    f.closed(2);
    assert_eq!(f.outputs(), ["ready 7", "gone 7"]);
    f.act(NodeAction::Connect { peer: PEER });
    let [dial] = f.dials()[..] else {
        panic!("one dial: {:?}", f.dials());
    };
    f.callback(TransportEvent::DialFailed { peer: PEER, dial });
    assert_eq!(f.outputs(), ["ready 7", "gone 7", "gone 7"]);

    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.opened(2);
    f.closed(2);
    f.act(NodeAction::Connect { peer: PEER });
    let [dial] = f.dials()[..] else {
        panic!("one dial: {:?}", f.dials());
    };
    f.connected(5, Some(dial));
    assert_eq!(f.connections(), [ConnectionId(5)]);
    assert_eq!(f.closes(), []);
    assert_eq!(f.outputs(), ["ready 7", "gone 7", "ready 7"]);
}

/// Disconnect, then connect: the dial still out is cancelled, and a fresh
/// one made; the cancelled dial's connection is closed, its failure inert.
#[test]
fn a_disconnect_then_a_connect_dials_afresh() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.act(NodeAction::Disconnect { peer: PEER });
    f.act(NodeAction::Connect { peer: PEER });
    let [cancelled, fresh] = f.dials()[..] else {
        panic!("two dials: {:?}", f.dials());
    };
    f.callback(TransportEvent::DialFailed {
        peer: PEER,
        dial: cancelled,
    });
    assert_eq!(f.node.state().p2p.dialing(PEER), Some(fresh));
    f.connected(5, Some(cancelled));
    assert_eq!(f.closes(), [ConnectionId(5)]);
    f.connected(6, Some(fresh));
    assert_eq!(f.connections(), [ConnectionId(6)]);
    assert_eq!(f.outputs(), ["gone 7", "ready 7"]);
}

/// Regression: with a connect replacing the dial still out, two
/// nodes whose parents connect again whenever told the peer is gone never
/// settled. Each replaced dial's connection came, and was closed by its
/// dialer; the other end took that for a lost connection, connected again,
/// and replaced its own dial, whose connection then met the same fate. Every
/// handshake succeeded and none was kept. Now both end connected.
///
/// Answered in the order made, the replaced dials did end, so the cycle needs
/// the order that kept it going: a dial its dialer no longer waits for is
/// answered first, the newest first, as a quicker handshake would be.
#[test]
fn two_nodes_retrying_on_gone_end_connected() {
    let (mut a, mut b) = (fixture(), fixture());
    a.act(NodeAction::Connect { peer: PEER });
    b.act(NodeAction::Connect { peer: PEER });
    // B's dial is answered first, while A's is still out; then the
    // connection breaks, and both parents connect again.
    a.opened(1);
    b.connected(1, Some(b.dials()[0]));
    a.closed(1);
    a.act(NodeAction::Connect { peer: PEER });
    b.closed(1);
    b.act(NodeAction::Connect { peer: PEER });
    // Every dial succeeds; a parent told the peer gone connects again.
    let mut out: Vec<(bool, DialId)> = a.dials().into_iter().map(|d| (true, d)).collect();
    out.extend(b.dials().into_iter().skip(1).map(|d| (false, d)));
    let mut next = 10;
    while !out.is_empty() {
        assert!(
            next < 40,
            "no end: {} and {} dials",
            a.dials().len(),
            b.dials().len()
        );
        let waits = |from_a: bool, dial: DialId| {
            let f = if from_a { &a } else { &b };
            f.node.state().p2p.dialing(PEER) == Some(dial)
        };
        let stale = out.iter().rposition(|&(from_a, dial)| !waits(from_a, dial));
        let (from_a, dial) = out.remove(stale.unwrap_or(0));
        let (dialer, other) = if from_a {
            (&mut a, &mut b)
        } else {
            (&mut b, &mut a)
        };
        other.opened(next);
        dialer.connected(next, Some(dial));
        if dialer.closes().contains(&ConnectionId(next)) {
            other.closed(next);
            let made = other.dials().len();
            other.act(NodeAction::Connect { peer: PEER });
            if other.dials().len() > made {
                out.push((!from_a, *other.dials().last().unwrap()));
            }
        }
        next += 1;
    }
    assert!(a.node.state().p2p.is_ready(PEER) && b.node.state().p2p.is_ready(PEER));
    assert_eq!((a.closes(), b.closes()), (vec![], vec![]));
}

/// Regression: Disconnect cancels the dial still out also when a
/// connection the peer opened is held; a connect then dials afresh, and the
/// cancelled dial's connection is closed.
#[test]
fn a_disconnect_holding_a_connection_cancels_the_dial_too() {
    let mut f = fixture();
    f.act(NodeAction::Connect { peer: PEER });
    f.opened(2);
    assert!(
        f.node.state().p2p.dialing(PEER).is_some(),
        "its dial is out"
    );
    f.act(NodeAction::Disconnect { peer: PEER });
    assert_eq!(
        f.node.state().p2p.dialing(PEER),
        None,
        "its dial is cancelled"
    );
    f.act(NodeAction::Connect { peer: PEER });
    let [cancelled, fresh] = f.dials()[..] else {
        panic!("two dials: {:?}", f.dials());
    };
    f.connected(5, Some(cancelled));
    assert_eq!(f.closes(), [ConnectionId(2), ConnectionId(5)]);
    f.connected(6, Some(fresh));
    assert_eq!(f.connections(), [ConnectionId(6)]);
    assert_eq!(f.outputs(), ["ready 7", "gone 7", "ready 7"]);
}
