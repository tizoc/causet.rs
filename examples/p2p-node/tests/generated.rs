//! Generated runs of two to four nodes over a simulated network: fetches
//! between them, links that connect, lose messages, fail sends at once and
//! break (with the dials in flight between the pair, at times), nodes that
//! dial each other at once, disconnect and reconnect (each new connection
//! numbering its wire IDs from 0 again), in half the runs parents that
//! connect again whenever told a peer they want is gone, stale callbacks
//! delivered on purpose, policy changes, and a server that answers at once,
//! late, or never.
//!
//! The network is as a real transport is: each end numbers its own
//! connections, a dial when it starts and an inbound connection when it is
//! accepted, so the two ends of a link know it by different numbers and the
//! order of numbers is not the order of establishment; a dial takes a while
//! to connect; a pair can have several links up, some opened by the network
//! itself; and a peer may send several requests at once.
//!
//! Checked after every root input: each node's invariants (P2P's limits;
//! routes and reply tokens agreeing with what P2P holds), and across the
//! nodes that every answer a node received is the one its peer sent for
//! that very request. From the node's executions: no request's abort is
//! reported after its peer's disconnection; P2P closes a connection only
//! when the node disconnects the peer, after a failed send on it, or when
//! it is a connection arriving that it will not use, never the answer to
//! the dial it waits for, nor below the limit one the peer opened; only
//! that dial fails, and only it or a connection the peer opened makes the
//! peer ready; ready and gone are each reported once in turn (gone again
//! only after a connect); a connect to a peer P2P holds no connection to
//! dials, unless a dial is out, and no dial goes out while another is; a
//! request is reported
//! failed only after the failed send of that request, and answered only by
//! a response on its own connection with its wire ID; and in the network, a
//! node answers only on the link the request came on. Once quiet: every
//! fetch offered has exactly one outcome, nothing is left in flight, both
//! ends hold every link that is up and nothing else, each node was last
//! told of each peer what it is, and told something after each connect
//! that found it holding none (the dial's outcome, or a connection the peer
//! opened), a parent that retries is connected to every peer it wants, and
//! over the whole check at least half the fetches were accepted, a fifth
//! answered, half the bounces with work in flight reached a new connection,
//! and the retrying runs retried, so that refusing everything cannot pass.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use causet::observers::{Execution, InlineCall, MessageView, Observer, Refusal};
use causet::{EventSink, ManualClock, Mount, Reply, Timer, Timestamp, root};
use causet::{ExecutionId, SessionId};
use causet_test::{
    Config, Driven, Group, InFlight, Node as Harness, Rng, Scenario, Swarm, World, check,
};
use p2p::{
    ConnectionId, DialId, Intent, MAX_CONNECTIONS, P2pAction, P2pEffect, P2pOutput, P2pServices,
    PeerId, Sent, Transport, TransportEvent, Wire, WireId,
};
use p2p_node::{
    Node, NodeAction, NodeP2p, NodeServices, NodeState, Outcome, P2P, Policy, ReplyToken, Req,
    Resp, Served, Server,
};

const MS: u64 = 1_000_000;
const START: Timestamp = 1_000 * MS;

static OFFERED: AtomicU64 = AtomicU64::new(0);
static REFUSED: AtomicU64 = AtomicU64::new(0);
static ANSWERED: AtomicU64 = AtomicU64::new(0);
static BOUNCES: AtomicU64 = AtomicU64::new(0);
static REBOUND: AtomicU64 = AtomicU64::new(0);
static RETRIES: AtomicU64 = AtomicU64::new(0);
static RETRYING: AtomicU64 = AtomicU64::new(0);
static OVERTAKEN: AtomicU64 = AtomicU64::new(0);
static KEPT: AtomicU64 = AtomicU64::new(0);
static CALLBACKS: AtomicU64 = AtomicU64::new(0);
static CALLBACKS_REFUSED: AtomicU64 = AtomicU64::new(0);
static INTENTS: AtomicU64 = AtomicU64::new(0);
static INTENTS_REFUSED: AtomicU64 = AtomicU64::new(0);

#[path = "support/checks.rs"]
mod checks;
#[path = "support/network.rs"]
mod network;

use checks::*;
use network::*;

// ---------------------------------------------------------------------------
// Runs

#[derive(Clone, Copy, Debug)]
enum Step {
    Deliver,
    Fetch,
    Connect,
    /// Two nodes dial each other at once.
    Simultaneous,
    /// The network opens another connection between two nodes.
    Extra,
    /// A peer sends six requests at once, and answers nobody asked for.
    Flood,
    Disconnect,
    /// One link of a pair breaks; half the time, the dials between them
    /// in flight fail too.
    Break,
    Stale,
    Policy,
    /// A node connects, and a connection the peer opened overtakes its dial;
    /// then the pair is cut off, so the node is told the peer gone with its
    /// dial still out, and connects again, which keeps the dial. However it
    /// ends (it fails, or its connection comes and is cut, or it failed
    /// before the cut and was not kept), the node must hear it.
    Overtaken,
    /// Every link of a pair whose server is working on an answer breaks;
    /// the requester, once it holds no connection, dials, and asks again as
    /// soon as it holds a new one, which numbers wire IDs from 0 again, as
    /// the old request's.
    Bounce,
}

const STEPS: [(Step, u64); 12] = [
    (Step::Deliver, 4),
    (Step::Fetch, 4),
    (Step::Connect, 2),
    (Step::Simultaneous, 1),
    (Step::Extra, 1),
    (Step::Flood, 1),
    (Step::Disconnect, 1),
    (Step::Break, 1),
    (Step::Stale, 1),
    (Step::Policy, 1),
    (Step::Bounce, 1),
    (Step::Overtaken, 1),
];

type TestNode = Harness<NodeState, NodeServices>;

fn nodes(all: &mut [TestNode]) -> Vec<&mut dyn Driven> {
    all.iter_mut().map(|n| n as &mut dyn Driven).collect()
}

/// The nodes' parents: what each wants to be connected to, and, in a run
/// that retries, connecting again to a peer it wants as soon as it is told
/// the peer is gone.
struct Parents {
    retry: bool,
    wants: BTreeSet<(usize, PeerId)>,
    mounts: Vec<Mount<Node>>,
    causes: Vec<Causes>,
}

impl Parents {
    fn connect(&mut self, all: &mut [TestNode], me: usize, peer: PeerId) {
        self.wants.insert((me, peer));
        all[me].dispatch(&self.mounts[me], NodeAction::Connect { peer });
    }

    fn disconnect(&mut self, all: &mut [TestNode], me: usize, peer: PeerId) {
        self.wants.remove(&(me, peer));
        all[me].dispatch(&self.mounts[me], NodeAction::Disconnect { peer });
    }

    /// The retries owed, then one step of the scenario. False once there is
    /// nothing left to do.
    fn step(&mut self, scenario: &mut Scenario, all: &mut [TestNode], shared: &mut Shared) -> bool {
        let mut retried = false;
        for (me, node) in all.iter_mut().enumerate() {
            let gone = std::mem::take(&mut self.causes[me].0.lock().unwrap().gone);
            for peer in gone {
                if self.retry && self.wants.contains(&(me, peer)) {
                    node.dispatch(&self.mounts[me], NodeAction::Connect { peer });
                    RETRIES.fetch_add(1, Ordering::Relaxed);
                    retried = true;
                }
            }
        }
        scenario.step(&mut nodes(all), shared).is_some() || retried
    }
}

fn generated(rng: &mut Rng) {
    let mut scenario = Scenario::with_rng(rng.clone(), START);
    let mut shared = Shared(Arc::new(Mutex::new(Net {
        world: rng.split(),
        clock: scenario.clock.clone(),
        nodes: BTreeMap::new(),
        links: BTreeMap::new(),
        next_link: 0,
        numbers: BTreeMap::new(),
        by_end: BTreeMap::new(),
        flight: InFlight::new(),
        delivered: Vec::new(),
        requests_in: BTreeSet::new(),
        cut: BTreeSet::new(),
        violations: Vec::new(),
    })));
    let group = Group::new("p2p");
    let ids: Vec<PeerId> = (0..rng.between(2, 4) as u32).map(PeerId).collect();
    let mut orders = Vec::new();
    let mut causes = Vec::new();
    let mut accounts = Vec::new();
    let mut all: Vec<TestNode> = Vec::new();
    let mut mounts = Vec::new();
    for id in &ids {
        let (timer, control) = Timer::simulated(&scenario.clock);
        let port = || Port {
            me: *id,
            net: shared.clone(),
        };
        let services = NodeServices {
            p2p: P2pServices {
                transport: Box::new(port()),
                timer,
            },
            server: Box::new(port()),
        };
        let mut node = Harness::new::<Node>(
            &name(*id),
            NodeState::new(*id),
            services,
            SessionId(1),
            &scenario.clock,
        )
        .with_timer(control)
        .check("node", NodeState::invariants)
        .publish(&group, view);
        let order = Order::default();
        node.store_mut().add_observer(order.clone());
        orders.push(order);
        let cause = Causes::default();
        node.store_mut().add_observer(cause.clone());
        causes.push(cause);
        let account = Accounts::default();
        node.store_mut().add_observer(account.clone());
        accounts.push(account);
        let p2p = node.mount(&root::<Node>().child(P2P));
        shared.net().nodes.insert(*id, (node.sink().clone(), p2p));
        mounts.push(node.mount(&root::<Node>()));
        all.push(node);
    }
    group_checks(&group, ids.clone());
    let mut parents = Parents {
        retry: rng.chance(1, 2),
        wants: BTreeSet::new(),
        mounts: mounts.clone(),
        causes: causes.clone(),
    };
    let other_index = |rng: &mut Rng, me: usize| {
        let peer = rng.below(ids.len() as u64 - 1) as usize;
        if peer >= me { peer + 1 } else { peer }
    };
    let other = |rng: &mut Rng, me: usize| ids[other_index(rng, me)];

    // Every pair connects first.
    for i in 0..ids.len() {
        for b in &ids[i + 1..] {
            parents.connect(&mut all, i, *b);
        }
    }
    for _ in 0..10_000 {
        if !parents.step(&mut scenario, &mut all, &mut shared) {
            break;
        }
    }

    let swarm = Swarm::new(rng, &STEPS);
    while rng.more() {
        let step = swarm.pick(rng);
        let me = rng.below(ids.len() as u64) as usize;
        match step {
            Step::Deliver => {
                for _ in 0..rng.between(1, 8) {
                    parents.step(&mut scenario, &mut all, &mut shared);
                }
            }
            Step::Fetch => {
                // A peer ready to take a request, mostly.
                let p2p = &all[me].state().p2p;
                let free: Vec<PeerId> = ids
                    .iter()
                    .copied()
                    .filter(|peer| p2p.is_ready(*peer) && p2p.outbound_to(*peer).is_none())
                    .collect();
                // With none free, a fetch is mostly not offered; one in four
                // is, and refused.
                if free.is_empty() && !rng.chance(1, 4) {
                    continue;
                }
                let peer = if free.is_empty() || rng.chance(1, 5) {
                    other(rng, me)
                } else {
                    free[rng.below(free.len() as u64) as usize]
                };
                let key = rng.below(20);
                all[me].dispatch(&mounts[me], NodeAction::Fetch { peer, key });
            }
            Step::Connect => {
                let peer = other(rng, me);
                parents.connect(&mut all, me, peer);
            }
            Step::Simultaneous => {
                let them = other_index(rng, me);
                let (a, b) = (ids[me], ids[them]);
                parents.connect(&mut all, me, b);
                parents.connect(&mut all, them, a);
            }
            Step::Extra => {
                let peer = other(rng, me);
                shared.net().open_extra(ids[me], peer);
            }
            Step::Flood => {
                let peer = other(rng, me);
                shared.net().flood(ids[me], peer);
            }
            Step::Disconnect => {
                let peer = other(rng, me);
                parents.disconnect(&mut all, me, peer);
            }
            Step::Break => {
                let peer = other(rng, me);
                let mut net = shared.net();
                let up = net.links_between(ids[me], peer);
                if !up.is_empty() {
                    let link = up[rng.below(up.len() as u64) as usize];
                    net.break_link(link);
                }
                // Half the time the pair is cut off for a moment: the
                // dials between them in flight fail too.
                if rng.chance(1, 2) {
                    net.cut_dials(ids[me], peer);
                }
            }
            Step::Stale => {
                let pick = rng.next_u64();
                let responses = rng.chance(2, 3);
                shared.net().replay_stale(pick, responses);
            }
            Step::Bounce => {
                let working = ids.iter().enumerate().find_map(|(server, _)| {
                    let token = all[server].state().tokens.iter().next()?;
                    let requester = ids.iter().position(|id| *id == token.peer)?;
                    Some((requester, server))
                });
                let (requester, server) = working.unwrap_or((me, other_index(rng, me)));
                let peer = ids[server];
                let held = |all: &[TestNode]| -> BTreeSet<ConnectionId> {
                    all[requester].state().p2p.connections(peer)
                };
                let old = held(&all);
                // Counted only with work in flight, on connections to break.
                let counted = working.is_some() && !old.is_empty();
                if counted {
                    BOUNCES.fetch_add(1, Ordering::Relaxed);
                }
                {
                    let mut net = shared.net();
                    for link in net.links_between(ids[requester], peer) {
                        net.break_link(link);
                    }
                }
                // The requester hears its connections closed, dials, and
                // holds another.
                for _ in 0..50 {
                    if held(&all).is_empty() {
                        break;
                    }
                    parents.step(&mut scenario, &mut all, &mut shared);
                }
                parents.connect(&mut all, requester, peer);
                let new = |all: &[TestNode]| held(all).iter().any(|c| !old.contains(c));
                for _ in 0..50 {
                    if new(&all) {
                        break;
                    }
                    parents.step(&mut scenario, &mut all, &mut shared);
                }
                if !new(&all) {
                    continue;
                }
                if counted {
                    REBOUND.fetch_add(1, Ordering::Relaxed);
                }
                let key = 10 + rng.below(10);
                all[requester].dispatch(&mounts[requester], NodeAction::Fetch { peer, key });
            }
            Step::Overtaken => {
                let them = other_index(rng, me);
                let peer = ids[them];
                if !all[me].state().p2p.connections(peer).is_empty() {
                    continue;
                }
                parents.connect(&mut all, me, peer);
                shared.net().open_extra(peer, ids[me]);
                let holds = |all: &[TestNode]| !all[me].state().p2p.connections(peer).is_empty();
                for _ in 0..50 {
                    if holds(&all) {
                        break;
                    }
                    parents.step(&mut scenario, &mut all, &mut shared);
                }
                {
                    let mut net = shared.net();
                    for link in net.links_between(ids[me], peer) {
                        net.break_link(link);
                    }
                    net.cut_dials(ids[me], peer);
                }
                for _ in 0..50 {
                    if !holds(&all) {
                        break;
                    }
                    parents.step(&mut scenario, &mut all, &mut shared);
                }
                // Every attempt counted; KEPT, those that reached the point:
                // told gone, the dial out.
                OVERTAKEN.fetch_add(1, Ordering::Relaxed);
                if !holds(&all) && all[me].state().p2p.is_dialing(peer) {
                    KEPT.fetch_add(1, Ordering::Relaxed);
                }
                parents.connect(&mut all, me, peer);
            }
            Step::Policy => {
                let policy = Policy {
                    fetch: !rng.chance(1, 5),
                    serve: !rng.chance(1, 5),
                };
                all[me].dispatch(&mounts[me], NodeAction::SetPolicy { policy });
            }
        }
    }
    let quiet = (0..100_000).any(|_| !parents.step(&mut scenario, &mut all, &mut shared));
    assert!(quiet, "still busy after 100,000 steps: no end");

    for (id, order) in ids.iter().zip(&orders) {
        let violations = order.0.lock().unwrap().1.clone();
        assert!(violations.is_empty(), "{id:?}: {violations:?}");
    }
    for (id, cause) in ids.iter().zip(&causes) {
        let violations = &cause.0.lock().unwrap().violations;
        assert!(violations.is_empty(), "{id:?}: {violations:?}");
    }
    for (id, account) in ids.iter().zip(&accounts) {
        let violations = &account.0.lock().unwrap().violations;
        assert!(violations.is_empty(), "{id:?}: {violations:?}");
    }
    let violations = shared.net().violations.clone();
    assert!(violations.is_empty(), "{violations:?}");
    links_agree(&shared.net(), &all, &ids, &causes);
    // A parent that connects again whenever told a peer it wants is gone
    // ends connected to it.
    if parents.retry {
        RETRYING.fetch_add(1, Ordering::Relaxed);
        for (me, peer) in &parents.wants {
            assert!(
                all[*me].state().p2p.is_ready(*peer),
                "{:?} wants {peer:?}, retried on every gone, and is not connected",
                ids[*me]
            );
        }
    }
    for node in &all {
        let state = node.state();
        for (correlation, fetch) in &state.fetches {
            assert!(
                fetch.outcome.is_some(),
                "{correlation:?} has no outcome: {fetch:?}"
            );
            OFFERED.fetch_add(1, Ordering::Relaxed);
            // A flood's unasked answer carries no value: not counted.
            match fetch.outcome {
                Some(Outcome::Answered(Some(_))) => ANSWERED.fetch_add(1, Ordering::Relaxed),
                Some(Outcome::Refused) => REFUSED.fetch_add(1, Ordering::Relaxed),
                _ => 0,
            };
        }
        assert!(state.routes.is_empty(), "routes left: {:?}", state.routes);
        assert!(
            state.tokens.is_empty(),
            "reply tokens left: {:?}",
            state.tokens
        );
        assert_eq!(state.p2p.inbound_count(), 0, "inbound requests left");
        assert_eq!(state.p2p.outbound().count(), 0, "outbound requests left");
    }
}

#[test]
fn generated_nodes_keep_their_requests_straight() {
    check(
        Config {
            runs: 10_000,
            ..Config::default()
        },
        generated,
    );
    // Refusing everything would end every fetch once: generation must have
    // most fetches accepted, and many answered.
    let offered = OFFERED.load(Ordering::Relaxed);
    let refused = REFUSED.load(Ordering::Relaxed);
    let answered = ANSWERED.load(Ordering::Relaxed);
    assert!(
        refused * 2 <= offered,
        "{refused} of {offered} fetches refused"
    );
    assert!(
        answered * 5 >= offered,
        "only {answered} of {offered} answered"
    );
    // Runs whose parents retry must retry: about three times a run.
    let (retrying, retries) = (
        RETRYING.load(Ordering::Relaxed),
        RETRIES.load(Ordering::Relaxed),
    );
    assert!(
        retries >= retrying,
        "only {retries} retries over {retrying} retrying runs"
    );
    // An overtaken dial must often reach its point, a gone with the dial
    // still out: over half of them do, and the floor is a third.
    let (overtaken, kept) = (
        OVERTAKEN.load(Ordering::Relaxed),
        KEPT.load(Ordering::Relaxed),
    );
    assert!(
        kept * 3 >= overtaken,
        "only {kept} of {overtaken} overtaken dials were still out when told gone"
    );
    // Refusals are told: stale callbacks and Intents P2P refuses must come
    // up, or the accounts check nothing.
    let (callbacks, callbacks_refused, intents, intents_refused) = (
        CALLBACKS.load(Ordering::Relaxed),
        CALLBACKS_REFUSED.load(Ordering::Relaxed),
        INTENTS.load(Ordering::Relaxed),
        INTENTS_REFUSED.load(Ordering::Relaxed),
    );
    // About one callback in 26 and one Intent in 3.5 are refused.
    assert!(
        callbacks_refused * 50 >= callbacks,
        "only {callbacks_refused} of {callbacks} callbacks refused"
    );
    assert!(
        intents_refused * 8 >= intents,
        "only {intents_refused} of {intents} Intents refused"
    );
    let bounces = BOUNCES.load(Ordering::Relaxed);
    let rebound = REBOUND.load(Ordering::Relaxed);
    assert!(
        rebound * 2 >= bounces,
        "only {rebound} of {bounces} bounces with work in flight reached a new connection"
    );
}
