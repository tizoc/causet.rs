//! Generated runs of a light node and its full peers: sends from a few
//! owners, hydrated over a simulated network by peers that lag, disconnect,
//! restart or lie, with new blocks, mempool transactions, a wallet seeded
//! late, and restarts of the light node.
//!
//! Checked after every root input: the light node's invariants (wallet
//! send's, the probe lifecycle's, hydration's, the network layer's), and two
//! checks across the nodes: everything the light node holds as checked at a
//! root is what the chain had there, a leaf held as present is stored, and
//! every probe it verifies is one its peer answered, as asked. Once a run is
//! quiet: every send of the current run was answered exactly once, nothing
//! stays reserved, no two queued transactions spend the same input, no send
//! collected while its hydration's write was in flight, a send rejected for
//! a failed write was rejected for its own hydration's, nothing is left in
//! flight, and every stored UTXO is the chain's.

mod sim;

use std::collections::{BTreeMap, BTreeSet};

use causet::SessionId;
use causet::{Mount, RequestId, Timestamp, root};
use causet_test::{Config, Driven, Group, Node as Harness, Rng, Scenario, Stop, Swarm, check};
use sim::{MS, Shared};
use wallet_send::domain::{BlockHash, Commitment, Owner, Root, RpcId, SendRequest, Utxo};
use wallet_send::full_peer::{Fault, FullPeer, FullPeerAction, FullPeerState, PeerServices};
use wallet_send::ledger::{Ledger, ProbeRequest};
use wallet_send::node::{LEDGER, Node, NodeAction, NodeServices, NodeState, PEERS};
use wallet_send::partial_ledger::{JobPhase, PartialLedgerAction, PartialLedgerSync, Stage};
use wallet_send::peers::PeerId;

const START: Timestamp = 1_000 * MS;
const OWNERS: u32 = 3;
const FAULTS: [Fault; 6] = [
    Fault::AlterItem,
    Fault::DropItem,
    Fault::DuplicateItem,
    Fault::ExtraItem,
    Fault::OtherRoot,
    Fault::Silent,
];

/// What a run does next. Each run uses some of these, picked by weight; the
/// first is the simplest.
#[derive(Clone, Copy, Debug)]
enum Step {
    /// A burst of deliveries.
    Deliver,
    Send,
    /// The heartbeat that continues mempool scans.
    Heartbeat,
    Mempool,
    Block,
    CatchUp,
    Link,
    Seed,
    /// An open send started again from outside.
    Start,
    /// An open send cancelled and started again.
    Cancel,
    RestartLight,
    RestartPeer,
}

const STEPS: [(Step, u64); 12] = [
    (Step::Deliver, 4),
    (Step::Send, 2),
    (Step::Heartbeat, 1),
    (Step::Mempool, 1),
    (Step::Block, 1),
    (Step::CatchUp, 1),
    (Step::Link, 1),
    (Step::Seed, 1),
    (Step::Start, 1),
    (Step::Cancel, 1),
    (Step::RestartLight, 1),
    (Step::RestartPeer, 1),
];

type LightNode = Harness<NodeState, NodeServices>;
type PeerNode = Harness<FullPeerState, PeerServices>;

struct Peer {
    id: PeerId,
    node: PeerNode,
    mount: Mount<FullPeer>,
    /// The chain position it caught up to.
    at: usize,
    session: u64,
}

/// What the light node shows the group: the cache entries checked at a
/// root, and the probes being verified.
struct LightView {
    checked: Vec<(Root, u64, Commitment, bool)>,
    verifying: Vec<(PeerId, RequestId, ProbeRequest)>,
}

fn light_view(state: &NodeState) -> LightView {
    LightView {
        checked: state.ledger.checked().collect(),
        verifying: state
            .ledger
            .jobs
            .values()
            .filter_map(|job| match job.phase {
                JobPhase::VerifyPending { request_id } => {
                    Some((job.peer, request_id, job.request.clone()))
                }
                _ => None,
            })
            .collect(),
    }
}

fn peer_name(peer: PeerId) -> String {
    format!("peer-{}", peer.0)
}

/// The two checks across nodes.
fn group_checks(group: &Group, shared: &Shared) {
    let chain = shared.clone();
    group.check("checked leaves are the chain's", move |views, _| {
        let Some(light) = views.get::<LightView>("light") else {
            return Ok(());
        };
        let sim = chain.sim();
        for (root, index, commitment, present) in &light.checked {
            let Some(ledger) = sim.ledger_at(*root) else {
                return Err(format!("leaf {index} checked at {root:?}, never a root"));
            };
            let there = ledger.leaves.get(index).map(Utxo::commitment) == Some(*commitment);
            if there != *present {
                return Err(format!(
                    "leaf {index} at {root:?}: held as {present}, the chain says {there}"
                ));
            }
        }
        Ok(())
    });
    let stores = shared.clone();
    group.check("leaves held as present are stored", move |views, _| {
        let Some(light) = views.get::<LightView>("light") else {
            return Ok(());
        };
        let sim = stores.sim();
        for (root, index, commitment, present) in &light.checked {
            let stored = sim.partial.get(root).and_then(|leaves| leaves.get(index));
            if *present && stored.map(Utxo::commitment) != Some(*commitment) {
                return Err(format!(
                    "leaf {index} at {root:?} is held as present, not stored"
                ));
            }
        }
        Ok(())
    });
    group.check("probes verified are the ones answered", |views, _| {
        let Some(light) = views.get::<LightView>("light") else {
            return Ok(());
        };
        for (peer, request_id, request) in &light.verifying {
            let answered = views.get::<BTreeMap<RequestId, ProbeRequest>>(&peer_name(*peer));
            match answered.and_then(|answered| answered.get(request_id)) {
                Some(asked) if asked == request => {}
                other => {
                    return Err(format!(
                        "{peer:?} is verified for {request_id:?}, answered as {other:?}"
                    ));
                }
            }
        }
        Ok(())
    });
}

/// Every node, as the scenario drives them.
fn nodes<'a>(light: &'a mut LightNode, peers: &'a mut [Peer]) -> Vec<&'a mut dyn Driven> {
    let mut nodes: Vec<&mut dyn Driven> = vec![light];
    nodes.extend(peers.iter_mut().map(|p| &mut p.node as &mut dyn Driven));
    nodes
}

fn new_utxo(shared: &Shared, rng: &mut Rng) -> Utxo {
    let owner = Owner(1 + rng.below(u64::from(OWNERS)) as u32);
    let nonce = shared.sim().fresh_nonce();
    Utxo::base(owner, 1 + rng.below(40), nonce)
}

/// The wallet learns of `ledger`'s UTXOs, each with chance `numerator` in 4.
fn learn(
    light: &mut LightNode,
    mount: &Mount<PartialLedgerSync>,
    ledger: &Ledger,
    rng: &mut Rng,
    numerator: u64,
) {
    for owner in 1..=OWNERS {
        let entries: Vec<(u64, Commitment, u64)> = ledger
            .owned(Owner(owner))
            .filter(|_| rng.chance(numerator, 4))
            .map(|(index, utxo)| (index, utxo.commitment(), utxo.amount))
            .collect();
        if !entries.is_empty() {
            let owner = Owner(owner);
            light.dispatch(mount, PartialLedgerAction::WalletSeen { owner, entries });
        }
    }
}

fn generated(rng: &mut Rng, lossy: bool) {
    let mut scenario = Scenario::with_rng(rng.clone(), START);
    // The world draws from a stream of its own.
    let mut shared = Shared::new(
        rng.split(),
        scenario.clock.clone(),
        lossy,
        Ledger::default(),
    );
    let mut genesis = Ledger::default();
    for _ in 0..rng.between(2, 4 * u64::from(OWNERS)) {
        let utxo = new_utxo(&shared, rng);
        let index = genesis.free_leaf().unwrap();
        genesis.leaves.insert(index, utxo);
    }
    shared.sim().chain = vec![(genesis.root(), genesis.clone())];
    let group = Group::new("ledger");

    let mut session = 1;
    let (services, timers) = shared.light_services(&scenario.clock);
    let mut light = Harness::new::<Node>(
        "light",
        NodeState::new(BlockHash(0), genesis.root()),
        services,
        SessionId(session),
        &scenario.clock,
    )
    .with_timer(timers)
    .check("light node", NodeState::invariants)
    .publish(&group, light_view);
    let mount = light.mount(&root::<Node>());
    let ledger_mount = light.mount(&root::<Node>().child(LEDGER));
    let peers_mount = light.mount(&root::<Node>().child(PEERS));
    shared.light_at(light.sink().clone(), peers_mount);

    let mut peers = Vec::new();
    for n in 1..=rng.between(1, 3) as u32 {
        let id = PeerId(n);
        let fault = if rng.chance(1, 2) {
            Fault::Honest
        } else {
            *rng.pick(&FAULTS)
        };
        let services = PeerServices {
            wire: Box::new(shared.clone()),
        };
        let mut node = Harness::new::<FullPeer>(
            &peer_name(id),
            FullPeerState::new(id, fault),
            services,
            SessionId(1),
            &scenario.clock,
        )
        .publish(&group, |state: &FullPeerState| state.answered.clone());
        let peer_mount = node.mount(&root::<FullPeer>());
        shared.peer_at(id, node.sink().clone(), peer_mount);
        if rng.chance(1, 4) {
            shared.slow(id);
        }
        let ledger = genesis.clone();
        node.dispatch(&peer_mount, FullPeerAction::CaughtUp { ledger });
        shared.connect(id);
        peers.push(Peer {
            id,
            node,
            mount: peer_mount,
            at: 0,
            session: 1,
        });
    }
    group_checks(&group, &shared);

    learn(&mut light, &ledger_mount, &genesis, rng, 3);
    let mut seeded = rng.chance(2, 3);
    if seeded {
        light.dispatch(&ledger_mount, PartialLedgerAction::Seeded);
    }
    let (mut tip, mut next_rpc, mut this_run) = (0, 1, BTreeSet::new());

    let swarm = Swarm::new(rng, &STEPS);
    while rng.more() {
        match swarm.pick(rng) {
            // A burst of deliveries, so sends finish between other steps
            // and an owner can send again.
            Step::Deliver => {
                for _ in 0..rng.between(1, 8) {
                    scenario.step(&mut nodes(&mut light, &mut peers), &mut shared);
                }
            }
            Step::Send => {
                // Owner 1 sends most, so sends of one owner follow each other.
                let from = Owner(if rng.chance(1, 2) {
                    1
                } else {
                    1 + rng.below(u64::from(OWNERS)) as u32
                });
                let to = Owner(1 + rng.below(u64::from(OWNERS)) as u32);
                let req = SendRequest {
                    from,
                    to,
                    amount: 1 + rng.below(60),
                    fee: Some(rng.below(3)),
                    memo: String::new(),
                };
                let rpc_id = RpcId(next_rpc);
                next_rpc += 1;
                this_run.insert(rpc_id);
                light.dispatch(&mount, NodeAction::Send { rpc_id, req });
            }
            Step::Heartbeat => {
                light.dispatch(&mount, NodeAction::CheckTimeouts);
            }
            Step::Mempool => {
                let outputs = vec![new_utxo(&shared, rng)];
                let inputs = Vec::new();
                light.dispatch(&mount, NodeAction::MempoolTx { inputs, outputs });
            }
            Step::Block => {
                // A new block: a UTXO spent elsewhere, one or two new ones,
                // which the wallet usually learns of.
                let mut ledger = shared.sim().newest().1.clone();
                if rng.chance(1, 3) && !ledger.leaves.is_empty() {
                    let leaves: Vec<u64> = ledger.leaves.keys().copied().collect();
                    ledger.leaves.remove(rng.pick(&leaves));
                }
                let mut added = Ledger::default();
                for _ in 0..rng.between(1, 2) {
                    let utxo = new_utxo(&shared, rng);
                    if let Some(index) = ledger.free_leaf() {
                        ledger.leaves.insert(index, utxo.clone());
                        added.leaves.insert(index, utxo);
                    }
                }
                let root = ledger.root();
                shared.sim().chain.push((root, ledger.clone()));
                tip += 1;
                let tip = BlockHash(tip);
                light.dispatch(&mount, NodeAction::NewTip { tip, root });
                learn(&mut light, &ledger_mount, &added, rng, 3);
                // Most peers follow the chain closely.
                let newest = shared.sim().chain.len() - 1;
                for peer in &mut peers {
                    if rng.chance(2, 3) {
                        peer.at = newest;
                        let ledger = ledger.clone();
                        peer.node
                            .dispatch(&peer.mount, FullPeerAction::CaughtUp { ledger });
                    }
                }
            }
            Step::CatchUp => {
                // A peer catches up: one block, or to the tip.
                let newest = shared.sim().chain.len() - 1;
                let index = rng.below(peers.len() as u64) as usize;
                let peer = &mut peers[index];
                if peer.at < newest {
                    peer.at = if rng.chance(1, 2) {
                        newest
                    } else {
                        peer.at + 1
                    };
                    let ledger = shared.sim().chain[peer.at].1.clone();
                    peer.node
                        .dispatch(&peer.mount, FullPeerAction::CaughtUp { ledger });
                }
            }
            Step::Link => {
                let id = peers[rng.below(peers.len() as u64) as usize].id;
                if shared.is_up(id) {
                    shared.disconnect(id);
                } else {
                    shared.connect(id);
                }
            }
            Step::Seed if !seeded => {
                let ledger = shared.sim().newest().1.clone();
                learn(&mut light, &ledger_mount, &ledger, rng, 4);
                light.dispatch(&ledger_mount, PartialLedgerAction::Seeded);
                seeded = true;
            }
            // An open send started again from outside, possibly while its
            // hydration probes or stores, which the node refuses.
            Step::Start => {
                let open: Vec<RpcId> = light.state().rpc.keys().copied().collect();
                if !open.is_empty() {
                    let rpc_id = open[rng.below(open.len() as u64) as usize];
                    light.dispatch(&mount, NodeAction::Start { rpc_id });
                }
            }
            // An open send cancelled and started again: a replacement
            // attempt, whose hydration may meet what the cancelled one left
            // in flight. No send waits for those writes any more. Usually
            // the run first goes on until some send's hydration is storing,
            // a short window a random step would rarely hit.
            Step::Cancel => {
                let storing = |light: &LightNode| {
                    let state = light.state();
                    state.rpc.keys().copied().find(|rpc_id| {
                        let h = state.ledger.hydrations.get(rpc_id);
                        h.is_some_and(|h| matches!(h.stage, Stage::Storing { .. }))
                    })
                };
                let mut target = None;
                if rng.chance(2, 3) {
                    for _ in 0..200 {
                        target = storing(&light);
                        if target.is_some() {
                            break;
                        }
                        let stepped =
                            scenario.step(&mut nodes(&mut light, &mut peers), &mut shared);
                        if stepped.is_none() {
                            break;
                        }
                    }
                }
                let open: Vec<RpcId> = light.state().rpc.keys().copied().collect();
                let target = target.or_else(|| {
                    (!open.is_empty()).then(|| open[rng.below(open.len() as u64) as usize])
                });
                if let Some(rpc_id) = target {
                    if let Some(query) = light.state().ledger.hydration_query(rpc_id) {
                        shared.sim().abandoned.insert(query);
                    }
                    light.dispatch(&mount, NodeAction::Cancel { rpc_id });
                    light.dispatch(&mount, NodeAction::Start { rpc_id });
                }
            }
            Step::Seed => {}
            Step::RestartLight => {
                // The light node restarts: it keeps the chain, the mempool
                // and the wallet cache; the RPCs, the work in progress, the
                // connections and the probes are gone.
                session += 1;
                let mut kept = light.state().clone();
                kept.rpc.clear();
                kept.wallet_send = Default::default();
                kept.deadlines = Default::default();
                kept.peers = Default::default();
                kept.ledger.jobs.clear();
                kept.ledger.hydrations.clear();
                kept.ledger.attempted.clear();
                let (services, timers) = shared.light_services(&scenario.clock);
                light = light
                    .restart(kept, services, SessionId(session))
                    .with_timer(timers);
                let peers_mount = light.mount(&root::<Node>().child(PEERS));
                shared.light_at(light.sink().clone(), peers_mount);
                this_run.clear();
            }
            Step::RestartPeer => {
                // A full peer restarts, keeping its ledgers.
                let index = rng.below(peers.len() as u64) as usize;
                let mut peer = peers.remove(index);
                peer.session += 1;
                let kept = peer.node.state().clone();
                let services = PeerServices {
                    wire: Box::new(shared.clone()),
                };
                peer.node = peer.node.restart(kept, services, SessionId(peer.session));
                peer.mount = peer.node.mount(&root::<FullPeer>());
                shared.peer_at(peer.id, peer.node.sink().clone(), peer.mount);
                peers.insert(index, peer);
            }
        }
    }

    // The world calms down: every peer caught up and connected, the wallet
    // seeded. Then the run goes quiet, the heartbeat given whenever a
    // mempool scan waits for it.
    let newest = shared.sim().chain.len() - 1;
    for peer in &mut peers {
        if peer.at < newest {
            peer.at = newest;
            let ledger = shared.sim().chain[newest].1.clone();
            peer.node
                .dispatch(&peer.mount, FullPeerAction::CaughtUp { ledger });
        }
        shared.connect(peer.id);
    }
    if !seeded {
        light.dispatch(&ledger_mount, PartialLedgerAction::Seeded);
    }
    let mut settled = false;
    for _ in 0..1_000 {
        {
            let mut nodes = nodes(&mut light, &mut peers);
            let stop = scenario.run_until(&mut nodes, &mut shared, 100_000, None, |_| false);
            assert_eq!(stop, Stop::Quiet, "{}", scenario.pending(&nodes, &shared));
        }
        if light.state().wallet_send.next_mempool_scan_request() == Ok(None) {
            settled = true;
            break;
        }
        light.dispatch(&mount, NodeAction::CheckTimeouts);
    }
    assert!(settled, "mempool scans never finished");

    let sim = shared.sim();
    assert!(sim.violations.is_empty(), "{:?}", sim.violations);
    for (rpc_id, responses) in &sim.responses {
        assert!(
            responses.len() == 1,
            "{rpc_id:?} was answered {} times: {responses:?}",
            responses.len()
        );
    }
    for rpc_id in &this_run {
        assert!(
            sim.responses.contains_key(rpc_id),
            "{rpc_id:?} was never answered"
        );
    }
    let state = light.state();
    assert!(state.rpc.is_empty(), "RPCs left pending: {:?}", state.rpc);
    assert_eq!(
        state.wallet_send.working().count(),
        0,
        "work left in progress"
    );
    for owner in 1..=OWNERS {
        assert!(
            state
                .wallet_send
                .owner_is_available(RpcId(u64::MAX), Owner(owner)),
            "{owner} left reserved"
        );
    }
    assert!(
        state.ledger.jobs.is_empty(),
        "probes left: {:?}",
        state.ledger.jobs
    );
    assert!(
        state.ledger.hydrations.is_empty(),
        "hydrations left: {:?}",
        state.ledger.hydrations
    );
    assert!(state.peers.outstanding.is_empty(), "requests left out");
    // No two queued transactions spend the same input.
    let mut spent = BTreeSet::new();
    for responses in sim.responses.values() {
        for tx_id in responses.iter().filter_map(|r| r.tx_id) {
            for input in &sim.proved[&tx_id].inputs {
                assert!(spent.insert(*input), "{input:?} is spent twice");
            }
        }
    }
    // Every UTXO stored as proved is the chain's at its root.
    for (root, leaves) in &sim.partial {
        let ledger = sim.ledger_at(*root).expect("a root of the chain");
        for (index, utxo) in leaves {
            let at = ledger.leaves.get(index);
            assert_eq!(at, Some(utxo), "stored leaf {index} at {root:?}");
        }
    }
}

#[test]
fn generated_light_nodes_keep_their_invariants_and_answer_once() {
    check(
        Config {
            runs: 5_000,
            ..Config::default()
        },
        |rng| generated(rng, false),
    );
}

/// Services that lose answers, or send them after the node's deadline: a
/// send then times out instead of waiting forever, is answered once, and a
/// late answer changes nothing. (Before the deadlines, generation found the
/// send waiting forever and shrank it to one send whose one answer was lost.)
#[test]
fn lost_and_late_answers_end_in_a_timeout() {
    check(
        Config {
            runs: 5_000,
            ..Config::default()
        },
        |rng| generated(rng, true),
    );
}
