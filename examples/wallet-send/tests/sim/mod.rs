//! The light node's world, simulated: the chain, the network between the
//! light node and its full peers, and the light node's Services (the UTXO
//! database over what hydration stored, the prover, the verifier, the two
//! stores). Everything answers after seeded delays, on the scenario's clock.
//!
//! The network keeps each link's messages in order of their links' events,
//! loses some messages and delays others past the request timeout, and
//! drops what was in flight on a link that went down.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use causet::hosting::TimerControl;
use causet::{EventSink, ManualClock, Mount, Reply, RequestId, Timer, Timestamp};
use causet_test::{InFlight, Rng, World};
use wallet_send::domain::{Commitment, Root, RpcId, SendResponse, SubmittedTx, Utxo};
use wallet_send::full_peer::{FullPeer, Incoming, PeerWire};
use wallet_send::ledger::{Ledger, PeerAnswer, ProbeRequest, ProbeResponse, verify_response};
use wallet_send::node::{Collected, NodeServices, Proved, Prover, Responder, UtxoDb};
use wallet_send::partial_ledger::{JobId, LedgerStores, Stored, Verifier, VerifyDone};
use wallet_send::peers::{LinkEvent, PeerId, Peers, Wire, WireAnswer};
use wallet_send::state::{CollectRequest, OwnedUtxosChunk, ProveRequest};

pub const MS: u64 = 1_000_000;

/// A link between the light node and a peer. Its epoch changes whenever it
/// goes down or an end restarts, which drops what was in flight on it.
pub struct Link {
    pub up: bool,
    /// A slow link takes 3 to 7 s each way, around the request timeout.
    pub slow: bool,
    epoch: u64,
}

/// A partial-store write: `(rpc, query, root, proved UTXOs by leaf)`.
type PartialWrite = (RpcId, RequestId, Root, Vec<(u64, Utxo)>);

enum Msg {
    Collected(Reply<Collected>, Collected),
    Proved(Reply<Proved>, Proved),
    Verified(Reply<VerifyDone>, VerifyDone),
    /// A store's acknowledgement, and for the partial store its write,
    /// which becomes visible only then.
    Stored {
        reply: Reply<Stored>,
        event: Stored,
        write: Option<PartialWrite>,
    },
    ToPeer {
        peer: PeerId,
        epoch: u64,
        request_id: RequestId,
        request: ProbeRequest,
    },
    ToLight {
        peer: PeerId,
        epoch: u64,
        reply: Reply<WireAnswer>,
        event: WireAnswer,
    },
    Link(LinkEvent),
}

pub struct Sim {
    pub rng: Rng,
    pub clock: ManualClock,
    /// The UTXO database and the prover sometimes never answer, or answer
    /// past the node's deadline; the verifier sometimes never answers.
    pub lossy: bool,
    /// Every ledger the chain has had, oldest first.
    pub chain: Vec<(Root, Ledger)>,
    /// The proved UTXOs hydration stored, by root: what the UTXO database
    /// reads.
    pub partial: BTreeMap<Root, BTreeMap<u64, Utxo>>,
    /// Partial-store writes in flight, by the send and the hydration they
    /// were made for.
    pub writes: BTreeMap<(RpcId, RequestId), u32>,
    /// Hydrations the test cancelled: their writes may still land, but no
    /// send waits for them.
    pub abandoned: BTreeSet<RequestId>,
    /// Store writes that failed, by send and hydration.
    pub failed: BTreeSet<(RpcId, RequestId)>,
    /// What the world saw go wrong.
    pub violations: Vec<String>,
    pub links: BTreeMap<PeerId, Link>,
    /// The root each peer last announced.
    pub roots: BTreeMap<PeerId, Root>,
    light: Option<(EventSink, Mount<Peers>)>,
    peers: BTreeMap<PeerId, (EventSink, Mount<FullPeer>)>,
    /// The light node's requests in flight, with the Reply that answers them.
    replies: BTreeMap<RequestId, (PeerId, u64, Reply<WireAnswer>)>,
    /// Link events in order on each peer's link.
    flight: InFlight<Msg, PeerId>,
    nonce: u64,
    /// Every answer each RPC got, over every run of the light node.
    pub responses: BTreeMap<RpcId, Vec<SendResponse>>,
    /// The transactions the prover made, by hash.
    pub proved: BTreeMap<u64, SubmittedTx>,
}

impl Sim {
    pub fn fresh_nonce(&mut self) -> u64 {
        self.nonce += 1;
        self.nonce
    }

    pub fn newest(&self) -> &(Root, Ledger) {
        self.chain.last().expect("a genesis ledger")
    }

    pub fn ledger_at(&self, root: Root) -> Option<&Ledger> {
        self.chain
            .iter()
            .rev()
            .find(|(at, _)| *at == root)
            .map(|(_, ledger)| ledger)
    }

    /// A local Service's answer, 1 to 20 ms from now.
    fn answer_later(&mut self, msg: Msg) {
        let at = self.clock.get() + MS + self.rng.below(20) * MS;
        self.flight.send_at(at, msg);
    }

    /// A store's acknowledgement: one in four from a busy disk, 100 to
    /// 300 ms from now.
    fn store_later(&mut self, msg: Msg) {
        let busy = if self.rng.chance(1, 4) {
            100 * MS + self.rng.below(200) * MS
        } else {
            0
        };
        let at = self.clock.get() + MS + self.rng.below(20) * MS + busy;
        self.flight.send_at(at, msg);
    }

    /// A lossy Service's answer: one in ten lost, one in ten past the
    /// node's deadline.
    fn answer_lossy(&mut self, msg: Msg) {
        if self.lossy && self.rng.chance(1, 10) {
            return;
        }
        let late = if self.lossy && self.rng.chance(1, 10) {
            40_000 * MS
        } else {
            0
        };
        let at = self.clock.get() + MS + self.rng.below(20) * MS + late;
        self.flight.send_at(at, msg);
    }

    /// A message on `peer`'s link: one in thirty lost, one in thirty past
    /// the request timeout, and on a slow link every one 3 to 7 s late.
    fn transmit(&mut self, peer: PeerId, msg: Msg) {
        if self.rng.chance(1, 30) {
            return;
        }
        let late = if self.links.get(&peer).is_some_and(|link| link.slow) {
            3_000 * MS + self.rng.below(4_000) * MS
        } else if self.rng.chance(1, 29) {
            6_000 * MS
        } else {
            0
        };
        let at = self.clock.get() + MS + self.rng.below(20) * MS + late;
        self.flight.send_at(at, msg);
    }

    /// A link event, after the link's previous one.
    fn link_event(&mut self, peer: PeerId, event: LinkEvent) {
        let at = self.clock.get() + MS + self.rng.below(20) * MS;
        self.flight.send_in_order(peer, at, Msg::Link(event));
    }

    /// The link goes down, or an end restarted: what was in flight on it is
    /// lost.
    fn cut(&mut self, peer: PeerId) {
        if let Some(link) = self.links.get_mut(&peer) {
            link.epoch += 1;
        }
        self.replies.retain(|_, (to, _, _)| *to != peer);
    }

    fn link_up(&self, peer: PeerId, epoch: u64) -> bool {
        self.links
            .get(&peer)
            .is_some_and(|link| link.up && link.epoch == epoch)
    }
}

/// The world, shared by the scenario and every Service that stands in it.
#[derive(Clone)]
pub struct Shared(pub Arc<Mutex<Sim>>);

impl Shared {
    pub fn new(rng: Rng, clock: ManualClock, lossy: bool, genesis: Ledger) -> Self {
        Self(Arc::new(Mutex::new(Sim {
            rng,
            clock,
            lossy,
            chain: vec![(genesis.root(), genesis)],
            partial: BTreeMap::new(),
            writes: BTreeMap::new(),
            abandoned: BTreeSet::new(),
            failed: BTreeSet::new(),
            violations: Vec::new(),
            links: BTreeMap::new(),
            roots: BTreeMap::new(),
            light: None,
            peers: BTreeMap::new(),
            replies: BTreeMap::new(),
            flight: InFlight::new(),
            nonce: 1_000,
            responses: BTreeMap::new(),
            proved: BTreeMap::new(),
        })))
    }

    /// The world, locked. Never hold it across a call into a node: the
    /// node's Services lock it too.
    pub fn sim(&self) -> MutexGuard<'_, Sim> {
        self.0.lock().unwrap()
    }

    /// The light node's Services, its simulated timer on the scenario's
    /// clock.
    pub fn light_services(&self, clock: &ManualClock) -> (NodeServices, TimerControl) {
        let (timer, control) = Timer::simulated(clock);
        let services = NodeServices {
            db: Box::new(self.clone()),
            prover: Box::new(self.clone()),
            responder: Box::new(self.clone()),
            wire: Box::new(self.clone()),
            verifier: Box::new(self.clone()),
            stores: Box::new(self.clone()),
            timer,
        };
        (services, control)
    }

    /// The light node is (again) at `sink`: the links' traffic of its
    /// previous run is lost, and each link up connects anew.
    pub fn light_at(&self, sink: EventSink, peers: Mount<Peers>) {
        let mut sim = self.sim();
        sim.light = Some((sink, peers));
        let ids: Vec<PeerId> = sim.links.keys().copied().collect();
        for peer in ids {
            sim.cut(peer);
            if sim.links[&peer].up {
                let root = sim.roots[&peer];
                sim.link_event(peer, LinkEvent::Connected { peer, root });
            }
        }
    }

    /// A full peer is (again) at `sink`; a restart cuts its link, which
    /// reconnects.
    pub fn peer_at(&self, peer: PeerId, sink: EventSink, mount: Mount<FullPeer>) {
        let mut sim = self.sim();
        let restarted = sim.peers.insert(peer, (sink, mount)).is_some();
        sim.links.entry(peer).or_insert(Link {
            up: false,
            slow: false,
            epoch: 0,
        });
        if restarted && sim.links[&peer].up {
            sim.cut(peer);
            sim.link_event(peer, LinkEvent::Disconnected { peer });
            let root = sim.roots[&peer];
            sim.link_event(peer, LinkEvent::Connected { peer, root });
        }
    }

    pub fn slow(&self, peer: PeerId) {
        self.sim().links.get_mut(&peer).expect("a link").slow = true;
    }

    pub fn is_up(&self, peer: PeerId) -> bool {
        self.sim().links.get(&peer).is_some_and(|link| link.up)
    }

    pub fn connect(&self, peer: PeerId) {
        let mut sim = self.sim();
        let Some(&root) = sim.roots.get(&peer) else {
            return;
        };
        let link = sim.links.get_mut(&peer).expect("a link");
        if link.up {
            return;
        }
        link.up = true;
        sim.link_event(peer, LinkEvent::Connected { peer, root });
    }

    pub fn disconnect(&self, peer: PeerId) {
        let mut sim = self.sim();
        let link = sim.links.get_mut(&peer).expect("a link");
        if !link.up {
            return;
        }
        link.up = false;
        sim.cut(peer);
        sim.link_event(peer, LinkEvent::Disconnected { peer });
    }
}

impl World for Shared {
    fn next_time(&self) -> Option<Timestamp> {
        self.sim().flight.next_time()
    }

    /// Delivers what is due, as the network and the Services' threads
    /// would. An answer for a node that restarted is refused at its old
    /// address, and lost.
    fn advance(&mut self, now: Timestamp) {
        let mut sim = self.sim();
        for msg in sim.flight.due(now) {
            match msg {
                Msg::Collected(reply, event) => drop(reply.try_send_as_worker(event)),
                Msg::Proved(reply, event) => drop(reply.try_send_as_worker(event)),
                Msg::Verified(reply, event) => drop(reply.try_send_as_worker(event)),
                Msg::Stored {
                    reply,
                    event,
                    write,
                } => {
                    if let Some((rpc_id, query, root, utxos)) = write {
                        let key = (rpc_id, query);
                        let writes = sim.writes.get_mut(&key).expect("a write in flight");
                        *writes -= 1;
                        if *writes == 0 {
                            sim.writes.remove(&key);
                        }
                        if matches!(event, Stored::Partial { ok: true, .. }) {
                            sim.partial.entry(root).or_default().extend(utxos);
                        }
                    }
                    drop(reply.try_send_as_worker(event));
                }
                Msg::ToPeer {
                    peer,
                    epoch,
                    request_id,
                    request,
                } => {
                    if sim.link_up(peer, epoch) {
                        let (sink, mount) = &sim.peers[&peer];
                        let incoming = Incoming {
                            request_id,
                            request,
                        };
                        drop(sink.try_send_to_as_worker(mount, incoming));
                    }
                }
                Msg::ToLight {
                    peer,
                    epoch,
                    reply,
                    event,
                } => {
                    if sim.link_up(peer, epoch) {
                        drop(reply.try_send_as_worker(event));
                    }
                }
                Msg::Link(event) => {
                    let (sink, peers) = sim.light.as_ref().expect("a light node");
                    drop(sink.try_send_to_as_worker(peers, event));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The light node's Services

impl UtxoDb for Shared {
    /// Reads the UTXOs hydration stored at the anchor root. A send whose
    /// hydration is still writing must not collect yet: what it reads
    /// would miss what that hydration proved.
    fn collect(&mut self, request: CollectRequest, reply: Reply<Collected>) {
        let mut sim = self.sim();
        let waited_for = |(rpc_id, query): &(RpcId, RequestId)| {
            *rpc_id == request.rpc_id && !sim.abandoned.contains(query)
        };
        if sim.writes.keys().any(waited_for) {
            let violation = format!(
                "{:?} collected while its hydration's write was in flight",
                request.rpc_id
            );
            sim.violations.push(violation);
        }
        let owned: Vec<(Commitment, Utxo)> = sim
            .partial
            .get(&request.anchor_root)
            .map(|leaves| {
                leaves
                    .values()
                    .filter(|utxo| {
                        utxo.recipient == wallet_send::domain::Recipient::Key(request.owner)
                    })
                    .map(|utxo| (utxo.commitment(), utxo.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let frontier = owned.len() as u64;
        let start = request.next_index.min(frontier) as usize;
        let take =
            (request.max_scan_positions.min(request.max_matches) as usize).min(owned.len() - start);
        let next_index = (start + take) as u64;
        let result = Ok(OwnedUtxosChunk {
            utxos: owned[start..start + take].to_vec(),
            frontier_exclusive: frontier,
            next_index,
            complete: next_index == frontier,
            scanned_positions: take as u32,
        });
        sim.answer_lossy(Msg::Collected(reply, Collected { request, result }));
    }
}

impl Prover for Shared {
    fn prove(&mut self, request: ProveRequest, reply: Reply<Proved>) {
        let mut sim = self.sim();
        let result = if sim.rng.chance(1, 5) {
            Err("the prover failed".to_owned())
        } else {
            // The largest inputs first, until they cover the amount and fee.
            let need = request.req.amount + request.req.fee.unwrap_or(0);
            let (mut inputs, mut total) = (Vec::new(), 0);
            for (commitment, utxo) in &request.available_utxos {
                if total >= need {
                    break;
                }
                inputs.push(*commitment);
                total += utxo.amount;
            }
            let mut outputs = vec![Utxo::base(
                request.req.to,
                request.req.amount,
                sim.fresh_nonce(),
            )];
            if total > need {
                let change = Utxo::base(request.req.from, total - need, sim.fresh_nonce());
                outputs.push(change);
            }
            let tx = SubmittedTx {
                hash: sim.fresh_nonce(),
                inputs,
                outputs,
            };
            sim.proved.insert(tx.hash, tx.clone());
            Ok(tx)
        };
        let proved = Proved {
            request_id: request.request_id,
            rpc_id: request.rpc_id,
            result,
        };
        sim.answer_lossy(Msg::Proved(reply, proved));
    }
}

impl Responder for Shared {
    /// Records the answer. A send rejected because a store failed must have
    /// had a write of its own current hydration fail, not one of a
    /// hydration cancelled since.
    fn respond(&mut self, rpc_id: RpcId, response: SendResponse) {
        let mut sim = self.sim();
        let store_failed = response
            .error
            .as_deref()
            .is_some_and(|error| error.starts_with("hydration: storing"));
        let current_failed = sim
            .failed
            .iter()
            .any(|(rpc, query)| *rpc == rpc_id && !sim.abandoned.contains(query));
        if store_failed && !current_failed {
            let violation = format!("{rpc_id:?} rejected for another hydration's failed write");
            sim.violations.push(violation);
        }
        sim.responses.entry(rpc_id).or_default().push(response);
    }
}

impl Wire for Shared {
    fn send(
        &mut self,
        peer: PeerId,
        request_id: RequestId,
        request: ProbeRequest,
        reply: Reply<WireAnswer>,
    ) {
        let mut sim = self.sim();
        let Some(link) = sim.links.get(&peer).filter(|link| link.up) else {
            return;
        };
        let epoch = link.epoch;
        sim.replies.insert(request_id, (peer, epoch, reply));
        sim.transmit(
            peer,
            Msg::ToPeer {
                peer,
                epoch,
                request_id,
                request,
            },
        );
    }
}

impl Verifier for Shared {
    fn verify(
        &mut self,
        job: JobId,
        peer: PeerId,
        request_id: RequestId,
        request: ProbeRequest,
        response: ProbeResponse,
        reply: Reply<VerifyDone>,
    ) {
        let result = verify_response(&request, &response);
        let done = VerifyDone {
            job,
            peer,
            request_id,
            request,
            result,
        };
        // A lossy verifier loses one answer in ten.
        let mut sim = self.sim();
        if sim.lossy && sim.rng.chance(1, 10) {
            return;
        }
        sim.answer_later(Msg::Verified(reply, done));
    }
}

impl LedgerStores for Shared {
    fn store_cache(
        &mut self,
        rpc_id: RpcId,
        query: RequestId,
        root: Root,
        _: Vec<u64>,
        _: Vec<u64>,
        reply: Reply<Stored>,
    ) {
        let mut sim = self.sim();
        let ok = !sim.rng.chance(1, 8);
        if !ok {
            sim.failed.insert((rpc_id, query));
        }
        let event = Stored::Cache {
            rpc_id,
            query,
            root,
            ok,
        };
        sim.store_later(Msg::Stored {
            reply,
            event,
            write: None,
        });
    }

    fn store_partial(
        &mut self,
        rpc_id: RpcId,
        query: RequestId,
        root: Root,
        utxos: Vec<(u64, Utxo)>,
        reply: Reply<Stored>,
    ) {
        let mut sim = self.sim();
        let ok = !sim.rng.chance(1, 8);
        if !ok {
            sim.failed.insert((rpc_id, query));
        }
        *sim.writes.entry((rpc_id, query)).or_default() += 1;
        let event = Stored::Partial {
            rpc_id,
            query,
            root,
            ok,
        };
        sim.store_later(Msg::Stored {
            reply,
            event,
            write: Some((rpc_id, query, root, utxos)),
        });
    }
}

// ---------------------------------------------------------------------------
// A full peer's Service

impl PeerWire for Shared {
    fn answer(&mut self, peer: PeerId, request_id: RequestId, answer: PeerAnswer) {
        let mut sim = self.sim();
        let Some((to, epoch, reply)) = sim.replies.remove(&request_id) else {
            return;
        };
        if to != peer || !sim.link_up(peer, epoch) {
            return;
        }
        let event = WireAnswer {
            peer,
            request_id,
            answer,
        };
        sim.transmit(
            peer,
            Msg::ToLight {
                peer,
                epoch,
                reply,
                event,
            },
        );
    }

    fn announce(&mut self, peer: PeerId, root: Root) {
        let mut sim = self.sim();
        sim.roots.insert(peer, root);
        if sim.links.get(&peer).is_some_and(|link| link.up) {
            sim.link_event(peer, LinkEvent::Announced { peer, root });
        }
    }
}
