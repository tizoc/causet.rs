//! A wallet-send node under test, its Services recording what they are asked.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use causet::SessionId;
use causet::observers::{Execution, MessageView, Observer};
use causet::{ManualClock, Mount, Reply, RequestId, Timer, root};
use causet_test::Node as Harness;
use wallet_send::domain::{
    BlockHash, Commitment, Owner, Root, RpcId, SendRequest, SendResponse, Utxo,
};
use wallet_send::ledger::{Ledger, ProbeRequest, ProbeResponse};
use wallet_send::node::{
    Collected, LEDGER, Node, NodeAction, NodeServices, NodeState, PEERS, Proved, Prover, Responder,
    RpcRequest, RpcStatus, UtxoDb,
};
use wallet_send::partial_ledger::{
    JobId, LedgerStores, PartialLedgerAction, PartialLedgerSync, Stored, Verifier, VerifyDone,
};
use wallet_send::peers::{PeerId, Peers, PeersAction, Wire, WireAnswer};
use wallet_send::state::{CollectRequest, OwnedUtxosChunk, ProveRequest};

/// What the Services were asked, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Asked {
    Collect(CollectRequest),
    Prove(ProveRequest),
}

/// A recorded request with the route its answer goes back on.
pub struct Pending<Req, Ev> {
    pub request: Req,
    pub reply: Reply<Ev>,
}

/// A wallet-cache write: `(rpc, query, root, found, missing)`.
pub type CacheWrite = (RpcId, RequestId, Root, Vec<u64>, Vec<u64>);
/// A partial-store write: `(rpc, query, root, proved UTXOs by leaf)`.
pub type PartialWrite = (RpcId, RequestId, Root, Vec<(u64, Utxo)>);

#[derive(Clone, Debug)]
pub struct VerifyAsk {
    pub job: JobId,
    pub peer: PeerId,
    pub request_id: RequestId,
    pub request: ProbeRequest,
    pub response: ProbeResponse,
}

#[derive(Default)]
pub struct Record {
    pub asked: Vec<Asked>,
    pub collects: Vec<Pending<CollectRequest, Collected>>,
    pub proves: Vec<Pending<ProveRequest, Proved>>,
    /// Probes sent to peers: `(peer, request ID, request)`.
    pub sends: Vec<Pending<(PeerId, RequestId, ProbeRequest), WireAnswer>>,
    pub verifies: Vec<Pending<VerifyAsk, VerifyDone>>,
    pub cache_stores: Vec<Pending<CacheWrite, Stored>>,
    pub partial_stores: Vec<Pending<PartialWrite, Stored>>,
    pub responses: Vec<(RpcId, SendResponse)>,
    /// Which RPC each mempool scan was for.
    pub scans: Vec<RpcId>,
}

#[derive(Clone, Default)]
pub struct Recorder(pub Arc<Mutex<Record>>);

impl Recorder {
    pub fn get(&self) -> std::sync::MutexGuard<'_, Record> {
        self.0.lock().unwrap()
    }

    pub fn services(&self, timer: Timer) -> NodeServices {
        NodeServices {
            db: Box::new(self.clone()),
            prover: Box::new(self.clone()),
            responder: Box::new(self.clone()),
            wire: Box::new(self.clone()),
            verifier: Box::new(self.clone()),
            stores: Box::new(self.clone()),
            timer,
        }
    }
}

impl UtxoDb for Recorder {
    fn collect(&mut self, request: CollectRequest, reply: Reply<Collected>) {
        let mut record = self.get();
        record.asked.push(Asked::Collect(request.clone()));
        record.collects.push(Pending { request, reply });
    }
}

impl Prover for Recorder {
    fn prove(&mut self, request: ProveRequest, reply: Reply<Proved>) {
        let mut record = self.get();
        record.asked.push(Asked::Prove(request.clone()));
        record.proves.push(Pending { request, reply });
    }
}

impl Responder for Recorder {
    fn respond(&mut self, rpc_id: RpcId, response: SendResponse) {
        self.get().responses.push((rpc_id, response));
    }
}

impl Wire for Recorder {
    fn send(
        &mut self,
        peer: PeerId,
        request_id: RequestId,
        request: ProbeRequest,
        reply: Reply<WireAnswer>,
    ) {
        self.get().sends.push(Pending {
            request: (peer, request_id, request),
            reply,
        });
    }
}

impl Verifier for Recorder {
    fn verify(
        &mut self,
        job: JobId,
        peer: PeerId,
        request_id: RequestId,
        request: ProbeRequest,
        response: ProbeResponse,
        reply: Reply<VerifyDone>,
    ) {
        let request = VerifyAsk {
            job,
            peer,
            request_id,
            request,
            response,
        };
        self.get().verifies.push(Pending { request, reply });
    }
}

impl LedgerStores for Recorder {
    fn store_cache(
        &mut self,
        rpc_id: RpcId,
        query: RequestId,
        root: Root,
        found: Vec<u64>,
        missing: Vec<u64>,
        reply: Reply<Stored>,
    ) {
        self.get().cache_stores.push(Pending {
            request: (rpc_id, query, root, found, missing),
            reply,
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
        self.get().partial_stores.push(Pending {
            request: (rpc_id, query, root, utxos),
            reply,
        });
    }
}

/// Records the RPC of every mempool scan, read from the Action itself.
struct Scans(Recorder);

impl Observer for Scans {
    fn action_start(&mut self, _: &Execution<'_>, payload: &dyn MessageView) {
        if let Some(NodeAction::ScanMempool { request }) = payload.downcast_ref::<NodeAction>() {
            self.0.get().scans.push(request.rpc_id);
        }
    }
}

pub type WalletNode = Harness<NodeState, NodeServices>;

pub struct Fixture {
    pub node: WalletNode,
    pub mount: Mount<Node>,
    pub ledger: Mount<PartialLedgerSync>,
    pub peers: Mount<Peers>,
    pub rec: Recorder,
    /// The node's clock, which its simulated timer runs on.
    pub clock: ManualClock,
}

pub const TIP: BlockHash = BlockHash(1);
pub const ROOT: Root = Root(100);

pub fn rpc_id() -> RpcId {
    RpcId(1)
}

pub fn second_rpc_id() -> RpcId {
    RpcId(2)
}

pub fn owner() -> Owner {
    Owner(1)
}

pub fn second_owner() -> Owner {
    Owner(2)
}

pub fn request_for(owner: Owner) -> SendRequest {
    SendRequest {
        from: owner,
        to: owner,
        amount: 10,
        fee: Some(0),
        memo: String::new(),
    }
}

pub fn request() -> SendRequest {
    request_for(owner())
}

/// A node with `rpc_id()` pending.
pub fn state_with_pending_request() -> NodeState {
    let mut state = NodeState::new(TIP, ROOT);
    state.rpc.insert(
        rpc_id(),
        RpcRequest {
            req: request(),
            status: RpcStatus::Pending,
        },
    );
    state
}

pub fn fixture(state: NodeState) -> Fixture {
    let rec = Recorder::default();
    let clock = ManualClock::new(1_000_000_000);
    let (timer, control) = Timer::simulated(&clock);
    let mut node = Harness::new::<Node>("node", state, rec.services(timer), SessionId(1), &clock)
        .with_timer(control)
        .check("wallet send", NodeState::invariants);
    node.store_mut().add_observer(Scans(rec.clone()));
    let mount = node.mount(&root::<Node>());
    let ledger = node.mount(&root::<Node>().child(LEDGER));
    let peers = node.mount(&root::<Node>().child(PEERS));
    Fixture {
        node,
        mount,
        ledger,
        peers,
        rec,
        clock,
    }
}

pub fn wallet_utxo_for(owner: Owner, amount: u64, nonce: u64) -> (Commitment, Utxo) {
    let utxo = Utxo::base(owner, amount, nonce);
    (utxo.commitment(), utxo)
}

pub fn wallet_utxo(amount: u64, nonce: u64) -> (Commitment, Utxo) {
    wallet_utxo_for(owner(), amount, nonce)
}

/// The Collect request asked at position `index` of `asked`.
pub fn collect_request(asked: &[Asked], index: usize) -> CollectRequest {
    match asked.get(index) {
        Some(Asked::Collect(request)) => request.clone(),
        other => panic!("expected a collect at {index}, got {other:?}"),
    }
}

/// A persisted page answering `request`.
pub fn page(
    request: &CollectRequest,
    utxos: Vec<(Commitment, Utxo)>,
    frontier_exclusive: u64,
    next_index: u64,
    complete: bool,
) -> Result<OwnedUtxosChunk, String> {
    Ok(OwnedUtxosChunk {
        scanned_positions: next_index.saturating_sub(request.next_index) as u32,
        utxos,
        frontier_exclusive,
        next_index,
        complete,
    })
}

/// A ledger at `ROOT`'s place: the owner's UTXOs of `amounts`, at leaves 0,
/// 1, ...
pub fn ledger_of(amounts: &[u64]) -> Ledger {
    let mut ledger = Ledger::default();
    for (index, amount) in amounts.iter().enumerate() {
        ledger
            .leaves
            .insert(index as u64, Utxo::base(owner(), *amount, index as u64));
    }
    ledger
}

impl Fixture {
    /// The chain moves to `ledger`'s root.
    pub fn tip(&mut self, ledger: &Ledger) {
        let root = ledger.root();
        self.node.dispatch(
            &self.mount,
            NodeAction::NewTip {
                tip: BlockHash(root.0),
                root,
            },
        );
    }

    /// The wallet learns of every UTXO of `ledger`, and is seeded.
    pub fn learn(&mut self, ledger: &Ledger) {
        let entries = ledger
            .owned(owner())
            .map(|(index, utxo)| (index, utxo.commitment(), utxo.amount))
            .collect();
        self.node.dispatch(
            &self.ledger,
            PartialLedgerAction::WalletSeen {
                owner: owner(),
                entries,
            },
        );
        self.node
            .dispatch(&self.ledger, PartialLedgerAction::Seeded);
    }

    /// `peer` connects, serving `root`.
    pub fn connect(&mut self, peer: u32, root: Root) {
        let peer = PeerId(peer);
        self.node
            .dispatch(&self.peers, PeersAction::Connected { peer, root });
    }

    /// Starts the send and lets its collection find nothing stored, so it
    /// asks for hydration.
    pub fn reach_hydration(&mut self) {
        self.node
            .dispatch(&self.mount, NodeAction::Start { rpc_id: rpc_id() });
        let request = self.rec.get().collects.last().unwrap().request.clone();
        let result = page(&request, Vec::new(), 0, 0, true);
        self.node
            .dispatch(&self.mount, NodeAction::PersistedPage { request, result });
    }

    /// Delivers what the node holds, until nothing is.
    pub fn deliver_all(&mut self) {
        while self.node.deliver_next().is_some() {}
    }
}
