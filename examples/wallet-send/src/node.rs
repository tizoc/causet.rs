//! The light node around wallet send: the root component, which holds the
//! rest of the node (the RPC requests, the chain's UTXO root, the mempool),
//! reads it to give its children their context (`plan_start`,
//! `collection_guard`, a hydration's peers and mempool spends), owns the
//! node's own I/O, and lowers its children's Outputs into its own Actions.
//! Its children are wallet send, partial-ledger sync (which hydrates a send
//! that lacks data) and the network layer, `Peers`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use causet::*;
use serde::{Deserialize, Serialize};

use crate::domain::{
    BlockHash, Commitment, Root, RpcId, SendRequest, SendResponse, SubmittedTx, Utxo,
};
use crate::mempool::Mempool;
use crate::partial_ledger::{
    JobId, JobPhase, LedgerStores, PartialLedgerAction, PartialLedgerOutput, PartialLedgerState,
    PartialLedgerSync, Stage, Verifier,
};
use crate::peers::{Correlation, Peers, PeersAction, PeersOutput, PeersState, Wire};
use crate::state::{
    CollectRequest, MempoolRequest, OwnedUtxosChunk, ProveRequest, WalletSendState,
    validate_persisted_chunk,
};
use crate::wallet_send::{
    CollectionGuard, StartPlan, WalletSend, WalletSendAction, WalletSendOutput,
};

pub const WALLET_SEND: Child<Node, WalletSend> = Child::new("wallet_send");
pub const LEDGER: Child<Node, PartialLedgerSync> = Child::new("ledger");
pub const PEERS: Child<Node, Peers> = Child::new("peers");

/// An RPC's progress, as far as wallet send is concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcStatus {
    Pending,
    /// Waiting for wallet-spend hydration.
    Syncing,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcRequest {
    pub req: SendRequest,
    pub status: RpcStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeState {
    /// Pending wallet-send RPCs; answered ones are removed.
    pub rpc: BTreeMap<RpcId, RpcRequest>,
    /// The best tip's UTXO root.
    pub chain_root: Root,
    pub mempool: Mempool,
    pub wallet_send: WalletSendState,
    /// Partial-ledger sync: probes, the wallet cache, hydrations.
    pub ledger: PartialLedgerState,
    pub peers: PeersState,
    /// A deadline for every answer the node waits for, by the deadline's
    /// own ID: armed with each request it sends out, cleared by that answer.
    /// A send can wait for two at once (a hydration query still out while a
    /// restarted collection reads pages), so each has its own.
    pub deadlines: Pending<Awaited>,
}

/// A map whose keys are not strings, saved as a list of pairs: JSON and
/// formats like it allow only string keys.
pub(crate) mod pairs {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<K: Serialize, V: Serialize, S: Serializer>(
        map: &BTreeMap<K, V>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(map.iter())
    }

    pub fn deserialize<'de, K, V, D>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        Ok(Vec::<(K, V)>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}

/// An answer a send waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Awaited {
    pub rpc_id: RpcId,
    pub answer: Answer,
}

/// Which answer, by the request it answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Answer {
    /// The next persisted page of a collection attempt.
    Page(RequestId),
    /// The proof of an attempt.
    Proof(RequestId),
    /// A hydration, by its query.
    Hydration(RequestId),
}

/// How long a send waits for any one answer (a page, a proof, a hydration)
/// before it gives up.
pub const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

impl NodeState {
    pub fn new(tip: BlockHash, root: Root) -> Self {
        Self {
            rpc: BTreeMap::new(),
            chain_root: root,
            mempool: Mempool::new(tip, root),
            wallet_send: WalletSendState::default(),
            ledger: PartialLedgerState::default(),
            peers: PeersState::default(),
            deadlines: Pending::new(),
        }
    }

    /// Whether the send's hydration is probing or storing.
    pub fn hydrating(&self, rpc_id: RpcId) -> bool {
        self.ledger
            .hydrations
            .get(&rpc_id)
            .is_some_and(|h| !matches!(h.stage, Stage::Waiting { .. }))
    }

    /// Whether the RPC is still waiting for its answer.
    pub fn rpc_pending(&self, rpc_id: RpcId) -> bool {
        self.rpc.contains_key(&rpc_id)
    }

    /// The children's invariants; that no send works while its hydration
    /// probes or stores; and that every probe out is a request the network
    /// has outstanding.
    pub fn invariants(&self) -> Result<(), String> {
        self.wallet_send.invariants()?;
        self.ledger.invariants()?;
        self.peers.invariants()?;
        if let Some(rpc_id) = self.wallet_send.working().find(|r| self.hydrating(*r)) {
            return Err(format!("{rpc_id:?} works while its hydration is out"));
        }
        for (job, probe) in &self.ledger.jobs {
            if let JobPhase::Pending { request_id } = probe.phase {
                let outstanding = self.peers.outstanding.get(&request_id);
                if outstanding.map(|o| (o.peer, o.correlation))
                    != Some((probe.peer, Correlation(job.0)))
                {
                    return Err(format!(
                        "{job:?} waits on {request_id:?}, which the network does not have out"
                    ));
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The node's Services: the UTXO database, the prover, the RPC transport,
// and partial-ledger sync's network, verifier and stores. Each answers
// through the Event it is given a Reply for.

#[derive(Debug, Event)]
#[causet(type = "node.collected")]
pub struct Collected {
    pub request: CollectRequest,
    pub result: Result<OwnedUtxosChunk, String>,
}

#[derive(Debug, Event)]
#[causet(type = "node.proved")]
pub struct Proved {
    pub request_id: RequestId,
    pub rpc_id: RpcId,
    pub result: Result<SubmittedTx, String>,
}

pub trait UtxoDb: Send {
    fn collect(&mut self, request: CollectRequest, reply: Reply<Collected>);
}

pub trait Prover: Send {
    fn prove(&mut self, request: ProveRequest, reply: Reply<Proved>);
}

pub trait Responder: Send {
    fn respond(&mut self, rpc_id: RpcId, response: SendResponse);
}

/// The Services of every component of the node.
pub struct NodeServices {
    /// Reads the proved UTXOs partial-ledger sync stored.
    pub db: Box<dyn UtxoDb>,
    pub prover: Box<dyn Prover>,
    pub responder: Box<dyn Responder>,
    pub wire: Box<dyn Wire>,
    pub verifier: Box<dyn Verifier>,
    pub stores: Box<dyn LedgerStores>,
    /// The answer deadlines, the network's timeouts, hydration's retries.
    pub timer: Timer,
}

// ---------------------------------------------------------------------------
// The root

#[derive(Debug, Action)]
#[causet(type = "node.action")]
pub enum NodeAction {
    /// A wallet-send RPC arrives, and wallet send starts on it.
    Send {
        rpc_id: RpcId,
        req: SendRequest,
    },
    Start {
        rpc_id: RpcId,
    },
    Cancel {
        rpc_id: RpcId,
    },
    /// The heartbeat: at most one mempool page continues, round robin.
    CheckTimeouts,
    /// A deadline fired: if its send still waits on that answer, the send
    /// gives up.
    Expired {
        deadline: RequestId,
    },
    ScanMempool {
        request: MempoolRequest,
    },
    PersistedPage {
        request: CollectRequest,
        result: Result<OwnedUtxosChunk, String>,
    },
    Proved {
        request_id: RequestId,
        rpc_id: RpcId,
        result: Result<SubmittedTx, String>,
    },
    /// Lowered from wallet send.
    Collect(CollectRequest),
    Prove(ProveRequest),
    HydrationQuery {
        rpc_id: RpcId,
        anchor_root: Root,
        selection_error: String,
    },
    HydrationForget {
        rpc_id: RpcId,
    },
    /// Lowered from partial-ledger sync and the network.
    Ledger(PartialLedgerOutput),
    Peers(PeersOutput),
    Submit(SubmittedTx),
    Respond {
        rpc_id: RpcId,
        response: SendResponse,
    },
    /// The world: a transaction enters the mempool, a new tip, the mempool
    /// alone moving to a new tip, an RPC whose client went away unanswered.
    MempoolTx {
        inputs: Vec<Commitment>,
        outputs: Vec<Utxo>,
    },
    NewTip {
        tip: BlockHash,
        root: Root,
    },
    MempoolTip {
        tip: BlockHash,
    },
    /// Test only: drops an RPC without answering it, which the node itself
    /// never does.
    RpcGone {
        rpc_id: RpcId,
    },
}

#[derive(Debug, Effect)]
#[causet(type = "node.effect")]
pub enum NodeEffect {
    Collect(CollectRequest),
    Prove(ProveRequest),
    Respond {
        rpc_id: RpcId,
        response: SendResponse,
    },
    Arm {
        deadline: RequestId,
    },
}

pub struct Node;

impl Component for Node {
    type State = NodeState;
    type Service = NodeServices;
    type Action = NodeAction;
    type Effect = NodeEffect;
    type Output = Never;

    /// The enabling checks that read the RPC and wallet send.
    fn is_enabled(state: &NodeState, action: &NodeAction, _: Timestamp) -> bool {
        let wallet_send = &state.wallet_send;
        match action {
            NodeAction::Send { rpc_id, .. } => !state.rpc.contains_key(rpc_id),
            // Not while the send's probe is out or its results are being
            // stored: a collection started then would outlive the
            // hydration, whose own retry would then be refused.
            NodeAction::Start { rpc_id } => {
                state.rpc_pending(*rpc_id)
                    && wallet_send.can_start(*rpc_id)
                    && !state.hydrating(*rpc_id)
            }
            NodeAction::Cancel { rpc_id } => wallet_send.has_work(*rpc_id),
            NodeAction::ScanMempool { request } => wallet_send.is_collecting_mempool(request),
            NodeAction::PersistedPage { request, .. } => {
                wallet_send.is_collecting_persisted(request)
            }
            NodeAction::Proved {
                request_id, rpc_id, ..
            } => wallet_send.is_proving(*rpc_id, *request_id),
            _ => true,
        }
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: NodeAction, meta: &ActionMeta) {
        match action {
            NodeAction::Send { rpc_id, req } => {
                ctx.state_mut().rpc.insert(
                    rpc_id,
                    RpcRequest {
                        req,
                        status: RpcStatus::Pending,
                    },
                );
                ctx.into_emitter().emit_action(NodeAction::Start { rpc_id });
            }
            NodeAction::Start { rpc_id } => {
                let plan = plan_start(ctx.state(), rpc_id, meta.time);
                ctx.reduce_child(WALLET_SEND, WalletSendAction::Start { rpc_id, plan });
            }
            NodeAction::Cancel { rpc_id } => {
                // Nothing of the cancelled work is awaited any more.
                ctx.state_mut()
                    .deadlines
                    .retain(|_, awaited| awaited.rpc_id != rpc_id);
                ctx.reduce_child(WALLET_SEND, WalletSendAction::Cancel { rpc_id });
            }
            NodeAction::CheckTimeouts => {
                match ctx.state().wallet_send.next_mempool_scan_request() {
                    Ok(Some(request)) => ctx
                        .into_emitter()
                        .emit_action(NodeAction::ScanMempool { request }),
                    Ok(None) => {}
                    Err(rpc_id) => {
                        bug_condition!(rpc_id = rpc_id; "wallet-send mempool scan readiness is inconsistent");
                    }
                }
            }
            NodeAction::ScanMempool { request } => {
                let state = ctx.state();
                let guard = collection_guard(state, request.rpc_id, request.request_id);
                let chunk = (guard == CollectionGuard::Continue).then(|| {
                    state.mempool.owned_outputs_chunk(
                        request.cursor,
                        request.max_scan_positions as usize,
                        request.max_matches as usize,
                    )
                });
                ctx.reduce_child(
                    WALLET_SEND,
                    WalletSendAction::MempoolChunk {
                        request,
                        guard,
                        chunk,
                    },
                );
            }
            NodeAction::PersistedPage { request, result } => {
                answered(ctx.state_mut(), Answer::Page(request.request_id));
                let state = ctx.state();
                let guard = collection_guard(state, request.rpc_id, request.request_id);
                let spent = match &result {
                    Ok(chunk)
                        if guard == CollectionGuard::Continue
                            && validate_persisted_chunk(&request, chunk).is_ok() =>
                    {
                        chunk
                            .utxos
                            .iter()
                            .map(|(c, _)| *c)
                            .filter(|c| state.mempool.has_spend(c))
                            .collect()
                    }
                    _ => BTreeSet::new(),
                };
                ctx.reduce_child(
                    WALLET_SEND,
                    WalletSendAction::OwnedChunk {
                        request,
                        result,
                        guard,
                        spent,
                    },
                );
            }
            NodeAction::Proved {
                request_id,
                rpc_id,
                result,
            } => {
                answered(ctx.state_mut(), Answer::Proof(request_id));
                let rpc_pending = ctx.state().rpc_pending(rpc_id);
                let action = match result {
                    Ok(tx) => WalletSendAction::Prepared {
                        request_id,
                        rpc_id,
                        tx,
                        rpc_pending,
                    },
                    Err(error) => WalletSendAction::ProvingRejected {
                        request_id,
                        rpc_id,
                        error,
                        rpc_pending,
                    },
                };
                ctx.reduce_child(WALLET_SEND, action);
            }
            NodeAction::Collect(request) => {
                let awaited = Answer::Page(request.request_id);
                let deadline = await_answer(&mut ctx, request.rpc_id, awaited);
                let mut out = ctx.into_emitter();
                out.emit_effect(NodeEffect::Collect(request));
                out.emit_effect(NodeEffect::Arm { deadline });
            }
            NodeAction::Prove(request) => {
                let awaited = Answer::Proof(request.request_id);
                let deadline = await_answer(&mut ctx, request.rpc_id, awaited);
                let mut out = ctx.into_emitter();
                out.emit_effect(NodeEffect::Prove(request));
                out.emit_effect(NodeEffect::Arm { deadline });
            }
            NodeAction::Expired { deadline } => {
                // An answer that came in time cleared its deadline: stale.
                let Some(awaited) = ctx.state_mut().deadlines.take(deadline) else {
                    return;
                };
                // So is one whose attempt has ended since, whatever ended it.
                if !still_awaited(ctx.state(), awaited) {
                    return;
                }
                let rpc_id = awaited.rpc_id;
                ctx.state_mut()
                    .deadlines
                    .retain(|_, awaited| awaited.rpc_id != rpc_id);
                ctx.reduce_child(WALLET_SEND, WalletSendAction::Cancel { rpc_id });
                let response = SendResponse::rejected("timed out waiting for an answer".to_owned());
                ctx.into_emitter()
                    .emit_action(NodeAction::Respond { rpc_id, response });
            }
            NodeAction::HydrationQuery {
                rpc_id,
                anchor_root,
                selection_error,
            } => {
                let query = hydration_query(ctx.state(), rpc_id, anchor_root, selection_error);
                ctx.reduce_child(LEDGER, query);
            }
            NodeAction::HydrationForget { rpc_id } => {
                ctx.reduce_child(LEDGER, PartialLedgerAction::Forget { rpc_id });
            }
            NodeAction::Ledger(output) => match output {
                PartialLedgerOutput::Request { job, peer, request } => {
                    let correlation = Correlation(job.0);
                    let request = PeersAction::Request {
                        peer,
                        correlation,
                        request,
                    };
                    ctx.reduce_child(PEERS, request);
                }
                // A hydration began: the send is syncing, and waits for it
                // at most `ANSWER_TIMEOUT`.
                PartialLedgerOutput::Syncing { rpc_id, query } => {
                    if let Some(rpc) = ctx.state_mut().rpc.get_mut(&rpc_id) {
                        rpc.status = RpcStatus::Syncing;
                    }
                    let deadline = await_answer(&mut ctx, rpc_id, Answer::Hydration(query));
                    ctx.into_emitter().emit_effect(NodeEffect::Arm { deadline });
                }
                PartialLedgerOutput::Retry { rpc_id } => {
                    hydration_ended(ctx.state_mut(), rpc_id);
                    if ctx.state().rpc_pending(rpc_id) {
                        ctx.into_emitter().emit_action(NodeAction::Start { rpc_id });
                    }
                }
                PartialLedgerOutput::Rejected { rpc_id, error } => {
                    hydration_ended(ctx.state_mut(), rpc_id);
                    if ctx.state().rpc_pending(rpc_id) {
                        ctx.reduce_child(WALLET_SEND, WalletSendAction::Rejected { rpc_id, error });
                    }
                }
            },
            NodeAction::Peers(output) => {
                let action = match output {
                    PeersOutput::Started {
                        correlation,
                        peer,
                        request_id,
                        request,
                    } => PartialLedgerAction::Started {
                        job: JobId(correlation.0),
                        peer,
                        request_id,
                        request,
                    },
                    PeersOutput::Answered {
                        correlation,
                        peer,
                        request_id,
                        answer,
                    } => PartialLedgerAction::Answered {
                        job: JobId(correlation.0),
                        peer,
                        request_id,
                        answer,
                    },
                    PeersOutput::Failed {
                        correlation,
                        peer,
                        request_id,
                        error,
                    } => PartialLedgerAction::Failed {
                        job: JobId(correlation.0),
                        peer,
                        request_id,
                        error,
                    },
                    PeersOutput::Refused { correlation, peer } => PartialLedgerAction::Refused {
                        job: JobId(correlation.0),
                        peer,
                    },
                    PeersOutput::Gone { peer } => PartialLedgerAction::PeerGone { peer },
                };
                ctx.reduce_child(LEDGER, action);
            }
            NodeAction::Submit(tx) => ctx.state_mut().mempool.submit(&tx),
            // The answer finishes the RPC: the request goes, and wallet send
            // is told to cancel whatever it still holds for it.
            NodeAction::Respond { rpc_id, response } => {
                ctx.state_mut()
                    .deadlines
                    .retain(|_, awaited| awaited.rpc_id != rpc_id);
                if ctx.state_mut().rpc.remove(&rpc_id).is_some() {
                    let mut out = ctx.into_emitter();
                    out.emit_effect(NodeEffect::Respond { rpc_id, response });
                    out.emit_action(NodeAction::Cancel { rpc_id });
                }
            }
            NodeAction::MempoolTx { inputs, outputs } => {
                ctx.state_mut().mempool.accept(&inputs, &outputs)
            }
            NodeAction::NewTip { tip, root } => {
                let state = ctx.state_mut();
                state.chain_root = root;
                state.mempool.set_tip(tip);
                state.mempool.tip_root = root;
            }
            NodeAction::MempoolTip { tip } => ctx.state_mut().mempool.set_tip(tip),
            NodeAction::RpcGone { rpc_id } => {
                ctx.state_mut().rpc.remove(&rpc_id);
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: NodeEffect, _: &EffectMeta) {
        match effect {
            NodeEffect::Collect(request) => {
                let reply = ctx.reply::<Collected>();
                ctx.service().db.collect(request, reply);
            }
            NodeEffect::Prove(request) => {
                let reply = ctx.reply::<Proved>();
                ctx.service().prover.prove(request, reply);
            }
            NodeEffect::Respond { rpc_id, response } => {
                ctx.service().responder.respond(rpc_id, response)
            }
            NodeEffect::Arm { deadline } => {
                let reply = ctx.reply::<TimerFired>();
                ctx.service().timer.after(ANSWER_TIMEOUT, reply, deadline);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(WALLET_SEND, lens!(NodeState => wallet_send))
            .on_output(lower_wallet_send)
            .bind();
        r.child(LEDGER, lens!(NodeState => ledger))
            .service(Lens::identity())
            .on_output(|_, output, port| port.emit_action(NodeAction::Ledger(output)))
            .bind();
        r.child(PEERS, lens!(NodeState => peers))
            .service(Lens::identity())
            .on_output(|_, output, port| port.emit_action(NodeAction::Peers(output)))
            .bind();
        r.event::<Collected>();
        r.event::<Proved>();
        r.event::<TimerFired>();
    }
}

impl Adapts<TimerFired> for Node {
    fn adapt(fired: TimerFired, port: &mut ActionPort<'_, Self>) {
        port.emit_action(NodeAction::Expired {
            deadline: fired.request,
        });
    }
}

/// Arms a deadline for an answer `rpc_id` now waits for.
fn await_answer(ctx: &mut ComponentContext<'_, Node>, rpc_id: RpcId, answer: Answer) -> RequestId {
    let deadline = ctx.new_request();
    ctx.state_mut()
        .deadlines
        .insert(deadline, Awaited { rpc_id, answer });
    deadline
}

/// Whether the send still waits for this answer: the attempt that asked is
/// still the send's current one.
fn still_awaited(state: &NodeState, awaited: Awaited) -> bool {
    let Awaited { rpc_id, answer } = awaited;
    match answer {
        Answer::Page(attempt) => state.wallet_send.collection_pins(rpc_id, attempt).is_some(),
        Answer::Proof(attempt) => state.wallet_send.is_proving(rpc_id, attempt),
        Answer::Hydration(query) => state.ledger.hydration_query(rpc_id) == Some(query),
    }
}

/// An awaited answer came: its deadline goes, and fires stale.
fn answered(state: &mut NodeState, answer: Answer) {
    state
        .deadlines
        .retain(|_, awaited| awaited.answer != answer);
}

impl Adapts<Collected> for Node {
    fn adapt(event: Collected, port: &mut ActionPort<'_, Self>) {
        port.emit_action(NodeAction::PersistedPage {
            request: event.request,
            result: event.result,
        });
    }
}

impl Adapts<Proved> for Node {
    fn adapt(event: Proved, port: &mut ActionPort<'_, Self>) {
        port.emit_action(NodeAction::Proved {
            request_id: event.request_id,
            rpc_id: event.rpc_id,
            result: event.result,
        });
    }
}

/// A send's hydration ended: its deadline goes.
fn hydration_ended(state: &mut NodeState, rpc_id: RpcId) {
    state.deadlines.retain(|_, awaited| {
        awaited.rpc_id != rpc_id || !matches!(awaited.answer, Answer::Hydration(_))
    });
}

/// A hydration query, with what partial-ledger sync cannot read itself: the
/// send's owner and amount, whether it is pending, the peers serving its
/// anchor root, and which of the owner's known UTXOs the mempool spends.
pub fn hydration_query(
    state: &NodeState,
    rpc_id: RpcId,
    anchor_root: Root,
    selection_error: String,
) -> PartialLedgerAction {
    let rpc = state.rpc.get(&rpc_id);
    let (owner, required) = rpc.map_or((crate::domain::Owner(0), 0), |rpc| {
        let fee = rpc.req.fee.unwrap_or(0);
        (rpc.req.from, rpc.req.amount.saturating_add(fee))
    });
    let spent = state
        .ledger
        .cache
        .get(&owner)
        .map_or_else(BTreeSet::new, |entries| {
            entries
                .values()
                .map(|entry| entry.commitment)
                .filter(|commitment| state.mempool.has_spend(commitment))
                .collect()
        });
    PartialLedgerAction::Query {
        rpc_id,
        anchor_root,
        selection_error,
        owner,
        required,
        rpc_pending: rpc.is_some(),
        serving: state.peers.serving(anchor_root),
        spent,
    }
}

/// Wallet send's Outputs become the node's Actions, in order.
fn lower_wallet_send(
    _: Child<Node, WalletSend>,
    output: WalletSendOutput,
    port: &mut ActionPort<'_, Node>,
) {
    port.emit_action(match output {
        WalletSendOutput::Collect(request) => NodeAction::Collect(request),
        WalletSendOutput::ScanMempool(request) => NodeAction::ScanMempool { request },
        WalletSendOutput::Prove(request) => NodeAction::Prove(request),
        WalletSendOutput::Restart { rpc_id } => NodeAction::Start { rpc_id },
        WalletSendOutput::HydrationQuery {
            rpc_id,
            anchor_root,
            selection_error,
        } => NodeAction::HydrationQuery {
            rpc_id,
            anchor_root,
            selection_error,
        },
        WalletSendOutput::HydrationForget { rpc_id } => NodeAction::HydrationForget { rpc_id },
        WalletSendOutput::Submit(tx) => NodeAction::Submit(tx),
        WalletSendOutput::Respond { rpc_id, response } => NodeAction::Respond { rpc_id, response },
    });
}

/// What `Start` will do, read from the node.
pub fn plan_start(state: &NodeState, rpc_id: RpcId, time: Timestamp) -> StartPlan {
    let Some(rpc) = state.rpc.get(&rpc_id) else {
        bug_condition!(rpc_id = rpc_id; "rpc not found in wallet-send start");
        return StartPlan::Noop;
    };
    let req = rpc.req.clone();
    // One send per owner: a concurrent send is rejected rather than given
    // its own inputs.
    if !state.wallet_send.owner_is_available(rpc_id, req.from) {
        return StartPlan::Reject {
            error: format!("wallet send already pending for owner {:?}", req.from),
            req,
        };
    }
    if req.amount == 0 {
        return StartPlan::Reject {
            req,
            error: "amount must be greater than zero".to_owned(),
        };
    }
    if req.amount.checked_add(req.fee.unwrap_or(0)).is_none() {
        return StartPlan::Reject {
            req,
            error: "amount+fee overflow".to_owned(),
        };
    }
    StartPlan::Collect {
        anchor_root: state.chain_root,
        timestamp_ms: time / 1_000_000,
        mempool_cursor: state.mempool.begin_owned_scan(req.from),
        req,
    }
}

/// Whether a collection may go on, read from the node.
pub fn collection_guard(
    state: &NodeState,
    rpc_id: RpcId,
    request_id: RequestId,
) -> CollectionGuard {
    let Some(rpc) = state.rpc.get(&rpc_id) else {
        return CollectionGuard::Noop;
    };
    if !state
        .wallet_send
        .collection_matches_rpc_request(rpc_id, request_id, &rpc.req)
    {
        return CollectionGuard::Noop;
    }
    let Some((anchor_root, cursor)) = state.wallet_send.collection_pins(rpc_id, request_id) else {
        return CollectionGuard::Noop;
    };
    if state.chain_root != anchor_root
        || state.mempool.tip_root != anchor_root
        || state.mempool.validate_cursor(&cursor).is_err()
    {
        return CollectionGuard::Retry;
    }
    CollectionGuard::Continue
}
