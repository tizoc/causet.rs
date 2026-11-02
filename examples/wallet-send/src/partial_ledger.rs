//! Partial-ledger sync: the light node asks full peers for some leaves of
//! the ledger, with proofs, and uses them without holding the whole ledger.
//!
//! Two parts share this component:
//!
//! - **The probe lifecycle**, shared by every consumer. A job is admitted
//!   (at most one per peer, whatever its phase; at most `MAX_IN_FLIGHT`),
//!   then goes `Init` → `Pending` (its request is out, under the network's
//!   request ID) → `VerifyPending` (answered, its proofs being checked).
//!   Four identities are kept apart: the job ID, the peer, the network's
//!   request ID and the exact request; a report that does not match all
//!   that apply is stale. Every ending, success or failure, is a
//!   `ProbeResult` that keeps the job's `Target`, and one exhaustive match
//!   on the target routes it (`route`). On a disconnect, the peer's `Init`
//!   and `Pending` jobs fail; `VerifyPending` ones are kept, since the
//!   exchange is over and verification needs no peer. Verification is the
//!   one wait nothing else bounds, so it runs only for a result its consumer
//!   still waits for: an answer that comes after is not verified, and a
//!   consumer that stops waiting retires a job being verified for it.
//! - **Wallet-spend hydration**, its one consumer here. When a send lacks
//!   funds in what the node has stored, it picks candidates from the wallet
//!   cache (UTXOs the wallet knows of, by leaf, not yet checked at the
//!   anchor root, not spent in the mempool), probes a peer serving that
//!   root, records what the probe proved in two stores, waits for both, in
//!   either order, and asks for the send to be tried again. Each write names
//!   the hydration (its query), so a cancelled hydration's acknowledgement
//!   cannot end the one that replaced it; and a cache entry is held as
//!   checked only once the store that makes it durable has acknowledged.
//!
//! The network is the `Peers` sibling: requests leave as Outputs, its
//! reports arrive as Actions from the root, which also passes in what this
//! component cannot read (which peers serve the anchor root, which
//! commitments the mempool spends, whether the send is still pending).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use causet::*;
use serde::{Deserialize, Serialize};

use crate::domain::{Commitment, Owner, Root, RpcId, Utxo};
use crate::ledger::{IndexRef, PeerAnswer, ProbeRequest, ProbeResponse, Verified, covers};
use crate::node::NodeServices;
use crate::peers::{PeerError, PeerId};

/// At most this many probes at once.
pub const MAX_IN_FLIGHT: usize = 4;
/// A hydration probes at most this many leaves.
pub const MAX_ITEMS: usize = 64;
/// How long a hydration waits before the send is tried again, when it
/// could not probe yet.
pub const RETRY_COOLDOWN: Duration = Duration::from_millis(500);

/// A probe job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JobId(pub RequestId);

/// Where a job's result goes: routing data, kept through every ending.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Target {
    SpendHydration { rpc_id: RpcId },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobPhase {
    /// Admitted; the request is not out yet.
    Init,
    /// The request is out under the network's `request_id`.
    Pending { request_id: RequestId },
    /// Answered; the proofs are being verified.
    VerifyPending { request_id: RequestId },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub target: Target,
    pub peer: PeerId,
    pub request: ProbeRequest,
    pub phase: JobPhase,
}

/// Why a probe failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncError {
    /// Not admitted, or the peer could not take it: nothing was asked.
    Rejected(String),
    Disconnected,
    Timeout,
    /// The peer does not have the ledger at that root.
    Unavailable,
    /// The answer, or its verification, is not for this exchange.
    Mismatch(String),
    BadProof(String),
    /// Answered after its consumer stopped waiting: not verified.
    Abandoned,
}

/// How a job ended.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeResult {
    pub job: JobId,
    pub target: Target,
    pub peer: PeerId,
    pub request: ProbeRequest,
    pub result: Result<Verified, SyncError>,
}

/// A UTXO the wallet knows of, by leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEntry {
    pub commitment: Commitment,
    pub amount: u64,
    /// The last root a probe checked it at, and whether it was there.
    pub checked: Option<(Root, bool)>,
}

/// One send's hydration, from the query that began it to its end.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hydration {
    /// Names this hydration to the root, which bounds it with a deadline.
    pub query: RequestId,
    pub owner: Owner,
    pub anchor_root: Root,
    pub stage: Stage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Stage {
    Probing {
        job: JobId,
    },
    /// The probe's results are being stored: which stores have not
    /// acknowledged yet, and the cache entries each one's acknowledgement
    /// marks as checked. A leaf found is marked only once the partial store
    /// holds it: until then, a later probe may find it again.
    Storing {
        awaiting_partial: bool,
        awaiting_cache: bool,
        found: Vec<IndexRef>,
        missing: Vec<IndexRef>,
    },
    /// Could not probe yet; the send is tried again when `retry` fires.
    Waiting {
        retry: RequestId,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialLedgerState {
    #[serde(with = "crate::node::pairs")]
    pub jobs: BTreeMap<JobId, Job>,
    pub cache: BTreeMap<Owner, BTreeMap<u64, CacheEntry>>,
    /// Whether the wallet cache has been filled once. Until then, a send
    /// that lacks funds waits rather than being rejected.
    pub seeded: bool,
    /// Sends that have probed since they last failed for good: one probe
    /// per send.
    pub attempted: BTreeSet<RpcId>,
    pub hydrations: BTreeMap<RpcId, Hydration>,
}

impl PartialLedgerState {
    pub fn has_job(&self, peer: PeerId) -> bool {
        self.jobs.values().any(|job| job.peer == peer)
    }

    /// The query of the send's current hydration.
    pub fn hydration_query(&self, rpc_id: RpcId) -> Option<RequestId> {
        self.hydrations.get(&rpc_id).map(|h| h.query)
    }

    /// Forgotten: no hydration, and no probe counted against the send.
    pub fn forgotten(&self, rpc_id: RpcId) -> bool {
        !self.attempted.contains(&rpc_id) && !self.hydrations.contains_key(&rpc_id)
    }

    /// The cache entries a probe has checked: `(root, leaf, commitment,
    /// present)`.
    pub fn checked(&self) -> impl Iterator<Item = (Root, u64, Commitment, bool)> + '_ {
        self.cache.values().flat_map(|entries| {
            entries.iter().filter_map(|(index, entry)| {
                let (root, present) = entry.checked?;
                Some((root, *index, entry.commitment, present))
            })
        })
    }

    /// The invariants of the lifecycle and of hydration.
    pub fn invariants(&self) -> Result<(), String> {
        if self.jobs.len() > MAX_IN_FLIGHT {
            return Err(format!("{} jobs in flight", self.jobs.len()));
        }
        let mut peers = BTreeSet::new();
        for job in self.jobs.values() {
            if !peers.insert(job.peer) {
                return Err(format!("two jobs for {:?}", job.peer));
            }
        }
        // A job still out on the network can outlive its hydration,
        // forgotten since: the network ends it, and its result is a no-op.
        // A job being verified cannot: nothing else would end it.
        for (id, job) in &self.jobs {
            let Target::SpendHydration { rpc_id } = job.target;
            let probing = Stage::Probing { job: *id };
            if matches!(job.phase, JobPhase::VerifyPending { .. })
                && self.hydrations.get(&rpc_id).map(|h| &h.stage) != Some(&probing)
            {
                return Err(format!("{id:?} is verified for no hydration"));
            }
        }
        for (rpc_id, h) in &self.hydrations {
            match &h.stage {
                Stage::Probing { job } => {
                    let Some(probe) = self.jobs.get(job) else {
                        return Err(format!("{rpc_id:?} probes with {job:?}, which is gone"));
                    };
                    if probe.target != (Target::SpendHydration { rpc_id: *rpc_id }) {
                        return Err(format!("{rpc_id:?} probes with another's {job:?}"));
                    }
                    if probe.request.root != h.anchor_root {
                        return Err(format!("{rpc_id:?} probes another root"));
                    }
                }
                Stage::Storing {
                    awaiting_partial,
                    awaiting_cache,
                    ..
                } => {
                    if !awaiting_partial && !awaiting_cache {
                        return Err(format!("{rpc_id:?} stored but not retried"));
                    }
                }
                Stage::Waiting { .. } => {}
            }
        }
        Ok(())
    }
}

/// The candidates a hydration probes: the owner's cache entries not yet
/// checked at `root` and not spent in the mempool, the newest first, until
/// they cover `required`, at most `MAX_ITEMS`.
pub fn candidates(
    state: &PartialLedgerState,
    owner: Owner,
    root: Root,
    spent: &BTreeSet<Commitment>,
    required: u64,
) -> Vec<IndexRef> {
    let mut items = Vec::new();
    let mut total = 0u64;
    let Some(entries) = state.cache.get(&owner) else {
        return items;
    };
    for (index, entry) in entries.iter().rev() {
        if total >= required || items.len() == MAX_ITEMS {
            break;
        }
        if entry.checked.is_some_and(|(at, _)| at == root) || spent.contains(&entry.commitment) {
            continue;
        }
        total = total.saturating_add(entry.amount);
        items.push(IndexRef {
            index: *index,
            expected: entry.commitment,
        });
    }
    items
}

// ---------------------------------------------------------------------------
// Services: the proof verifier, and the two stores a hydration writes.

#[derive(Debug, Event)]
#[causet(type = "ledger.verified")]
pub struct VerifyDone {
    pub job: JobId,
    pub peer: PeerId,
    pub request_id: RequestId,
    pub request: ProbeRequest,
    pub result: Result<Verified, String>,
}

/// A store's acknowledgement, naming the hydration whose write it was.
#[derive(Debug, Event)]
#[causet(type = "ledger.stored")]
pub enum Stored {
    /// The wallet cache's record of what the probe checked.
    Cache {
        rpc_id: RpcId,
        query: RequestId,
        root: Root,
        ok: bool,
    },
    /// The proved UTXOs, which the UTXO database reads.
    Partial {
        rpc_id: RpcId,
        query: RequestId,
        root: Root,
        ok: bool,
    },
}

pub trait Verifier: Send {
    /// Verifies the response's proofs (`ledger::verify_response`), and
    /// answers with the request's identities.
    fn verify(
        &mut self,
        job: JobId,
        peer: PeerId,
        request_id: RequestId,
        request: ProbeRequest,
        response: ProbeResponse,
        reply: Reply<VerifyDone>,
    );
}

/// Each write names the hydration it is for (`rpc_id`, `query`), which its
/// acknowledgement carries back.
pub trait LedgerStores: Send {
    fn store_cache(
        &mut self,
        rpc_id: RpcId,
        query: RequestId,
        root: Root,
        found: Vec<u64>,
        missing: Vec<u64>,
        reply: Reply<Stored>,
    );
    fn store_partial(
        &mut self,
        rpc_id: RpcId,
        query: RequestId,
        root: Root,
        utxos: Vec<(u64, Utxo)>,
        reply: Reply<Stored>,
    );
}

// ---------------------------------------------------------------------------
// The component

#[derive(Debug, Action)]
#[causet(type = "ledger.action")]
pub enum PartialLedgerAction {
    /// A send lacks funds: hydrate, with what the root read for it.
    Query {
        rpc_id: RpcId,
        anchor_root: Root,
        selection_error: String,
        owner: Owner,
        required: u64,
        rpc_pending: bool,
        /// The connected peers serving the anchor root, longest connected
        /// first.
        serving: Vec<PeerId>,
        /// The owner's cache entries the mempool spends.
        spent: BTreeSet<Commitment>,
    },
    Forget {
        rpc_id: RpcId,
    },
    RetryDue {
        retry: RequestId,
    },
    CacheStored {
        rpc_id: RpcId,
        query: RequestId,
        root: Root,
        ok: bool,
    },
    PartialStored {
        rpc_id: RpcId,
        query: RequestId,
        root: Root,
        ok: bool,
    },
    /// The world: UTXOs the wallet learns of, and the end of seeding.
    WalletSeen {
        owner: Owner,
        entries: Vec<(u64, Commitment, u64)>,
    },
    Seeded,
    /// The network's reports, lowered by the root.
    Started {
        job: JobId,
        peer: PeerId,
        request_id: RequestId,
        request: ProbeRequest,
    },
    Answered {
        job: JobId,
        peer: PeerId,
        request_id: RequestId,
        answer: PeerAnswer,
    },
    Failed {
        job: JobId,
        peer: PeerId,
        request_id: RequestId,
        error: PeerError,
    },
    Refused {
        job: JobId,
        peer: PeerId,
    },
    PeerGone {
        peer: PeerId,
    },
    Verified {
        job: JobId,
        peer: PeerId,
        request_id: RequestId,
        request: ProbeRequest,
        result: Result<Verified, String>,
    },
}

#[derive(Debug, Effect)]
#[causet(type = "ledger.effect")]
pub enum PartialLedgerEffect {
    Verify {
        job: JobId,
        peer: PeerId,
        request_id: RequestId,
        request: ProbeRequest,
        response: ProbeResponse,
    },
    StoreCache {
        rpc_id: RpcId,
        query: RequestId,
        root: Root,
        found: Vec<u64>,
        missing: Vec<u64>,
    },
    StorePartial {
        rpc_id: RpcId,
        query: RequestId,
        root: Root,
        utxos: Vec<(u64, Utxo)>,
    },
    ArmRetry {
        retry: RequestId,
    },
}

#[derive(Debug, Output)]
#[causet(type = "ledger.output")]
pub enum PartialLedgerOutput {
    /// Send this probe to the peer.
    Request {
        job: JobId,
        peer: PeerId,
        request: ProbeRequest,
    },
    /// A hydration began for the send.
    Syncing { rpc_id: RpcId, query: RequestId },
    /// The hydration ended: try the send again.
    Retry { rpc_id: RpcId },
    /// The hydration ended: reject the send.
    Rejected { rpc_id: RpcId, error: String },
}

/// What a reduction emits, gathered while the State is borrowed.
enum Emit {
    Output(PartialLedgerOutput),
    Effect(PartialLedgerEffect),
}

type Ctx<'a> = ComponentContext<'a, PartialLedgerSync>;

pub struct PartialLedgerSync;

impl Component for PartialLedgerSync {
    type State = PartialLedgerState;
    type Service = NodeServices;
    type Action = PartialLedgerAction;
    type Effect = PartialLedgerEffect;
    type Output = PartialLedgerOutput;

    fn reduce(mut ctx: Ctx<'_>, action: PartialLedgerAction, _: &ActionMeta) {
        let mut emits = Vec::new();
        match action {
            PartialLedgerAction::Query {
                rpc_id,
                anchor_root,
                selection_error,
                owner,
                required,
                rpc_pending,
                serving,
                spent,
            } => {
                let query = Query {
                    rpc_id,
                    anchor_root,
                    selection_error,
                    owner,
                    required,
                    rpc_pending,
                    serving,
                    spent,
                };
                plan(&mut ctx, &mut emits, query);
            }
            PartialLedgerAction::Forget { rpc_id } => forget(ctx.state_mut(), rpc_id),
            PartialLedgerAction::RetryDue { retry } => {
                let state = ctx.state_mut();
                let due = state.hydrations.iter().find_map(|(rpc_id, h)| {
                    (h.stage == Stage::Waiting { retry }).then_some(*rpc_id)
                });
                if let Some(rpc_id) = due {
                    state.hydrations.remove(&rpc_id);
                    emits.push(Emit::Output(PartialLedgerOutput::Retry { rpc_id }));
                }
            }
            PartialLedgerAction::CacheStored {
                rpc_id,
                query,
                root,
                ok,
            } => {
                let ack = Ack {
                    rpc_id,
                    query,
                    root,
                    ok,
                    store: Store::Cache,
                };
                stored(ctx.state_mut(), &mut emits, ack);
            }
            PartialLedgerAction::PartialStored {
                rpc_id,
                query,
                root,
                ok,
            } => {
                let ack = Ack {
                    rpc_id,
                    query,
                    root,
                    ok,
                    store: Store::Partial,
                };
                stored(ctx.state_mut(), &mut emits, ack);
            }
            PartialLedgerAction::WalletSeen { owner, entries } => {
                // A leaf can hold another UTXO since: the newer one replaces
                // what the cache knew of it.
                let cache = ctx.state_mut().cache.entry(owner).or_default();
                for (index, commitment, amount) in entries {
                    if cache.get(&index).is_none_or(|e| e.commitment != commitment) {
                        let entry = CacheEntry {
                            commitment,
                            amount,
                            checked: None,
                        };
                        cache.insert(index, entry);
                    }
                }
            }
            PartialLedgerAction::Seeded => ctx.state_mut().seeded = true,
            PartialLedgerAction::Started {
                job,
                peer,
                request_id,
                request,
            } => {
                if let Some(pending) = ctx.state_mut().jobs.get_mut(&job)
                    && pending.phase == JobPhase::Init
                    && pending.peer == peer
                    && pending.request == request
                {
                    pending.phase = JobPhase::Pending { request_id };
                }
            }
            PartialLedgerAction::Answered {
                job,
                peer,
                request_id,
                answer,
            } => {
                let Some(pending) = current(ctx.state(), job, peer, request_id) else {
                    return;
                };
                let request = pending.request.clone();
                // Verification is the one wait nothing else bounds: it is
                // started only for a result someone still waits for.
                if !awaited(ctx.state(), job, pending.target) {
                    finish(&mut ctx, &mut emits, job, Err(SyncError::Abandoned));
                    return;
                }
                match answer {
                    PeerAnswer::Unavailable => {
                        finish(&mut ctx, &mut emits, job, Err(SyncError::Unavailable))
                    }
                    PeerAnswer::Data(response) => match covers(&request, &response) {
                        Err(error) => {
                            finish(&mut ctx, &mut emits, job, Err(SyncError::Mismatch(error)))
                        }
                        Ok(()) => {
                            if let Some(pending) = ctx.state_mut().jobs.get_mut(&job) {
                                pending.phase = JobPhase::VerifyPending { request_id };
                            }
                            emits.push(Emit::Effect(PartialLedgerEffect::Verify {
                                job,
                                peer,
                                request_id,
                                request,
                                response,
                            }));
                        }
                    },
                }
            }
            PartialLedgerAction::Failed {
                job,
                peer,
                request_id,
                error,
            } => {
                if current(ctx.state(), job, peer, request_id).is_some() {
                    let error = match error {
                        PeerError::Timeout => SyncError::Timeout,
                        PeerError::Disconnected => SyncError::Disconnected,
                    };
                    finish(&mut ctx, &mut emits, job, Err(error));
                }
            }
            PartialLedgerAction::Refused { job, peer } => {
                if ctx
                    .state()
                    .jobs
                    .get(&job)
                    .is_some_and(|j| j.phase == JobPhase::Init && j.peer == peer)
                {
                    let error = SyncError::Rejected("the peer cannot take a request".to_owned());
                    finish(&mut ctx, &mut emits, job, Err(error));
                }
            }
            PartialLedgerAction::PeerGone { peer } => {
                // The exchange of a job verifying is over: it stays.
                let gone: Vec<JobId> = ctx
                    .state()
                    .jobs
                    .iter()
                    .filter(|(_, j)| {
                        j.peer == peer && !matches!(j.phase, JobPhase::VerifyPending { .. })
                    })
                    .map(|(id, _)| *id)
                    .collect();
                for job in gone {
                    finish(&mut ctx, &mut emits, job, Err(SyncError::Disconnected));
                }
            }
            PartialLedgerAction::Verified {
                job,
                peer,
                request_id,
                request,
                result,
            } => {
                let Some(pending) = ctx.state().jobs.get(&job) else {
                    return;
                };
                let result = if pending.phase == (JobPhase::VerifyPending { request_id })
                    && pending.peer == peer
                    && pending.request == request
                {
                    result.map_err(SyncError::BadProof)
                } else {
                    Err(SyncError::Mismatch(
                        "a verification for another exchange".to_owned(),
                    ))
                };
                finish(&mut ctx, &mut emits, job, result);
            }
        }
        let mut out = ctx.into_emitter();
        for emit in emits {
            match emit {
                Emit::Output(output) => out.emit_output(output),
                Emit::Effect(effect) => out.emit_effect(effect),
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: PartialLedgerEffect, _: &EffectMeta) {
        match effect {
            PartialLedgerEffect::Verify {
                job,
                peer,
                request_id,
                request,
                response,
            } => {
                let reply = ctx.reply::<VerifyDone>();
                ctx.service()
                    .verifier
                    .verify(job, peer, request_id, request, response, reply);
            }
            PartialLedgerEffect::StoreCache {
                rpc_id,
                query,
                root,
                found,
                missing,
            } => {
                let reply = ctx.reply::<Stored>();
                ctx.service()
                    .stores
                    .store_cache(rpc_id, query, root, found, missing, reply);
            }
            PartialLedgerEffect::StorePartial {
                rpc_id,
                query,
                root,
                utxos,
            } => {
                let reply = ctx.reply::<Stored>();
                ctx.service()
                    .stores
                    .store_partial(rpc_id, query, root, utxos, reply);
            }
            PartialLedgerEffect::ArmRetry { retry } => {
                let reply = ctx.reply::<TimerFired>();
                ctx.service().timer.after(RETRY_COOLDOWN, reply, retry);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<VerifyDone>();
        r.event::<Stored>();
        r.event::<TimerFired>();
    }
}

impl Adapts<VerifyDone> for PartialLedgerSync {
    fn adapt(event: VerifyDone, port: &mut ActionPort<'_, Self>) {
        port.emit_action(PartialLedgerAction::Verified {
            job: event.job,
            peer: event.peer,
            request_id: event.request_id,
            request: event.request,
            result: event.result,
        });
    }
}

impl Adapts<Stored> for PartialLedgerSync {
    fn adapt(event: Stored, port: &mut ActionPort<'_, Self>) {
        port.emit_action(match event {
            Stored::Cache {
                rpc_id,
                query,
                root,
                ok,
            } => PartialLedgerAction::CacheStored {
                rpc_id,
                query,
                root,
                ok,
            },
            Stored::Partial {
                rpc_id,
                query,
                root,
                ok,
            } => PartialLedgerAction::PartialStored {
                rpc_id,
                query,
                root,
                ok,
            },
        });
    }
}

impl Adapts<TimerFired> for PartialLedgerSync {
    fn adapt(fired: TimerFired, port: &mut ActionPort<'_, Self>) {
        port.emit_action(PartialLedgerAction::RetryDue {
            retry: fired.request,
        });
    }
}

// ---------------------------------------------------------------------------
// The lifecycle

/// The job, if it is `Pending` under exactly this peer and request ID.
fn current(
    state: &PartialLedgerState,
    job: JobId,
    peer: PeerId,
    request_id: RequestId,
) -> Option<&Job> {
    state
        .jobs
        .get(&job)
        .filter(|j| j.peer == peer && j.phase == JobPhase::Pending { request_id })
}

/// Admits a job, or ends it at once with `Rejected`.
fn start(ctx: &mut Ctx<'_>, emits: &mut Vec<Emit>, job: JobId, mut probe: Job, serving: &[PeerId]) {
    let state = ctx.state();
    let refused = if state.jobs.contains_key(&job) {
        Some("duplicate job ID")
    } else if state.jobs.len() >= MAX_IN_FLIGHT {
        Some("probes in flight at the limit")
    } else if state.has_job(probe.peer) {
        Some("the peer already has a job")
    } else if !serving.contains(&probe.peer) {
        Some("the peer cannot take a request")
    } else {
        None
    };
    if let Some(reason) = refused {
        let result = ProbeResult {
            job,
            target: probe.target,
            peer: probe.peer,
            request: probe.request,
            result: Err(SyncError::Rejected(reason.to_owned())),
        };
        route(ctx, emits, result);
        return;
    }
    probe.phase = JobPhase::Init;
    emits.push(Emit::Output(PartialLedgerOutput::Request {
        job,
        peer: probe.peer,
        request: probe.request.clone(),
    }));
    ctx.state_mut().jobs.insert(job, probe);
}

/// Ends a job and routes its result.
fn finish(
    ctx: &mut Ctx<'_>,
    emits: &mut Vec<Emit>,
    job: JobId,
    result: Result<Verified, SyncError>,
) {
    let Some(probe) = ctx.state_mut().jobs.remove(&job) else {
        return;
    };
    let result = ProbeResult {
        job,
        target: probe.target,
        peer: probe.peer,
        request: probe.request,
        result,
    };
    route(ctx, emits, result);
}

/// Whether the job's consumer still waits for its result.
fn awaited(state: &PartialLedgerState, job: JobId, target: Target) -> bool {
    match target {
        Target::SpendHydration { rpc_id } => state
            .hydrations
            .get(&rpc_id)
            .is_some_and(|h| h.stage == Stage::Probing { job }),
    }
}

/// The one place a result picks its consumer.
fn route(ctx: &mut Ctx<'_>, emits: &mut Vec<Emit>, result: ProbeResult) {
    match result.target {
        Target::SpendHydration { rpc_id } => probed(ctx, emits, rpc_id, result),
    }
}

// ---------------------------------------------------------------------------
// Wallet-spend hydration

struct Query {
    rpc_id: RpcId,
    anchor_root: Root,
    selection_error: String,
    owner: Owner,
    required: u64,
    rpc_pending: bool,
    serving: Vec<PeerId>,
    spent: BTreeSet<Commitment>,
}

fn plan(ctx: &mut Ctx<'_>, emits: &mut Vec<Emit>, query: Query) {
    let Query {
        rpc_id,
        anchor_root,
        selection_error,
        owner,
        required,
        rpc_pending,
        serving,
        spent,
    } = query;
    if !rpc_pending {
        forget(ctx.state_mut(), rpc_id);
        return;
    }
    // One probe per send at a time: while one is out or being stored, its
    // result decides.
    let ongoing = ctx.state().hydrations.get(&rpc_id).cloned();
    if ongoing
        .as_ref()
        .is_some_and(|h| !matches!(h.stage, Stage::Waiting { .. }))
    {
        return;
    }
    let state = ctx.state();
    let items = candidates(state, owner, anchor_root, &spent, required);
    let peer = serving.iter().copied().find(|peer| !state.has_job(*peer));
    let can_probe = !items.is_empty() && peer.is_some() && !state.attempted.contains(&rpc_id);
    if !can_probe && state.seeded {
        ctx.state_mut().hydrations.remove(&rpc_id);
        emits.push(Emit::Output(PartialLedgerOutput::Rejected {
            rpc_id,
            error: selection_error,
        }));
        return;
    }
    // A hydration already waiting goes on under its query.
    let query = match &ongoing {
        Some(h) => h.query,
        None => {
            let query = ctx.new_request();
            emits.push(Emit::Output(PartialLedgerOutput::Syncing { rpc_id, query }));
            query
        }
    };
    let job = JobId(ctx.new_request());
    let hydration = Hydration {
        query,
        owner,
        anchor_root,
        stage: Stage::Probing { job },
    };
    let (Some(peer), true) = (peer, can_probe) else {
        wait(ctx, emits, rpc_id, hydration);
        return;
    };
    let state = ctx.state_mut();
    state.attempted.insert(rpc_id);
    state.hydrations.insert(rpc_id, hydration);
    let probe = Job {
        target: Target::SpendHydration { rpc_id },
        peer,
        request: ProbeRequest {
            root: anchor_root,
            items,
        },
        phase: JobPhase::Init,
    };
    start(ctx, emits, job, probe, &serving);
}

/// The send's hydration ends unfinished, and its probe with it if it is
/// being verified: the network ends a probe still out on it, but nothing
/// would end a verification whose answer is lost, and the job would hold
/// its peer and a place in flight for good.
fn forget(state: &mut PartialLedgerState, rpc_id: RpcId) {
    state.attempted.remove(&rpc_id);
    if let Some(Hydration {
        stage: Stage::Probing { job },
        ..
    }) = state.hydrations.remove(&rpc_id)
        && state
            .jobs
            .get(&job)
            .is_some_and(|j| matches!(j.phase, JobPhase::VerifyPending { .. }))
    {
        state.jobs.remove(&job);
    }
}

/// Waits `RETRY_COOLDOWN`, then has the send tried again.
fn wait(ctx: &mut Ctx<'_>, emits: &mut Vec<Emit>, rpc_id: RpcId, mut hydration: Hydration) {
    let retry = ctx.new_request();
    hydration.stage = Stage::Waiting { retry };
    ctx.state_mut().hydrations.insert(rpc_id, hydration);
    emits.push(Emit::Effect(PartialLedgerEffect::ArmRetry { retry }));
}

/// A hydration's probe ended.
fn probed(ctx: &mut Ctx<'_>, emits: &mut Vec<Emit>, rpc_id: RpcId, result: ProbeResult) {
    let Some(h) = ctx.state().hydrations.get(&rpc_id).cloned() else {
        return;
    };
    if h.stage != (Stage::Probing { job: result.job }) {
        return;
    }
    match result.result {
        // Nothing was asked: the probe does not count, and the send waits.
        Err(SyncError::Rejected(_)) => {
            ctx.state_mut().attempted.remove(&rpc_id);
            wait(ctx, emits, rpc_id, h);
        }
        Err(error) => {
            ctx.state_mut().hydrations.remove(&rpc_id);
            emits.push(Emit::Output(PartialLedgerOutput::Rejected {
                rpc_id,
                error: format!("hydration probe failed: {error:?}"),
            }));
        }
        Ok(verified) => {
            let root = verified.root;
            let found_at: BTreeSet<u64> = verified.found.iter().map(|(index, _)| *index).collect();
            let (found, missing): (Vec<IndexRef>, Vec<IndexRef>) = result
                .request
                .items
                .iter()
                .partition(|item| found_at.contains(&item.index));
            let awaiting_partial = !verified.found.is_empty();
            if let Some(h) = ctx.state_mut().hydrations.get_mut(&rpc_id) {
                h.stage = Stage::Storing {
                    awaiting_partial,
                    awaiting_cache: true,
                    found,
                    missing,
                };
            }
            emits.push(Emit::Effect(PartialLedgerEffect::StoreCache {
                rpc_id,
                query: h.query,
                root,
                found: found_at.into_iter().collect(),
                missing: verified.missing,
            }));
            if awaiting_partial {
                emits.push(Emit::Effect(PartialLedgerEffect::StorePartial {
                    rpc_id,
                    query: h.query,
                    root,
                    utxos: verified.found,
                }));
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Store {
    Cache,
    Partial,
}

struct Ack {
    rpc_id: RpcId,
    query: RequestId,
    root: Root,
    ok: bool,
    store: Store,
}

/// A store acknowledged a write of the send's current hydration (one of an
/// earlier hydration, cancelled since, is stale). Its success marks the
/// cache entries it makes durable; once both stores have, the send is
/// tried again. A failure rejects it.
fn stored(state: &mut PartialLedgerState, emits: &mut Vec<Emit>, ack: Ack) {
    let Ack {
        rpc_id,
        query,
        root,
        ok,
        store,
    } = ack;
    let Some(h) = state.hydrations.get_mut(&rpc_id) else {
        return;
    };
    let Stage::Storing {
        awaiting_partial,
        awaiting_cache,
        found,
        missing,
    } = &mut h.stage
    else {
        return;
    };
    let awaiting = match store {
        Store::Cache => *awaiting_cache,
        Store::Partial => *awaiting_partial,
    };
    if h.query != query || h.anchor_root != root || !awaiting {
        return;
    }
    if !ok {
        state.hydrations.remove(&rpc_id);
        let error = match store {
            Store::Cache => "hydration: storing the wallet cache failed",
            Store::Partial => "hydration: storing the proved UTXOs failed",
        };
        emits.push(Emit::Output(PartialLedgerOutput::Rejected {
            rpc_id,
            error: error.to_owned(),
        }));
        return;
    }
    let (marks, present) = match store {
        Store::Cache => {
            *awaiting_cache = false;
            (std::mem::take(missing), false)
        }
        Store::Partial => {
            *awaiting_partial = false;
            (std::mem::take(found), true)
        }
    };
    let done = !*awaiting_partial && !*awaiting_cache;
    let entries = state.cache.entry(h.owner).or_default();
    for item in marks {
        if let Some(entry) = entries.get_mut(&item.index)
            && entry.commitment == item.expected
        {
            entry.checked = Some((root, present));
        }
    }
    if !done {
        return;
    }
    state.hydrations.remove(&rpc_id);
    emits.push(Emit::Output(PartialLedgerOutput::Retry { rpc_id }));
}
