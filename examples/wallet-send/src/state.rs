//! Wallet send's lifecycle State and its transitions. A request ID is
//! causet's `RequestId`, made by the reducer (`ComponentContext::new_request`)
//! rather than a counter kept in State, so it also tells two runs of the node
//! apart.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use causet::RequestId;
use serde::{Deserialize, Serialize};

use crate::domain::{
    BASE_TOKEN, Commitment, MAX_INPUTS, Owner, Recipient, Root, RpcId, SendRequest, Utxo,
};
use crate::mempool::{OwnedOutputsChunk, OwnedOutputsCursor};

pub const OWNER_MAX_SCAN_POSITIONS: u32 = 4_096;
pub const OWNER_MAX_MATCHES: u32 = 32;
pub const MEMPOOL_MAX_SCAN_POSITIONS: u32 = 32;
pub const MEMPOOL_MAX_MATCHES: u32 = 32;

/// A snapshot- and cursor-pinned request for one bounded persisted page.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectRequest {
    pub request_id: RequestId,
    pub rpc_id: RpcId,
    pub chunk_seq: u64,
    pub anchor_root: Root,
    pub owner: Owner,
    pub next_index: u64,
    pub max_scan_positions: u32,
    pub max_matches: u32,
    pub mempool_cursor: OwnedOutputsCursor,
}

/// One persisted page, as the UTXO database answers it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedUtxosChunk {
    pub utxos: Vec<(Commitment, Utxo)>,
    pub frontier_exclusive: u64,
    pub next_index: u64,
    pub complete: bool,
    pub scanned_positions: u32,
}

/// A correlated request for one bounded mempool page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MempoolRequest {
    pub request_id: RequestId,
    pub rpc_id: RpcId,
    pub chunk_seq: u64,
    pub anchor_root: Root,
    pub cursor: OwnedOutputsCursor,
    pub max_scan_positions: u32,
    pub max_matches: u32,
}

/// What proving needs, self-contained.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProveRequest {
    pub request_id: RequestId,
    pub rpc_id: RpcId,
    pub anchor_root: Root,
    pub timestamp_ms: u64,
    pub req: SendRequest,
    pub available_utxos: Vec<(Commitment, Utxo)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum CollectionSource {
    Persisted { chunk_seq: u64, next_index: u64 },
    Mempool { chunk_seq: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Collection {
    request_id: RequestId,
    anchor_root: Root,
    timestamp_ms: u64,
    req: SendRequest,
    selector: InputSelector,
    mempool_cursor: OwnedOutputsCursor,
    source: CollectionSource,
}

/// Selects base-currency inputs incrementally: the largest candidates, in
/// stable descending order, at most `MAX_INPUTS` of them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct InputSelector {
    required: u64,
    eligible_total: u64,
    candidates: Vec<(Commitment, Utxo)>,
}

impl InputSelector {
    fn new(amount: u64, fee: u64) -> Result<Self, String> {
        if amount == 0 {
            return Err("amount must be greater than zero".to_owned());
        }
        let required = amount
            .checked_add(fee)
            .ok_or_else(|| "amount+fee overflow".to_owned())?;
        Ok(Self {
            required,
            eligible_total: 0,
            candidates: Vec::new(),
        })
    }

    fn consider(&mut self, commitment: Commitment, utxo: &Utxo) -> Result<(), String> {
        if !matches!(utxo.recipient, Recipient::Key(_)) {
            return Ok(());
        }
        utxo.ensure_key_utxo_nonzero_amount().map_err(|err| {
            format!("owned utxo {commitment:?} violates spendable single-asset invariant: {err}")
        })?;
        if utxo.token != BASE_TOKEN || utxo.data != 0 {
            return Ok(());
        }
        self.eligible_total = self.eligible_total.saturating_add(utxo.amount);
        let insert_at = self
            .candidates
            .partition_point(|(_, candidate)| candidate.amount >= utxo.amount);
        if insert_at >= MAX_INPUTS {
            return Ok(());
        }
        self.candidates
            .insert(insert_at, (commitment, utxo.clone()));
        if self.candidates.len() > MAX_INPUTS {
            self.candidates.pop();
        }
        Ok(())
    }

    fn finish(self) -> Result<Vec<(Commitment, Utxo)>, String> {
        let mut retained_total = 0u64;
        for (_, utxo) in &self.candidates {
            retained_total = retained_total
                .checked_add(utxo.amount)
                .ok_or_else(|| "base funds overflow when summing inputs".to_owned())?;
            if retained_total >= self.required {
                return Ok(self.candidates);
            }
        }
        if self.eligible_total < self.required {
            return Err(format!(
                "insufficient base funds (have={}, need={})",
                self.eligible_total, self.required
            ));
        }
        Err(format!(
            "transfer requires more than {MAX_INPUTS} inputs but hard max is {MAX_INPUTS}"
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Phase {
    Collecting(Box<Collection>),
    Proving { request_id: RequestId },
}

pub(crate) enum PersistedTransition {
    Collect(CollectRequest),
    ScanMempool(MempoolRequest),
    Hydrate {
        anchor_root: Root,
        selection_error: String,
    },
}

pub(crate) enum MempoolTransition {
    Continue,
    Prove(ProveRequest),
    Hydrate {
        anchor_root: Root,
        selection_error: String,
    },
}

/// Deterministic correlation for the asynchronous wallet-send lifecycle.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletSendState {
    pending: BTreeMap<RpcId, Phase>,
    owner_by_rpc: BTreeMap<RpcId, Owner>,
    rpc_by_owner: BTreeMap<Owner, RpcId>,
    mempool_scan_ready: BTreeSet<RpcId>,
    last_mempool_scan_rpc: Option<RpcId>,
}

impl WalletSendState {
    pub fn has_work(&self, rpc_id: RpcId) -> bool {
        self.pending.contains_key(&rpc_id) || self.owner_by_rpc.contains_key(&rpc_id)
    }

    pub fn owner_is_available(&self, rpc_id: RpcId, owner: Owner) -> bool {
        self.rpc_by_owner
            .get(&owner)
            .is_none_or(|reserved| *reserved == rpc_id)
    }

    pub(crate) fn reserve_owner(&mut self, rpc_id: RpcId, owner: Owner) -> bool {
        if !self.owner_is_available(rpc_id, owner) {
            return false;
        }
        if let Some(previous) = self.owner_by_rpc.insert(rpc_id, owner)
            && previous != owner
        {
            self.rpc_by_owner.remove(&previous);
        }
        self.rpc_by_owner.insert(owner, rpc_id);
        true
    }

    pub(crate) fn release_owner(&mut self, rpc_id: RpcId) {
        if let Some(owner) = self.owner_by_rpc.remove(&rpc_id)
            && self.rpc_by_owner.get(&owner) == Some(&rpc_id)
        {
            self.rpc_by_owner.remove(&owner);
        }
    }

    pub fn can_start(&self, rpc_id: RpcId) -> bool {
        !self.pending.contains_key(&rpc_id)
    }

    pub(crate) fn begin_collecting(
        &mut self,
        request_id: RequestId,
        rpc_id: RpcId,
        anchor_root: Root,
        timestamp_ms: u64,
        req: SendRequest,
        mempool_cursor: OwnedOutputsCursor,
    ) -> Result<CollectRequest, String> {
        self.mempool_scan_ready.remove(&rpc_id);
        if mempool_cursor.owner() != req.from {
            return Err("wallet-send mempool cursor owner mismatch".to_owned());
        }
        let selector = InputSelector::new(req.amount, req.fee.unwrap_or(0))?;
        let collection = Collection {
            request_id,
            anchor_root,
            timestamp_ms,
            req,
            selector,
            mempool_cursor,
            source: CollectionSource::Persisted {
                chunk_seq: 0,
                next_index: 0,
            },
        };
        let request = persisted_request(rpc_id, &collection)
            .ok_or_else(|| "wallet-send persisted collection phase missing".to_owned())?;
        self.pending
            .insert(rpc_id, Phase::Collecting(Box::new(collection)));
        Ok(request)
    }

    pub fn is_collecting_persisted(&self, request: &CollectRequest) -> bool {
        self.pending.get(&request.rpc_id).is_some_and(|phase| {
            let Phase::Collecting(collection) = phase else {
                return false;
            };
            matches!(&collection.source, CollectionSource::Persisted { .. })
                && persisted_request(request.rpc_id, collection).as_ref() == Some(request)
        })
    }

    pub fn is_collecting_mempool(&self, request: &MempoolRequest) -> bool {
        self.pending.get(&request.rpc_id).is_some_and(|phase| {
            let Phase::Collecting(collection) = phase else {
                return false;
            };
            matches!(&collection.source, CollectionSource::Mempool { .. })
                && mempool_request(request.rpc_id, collection).as_ref() == Some(request)
        })
    }

    pub fn collection_matches_rpc_request(
        &self,
        rpc_id: RpcId,
        request_id: RequestId,
        current: &SendRequest,
    ) -> bool {
        self.collection(rpc_id, request_id)
            .is_some_and(|collection| collection.req == *current)
    }

    pub fn collection_pins(
        &self,
        rpc_id: RpcId,
        request_id: RequestId,
    ) -> Option<(Root, OwnedOutputsCursor)> {
        let collection = self.collection(rpc_id, request_id)?;
        Some((collection.anchor_root, collection.mempool_cursor))
    }

    /// The next owner whose mempool scan continues, round robin. `Err` when
    /// readiness names an RPC that is not collecting from the mempool.
    pub fn next_mempool_scan_request(&self) -> Result<Option<MempoolRequest>, RpcId> {
        let Some(rpc_id) = self.next_mempool_scan_rpc() else {
            return Ok(None);
        };
        let Some(Phase::Collecting(collection)) = self.pending.get(&rpc_id) else {
            return Err(rpc_id);
        };
        mempool_request(rpc_id, collection).map(Some).ok_or(rpc_id)
    }

    fn next_mempool_scan_rpc(&self) -> Option<RpcId> {
        let after_last = self.last_mempool_scan_rpc.and_then(|rpc_id| {
            self.mempool_scan_ready
                .range((Excluded(rpc_id), Unbounded))
                .next()
                .copied()
        });
        after_last.or_else(|| self.mempool_scan_ready.first().copied())
    }

    pub(crate) fn apply_persisted_chunk(
        &mut self,
        request: &CollectRequest,
        result: &Result<OwnedUtxosChunk, String>,
        spent: &BTreeSet<Commitment>,
    ) -> Option<PersistedTransition> {
        let phase = self.pending.remove(&request.rpc_id)?;
        let Phase::Collecting(mut collection) = phase else {
            self.pending.insert(request.rpc_id, phase);
            return None;
        };
        if persisted_request(request.rpc_id, &collection).as_ref() != Some(request) {
            self.pending
                .insert(request.rpc_id, Phase::Collecting(collection));
            return None;
        }
        if let Ok(chunk) = result {
            if let Err(error) = validate_persisted_chunk(request, chunk) {
                self.mempool_scan_ready.remove(&request.rpc_id);
                return Some(PersistedTransition::Hydrate {
                    anchor_root: collection.anchor_root,
                    selection_error: error,
                });
            }
            for (commitment, utxo) in &chunk.utxos {
                if spent.contains(commitment) {
                    continue;
                }
                if let Err(selection_error) = collection.selector.consider(*commitment, utxo) {
                    self.mempool_scan_ready.remove(&request.rpc_id);
                    return Some(PersistedTransition::Hydrate {
                        anchor_root: collection.anchor_root,
                        selection_error,
                    });
                }
            }
            if !chunk.complete {
                collection.source = CollectionSource::Persisted {
                    chunk_seq: request.chunk_seq.saturating_add(1),
                    next_index: chunk.next_index,
                };
                let Some(next) = persisted_request(request.rpc_id, &collection) else {
                    return Some(PersistedTransition::Hydrate {
                        anchor_root: collection.anchor_root,
                        selection_error: "wallet-send persisted collection phase missing"
                            .to_owned(),
                    });
                };
                self.pending
                    .insert(request.rpc_id, Phase::Collecting(collection));
                return Some(PersistedTransition::Collect(next));
            }
        }
        // Complete, or a failed read: go on with the candidates so far, from
        // the mempool.
        collection.source = CollectionSource::Mempool {
            chunk_seq: request.chunk_seq.saturating_add(1),
        };
        self.mempool_scan_ready.insert(request.rpc_id);
        let Some(next) = mempool_request(request.rpc_id, &collection) else {
            self.mempool_scan_ready.remove(&request.rpc_id);
            return Some(PersistedTransition::Hydrate {
                anchor_root: collection.anchor_root,
                selection_error: "wallet-send mempool collection phase missing".to_owned(),
            });
        };
        self.pending
            .insert(request.rpc_id, Phase::Collecting(collection));
        Some(PersistedTransition::ScanMempool(next))
    }

    pub(crate) fn apply_mempool_chunk(
        &mut self,
        request: &MempoolRequest,
        chunk: OwnedOutputsChunk,
    ) -> Option<MempoolTransition> {
        let phase = self.pending.remove(&request.rpc_id)?;
        let Phase::Collecting(mut collection) = phase else {
            self.pending.insert(request.rpc_id, phase);
            return None;
        };
        if mempool_request(request.rpc_id, &collection).as_ref() != Some(request) {
            self.pending
                .insert(request.rpc_id, Phase::Collecting(collection));
            return None;
        }
        if chunk.items.len() > request.max_matches as usize
            || chunk.scanned_positions > request.max_scan_positions
            || chunk.items.len() > chunk.scanned_positions as usize
        {
            self.mempool_scan_ready.remove(&request.rpc_id);
            return Some(MempoolTransition::Hydrate {
                anchor_root: collection.anchor_root,
                selection_error: "invalid wallet-send mempool UTXO chunk bounds".to_owned(),
            });
        }
        self.last_mempool_scan_rpc = Some(request.rpc_id);
        for (commitment, utxo) in chunk.items {
            if let Err(selection_error) = collection.selector.consider(commitment, &utxo) {
                self.mempool_scan_ready.remove(&request.rpc_id);
                return Some(MempoolTransition::Hydrate {
                    anchor_root: collection.anchor_root,
                    selection_error,
                });
            }
        }
        if let Some(next_cursor) = chunk.next {
            if next_cursor == collection.mempool_cursor
                || next_cursor.owner() != collection.req.from
            {
                self.mempool_scan_ready.remove(&request.rpc_id);
                return Some(MempoolTransition::Hydrate {
                    anchor_root: collection.anchor_root,
                    selection_error: "invalid wallet-send mempool UTXO cursor progress".to_owned(),
                });
            }
            collection.mempool_cursor = next_cursor;
            collection.source = CollectionSource::Mempool {
                chunk_seq: request.chunk_seq.saturating_add(1),
            };
            self.pending
                .insert(request.rpc_id, Phase::Collecting(collection));
            return Some(MempoolTransition::Continue);
        }
        let available_utxos = match collection.selector.finish() {
            Ok(candidates) => candidates,
            Err(selection_error) => {
                self.mempool_scan_ready.remove(&request.rpc_id);
                return Some(MempoolTransition::Hydrate {
                    anchor_root: collection.anchor_root,
                    selection_error,
                });
            }
        };
        let prove = ProveRequest {
            request_id: collection.request_id,
            rpc_id: request.rpc_id,
            anchor_root: collection.anchor_root,
            timestamp_ms: collection.timestamp_ms,
            req: collection.req,
            available_utxos,
        };
        self.pending.insert(
            request.rpc_id,
            Phase::Proving {
                request_id: prove.request_id,
            },
        );
        self.mempool_scan_ready.remove(&request.rpc_id);
        Some(MempoolTransition::Prove(prove))
    }

    pub(crate) fn finish_collecting(&mut self, rpc_id: RpcId, request_id: RequestId) -> bool {
        if self.collection(rpc_id, request_id).is_none() {
            return false;
        }
        self.pending.remove(&rpc_id);
        self.mempool_scan_ready.remove(&rpc_id);
        true
    }

    pub fn is_proving(&self, rpc_id: RpcId, request_id: RequestId) -> bool {
        matches!(self.pending.get(&rpc_id), Some(Phase::Proving { request_id: actual }) if *actual == request_id)
    }

    pub(crate) fn finish_proving(&mut self, rpc_id: RpcId, request_id: RequestId) -> bool {
        if !self.is_proving(rpc_id, request_id) {
            return false;
        }
        self.pending.remove(&rpc_id);
        self.mempool_scan_ready.remove(&rpc_id);
        true
    }

    pub(crate) fn cancel(&mut self, rpc_id: RpcId) -> bool {
        let had_pending = self.pending.remove(&rpc_id).is_some();
        let had_owner = self.owner_by_rpc.contains_key(&rpc_id);
        self.mempool_scan_ready.remove(&rpc_id);
        self.release_owner(rpc_id);
        had_pending || had_owner
    }

    fn collection(&self, rpc_id: RpcId, request_id: RequestId) -> Option<&Collection> {
        match self.pending.get(&rpc_id)? {
            Phase::Collecting(collection) if collection.request_id == request_id => {
                Some(collection)
            }
            _ => None,
        }
    }

    /// The RPCs with work in progress.
    pub fn working(&self) -> impl Iterator<Item = RpcId> + '_ {
        self.pending.keys().copied()
    }

    /// The structural invariants the lifecycle relies on: the two
    /// reservation maps are each other's inverse, so an owner has at most one send; every send in
    /// progress holds its owner; and only an RPC collecting from the mempool
    /// is ready for a mempool scan.
    pub fn invariants(&self) -> Result<(), String> {
        for (rpc_id, owner) in &self.owner_by_rpc {
            if self.rpc_by_owner.get(owner) != Some(rpc_id) {
                return Err(format!(
                    "{rpc_id:?} holds {owner:?}, which is reserved for {:?}",
                    self.rpc_by_owner.get(owner)
                ));
            }
        }
        for (owner, rpc_id) in &self.rpc_by_owner {
            if self.owner_by_rpc.get(rpc_id) != Some(owner) {
                return Err(format!(
                    "{owner:?} is reserved for {rpc_id:?}, which holds {:?}",
                    self.owner_by_rpc.get(rpc_id)
                ));
            }
        }
        if let Some(rpc_id) = self
            .pending
            .keys()
            .find(|rpc_id| !self.owner_by_rpc.contains_key(rpc_id))
        {
            return Err(format!(
                "{rpc_id:?} is in progress without its owner reserved"
            ));
        }
        for rpc_id in &self.mempool_scan_ready {
            let collecting_mempool = matches!(
                self.pending.get(rpc_id),
                Some(Phase::Collecting(collection)) if matches!(collection.source, CollectionSource::Mempool { .. })
            );
            if !collecting_mempool {
                return Err(format!(
                    "{rpc_id:?} is ready for a mempool scan but not collecting from the mempool"
                ));
            }
        }
        Ok(())
    }
}

fn persisted_request(rpc_id: RpcId, collection: &Collection) -> Option<CollectRequest> {
    let CollectionSource::Persisted {
        chunk_seq,
        next_index,
    } = &collection.source
    else {
        return None;
    };
    Some(CollectRequest {
        request_id: collection.request_id,
        rpc_id,
        chunk_seq: *chunk_seq,
        anchor_root: collection.anchor_root,
        owner: collection.req.from,
        next_index: *next_index,
        max_scan_positions: OWNER_MAX_SCAN_POSITIONS,
        max_matches: OWNER_MAX_MATCHES,
        mempool_cursor: collection.mempool_cursor,
    })
}

fn mempool_request(rpc_id: RpcId, collection: &Collection) -> Option<MempoolRequest> {
    let CollectionSource::Mempool { chunk_seq } = &collection.source else {
        return None;
    };
    Some(MempoolRequest {
        request_id: collection.request_id,
        rpc_id,
        chunk_seq: *chunk_seq,
        anchor_root: collection.anchor_root,
        cursor: collection.mempool_cursor,
        max_scan_positions: MEMPOOL_MAX_SCAN_POSITIONS,
        max_matches: MEMPOOL_MAX_MATCHES,
    })
}

pub fn validate_persisted_chunk(
    request: &CollectRequest,
    chunk: &OwnedUtxosChunk,
) -> Result<(), String> {
    let advanced = chunk.next_index.saturating_sub(request.next_index);
    let bounds_valid = chunk.utxos.len() <= request.max_matches as usize
        && chunk.scanned_positions <= request.max_scan_positions
        && chunk.utxos.len() <= chunk.scanned_positions as usize
        && chunk.next_index >= request.next_index
        && chunk.next_index <= chunk.frontier_exclusive
        && advanced == u64::from(chunk.scanned_positions)
        && chunk.complete == (chunk.next_index == chunk.frontier_exclusive)
        && (chunk.complete || chunk.next_index > request.next_index);
    if bounds_valid {
        Ok(())
    } else {
        Err("invalid wallet-send persisted UTXO chunk bounds or cursor progress".to_owned())
    }
}
