//! Wallet send's transitions, one scenario per test: paging, stale pins,
//! mempool spends and outputs, round-robin mempool scans, cancellation,
//! late results and rejections. Where a test needs the world to change
//! (the mempool alone moving to a new tip, an RPC dropped unanswered), it
//! dispatches a world Action (`MempoolTip`, `RpcGone`): a Store has no back
//! door into its State.

mod common;

use causet::DispatchOutcome::{Disabled, Executed};
use common::*;
use wallet_send::domain::{SendResponse, SubmittedTx, Utxo};
use wallet_send::node::{NodeAction, NodeState, RpcRequest, RpcStatus};
use wallet_send::state::{MEMPOOL_MAX_SCAN_POSITIONS, ProveRequest};

fn start(f: &mut Fixture, rpc_id: wallet_send::domain::RpcId) -> causet::DispatchOutcome {
    f.node.dispatch(&f.mount, NodeAction::Start { rpc_id })
}

fn persisted(
    f: &mut Fixture,
    action: (
        wallet_send::state::CollectRequest,
        Result<wallet_send::state::OwnedUtxosChunk, String>,
    ),
) -> causet::DispatchOutcome {
    let (request, result) = action;
    f.node
        .dispatch(&f.mount, NodeAction::PersistedPage { request, result })
}

fn last_prove(f: &Fixture) -> Option<ProveRequest> {
    match f.rec.get().asked.last() {
        Some(Asked::Prove(request)) => Some(request.clone()),
        _ => None,
    }
}

fn commitments(
    utxos: &[(wallet_send::domain::Commitment, Utxo)],
) -> Vec<wallet_send::domain::Commitment> {
    utxos.iter().map(|(c, _)| *c).collect()
}

#[test]
fn bounded_chunks_preserve_split_funds_across_pages() {
    let mut f = fixture(state_with_pending_request());
    assert_eq!(start(&mut f, rpc_id()), Executed);
    let first = collect_request(&f.rec.get().asked, 0);
    assert_eq!(
        (first.chunk_seq, first.next_index, first.anchor_root),
        (0, 0, ROOT)
    );
    assert!(f.node.state().wallet_send.is_collecting_persisted(&first));

    let mut wrong = first.clone();
    wrong.chunk_seq = 99;
    let wrong_page = page(&wrong, Vec::new(), 0, 0, true);
    assert_eq!(persisted(&mut f, (wrong, wrong_page)), Disabled);
    assert_eq!(f.rec.get().asked.len(), 1);

    let (early_commitment, early) = wallet_utxo(4, 2);
    let early_page = page(&first, vec![(early_commitment, early)], 2, 1, false);
    assert_eq!(persisted(&mut f, (first.clone(), early_page)), Executed);
    let second = collect_request(&f.rec.get().asked, 1);
    assert_eq!(
        (second.request_id, second.chunk_seq, second.next_index),
        (first.request_id, 1, 1)
    );

    let (later_commitment, later) = wallet_utxo(6, 3);
    let later_page = page(&second, vec![(later_commitment, later)], 2, 2, true);
    assert_eq!(persisted(&mut f, (second, later_page)), Executed);
    assert_eq!(f.rec.get().asked.len(), 3);
    let prove = last_prove(&f).expect("proving starts");
    assert_eq!(prove.request_id, first.request_id);
    assert_eq!(
        commitments(&prove.available_utxos),
        [later_commitment, early_commitment]
    );
    assert!(
        f.node
            .state()
            .wallet_send
            .is_proving(rpc_id(), first.request_id)
    );
}

#[test]
fn mempool_generation_change_restarts_with_a_fresh_request() {
    let mut f = fixture(state_with_pending_request());
    assert_eq!(start(&mut f, rpc_id()), Executed);
    let first = collect_request(&f.rec.get().asked, 0);
    f.node.dispatch(
        &f.mount,
        NodeAction::MempoolTip {
            tip: wallet_send::domain::BlockHash(9),
        },
    );
    let empty = page(&first, Vec::new(), 0, 0, true);
    assert_eq!(persisted(&mut f, (first.clone(), empty)), Executed);

    let restarted = collect_request(&f.rec.get().asked, 1);
    assert!(restarted.request_id > first.request_id, "a fresh request");
    assert_eq!(restarted.chunk_seq, 0);
    assert!(
        f.node
            .state()
            .wallet_send
            .is_collecting_persisted(&restarted)
    );
    assert!(
        !f.node
            .state()
            .wallet_send
            .owner_is_available(second_rpc_id(), owner())
    );
}

#[test]
fn persisted_spends_are_excluded_and_mempool_outputs_join_selection() {
    let mut state = state_with_pending_request();
    let (spent_commitment, spent) = wallet_utxo(11, 20);
    let (retained_commitment, retained) = wallet_utxo(20, 21);
    let (created_commitment, created) = wallet_utxo(12, 22);
    state.mempool.accept(&[spent_commitment], &[created]);
    let mut f = fixture(state);

    assert_eq!(start(&mut f, rpc_id()), Executed);
    let request = collect_request(&f.rec.get().asked, 0);
    let result = page(
        &request,
        vec![(spent_commitment, spent), (retained_commitment, retained)],
        2,
        2,
        true,
    );
    assert_eq!(persisted(&mut f, (request, result)), Executed);
    let prove = last_prove(&f).expect("proving starts");
    assert_eq!(
        commitments(&prove.available_utxos),
        [retained_commitment, created_commitment]
    );
}

#[test]
fn transfer_selection_continues_across_multiple_mempool_pages() {
    let mut state = state_with_pending_request();
    let mut outputs: Vec<Utxo> = (1..=64).map(|nonce| wallet_utxo(20, nonce).1).collect();
    let winner = (65..=255)
        .map(|nonce| wallet_utxo(10, nonce))
        .find(|(candidate, _)| {
            outputs
                .iter()
                .filter(|utxo| utxo.commitment() < *candidate)
                .count()
                >= MEMPOOL_MAX_SCAN_POSITIONS as usize
        })
        .expect("a winner that sorts beyond the first mempool page");
    let winner_commitment = winner.0;
    outputs.push(winner.1);
    state.mempool.accept(&[], &outputs);
    let mut f = fixture(state);

    assert_eq!(start(&mut f, rpc_id()), Executed);
    let request = collect_request(&f.rec.get().asked, 0);
    let empty = page(&request, Vec::new(), 0, 0, true);
    assert_eq!(persisted(&mut f, (request, empty)), Executed);
    assert_eq!(f.rec.get().scans.len(), 1);
    assert!(last_prove(&f).is_none());

    // The continuation survives a save and a restore of State.
    let saved = serde_json::to_string(f.node.state()).expect("State encodes");
    let restored: NodeState = serde_json::from_str(&saved).expect("State decodes");
    assert_eq!(
        restored.wallet_send.next_mempool_scan_request(),
        f.node.state().wallet_send.next_mempool_scan_request()
    );

    f.node.dispatch(&f.mount, NodeAction::CheckTimeouts);
    assert_eq!(f.rec.get().scans.len(), 2);
    assert!(last_prove(&f).is_none());

    f.node.dispatch(&f.mount, NodeAction::CheckTimeouts);
    assert_eq!(f.rec.get().scans.len(), 3);
    let prove = last_prove(&f).expect("proving starts");
    assert_eq!(prove.available_utxos.len(), 65);
    assert!(
        prove
            .available_utxos
            .iter()
            .any(|(c, _)| *c == winner_commitment)
    );

    f.node.dispatch(&f.mount, NodeAction::CheckTimeouts);
    assert_eq!(f.rec.get().scans.len(), 3);
}

#[test]
fn concurrent_mempool_scans_rotate_after_their_immediate_pages() {
    let mut state = state_with_pending_request();
    state.rpc.insert(
        second_rpc_id(),
        RpcRequest {
            req: request_for(second_owner()),
            status: RpcStatus::Pending,
        },
    );
    let first: Vec<Utxo> = (1..=33)
        .map(|nonce| wallet_utxo_for(owner(), 20, nonce).1)
        .collect();
    let second: Vec<Utxo> = (1..=33)
        .map(|nonce| wallet_utxo_for(second_owner(), 20, nonce).1)
        .collect();
    state.mempool.accept(&[], &first);
    state.mempool.accept(&[], &second);
    let mut f = fixture(state);

    assert_eq!(start(&mut f, rpc_id()), Executed);
    let first_request = collect_request(&f.rec.get().asked, 0);
    let empty = page(&first_request, Vec::new(), 0, 0, true);
    assert_eq!(persisted(&mut f, (first_request, empty)), Executed);
    assert_eq!(start(&mut f, second_rpc_id()), Executed);
    let second_request = collect_request(&f.rec.get().asked, 1);
    let empty = page(&second_request, Vec::new(), 0, 0, true);
    assert_eq!(persisted(&mut f, (second_request, empty)), Executed);

    f.node.dispatch(&f.mount, NodeAction::CheckTimeouts);
    f.node.dispatch(&f.mount, NodeAction::CheckTimeouts);
    assert_eq!(
        f.rec.get().scans,
        [rpc_id(), second_rpc_id(), rpc_id(), second_rpc_id()]
    );
}

#[test]
fn equal_persisted_and_mempool_candidates_keep_the_persisted_input() {
    let mut state = state_with_pending_request();
    let (persisted_commitment, persisted_utxo) = wallet_utxo(10, 70);
    state.mempool.accept(&[], &[wallet_utxo(10, 71).1]);
    let mut f = fixture(state);

    assert_eq!(start(&mut f, rpc_id()), Executed);
    let request = collect_request(&f.rec.get().asked, 0);
    let result = page(
        &request,
        vec![(persisted_commitment, persisted_utxo)],
        1,
        1,
        true,
    );
    assert_eq!(persisted(&mut f, (request, result)), Executed);
    let prove = last_prove(&f).expect("proving starts");
    assert_eq!(
        prove.available_utxos.first().map(|(c, _)| *c),
        Some(persisted_commitment)
    );
}

#[test]
fn cancellation_disables_a_late_chunk_without_continuation() {
    let mut f = fixture(state_with_pending_request());
    assert_eq!(start(&mut f, rpc_id()), Executed);
    let request = collect_request(&f.rec.get().asked, 0);
    assert_eq!(
        f.node
            .dispatch(&f.mount, NodeAction::Cancel { rpc_id: rpc_id() }),
        Executed
    );
    let late = page(&request, Vec::new(), 0, 0, true);
    assert_eq!(persisted(&mut f, (request, late)), Disabled);
    assert_eq!(f.rec.get().asked.len(), 1);
    assert!(!f.node.state().wallet_send.has_work(rpc_id()));
}

#[test]
fn current_collecting_state_roundtrips() {
    let mut f = fixture(state_with_pending_request());
    assert_eq!(start(&mut f, rpc_id()), Executed);
    let request = collect_request(&f.rec.get().asked, 0);
    let saved = serde_json::to_string(f.node.state()).expect("State encodes");
    let restored: NodeState = serde_json::from_str(&saved).expect("State decodes");
    assert!(restored.wallet_send.is_collecting_persisted(&request));
}

#[test]
fn late_prepared_result_clears_correlation_without_dispatching_children() {
    let mut f = fixture(state_with_pending_request());
    assert_eq!(start(&mut f, rpc_id()), Executed);
    let request = collect_request(&f.rec.get().asked, 0);
    let (commitment, utxo) = wallet_utxo(10, 4);
    let result = page(&request, vec![(commitment, utxo)], 1, 1, true);
    assert_eq!(persisted(&mut f, (request.clone(), result)), Executed);
    f.node
        .dispatch(&f.mount, NodeAction::RpcGone { rpc_id: rpc_id() });
    let (asked, mempool) = (f.rec.get().asked.len(), f.node.state().mempool.clone());

    let tx = SubmittedTx {
        hash: 1,
        inputs: Vec::new(),
        outputs: Vec::new(),
    };
    let proved = NodeAction::Proved {
        request_id: request.request_id,
        rpc_id: rpc_id(),
        result: Ok(tx),
    };
    assert_eq!(f.node.dispatch(&f.mount, proved), Executed);
    assert!(!f.node.state().wallet_send.has_work(rpc_id()));
    assert_eq!(f.node.state().mempool, mempool);
    assert!(f.node.state().rpc.is_empty());
    assert_eq!(f.rec.get().asked.len(), asked);
    assert!(f.rec.get().responses.is_empty());
}

/// A node whose send is already waiting for hydration.
fn with_hydration_bookkeeping(mut state: NodeState) -> NodeState {
    state.rpc.get_mut(&rpc_id()).unwrap().status = RpcStatus::Syncing;
    state.ledger.attempted.insert(rpc_id());
    let id = |index| causet::RequestId {
        session: causet::SessionId(1),
        execution: causet::ExecutionId(1),
        index,
    };
    let hydration = wallet_send::partial_ledger::Hydration {
        query: id(0),
        owner: owner(),
        anchor_root: ROOT,
        stage: wallet_send::partial_ledger::Stage::Waiting { retry: id(1) },
    };
    state.ledger.hydrations.insert(rpc_id(), hydration);
    state
}

#[test]
fn start_stage_rejection_forgets_wallet_spend_hydration() {
    let mut f = fixture(with_hydration_bookkeeping(state_with_pending_request()));
    assert_eq!(start(&mut f, rpc_id()), Executed);
    let request = collect_request(&f.rec.get().asked, 0);
    let (commitment, utxo) = wallet_utxo(10, 80);
    let result = page(&request, vec![(commitment, utxo)], 1, 1, true);
    assert_eq!(persisted(&mut f, (request.clone(), result)), Executed);
    assert!(
        f.node
            .state()
            .wallet_send
            .is_proving(rpc_id(), request.request_id)
    );

    let rejected = NodeAction::Proved {
        request_id: request.request_id,
        rpc_id: rpc_id(),
        result: Err("prover queue unavailable".to_owned()),
    };
    assert_eq!(f.node.dispatch(&f.mount, rejected), Executed);
    assert!(f.node.state().ledger.forgotten(rpc_id()));
    assert!(!f.node.state().wallet_send.has_work(rpc_id()));
    assert!(!f.node.state().rpc.contains_key(&rpc_id()));
    assert_eq!(
        f.rec.get().responses,
        [(
            rpc_id(),
            SendResponse::rejected("prover queue unavailable".to_owned())
        )]
    );
}

#[test]
fn start_plan_rejection_forgets_wallet_spend_hydration() {
    let mut state = with_hydration_bookkeeping(state_with_pending_request());
    state.rpc.get_mut(&rpc_id()).unwrap().req.amount = 0;
    let mut f = fixture(state);

    assert_eq!(start(&mut f, rpc_id()), Executed);
    assert!(f.node.state().ledger.forgotten(rpc_id()));
    assert!(!f.node.state().wallet_send.has_work(rpc_id()));
    assert!(!f.node.state().rpc.contains_key(&rpc_id()));
    assert_eq!(f.rec.get().responses.len(), 1);
    assert!(f.rec.get().asked.is_empty());
}
