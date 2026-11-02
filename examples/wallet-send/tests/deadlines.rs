//! A send waits for each answer at most `ANSWER_TIMEOUT`: past it, the send
//! gives up and is answered, and a late answer changes nothing. An answer in
//! time leaves its deadline to fire stale.

mod common;

use causet::DispatchOutcome::Disabled;
use common::*;
use wallet_send::domain::{SendResponse, SubmittedTx};
use wallet_send::node::{ANSWER_TIMEOUT, NodeAction};

fn timeout_ns() -> u64 {
    ANSWER_TIMEOUT.as_nanos() as u64
}

/// Starts the send and answers its page, so it waits for its proof.
fn waiting_for_the_proof(f: &mut Fixture) -> wallet_send::state::ProveRequest {
    f.node
        .dispatch(&f.mount, NodeAction::Start { rpc_id: rpc_id() });
    let request = f.rec.get().collects[0].request.clone();
    let (commitment, utxo) = wallet_utxo(10, 1);
    let result = page(&request, vec![(commitment, utxo)], 1, 1, true);
    f.node
        .dispatch(&f.mount, NodeAction::PersistedPage { request, result });
    f.rec.get().proves[0].request.clone()
}

#[test]
fn a_lost_proof_times_out_and_a_late_one_changes_nothing() {
    let mut f = fixture(state_with_pending_request());
    let prove = waiting_for_the_proof(&mut f);

    f.clock.advance(timeout_ns());
    assert_eq!(
        f.node.fire_timers(),
        2,
        "the page's deadline, stale, and the proof's"
    );
    while f.node.deliver_next().is_some() {}
    assert_eq!(
        f.rec.get().responses,
        [(
            rpc_id(),
            SendResponse::rejected("timed out waiting for an answer".to_owned())
        )]
    );
    assert!(!f.node.state().wallet_send.has_work(rpc_id()));
    assert!(
        f.node
            .state()
            .wallet_send
            .owner_is_available(second_rpc_id(), owner())
    );
    assert!(f.node.state().deadlines.is_empty());

    let tx = SubmittedTx {
        hash: 1,
        inputs: Vec::new(),
        outputs: Vec::new(),
    };
    let late = NodeAction::Proved {
        request_id: prove.request_id,
        rpc_id: rpc_id(),
        result: Ok(tx),
    };
    assert_eq!(f.node.dispatch(&f.mount, late), Disabled);
    assert_eq!(f.rec.get().responses.len(), 1);
}

#[test]
fn an_answer_in_time_leaves_its_deadline_to_fire_stale() {
    let mut f = fixture(state_with_pending_request());
    let prove = waiting_for_the_proof(&mut f);
    let tx = SubmittedTx {
        hash: 1,
        inputs: Vec::new(),
        outputs: Vec::new(),
    };
    let proved = NodeAction::Proved {
        request_id: prove.request_id,
        rpc_id: rpc_id(),
        result: Ok(tx),
    };
    f.node.dispatch(&f.mount, proved);
    assert_eq!(f.rec.get().responses.len(), 1, "queued");

    f.clock.advance(timeout_ns());
    f.node.fire_timers();
    while f.node.deliver_next().is_some() {}
    assert_eq!(f.rec.get().responses.len(), 1, "no second answer");
    assert!(f.rec.get().responses[0].1.queued);
}

#[test]
fn a_cancelled_attempt_s_deadline_cannot_end_its_replacement() {
    let mut f = fixture(state_with_pending_request());
    f.node
        .dispatch(&f.mount, NodeAction::Start { rpc_id: rpc_id() });
    assert_eq!(f.rec.get().collects.len(), 1);

    // Halfway through the timeout, the attempt is cancelled and replaced.
    f.clock.advance(timeout_ns() / 2);
    f.node
        .dispatch(&f.mount, NodeAction::Cancel { rpc_id: rpc_id() });
    f.node
        .dispatch(&f.mount, NodeAction::Start { rpc_id: rpc_id() });
    assert_eq!(f.rec.get().collects.len(), 2);

    // The cancelled attempt's deadline passes: the replacement goes on.
    f.clock.advance(timeout_ns() / 2);
    f.node.fire_timers();
    while f.node.deliver_next().is_some() {}
    assert!(
        f.rec.get().responses.is_empty(),
        "{:?}",
        f.rec.get().responses
    );
    let replacement = f.rec.get().collects[1].request.clone();
    assert!(
        f.node
            .state()
            .wallet_send
            .is_collecting_persisted(&replacement)
    );

    // Its own deadline still holds.
    f.clock.advance(timeout_ns() / 2);
    f.node.fire_timers();
    while f.node.deliver_next().is_some() {}
    assert_eq!(f.rec.get().responses.len(), 1);
}
