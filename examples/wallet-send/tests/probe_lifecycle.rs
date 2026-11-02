//! Partial-ledger sync's probe lifecycle and wallet-spend hydration, step by
//! step: a probe that hydrates a send, the identities that make stale
//! reports inert, disconnects, admission, and waiting for the wallet.

mod common;

use causet::RequestId;
use common::*;
use wallet_send::domain::SendResponse;
use wallet_send::ledger::{
    Ledger, PeerAnswer, ProbeRequest, ProbeResponse, ProvedItem, verify_response,
};
use wallet_send::node::NodeAction;
use wallet_send::partial_ledger::{
    Job, JobId, JobPhase, RETRY_COOLDOWN, Stage, Stored, Target, VerifyDone,
};
use wallet_send::peers::{LinkEvent, PeerId, REQUEST_TIMEOUT, WireAnswer};

fn setup(amounts: &[u64]) -> (Fixture, Ledger) {
    let mut f = fixture(state_with_pending_request());
    let ledger = ledger_of(amounts);
    f.tip(&ledger);
    f.learn(&ledger);
    f.connect(1, ledger.root());
    (f, ledger)
}

/// What an honest peer answers.
fn honest(ledger: &Ledger, request: &ProbeRequest) -> PeerAnswer {
    let items = request
        .items
        .iter()
        .map(|item| ProvedItem {
            index: item.index,
            utxo: ledger.leaves.get(&item.index).cloned(),
            proof: ledger.prove(item.index),
        })
        .collect();
    PeerAnswer::Data(ProbeResponse {
        root: request.root,
        items,
    })
}

/// Answers the oldest probe sent, and delivers.
fn answer(f: &mut Fixture, answer: impl FnOnce(&ProbeRequest) -> PeerAnswer) {
    let send = f.rec.get().sends.remove(0);
    let (peer, request_id, request) = &send.request;
    let event = WireAnswer {
        peer: *peer,
        request_id: *request_id,
        answer: answer(request),
    };
    send.reply.try_send_as_worker(event).unwrap();
    f.deliver_all();
}

/// Verifies the oldest response asked, as the verifier would, and delivers.
fn verify(f: &mut Fixture) {
    let ask = f.rec.get().verifies.remove(0);
    let v = &ask.request;
    let done = VerifyDone {
        job: v.job,
        peer: v.peer,
        request_id: v.request_id,
        request: v.request.clone(),
        result: verify_response(&v.request, &v.response),
    };
    ask.reply.try_send_as_worker(done).unwrap();
    f.deliver_all();
}

fn ack_cache(f: &mut Fixture, ok: bool) {
    let store = f.rec.get().cache_stores.remove(0);
    let (rpc_id, query, root, _, _) = store.request;
    let ack = Stored::Cache {
        rpc_id,
        query,
        root,
        ok,
    };
    store.reply.try_send_as_worker(ack).unwrap();
    f.deliver_all();
}

fn ack_partial(f: &mut Fixture, ok: bool) {
    let store = f.rec.get().partial_stores.remove(0);
    let (rpc_id, query, root, _) = store.request;
    let ack = Stored::Partial {
        rpc_id,
        query,
        root,
        ok,
    };
    store.reply.try_send_as_worker(ack).unwrap();
    f.deliver_all();
}

fn stage(f: &Fixture) -> Option<Stage> {
    f.node
        .state()
        .ledger
        .hydrations
        .get(&rpc_id())
        .map(|h| h.stage.clone())
}

fn ns(duration: std::time::Duration) -> u64 {
    duration.as_nanos() as u64
}

#[test]
fn a_probe_hydrates_the_send_which_is_tried_again_once_both_stores_are_done() {
    for cache_first in [false, true] {
        let (mut f, ledger) = setup(&[30, 20]);
        f.reach_hydration();
        // The newest leaf covers the amount.
        let asked = f.rec.get().sends[0].request.clone();
        assert_eq!(asked.0, PeerId(1));
        assert_eq!(asked.2.items.len(), 1);
        assert_eq!(asked.2.items[0].index, 1);

        answer(&mut f, |request| honest(&ledger, request));
        verify(&mut f);
        assert!(matches!(stage(&f), Some(Stage::Storing { .. })));
        let stored = f.rec.get().partial_stores[0].request.3.clone();
        assert_eq!(stored, [(1, ledger.leaves[&1].clone())]);

        // Not before both stores are done, in either order.
        if cache_first {
            ack_cache(&mut f, true);
        } else {
            ack_partial(&mut f, true);
        }
        assert_eq!(f.rec.get().collects.len(), 1);
        if cache_first {
            ack_partial(&mut f, true);
        } else {
            ack_cache(&mut f, true);
        }
        assert_eq!(f.rec.get().collects.len(), 2, "tried again");
        let hydration_deadline = |a: &wallet_send::node::Awaited| {
            matches!(a.answer, wallet_send::node::Answer::Hydration(_))
        };
        assert!(!f.node.state().deadlines.values().any(hydration_deadline));

        // One probe per send: still short of funds, it is rejected.
        let request = f.rec.get().collects[1].request.clone();
        let result = page(&request, Vec::new(), 0, 0, true);
        f.node
            .dispatch(&f.mount, NodeAction::PersistedPage { request, result });
        assert_eq!(f.rec.get().sends.len(), 0);
        let responses = f.rec.get().responses.clone();
        assert_eq!(responses.len(), 1);
        assert!(
            responses[0]
                .1
                .error
                .as_deref()
                .unwrap()
                .contains("insufficient")
        );
    }
}

#[test]
fn a_store_failure_rejects_the_send() {
    let (mut f, ledger) = setup(&[30]);
    f.reach_hydration();
    answer(&mut f, |request| honest(&ledger, request));
    verify(&mut f);
    ack_cache(&mut f, false);
    assert_eq!(
        f.rec.get().responses,
        [(
            rpc_id(),
            SendResponse::rejected("hydration: storing the wallet cache failed".to_owned())
        )]
    );
    // The other store's acknowledgement, late, changes nothing.
    ack_partial(&mut f, true);
    assert_eq!(f.rec.get().collects.len(), 1);
}

#[test]
fn a_forgotten_hydration_s_probe_cannot_finish_its_replacement() {
    let (mut f, ledger) = setup(&[30]);
    f.connect(2, ledger.root());
    f.reach_hydration();
    f.node
        .dispatch(&f.mount, NodeAction::Cancel { rpc_id: rpc_id() });
    assert!(f.node.state().ledger.forgotten(rpc_id()));

    // The replacement probes the other peer: the first still has its job.
    f.reach_hydration();
    assert_eq!(f.rec.get().sends[1].request.0, PeerId(2));
    let Some(Stage::Probing { job }) = stage(&f) else {
        panic!("probing");
    };

    // The first probe fails, late: its result reaches no hydration.
    answer(&mut f, |_| PeerAnswer::Unavailable);
    assert!(
        f.rec.get().responses.is_empty(),
        "{:?}",
        f.rec.get().responses
    );
    assert_eq!(stage(&f), Some(Stage::Probing { job }));
    assert_eq!(f.node.state().ledger.jobs.len(), 1);
}

#[test]
fn another_start_waits_for_the_probe() {
    let (mut f, _) = setup(&[30]);
    f.connect(2, f.node.state().chain_root);
    f.reach_hydration();
    let start = NodeAction::Start { rpc_id: rpc_id() };
    assert_eq!(
        f.node.dispatch(&f.mount, start),
        causet::DispatchOutcome::Disabled
    );
    assert_eq!(f.rec.get().collects.len(), 1);
    assert_eq!(f.rec.get().sends.len(), 1, "one probe out at a time");
}

#[test]
fn a_probe_times_out_and_its_late_answer_changes_nothing() {
    let (mut f, ledger) = setup(&[30]);
    f.reach_hydration();
    f.clock.advance(ns(REQUEST_TIMEOUT));
    f.node.fire_timers();
    f.deliver_all();
    assert_eq!(
        f.rec.get().responses,
        [(
            rpc_id(),
            SendResponse::rejected("hydration probe failed: Timeout".to_owned())
        )]
    );
    assert!(f.node.state().peers.outstanding.is_empty());

    answer(&mut f, |request| honest(&ledger, request));
    assert!(
        f.rec.get().verifies.is_empty(),
        "dropped by the network layer"
    );
    assert_eq!(f.rec.get().responses.len(), 1);
}

#[test]
fn a_disconnect_fails_a_pending_probe() {
    let (mut f, _) = setup(&[30]);
    f.reach_hydration();
    let peer = PeerId(1);
    f.node.dispatch(
        &f.peers,
        wallet_send::peers::PeersAction::Disconnected { peer },
    );
    assert_eq!(
        f.rec.get().responses,
        [(
            rpc_id(),
            SendResponse::rejected("hydration probe failed: Disconnected".to_owned())
        )]
    );
    assert!(f.node.state().ledger.jobs.is_empty());
}

#[test]
fn a_disconnect_keeps_a_probe_being_verified() {
    let (mut f, ledger) = setup(&[30]);
    f.reach_hydration();
    answer(&mut f, |request| honest(&ledger, request));
    let link = LinkEvent::Disconnected { peer: PeerId(1) };
    f.node.sink().try_send_to_as_worker(&f.peers, link).unwrap();
    f.deliver_all();
    assert_eq!(f.node.state().ledger.jobs.len(), 1, "kept");
    verify(&mut f);
    assert!(f.node.state().ledger.jobs.is_empty());
    assert!(matches!(stage(&f), Some(Stage::Storing { .. })));
    assert!(f.rec.get().responses.is_empty());
}

#[test]
fn a_verification_for_another_exchange_ends_the_job() {
    let (mut f, ledger) = setup(&[30]);
    f.reach_hydration();
    answer(&mut f, |request| honest(&ledger, request));
    let ask = f.rec.get().verifies.remove(0);
    let v = &ask.request;
    let done = VerifyDone {
        job: v.job,
        peer: v.peer,
        request_id: RequestId {
            index: v.request_id.index + 100,
            ..v.request_id
        },
        request: v.request.clone(),
        result: verify_response(&v.request, &v.response),
    };
    ask.reply.try_send_as_worker(done).unwrap();
    f.deliver_all();
    assert!(f.node.state().ledger.jobs.is_empty());
    let responses = f.rec.get().responses.clone();
    assert!(
        responses[0]
            .1
            .error
            .as_deref()
            .unwrap()
            .contains("Mismatch")
    );
    assert!(f.rec.get().cache_stores.is_empty());
}

#[test]
fn an_answer_that_does_not_cover_the_request_is_not_verified() {
    let (mut f, ledger) = setup(&[30]);
    f.reach_hydration();
    answer(&mut f, |request| {
        let PeerAnswer::Data(mut response) = honest(&ledger, request) else {
            unreachable!()
        };
        response.items.push(response.items[0].clone());
        PeerAnswer::Data(response)
    });
    assert!(f.rec.get().verifies.is_empty());
    let responses = f.rec.get().responses.clone();
    assert!(responses[0].1.error.as_deref().unwrap().contains("twice"));
}

#[test]
fn a_bad_proof_rejects_the_send_and_stores_nothing() {
    let (mut f, ledger) = setup(&[30]);
    f.reach_hydration();
    answer(&mut f, |request| {
        let PeerAnswer::Data(mut response) = honest(&ledger, request) else {
            unreachable!()
        };
        response.items[0].utxo.as_mut().unwrap().amount += 1_000;
        PeerAnswer::Data(response)
    });
    verify(&mut f);
    let responses = f.rec.get().responses.clone();
    assert!(
        responses[0]
            .1
            .error
            .as_deref()
            .unwrap()
            .contains("BadProof")
    );
    assert!(f.rec.get().partial_stores.is_empty());
    assert_eq!(f.node.state().ledger.checked().count(), 0);
}

#[test]
fn a_probe_over_capacity_waits_and_the_send_is_tried_again() {
    let mut state = state_with_pending_request();
    let ledger = ledger_of(&[30]);
    // Four probes already in flight, to other peers.
    for n in 0..4 {
        let job = JobId(RequestId {
            session: causet::SessionId(9),
            execution: causet::ExecutionId(9),
            index: n,
        });
        let probe = Job {
            target: Target::SpendHydration {
                rpc_id: wallet_send::domain::RpcId(100 + u64::from(n)),
            },
            peer: PeerId(10 + n),
            request: ProbeRequest {
                root: ledger.root(),
                items: Vec::new(),
            },
            phase: JobPhase::Init,
        };
        state.ledger.jobs.insert(job, probe);
    }
    let mut f = fixture(state);
    f.tip(&ledger);
    f.learn(&ledger);
    f.connect(1, ledger.root());
    f.reach_hydration();
    assert!(f.rec.get().sends.is_empty());
    assert!(matches!(stage(&f), Some(Stage::Waiting { .. })));
    assert!(!f.node.state().ledger.attempted.contains(&rpc_id()));

    f.clock.advance(ns(RETRY_COOLDOWN));
    f.node.fire_timers();
    f.deliver_all();
    assert_eq!(f.rec.get().collects.len(), 2, "tried again");
    assert!(f.rec.get().responses.is_empty());
}

#[test]
fn an_unseeded_wallet_waits_and_a_seeded_one_rejects() {
    let mut f = fixture(state_with_pending_request());
    f.reach_hydration();
    assert!(matches!(stage(&f), Some(Stage::Waiting { .. })));
    assert!(f.rec.get().responses.is_empty());
    f.clock.advance(ns(RETRY_COOLDOWN));
    f.node.fire_timers();
    f.deliver_all();
    assert_eq!(f.rec.get().collects.len(), 2, "tried again");

    f.node.dispatch(
        &f.ledger,
        wallet_send::partial_ledger::PartialLedgerAction::Seeded,
    );
    let request = f.rec.get().collects[1].request.clone();
    let result = page(&request, Vec::new(), 0, 0, true);
    f.node
        .dispatch(&f.mount, NodeAction::PersistedPage { request, result });
    let responses = f.rec.get().responses.clone();
    assert!(
        responses[0]
            .1
            .error
            .as_deref()
            .unwrap()
            .contains("insufficient")
    );
    assert!(f.node.state().ledger.forgotten(rpc_id()));
    assert!(f.node.state().deadlines.is_empty());
}

/// Regression: a store acknowledgement names its hydration. One of a
/// hydration cancelled since cannot end the one that replaced it, at the
/// same RPC and root.
#[test]
fn an_old_store_acknowledgement_cannot_end_the_replacement() {
    let (mut f, ledger) = setup(&[30, 20]);
    f.reach_hydration();
    answer(&mut f, |request| honest(&ledger, request));
    verify(&mut f);
    let obsolete = f.rec.get().cache_stores.remove(0);
    let first_query = f.node.state().ledger.hydration_query(rpc_id()).unwrap();

    f.node
        .dispatch(&f.mount, NodeAction::Cancel { rpc_id: rpc_id() });
    f.reach_hydration();
    answer(&mut f, |request| honest(&ledger, request));
    verify(&mut f);
    let current_query = f.node.state().ledger.hydration_query(rpc_id()).unwrap();
    assert_ne!(first_query, current_query);
    assert!(matches!(stage(&f), Some(Stage::Storing { .. })));

    let (rpc_id, query, root, _, _) = obsolete.request;
    let ack = Stored::Cache {
        rpc_id,
        query,
        root,
        ok: false,
    };
    obsolete.reply.try_send_as_worker(ack).unwrap();
    f.deliver_all();
    let responses = f.rec.get().responses.clone();
    assert!(responses.is_empty(), "{responses:?}");
    assert_eq!(
        f.node.state().ledger.hydration_query(rpc_id),
        Some(current_query)
    );
}

/// Regression: a verification whose answer is lost does not hold its
/// peer once the send has ended.
#[test]
fn a_lost_verification_does_not_hold_its_peer_after_the_send_ends() {
    let (mut f, ledger) = setup(&[30]);
    f.reach_hydration();
    answer(&mut f, |request| honest(&ledger, request));
    let phase = f.node.state().ledger.jobs.values().next().unwrap().phase;
    assert!(matches!(phase, JobPhase::VerifyPending { .. }));

    // The verifier never answers; the send times out.
    f.clock.advance(ns(wallet_send::node::ANSWER_TIMEOUT));
    f.node.fire_timers();
    f.deliver_all();
    assert_eq!(f.rec.get().responses.len(), 1);
    assert!(f.node.state().ledger.hydrations.is_empty());
    assert!(
        f.node.state().ledger.jobs.is_empty(),
        "{:?}",
        f.node.state().ledger.jobs
    );
}

/// Regression: a leaf found is not held as checked until the partial
/// store has it, so a failed write leaves it for a later send to probe.
#[test]
fn a_failed_partial_write_can_be_probed_again() {
    let (mut f, ledger) = setup(&[30]);
    f.reach_hydration();
    answer(&mut f, |request| honest(&ledger, request));
    verify(&mut f);
    ack_cache(&mut f, true);
    ack_partial(&mut f, false);
    assert_eq!(f.rec.get().responses.len(), 1);
    assert_eq!(f.node.state().ledger.checked().count(), 0);

    let req = request();
    let rpc_id = second_rpc_id();
    f.node.dispatch(&f.mount, NodeAction::Send { rpc_id, req });
    let next = f.rec.get().collects.last().unwrap().request.clone();
    let result = page(&next, Vec::new(), 0, 0, true);
    let request = next;
    f.node
        .dispatch(&f.mount, NodeAction::PersistedPage { request, result });
    let responses = f.rec.get().responses.clone();
    assert_eq!(responses.len(), 1, "{responses:?}");
    assert_eq!(f.rec.get().sends.len(), 1, "probed again at the same root");
}
