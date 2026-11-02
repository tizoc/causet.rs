//! A full peer: a node of its own, holding the whole ledger at the last few
//! roots it caught up to, and answering probes for leaves with their proofs.
//! A faulty peer is the same machine with a fault: it lies in one way on
//! every answer, or stays silent.

use std::collections::BTreeMap;

use causet::*;
use serde::{Deserialize, Serialize};

use crate::domain::{Root, Utxo};
use crate::ledger::{Ledger, PeerAnswer, ProbeRequest, ProbeResponse, ProvedItem};
use crate::peers::PeerId;

/// How many recent roots a peer serves.
pub const KEPT_ROOTS: usize = 3;

/// How a peer answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fault {
    Honest,
    /// One item's UTXO is altered (its proof is for the original).
    AlterItem,
    /// One item is left out.
    DropItem,
    /// One item is answered twice.
    DuplicateItem,
    /// A leaf nobody asked for is added, with a valid proof.
    ExtraItem,
    /// The answer is for the peer's newest root, whatever was asked.
    OtherRoot,
    /// No answer.
    Silent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullPeerState {
    pub id: PeerId,
    pub fault: Fault,
    /// The ledgers it serves, oldest first.
    pub ledgers: Vec<(Root, Ledger)>,
    /// Every request it answered, by the light node's request ID.
    #[serde(with = "crate::node::pairs")]
    pub answered: BTreeMap<RequestId, ProbeRequest>,
}

impl FullPeerState {
    pub fn new(id: PeerId, fault: Fault) -> Self {
        Self {
            id,
            fault,
            ledgers: Vec::new(),
            answered: BTreeMap::new(),
        }
    }

    pub fn newest_root(&self) -> Option<Root> {
        self.ledgers.last().map(|(root, _)| *root)
    }

    fn ledger(&self, root: Root) -> Option<&Ledger> {
        self.ledgers
            .iter()
            .find(|(at, _)| *at == root)
            .map(|(_, ledger)| ledger)
    }
}

/// A probe from the light node, through the network.
#[derive(Debug, Event)]
#[causet(type = "peer.incoming")]
pub struct Incoming {
    pub request_id: RequestId,
    pub request: ProbeRequest,
}

pub trait PeerWire: Send {
    fn answer(&mut self, peer: PeerId, request_id: RequestId, answer: PeerAnswer);
    fn announce(&mut self, peer: PeerId, root: Root);
}

pub struct PeerServices {
    pub wire: Box<dyn PeerWire>,
}

#[derive(Debug, Action)]
#[causet(type = "peer.action")]
pub enum FullPeerAction {
    /// The peer caught up to a new block's ledger.
    CaughtUp { ledger: Ledger },
    Probe {
        request_id: RequestId,
        request: ProbeRequest,
    },
}

#[derive(Debug, Effect)]
#[causet(type = "peer.effect")]
pub enum FullPeerEffect {
    Answer {
        request_id: RequestId,
        answer: PeerAnswer,
    },
    Announce {
        root: Root,
    },
}

pub struct FullPeer;

impl Component for FullPeer {
    type State = FullPeerState;
    type Service = PeerServices;
    type Action = FullPeerAction;
    type Effect = FullPeerEffect;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: FullPeerAction, _: &ActionMeta) {
        match action {
            FullPeerAction::CaughtUp { ledger } => {
                let root = ledger.root();
                let state = ctx.state_mut();
                if state.newest_root() == Some(root) {
                    return;
                }
                state.ledgers.push((root, ledger));
                if state.ledgers.len() > KEPT_ROOTS {
                    state.ledgers.remove(0);
                }
                ctx.into_emitter()
                    .emit_effect(FullPeerEffect::Announce { root });
            }
            FullPeerAction::Probe {
                request_id,
                request,
            } => {
                let state = ctx.state_mut();
                if state.fault == Fault::Silent {
                    return;
                }
                state.answered.insert(request_id, request.clone());
                let answer = answer(state, &request);
                ctx.into_emitter()
                    .emit_effect(FullPeerEffect::Answer { request_id, answer });
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: FullPeerEffect, _: &EffectMeta) {
        let Some(peer) = ctx.state().map(|state| state.id) else {
            return;
        };
        match effect {
            FullPeerEffect::Answer { request_id, answer } => {
                ctx.service().wire.answer(peer, request_id, answer)
            }
            FullPeerEffect::Announce { root } => ctx.service().wire.announce(peer, root),
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Incoming>();
    }
}

impl Adapts<Incoming> for FullPeer {
    fn adapt(event: Incoming, port: &mut ActionPort<'_, Self>) {
        port.emit_action(FullPeerAction::Probe {
            request_id: event.request_id,
            request: event.request,
        });
    }
}

/// The peer's answer, with its fault applied.
fn answer(state: &FullPeerState, request: &ProbeRequest) -> PeerAnswer {
    let root = match state.fault {
        Fault::OtherRoot => match state.newest_root() {
            Some(root) => root,
            None => return PeerAnswer::Unavailable,
        },
        _ => request.root,
    };
    let Some(ledger) = state.ledger(root) else {
        return PeerAnswer::Unavailable;
    };
    let item = |index: u64| ProvedItem {
        index,
        utxo: ledger.leaves.get(&index).cloned(),
        proof: ledger.prove(index),
    };
    let mut items: Vec<ProvedItem> = request.items.iter().map(|i| item(i.index)).collect();
    match state.fault {
        Fault::AlterItem => {
            if let Some(first) = items.first_mut() {
                let utxo = first
                    .utxo
                    .get_or_insert_with(|| Utxo::base(crate::domain::Owner(0), 1, 0));
                utxo.amount += 1_000;
            }
        }
        Fault::DropItem => {
            items.pop();
        }
        Fault::DuplicateItem => {
            if let Some(first) = items.first().cloned() {
                items.push(first);
            }
        }
        Fault::ExtraItem => {
            let asked = |index: u64| request.items.iter().any(|i| i.index == index);
            if let Some(index) = ledger.leaves.keys().copied().find(|index| !asked(*index)) {
                items.push(item(index));
            }
        }
        Fault::Honest | Fault::OtherRoot | Fault::Silent => {}
    }
    PeerAnswer::Data(ProbeResponse { root, items })
}
