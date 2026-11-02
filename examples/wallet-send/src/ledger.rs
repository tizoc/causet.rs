//! The UTXO ledger as a fixed-depth Merkle tree, and what a light node asks
//! a full peer for: some of its UTXOs by leaf index, each with its proof.
//!
//! The hash is small and not cryptographic, but the tree is real: a peer
//! that alters an item, or answers for another index, gives a proof that
//! does not lead to the root.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::domain::{Commitment, Owner, Recipient, Root, Utxo};

/// The tree's depth: `LEAVES` leaves.
pub const DEPTH: usize = 8;
pub const LEAVES: u64 = 1 << DEPTH;

/// The ledger: a UTXO at some of its leaves.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ledger {
    pub leaves: BTreeMap<u64, Utxo>,
}

/// A leaf's siblings, from the leaf up.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proof(pub Vec<u64>);

impl Ledger {
    pub fn root(&self) -> Root {
        Root(self.levels()[DEPTH][0])
    }

    /// The proof of the leaf at `index`, present or empty.
    pub fn prove(&self, index: u64) -> Proof {
        let levels = self.levels();
        let mut position = index as usize;
        let mut siblings = Vec::with_capacity(DEPTH);
        for level in &levels[..DEPTH] {
            siblings.push(level[position ^ 1]);
            position /= 2;
        }
        Proof(siblings)
    }

    /// The UTXOs of `owner`, by leaf index.
    pub fn owned(&self, owner: Owner) -> impl Iterator<Item = (u64, &Utxo)> {
        self.leaves
            .iter()
            .filter(move |(_, utxo)| utxo.recipient == Recipient::Key(owner))
            .map(|(index, utxo)| (*index, utxo))
    }

    /// A free leaf, the lowest.
    pub fn free_leaf(&self) -> Option<u64> {
        (0..LEAVES).find(|index| !self.leaves.contains_key(index))
    }

    /// Every level, the leaves' hashes first and the root last.
    fn levels(&self) -> Vec<Vec<u64>> {
        let mut level: Vec<u64> = (0..LEAVES)
            .map(|index| leaf_hash(index, self.leaves.get(&index).map(Utxo::commitment)))
            .collect();
        let mut levels = Vec::with_capacity(DEPTH + 1);
        for _ in 0..DEPTH {
            let up = level
                .chunks(2)
                .map(|pair| node_hash(pair[0], pair[1]))
                .collect();
            levels.push(std::mem::replace(&mut level, up));
        }
        levels.push(level);
        levels
    }
}

/// Whether `proof` shows `leaf` (a commitment, or empty) at `index` under
/// `root`.
pub fn verify(root: Root, index: u64, leaf: Option<Commitment>, proof: &Proof) -> bool {
    if index >= LEAVES || proof.0.len() != DEPTH {
        return false;
    }
    let mut hash = leaf_hash(index, leaf);
    let mut position = index;
    for sibling in &proof.0 {
        hash = if position.is_multiple_of(2) {
            node_hash(hash, *sibling)
        } else {
            node_hash(*sibling, hash)
        };
        position /= 2;
    }
    Root(hash) == root
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn leaf_hash(index: u64, leaf: Option<Commitment>) -> u64 {
    let leaf = leaf.map_or(0x5bd1_e995, |Commitment(c)| mix(c) | 1);
    mix(index.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ leaf)
}

fn node_hash(left: u64, right: u64) -> u64 {
    mix(left ^ mix(right.wrapping_add(0x9e37_79b9_7f4a_7c15)))
}

// ---------------------------------------------------------------------------
// What a light node asks a full peer, and what it answers.

/// A leaf the light node expects to hold `expected`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRef {
    pub index: u64,
    pub expected: Commitment,
}

/// Some leaves of the ledger at `root`, each with its proof.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeRequest {
    pub root: Root,
    pub items: Vec<IndexRef>,
}

/// One leaf as the peer answers it: the UTXO there, if any, and its proof.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvedItem {
    pub index: u64,
    pub utxo: Option<Utxo>,
    pub proof: Proof,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeResponse {
    pub root: Root,
    pub items: Vec<ProvedItem>,
}

/// A peer's answer to a probe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerAnswer {
    Data(ProbeResponse),
    /// The peer does not have the ledger at that root.
    Unavailable,
}

/// What a verified probe shows at its root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verified {
    pub root: Root,
    /// The leaves that hold the expected UTXO.
    pub found: Vec<(u64, Utxo)>,
    /// The leaves that do not: the expected UTXO is not in the ledger.
    pub missing: Vec<u64>,
}

/// Whether the response answers exactly the request: its root, and every
/// requested leaf once, nothing else. Checked before verification.
pub fn covers(request: &ProbeRequest, response: &ProbeResponse) -> Result<(), String> {
    if response.root != request.root {
        return Err(format!(
            "answered root {:?}, asked {:?}",
            response.root, request.root
        ));
    }
    let asked: BTreeSet<u64> = request.items.iter().map(|item| item.index).collect();
    let mut answered = BTreeSet::new();
    for item in &response.items {
        if !answered.insert(item.index) {
            return Err(format!("leaf {} answered twice", item.index));
        }
    }
    if answered != asked {
        return Err("answered other leaves than asked".to_owned());
    }
    Ok(())
}

/// Verifies every item's proof, and says which expected UTXOs are there.
pub fn verify_response(
    request: &ProbeRequest,
    response: &ProbeResponse,
) -> Result<Verified, String> {
    let expected: BTreeMap<u64, Commitment> = request
        .items
        .iter()
        .map(|item| (item.index, item.expected))
        .collect();
    let mut verified = Verified {
        root: response.root,
        found: Vec::new(),
        missing: Vec::new(),
    };
    for item in &response.items {
        let leaf = item.utxo.as_ref().map(Utxo::commitment);
        if !verify(response.root, item.index, leaf, &item.proof) {
            return Err(format!("the proof of leaf {} fails", item.index));
        }
        match &item.utxo {
            Some(utxo) if Some(&utxo.commitment()) == expected.get(&item.index) => {
                verified.found.push((item.index, utxo.clone()))
            }
            _ => verified.missing.push(item.index),
        }
    }
    Ok(verified)
}
