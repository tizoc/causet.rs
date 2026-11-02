//! The part of the mempool wallet send reads: which commitments its
//! transactions spend, and a bounded, cursor-pinned scan of one owner's
//! unspent outputs.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use serde::{Deserialize, Serialize};

use crate::domain::{BlockHash, Commitment, Owner, Recipient, Root, SubmittedTx, Utxo};

/// A stable cursor for one owner's scan, valid for one mempool generation at
/// one chain tip.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedOutputsCursor {
    owner: Owner,
    generation: u64,
    tip: BlockHash,
    after_exclusive: Option<Commitment>,
}

impl OwnedOutputsCursor {
    pub fn owner(&self) -> Owner {
        self.owner
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedOutputsChunk {
    pub items: Vec<(Commitment, Utxo)>,
    pub next: Option<OwnedOutputsCursor>,
    pub scanned_positions: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OwnedOutputsReadError {
    InvalidLimits,
    Stale,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mempool {
    /// Bumped on every structural change and every new tip.
    generation: u64,
    tip: BlockHash,
    /// The UTXO root the mempool is built on.
    pub tip_root: Root,
    spends: BTreeSet<Commitment>,
    outputs: BTreeMap<Commitment, Utxo>,
    owner_outputs: BTreeMap<Owner, BTreeSet<Commitment>>,
}

impl Mempool {
    pub fn new(tip: BlockHash, tip_root: Root) -> Self {
        Self {
            tip,
            tip_root,
            ..Self::default()
        }
    }

    /// Accepts a transaction: its inputs are spent, its outputs indexed.
    pub fn accept(&mut self, inputs: &[Commitment], outputs: &[Utxo]) {
        self.spends.extend(inputs.iter().copied());
        for utxo in outputs {
            let commitment = utxo.commitment();
            if let Recipient::Key(owner) = utxo.recipient {
                self.owner_outputs
                    .entry(owner)
                    .or_default()
                    .insert(commitment);
            }
            self.outputs.insert(commitment, utxo.clone());
        }
        self.generation = self.generation.wrapping_add(1);
    }

    /// A submitted transaction enters the mempool.
    pub fn submit(&mut self, tx: &SubmittedTx) {
        self.accept(&tx.inputs, &tx.outputs);
    }

    /// A new chain tip.
    pub fn set_tip(&mut self, tip: BlockHash) {
        self.tip = tip;
        self.generation = self.generation.wrapping_add(1);
    }

    pub fn has_spend(&self, commitment: &Commitment) -> bool {
        self.spends.contains(commitment)
    }

    pub fn begin_owned_scan(&self, owner: Owner) -> OwnedOutputsCursor {
        OwnedOutputsCursor {
            owner,
            generation: self.generation,
            tip: self.tip,
            after_exclusive: None,
        }
    }

    pub fn validate_cursor(
        &self,
        cursor: &OwnedOutputsCursor,
    ) -> Result<(), OwnedOutputsReadError> {
        if cursor.generation == self.generation && cursor.tip == self.tip {
            Ok(())
        } else {
            Err(OwnedOutputsReadError::Stale)
        }
    }

    /// One bounded page of the owner's unspent outputs. The cursor advances
    /// over every inspected position, spent ones included.
    pub fn owned_outputs_chunk(
        &self,
        mut cursor: OwnedOutputsCursor,
        max_scan_positions: usize,
        max_matches: usize,
    ) -> Result<OwnedOutputsChunk, OwnedOutputsReadError> {
        if max_scan_positions == 0 || max_matches == 0 {
            return Err(OwnedOutputsReadError::InvalidLimits);
        }
        self.validate_cursor(&cursor)?;
        let Some(owned) = self.owner_outputs.get(&cursor.owner) else {
            return Ok(OwnedOutputsChunk {
                items: Vec::new(),
                next: None,
                scanned_positions: 0,
            });
        };
        let start = cursor.after_exclusive.map_or(Unbounded, Excluded);
        let mut positions = owned.range((start, Unbounded)).peekable();
        let (mut items, mut scanned) = (Vec::new(), 0usize);
        while scanned < max_scan_positions && items.len() < max_matches {
            let Some(commitment) = positions.next() else {
                break;
            };
            cursor.after_exclusive = Some(*commitment);
            scanned += 1;
            if self.has_spend(commitment) {
                continue;
            }
            items.push((*commitment, self.outputs[commitment].clone()));
        }
        let next = positions.peek().is_some().then_some(cursor);
        Ok(OwnedOutputsChunk {
            items,
            next,
            scanned_positions: scanned as u32,
        })
    }
}
