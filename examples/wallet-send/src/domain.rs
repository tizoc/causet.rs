//! The domain values wallet send works with, without cryptography.

use serde::{Deserialize, Serialize};

/// A wallet owner (a public key's hash).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Owner(pub u32);

/// A UTXO's commitment: a hash of the UTXO.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Commitment(pub u64);

/// The UTXO tree's root.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Root(pub u64);

/// A block's hash.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct BlockHash(pub u64);

/// An RPC's identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RpcId(pub u64);

/// The base currency's token.
pub const BASE_TOKEN: u32 = 0;

/// A transaction takes at most this many inputs.
pub const MAX_INPUTS: usize = 124;

/// Who may spend a UTXO.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Recipient {
    Key(Owner),
    Script,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Utxo {
    pub recipient: Recipient,
    pub token: u32,
    pub amount: u64,
    /// Attached data; spendable base-currency UTXOs carry none.
    pub data: u64,
    pub nonce: u64,
}

impl Utxo {
    /// A plain base-currency UTXO of `owner`.
    pub fn base(owner: Owner, amount: u64, nonce: u64) -> Self {
        Self {
            recipient: Recipient::Key(owner),
            token: BASE_TOKEN,
            amount,
            data: 0,
            nonce,
        }
    }

    /// Its commitment: FNV-1a over its fields, stable across platforms.
    pub fn commitment(&self) -> Commitment {
        let (kind, owner) = match self.recipient {
            Recipient::Key(Owner(o)) => (1u64, u64::from(o)),
            Recipient::Script => (2, 0),
        };
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for word in [
            kind,
            owner,
            u64::from(self.token),
            self.amount,
            self.data,
            self.nonce,
        ] {
            for byte in word.to_le_bytes() {
                hash = (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
            }
        }
        Commitment(hash)
    }

    /// A key UTXO must carry an amount.
    pub fn ensure_key_utxo_nonzero_amount(&self) -> Result<(), String> {
        if matches!(self.recipient, Recipient::Key(_)) && self.amount == 0 {
            return Err("key utxo with zero amount".to_owned());
        }
        Ok(())
    }
}

/// A wallet-send RPC request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendRequest {
    pub from: Owner,
    pub to: Owner,
    pub amount: u64,
    pub fee: Option<u64>,
    pub memo: String,
}

/// A wallet-send RPC's answer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendResponse {
    pub queued: bool,
    pub error: Option<String>,
    pub tx_id: Option<u64>,
}

impl SendResponse {
    pub fn rejected(error: String) -> Self {
        Self {
            queued: false,
            error: Some(error),
            tx_id: None,
        }
    }
}

/// A proved transaction, ready for the mempool.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmittedTx {
    pub hash: u64,
    pub inputs: Vec<Commitment>,
    pub outputs: Vec<Utxo>,
}
