//! A wallet send on a light node, on causet: two real workflows, larger
//! than the guide's shop.
//!
//! A send selects its inputs through bounded pages of persisted and mempool
//! UTXOs, pins each attempt to a snapshot so stale results are inert, asks
//! for hydration when the node lacks data, retries, can be cancelled, and
//! proves the transaction asynchronously, with at most one send per owner.
//! The light node does not hold the ledger: hydration asks full peers for
//! some of its leaves, with Merkle proofs, over the network, and stores what
//! they prove (partial-ledger sync).
//!
//! - [`state`]: wallet send's lifecycle State and its transitions.
//! - [`wallet_send`]: the component. What it needs to know of the rest of
//!   the node arrives in its Actions; what it asks of it leaves as Outputs.
//! - [`partial_ledger`]: the probe lifecycle and wallet-spend hydration.
//! - [`peers`]: the network layer: connections, request IDs, timeouts.
//! - [`node`]: the root, holding the rest of the node (RPC requests, the
//!   chain's root, the mempool), computing its children's context, lowering
//!   their Outputs, and owning the node's own I/O.
//! - [`full_peer`]: a full peer, a node of its own, honest or faulty.
//! - [`ledger`]: the ledger as a Merkle tree, and the probe's messages.
//! - [`mempool`] and [`domain`]: the mempool and the domain values.
//!
//! The tests check wallet send's transitions (`tests/transitions.rs`) and
//! the probe lifecycle's (`tests/probe_lifecycle.rs`), then run a light node
//! and up to three full peers on a simulated network, with lying, slow and
//! restarting peers, late answers and restarts, and checks across the nodes
//! (`tests/generated.rs`, its world in `tests/sim`).
//!
//! Every answer a send waits for (a page, a proof, a hydration) has a
//! deadline (`node::ANSWER_TIMEOUT`): a send whose answer is lost gives up
//! and is answered, and an answer that comes later changes nothing. One
//! limitation is deliberate: the owner is released once its transaction is
//! queued, before the mempool admits it.

pub mod domain;
pub mod full_peer;
pub mod ledger;
pub mod mempool;
pub mod node;
pub mod partial_ledger;
pub mod peers;
pub mod state;
pub mod wallet_send;
