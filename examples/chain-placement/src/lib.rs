//! Where each produced block landed, decided from nodes' reports that
//! arrive late and out of order: a projection on causet.
//!
//! Nodes report the blocks they produce and the tips they adopt. For each
//! produced block, the projection finds its facts in the reports around it
//! by event time (the chain's height then, whether its parent was seen,
//! who else adopted it, which blocks competed for its slot, whether the
//! chain moved two blocks past it) and classifies it: `canonical`,
//! `orphaned`, `likely_main_chain`, `likely_side_chain`, `stale_parent` or
//! `unknown`. It outputs a block's status whenever it changes, and its
//! placement once, when the chain's progress makes it final.
//!
//! - [`record`]: the log's records, typed.
//! - [`classify`]: the classifier, a pure function, and its windows.
//! - [`projection`]: the projection, with its bounded context.
//!
//! The tests check the classifier and the projection step by step
//! (`tests/parity.rs`), then generate chains, nodes and late reports, and
//! check that a restart changes no output, that recomputing only what a
//! record changed agrees with recomputing everything, and the output's
//! contract (`tests/generated.rs`). What generation found is pinned in
//! `tests/findings.rs`: chain reports are kept as far back as a kept block
//! reads them, and finality does not stop a late competitor from also
//! being finalized canonical for its slot.

pub mod classify;
pub mod projection;
pub mod record;
