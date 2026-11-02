//! The wallet-send component.
//!
//! Its decisions depend on the rest of the node: whether the RPC is still
//! pending, whether the chain and mempool still match an attempt's pins,
//! which commitments the mempool already spends. A component sees its own
//! State only, so that context arrives as data in its Actions (`StartPlan`,
//! `CollectionGuard`, the spent commitments, whether the RPC is pending),
//! computed by the parent, which can read it. Everything it asks of the rest
//! of the node leaves as an Output, which the parent lowers (`crate::node`).

use std::collections::BTreeSet;

use causet::*;
use serde::{Deserialize, Serialize};

use crate::domain::{Commitment, Root, RpcId, SendRequest, SendResponse, SubmittedTx};
use crate::mempool::{OwnedOutputsChunk, OwnedOutputsCursor, OwnedOutputsReadError};
use crate::state::{
    CollectRequest, MempoolRequest, MempoolTransition, OwnedUtxosChunk, PersistedTransition,
    ProveRequest, WalletSendState,
};

/// What `Start` found when it read the node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StartPlan {
    Noop,
    Reject {
        req: SendRequest,
        error: String,
    },
    Collect {
        anchor_root: Root,
        timestamp_ms: u64,
        req: SendRequest,
        mempool_cursor: OwnedOutputsCursor,
    },
}

/// Whether a collection may go on, given the node's RPC, chain and mempool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectionGuard {
    /// Everything still matches.
    Continue,
    /// A pin went stale: start the attempt again.
    Retry,
    /// The RPC is gone or changed: drop the attempt.
    Noop,
}

#[derive(Debug, Action)]
#[causet(type = "wallet_send.action")]
pub enum WalletSendAction {
    Cancel {
        rpc_id: RpcId,
    },
    Start {
        rpc_id: RpcId,
        plan: StartPlan,
    },
    OwnedChunk {
        request: CollectRequest,
        result: Result<OwnedUtxosChunk, String>,
        guard: CollectionGuard,
        /// The page's commitments the mempool already spends.
        spent: BTreeSet<Commitment>,
    },
    MempoolChunk {
        request: MempoolRequest,
        guard: CollectionGuard,
        /// The page, read when the guard says to continue.
        chunk: Option<Result<OwnedOutputsChunk, OwnedOutputsReadError>>,
    },
    Prepared {
        request_id: RequestId,
        rpc_id: RpcId,
        tx: SubmittedTx,
        rpc_pending: bool,
    },
    ProvingRejected {
        request_id: RequestId,
        rpc_id: RpcId,
        error: String,
        rpc_pending: bool,
    },
    Rejected {
        rpc_id: RpcId,
        error: String,
    },
}

/// What wallet send asks of the rest of the node, in order.
#[derive(Debug, Output)]
#[causet(type = "wallet_send.output")]
pub enum WalletSendOutput {
    /// Read one persisted page.
    Collect(CollectRequest),
    /// Read one mempool page now.
    ScanMempool(MempoolRequest),
    /// Build, sign and prove the transaction.
    Prove(ProveRequest),
    /// Start the attempt again with a fresh plan.
    Restart {
        rpc_id: RpcId,
    },
    HydrationQuery {
        rpc_id: RpcId,
        anchor_root: Root,
        selection_error: String,
    },
    HydrationForget {
        rpc_id: RpcId,
    },
    Submit(SubmittedTx),
    Respond {
        rpc_id: RpcId,
        response: SendResponse,
    },
}

pub struct WalletSend;

impl Component for WalletSend {
    type State = WalletSendState;
    type Action = WalletSendAction;
    type Output = WalletSendOutput;
    no_effects!();

    /// The checks on wallet send's own State; those that read the RPC are
    /// the parent's, since only it can read the RPC.
    fn is_enabled(state: &WalletSendState, action: &WalletSendAction, _: Timestamp) -> bool {
        match action {
            WalletSendAction::Cancel { rpc_id } => state.has_work(*rpc_id),
            WalletSendAction::Start { rpc_id, .. } | WalletSendAction::Rejected { rpc_id, .. } => {
                state.can_start(*rpc_id)
            }
            WalletSendAction::OwnedChunk { request, .. } => state.is_collecting_persisted(request),
            WalletSendAction::MempoolChunk { request, .. } => state.is_collecting_mempool(request),
            WalletSendAction::Prepared {
                request_id, rpc_id, ..
            }
            | WalletSendAction::ProvingRejected {
                request_id, rpc_id, ..
            } => state.is_proving(*rpc_id, *request_id),
        }
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: WalletSendAction, _: &ActionMeta) {
        match action {
            WalletSendAction::Cancel { rpc_id } => {
                if ctx.state_mut().cancel(rpc_id) {
                    ctx.into_emitter()
                        .emit_output(WalletSendOutput::HydrationForget { rpc_id });
                }
            }
            WalletSendAction::Start { rpc_id, plan } => match plan {
                StartPlan::Noop => {}
                StartPlan::Reject { error, .. } => {
                    ctx.state_mut().release_owner(rpc_id);
                    terminal_rejection(ctx, rpc_id, error);
                }
                StartPlan::Collect {
                    anchor_root,
                    timestamp_ms,
                    req,
                    mempool_cursor,
                } => {
                    if !ctx.state_mut().reserve_owner(rpc_id, req.from) {
                        bug_condition!(rpc_id = rpc_id; "wallet-send owner reservation changed during reduction");
                        return;
                    }
                    let request_id = ctx.new_request();
                    let state = ctx.state_mut();
                    match state.begin_collecting(
                        request_id,
                        rpc_id,
                        anchor_root,
                        timestamp_ms,
                        req,
                        mempool_cursor,
                    ) {
                        Ok(request) => ctx
                            .into_emitter()
                            .emit_output(WalletSendOutput::Collect(request)),
                        Err(error) => {
                            state.release_owner(rpc_id);
                            terminal_rejection(ctx, rpc_id, error);
                        }
                    }
                }
            },
            WalletSendAction::OwnedChunk {
                request,
                result,
                guard,
                spent,
            } => {
                let gate = gate(ctx.state_mut(), request.rpc_id, request.request_id, guard);
                let Some(mut ctx) = pass(ctx, request.rpc_id, gate) else {
                    return;
                };
                let Some(transition) = ctx
                    .state_mut()
                    .apply_persisted_chunk(&request, &result, &spent)
                else {
                    bug_condition!(rpc_id = request.rpc_id; "wallet-send persisted chunk correlation changed during reduction");
                    return;
                };
                let mut out = ctx.into_emitter();
                match transition {
                    PersistedTransition::Collect(next) => {
                        out.emit_output(WalletSendOutput::Collect(next))
                    }
                    PersistedTransition::ScanMempool(next) => {
                        out.emit_output(WalletSendOutput::ScanMempool(next))
                    }
                    PersistedTransition::Hydrate {
                        anchor_root,
                        selection_error,
                    } => out.emit_output(WalletSendOutput::HydrationQuery {
                        rpc_id: request.rpc_id,
                        anchor_root,
                        selection_error,
                    }),
                }
            }
            WalletSendAction::MempoolChunk {
                request,
                guard,
                chunk,
            } => {
                let gate = gate(ctx.state_mut(), request.rpc_id, request.request_id, guard);
                let Some(mut ctx) = pass(ctx, request.rpc_id, gate) else {
                    return;
                };
                let Some(chunk) = chunk else {
                    bug_condition!(rpc_id = request.rpc_id; "missing wallet-send mempool chunk context");
                    return;
                };
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(OwnedOutputsReadError::Stale) => {
                        if ctx
                            .state_mut()
                            .finish_collecting(request.rpc_id, request.request_id)
                        {
                            ctx.into_emitter().emit_output(WalletSendOutput::Restart {
                                rpc_id: request.rpc_id,
                            });
                        }
                        return;
                    }
                    Err(error) => {
                        bug_condition!(rpc_id = request.rpc_id, error = error; "wallet-send mempool owner scan failed");
                        if ctx
                            .state_mut()
                            .finish_collecting(request.rpc_id, request.request_id)
                        {
                            ctx.into_emitter().emit_action(WalletSendAction::Rejected {
                                rpc_id: request.rpc_id,
                                error: format!("wallet-send mempool owner scan failed: {error:?}"),
                            });
                        }
                        return;
                    }
                };
                let Some(transition) = ctx.state_mut().apply_mempool_chunk(&request, chunk) else {
                    bug_condition!(rpc_id = request.rpc_id; "wallet-send mempool chunk correlation changed during reduction");
                    return;
                };
                let mut out = ctx.into_emitter();
                match transition {
                    MempoolTransition::Continue => {}
                    MempoolTransition::Prove(prove) => {
                        out.emit_output(WalletSendOutput::Prove(prove))
                    }
                    MempoolTransition::Hydrate {
                        anchor_root,
                        selection_error,
                    } => out.emit_output(WalletSendOutput::HydrationQuery {
                        rpc_id: request.rpc_id,
                        anchor_root,
                        selection_error,
                    }),
                }
            }
            WalletSendAction::Prepared {
                request_id,
                rpc_id,
                tx,
                rpc_pending,
            } => {
                let state = ctx.state_mut();
                if !state.finish_proving(rpc_id, request_id) {
                    return;
                }
                // A deliberate limitation: the owner is released once the
                // work is queued, before the mempool admits it.
                state.release_owner(rpc_id);
                if !rpc_pending {
                    return;
                }
                let tx_id = tx.hash;
                let mut out = ctx.into_emitter();
                out.emit_output(WalletSendOutput::HydrationForget { rpc_id });
                out.emit_output(WalletSendOutput::Submit(tx));
                out.emit_output(WalletSendOutput::Respond {
                    rpc_id,
                    response: SendResponse {
                        queued: true,
                        error: None,
                        tx_id: Some(tx_id),
                    },
                });
            }
            WalletSendAction::ProvingRejected {
                request_id,
                rpc_id,
                error,
                rpc_pending,
            } => {
                let state = ctx.state_mut();
                if !state.finish_proving(rpc_id, request_id) {
                    return;
                }
                state.release_owner(rpc_id);
                if rpc_pending {
                    terminal_rejection(ctx, rpc_id, error);
                }
            }
            WalletSendAction::Rejected { rpc_id, error } => {
                ctx.state_mut().release_owner(rpc_id);
                terminal_rejection(ctx, rpc_id, error);
            }
        }
    }
}

/// What a collection guard leaves to do.
enum Gate {
    Go,
    Stop,
    Restart,
}

/// What a `Noop` or `Retry` guard does to the attempt.
fn gate(
    state: &mut WalletSendState,
    rpc_id: RpcId,
    request_id: RequestId,
    guard: CollectionGuard,
) -> Gate {
    match guard {
        CollectionGuard::Continue => Gate::Go,
        CollectionGuard::Noop => {
            if state.finish_collecting(rpc_id, request_id) {
                state.release_owner(rpc_id);
            }
            Gate::Stop
        }
        CollectionGuard::Retry if state.finish_collecting(rpc_id, request_id) => Gate::Restart,
        CollectionGuard::Retry => Gate::Stop,
    }
}

/// Applies a gate: the context back when the reducer goes on.
fn pass(
    ctx: ComponentContext<'_, WalletSend>,
    rpc_id: RpcId,
    gate: Gate,
) -> Option<ComponentContext<'_, WalletSend>> {
    match gate {
        Gate::Go => Some(ctx),
        Gate::Stop => None,
        Gate::Restart => {
            ctx.into_emitter()
                .emit_output(WalletSendOutput::Restart { rpc_id });
            None
        }
    }
}

/// Ends the send with an error: hydration is forgotten, the RPC answered.
fn terminal_rejection(ctx: ComponentContext<'_, WalletSend>, rpc_id: RpcId, error: String) {
    let mut out = ctx.into_emitter();
    out.emit_output(WalletSendOutput::HydrationForget { rpc_id });
    out.emit_output(WalletSendOutput::Respond {
        rpc_id,
        response: SendResponse::rejected(error),
    });
}
