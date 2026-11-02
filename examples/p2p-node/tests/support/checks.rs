//! Cross-node and execution-order invariant checks.

use super::*;

// ---------------------------------------------------------------------------
// Checks

/// What a node shows the group: what it served, and what it was answered.
pub(super) struct View {
    pub(super) served: BTreeMap<(PeerId, u64), Resp>,
    /// `(server, key, tag, answer)`.
    pub(super) answered: Vec<(PeerId, u64, u64, Resp)>,
}

pub(super) fn view(state: &NodeState) -> View {
    View {
        served: state.served.clone(),
        answered: state
            .fetches
            .iter()
            .filter_map(|(correlation, fetch)| match &fetch.outcome {
                Some(Outcome::Answered(Some(answer))) => {
                    Some((fetch.peer, fetch.key, correlation.0, answer.clone()))
                }
                _ => None,
            })
            .collect(),
    }
}

pub(super) fn name(peer: PeerId) -> String {
    format!("node-{}", peer.0)
}

pub(super) fn group_checks(group: &Group, peers: Vec<PeerId>) {
    group.check("an answer is the one its peer sent for that request", move |views, _| {
        for me in &peers {
            let Some(mine) = views.get::<View>(&name(*me)) else {
                continue;
            };
            for (server, key, tag, answer) in &mine.answered {
                let sent = views
                    .get::<View>(&name(*server))
                    .and_then(|theirs| theirs.served.get(&(*me, *tag)));
                if answer.tag != *tag || answer.key != *key || sent != Some(answer) {
                    return Err(format!(
                        "{me:?} took {answer:?} from {server:?} for key {key} tag {tag}; it sent {sent:?}"
                    ));
                }
            }
        }
        Ok(())
    });
}

/// Check 4: in the order the node lowered them, an abort for a peer never
/// follows its disconnection without a new request between.
#[derive(Clone, Default)]
pub(super) struct Order(pub(super) Arc<Mutex<Seen>>);

/// Per peer, whether its disconnection is the latest thing seen of it; and
/// the violations.
pub(super) type Seen = (BTreeMap<PeerId, bool>, Vec<String>);

impl Observer for Order {
    fn action_start(&mut self, _: &Execution<'_>, payload: &dyn MessageView) {
        let Some(NodeAction::FromP2p(output)) = payload.downcast_ref::<NodeAction>() else {
            return;
        };
        let mut order = self.0.lock().unwrap();
        let (gone, violations) = &mut *order;
        match output {
            P2pOutput::PeerDisconnected { peer } => {
                gone.insert(*peer, true);
            }
            P2pOutput::RpcRequestStarted { peer, .. } => {
                gone.insert(*peer, false);
            }
            P2pOutput::RpcRequestAborted { peer, .. }
                if gone.get(peer).copied().unwrap_or(false) =>
            {
                violations.push(format!("{peer:?}: aborted after its disconnection"));
            }
            _ => {}
        }
    }
}

/// What a node's P2P is executing, for the effects and outputs it makes.
#[derive(Clone)]
pub(super) enum Cause {
    Disconnect(PeerId),
    Transport(TransportEvent<Req, Resp>),
    Other,
}

/// Every input reaching P2P is accounted for, from the observer calls alone:
/// an Event at P2P (a transport callback or a timer) is acted on or refused,
/// as the Action its adapter queued, before it settles; an Intent the node
/// calls inline for a connect or a disconnect is reduced or refused within
/// the node's execution.
#[derive(Clone, Default)]
pub(super) struct Accounts(pub(super) Arc<Mutex<AccountsSeen>>);

#[derive(Default)]
pub(super) struct AccountsSeen {
    /// Events at P2P whose Action was neither acted on nor refused yet.
    pub(super) callbacks: BTreeSet<ExecutionId>,
    /// The node's executions whose Intent was neither reduced nor refused.
    pub(super) intents: BTreeSet<ExecutionId>,
    pub(super) violations: Vec<String>,
}

pub(super) const P2P_PATH: &str = "/p2p";

impl Observer for Accounts {
    fn event_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        if &**exec.path == P2P_PATH {
            self.0.lock().unwrap().callbacks.insert(exec.meta.id);
            CALLBACKS.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn action_start(&mut self, exec: &Execution<'_>, payload: &dyn MessageView) {
        let mut seen = self.0.lock().unwrap();
        if &**exec.path == P2P_PATH {
            seen.callbacks.remove(&exec.meta.causal_execution_id());
        } else if let Some(NodeAction::Connect { .. } | NodeAction::Disconnect { .. }) =
            payload.downcast_ref::<NodeAction>()
        {
            seen.intents.insert(exec.meta.id);
            INTENTS.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn inline_action(&mut self, inline: &InlineCall<'_>, _: &dyn MessageView) {
        if &**inline.path == P2P_PATH {
            self.0.lock().unwrap().intents.remove(&inline.within);
        }
    }

    fn action_refused(&mut self, refusal: &Refusal<'_>, _: &dyn MessageView) {
        if &**refusal.path != P2P_PATH {
            return;
        }
        let mut seen = self.0.lock().unwrap();
        match refusal.sent {
            causet::observers::SentFrom::Queued(origin) => {
                if seen.callbacks.remove(&origin.causal) {
                    CALLBACKS_REFUSED.fetch_add(1, Ordering::Relaxed);
                }
            }
            causet::observers::SentFrom::Inline(within) => {
                if seen.intents.remove(&within) {
                    INTENTS_REFUSED.fetch_add(1, Ordering::Relaxed);
                }
            }
            sent => {
                let line = format!("{}: refused, sent {sent:?}", refusal.kind);
                seen.violations.push(line);
            }
        }
    }

    fn action_finish(&mut self, exec: &Execution<'_>) {
        let mut seen = self.0.lock().unwrap();
        if seen.intents.remove(&exec.meta.id) {
            let line = format!("{}: its Intent was neither reduced nor refused", exec.kind);
            seen.violations.push(line);
        }
    }

    fn event_settled(&mut self, exec: &Execution<'_>) {
        let mut seen = self.0.lock().unwrap();
        if seen.callbacks.remove(&exec.meta.id) {
            let line = format!("{}: neither acted on nor refused", exec.kind);
            seen.violations.push(line);
        }
    }
}

/// Checks 5 to 7, from the node's executions: why P2P closes a connection,
/// which dial can fail or make the peer ready, which send a failed request's
/// was, which message answered a request, that a connect to a peer P2P holds
/// no connection to dials unless a dial is out, and that no dial goes out
/// while another is. The dial waited for is the last one asked of the
/// transport, until answered or the node disconnects the peer. None of this
/// restates how P2P chooses among connections, only what it may do.
///
/// The node's Intents reach P2P inline, and are read as P2P's own, through
/// `inline_action`; transport callbacks are P2P's queued ones. A lowered
/// output's action runs inside the execution that made it, so it leaves the
/// cause.
#[derive(Clone)]
pub(super) struct Causes(pub(super) Arc<Mutex<CausesSeen>>);

pub(super) struct CausesSeen {
    pub(super) cause: Cause,
    pub(super) awaited: BTreeMap<PeerId, DialId>,
    /// The connection of the `Connected` executing, if it answers the dial
    /// waited for.
    pub(super) answering: Option<(PeerId, ConnectionId)>,
    /// Each peer's last request sent: its connection and wire ID.
    pub(super) last_request: BTreeMap<PeerId, (ConnectionId, WireId)>,
    /// What P2P holds: each connection from its admitted `Connected` until
    /// it closes, fails, or P2P closes it; for the limit.
    pub(super) held: BTreeMap<PeerId, BTreeSet<ConnectionId>>,
    /// Each peer's last readiness report (ready or gone), and whether a
    /// report is owed since: a dial went out, or a connect kept the one out.
    pub(super) reported: BTreeMap<PeerId, (bool, bool)>,
    /// P2P was asked to dial a peer it holds no connection to and is not
    /// dialing, inline within this execution: a dial must go out before it
    /// settles.
    pub(super) connecting: Option<(PeerId, ExecutionId)>,
    /// The peers reported gone, for the parent's retries.
    pub(super) gone: Vec<PeerId>,
    pub(super) violations: Vec<String>,
}

impl Default for Causes {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(CausesSeen {
            cause: Cause::Other,
            awaited: BTreeMap::new(),
            answering: None,
            last_request: BTreeMap::new(),
            held: BTreeMap::new(),
            reported: BTreeMap::new(),
            connecting: None,
            gone: Vec::new(),
            violations: Vec::new(),
        })))
    }
}

impl Observer for Causes {
    fn action_start(&mut self, _: &Execution<'_>, payload: &dyn MessageView) {
        let mut seen = self.0.lock().unwrap();
        if let Some(action) = payload.downcast_ref::<NodeAction>() {
            match action {
                NodeAction::FromP2p(P2pOutput::RpcFailed { peer, .. }) => {
                    let own = match &seen.cause {
                        Cause::Transport(TransportEvent::SendFailed {
                            peer: failed,
                            connection,
                            sent: Sent::Request(wire),
                        }) => {
                            failed == peer
                                && seen.last_request.get(peer) == Some(&(*connection, *wire))
                        }
                        _ => false,
                    };
                    if !own {
                        let line = format!("{peer:?}: failed, not after its request's send");
                        seen.violations.push(line);
                    }
                }
                // Answered only on its request's connection, with its wire
                // ID.
                NodeAction::FromP2p(P2pOutput::RpcResponseReceived { peer, .. }) => {
                    let own = match &seen.cause {
                        Cause::Transport(TransportEvent::Received {
                            peer: from,
                            connection,
                            message: Wire::Response { wire, .. },
                        }) => {
                            from == peer
                                && seen.last_request.get(peer) == Some(&(*connection, *wire))
                        }
                        _ => false,
                    };
                    if !own {
                        let line = format!("{peer:?}: answered off its request's connection");
                        seen.violations.push(line);
                    }
                }
                // Gone once, but again after a connect.
                NodeAction::FromP2p(P2pOutput::PeerDisconnected { peer }) => {
                    if seen.reported.get(peer) == Some(&(false, false)) {
                        let line = format!("{peer:?}: reported gone twice");
                        seen.violations.push(line);
                    }
                    seen.reported.insert(*peer, (false, false));
                    seen.gone.push(*peer);
                }
                // Ready only by the dial waited for, or a connection the
                // peer opened; and not twice.
                NodeAction::FromP2p(P2pOutput::PeerReady { peer }) => {
                    if seen.reported.get(peer).is_some_and(|(ready, _)| *ready) {
                        let line = format!("{peer:?}: reported ready twice");
                        seen.violations.push(line);
                    }
                    seen.reported.insert(*peer, (true, false));
                    let by = match &seen.cause {
                        Cause::Transport(TransportEvent::Connected {
                            peer: arriving,
                            connection,
                            dial,
                        }) => {
                            arriving == peer
                                && (dial.is_none() || seen.answering == Some((*peer, *connection)))
                        }
                        _ => false,
                    };
                    if !by {
                        let line = format!("{peer:?}: ready, not by a dial it waits for");
                        seen.violations.push(line);
                    }
                }
                NodeAction::FromP2p(_) => {}
                _ => seen.cause = Cause::Other,
            }
            return;
        }
        let Some(action) = payload.downcast_ref::<P2pAction<Req, Resp>>() else {
            return;
        };
        seen.answering = None;
        let Some(event) = action.transport() else {
            seen.cause = Cause::Other;
            return;
        };
        match event {
            TransportEvent::DialFailed { peer, dial } => {
                if seen.awaited.get(peer) != Some(dial) {
                    let line = format!("{peer:?}: a dial it no longer waits for failed it");
                    seen.violations.push(line);
                }
                seen.awaited.remove(peer);
            }
            TransportEvent::Connected {
                peer,
                connection,
                dial,
            } => {
                if dial.is_some() && seen.awaited.get(peer) == dial.as_ref() {
                    seen.awaited.remove(peer);
                    seen.answering = Some((*peer, *connection));
                }
                seen.held.entry(*peer).or_default().insert(*connection);
            }
            TransportEvent::Closed { peer, connection }
            | TransportEvent::SendFailed {
                peer, connection, ..
            } => {
                seen.held.entry(*peer).or_default().remove(connection);
            }
            _ => {}
        }
        seen.cause = Cause::Transport(event.clone());
    }

    /// P2P's own Intents, which the node calls inline: read as P2P's, so
    /// any parent calling it inline is observed alike.
    fn inline_action(&mut self, inline: &InlineCall<'_>, payload: &dyn MessageView) {
        let Some(intent) = payload
            .downcast_ref::<P2pAction<Req, Resp>>()
            .and_then(P2pAction::intent)
        else {
            return;
        };
        let mut seen = self.0.lock().unwrap();
        match intent {
            Intent::Disconnect { peer } => {
                seen.awaited.remove(peer);
                seen.held.remove(peer);
                seen.cause = Cause::Disconnect(*peer);
            }
            // Holding none, it dials; already dialing, that dial is kept and
            // owes a report again.
            Intent::Dial { peer } => {
                if seen.held.get(peer).is_none_or(BTreeSet::is_empty) {
                    if seen.awaited.contains_key(peer) {
                        seen.reported.entry(*peer).or_insert((false, false)).1 = true;
                    } else {
                        seen.connecting = Some((*peer, inline.within));
                    }
                }
                seen.cause = Cause::Other;
            }
            _ => {}
        }
    }

    /// A connect is refused only while P2P holds a connection to the peer.
    fn action_refused(&mut self, _: &Refusal<'_>, payload: &dyn MessageView) {
        let Some(Intent::Dial { peer }) = payload
            .downcast_ref::<P2pAction<Req, Resp>>()
            .and_then(P2pAction::intent)
        else {
            return;
        };
        let mut seen = self.0.lock().unwrap();
        if seen.held.get(peer).is_none_or(BTreeSet::is_empty) {
            let line = format!("{peer:?}: a connect refused, holding none");
            seen.violations.push(line);
        }
    }

    fn action_settled(&mut self, exec: &Execution<'_>) {
        let mut seen = self.0.lock().unwrap();
        if let Some((peer, _)) = seen
            .connecting
            .take_if(|(_, within)| *within == exec.meta.id)
        {
            let line = format!("{peer:?}: asked to connect, holding none, and no dial went out");
            seen.violations.push(line);
        }
    }

    fn effect_start(&mut self, _: &Execution<'_>, payload: &dyn MessageView) {
        let Some(effect) = payload.downcast_ref::<P2pEffect<Req, Resp>>() else {
            return;
        };
        let mut seen = self.0.lock().unwrap();
        match effect {
            P2pEffect::Dial { peer, dial } => {
                // A dial replacing one still out would close the other's
                // connection when it came.
                if seen.awaited.contains_key(peer) {
                    let line = format!("{peer:?}: dialed while a dial was still out");
                    seen.violations.push(line);
                }
                seen.awaited.insert(*peer, *dial);
                seen.reported.entry(*peer).or_insert((false, false)).1 = true;
                if seen
                    .connecting
                    .is_some_and(|(connecting, _)| connecting == *peer)
                {
                    seen.connecting = None;
                }
            }
            P2pEffect::Send {
                peer,
                connection,
                message: Wire::Request { wire, .. },
            } => {
                seen.last_request.insert(*peer, (*connection, *wire));
            }
            P2pEffect::Close { peer, connection } => {
                let allowed = match &seen.cause {
                    Cause::Disconnect(disconnected) => disconnected == peer,
                    Cause::Transport(TransportEvent::SendFailed {
                        peer: failed,
                        connection: on,
                        ..
                    }) => (failed, on) == (peer, connection),
                    // The arriving connection only: a cancelled dial's, or
                    // any past the limit (the dial's answer, or one the peer
                    // opened, is kept below it).
                    Cause::Transport(TransportEvent::Connected {
                        peer: arriving,
                        connection: on,
                        dial,
                    }) => {
                        let others = seen.held.get(peer).map_or(0, |held| held.len() - 1);
                        let cancelled =
                            dial.is_some() && seen.answering != Some((*peer, *connection));
                        (arriving, on) == (peer, connection)
                            && (cancelled || others >= MAX_CONNECTIONS)
                    }
                    _ => false,
                };
                if !allowed {
                    let line = format!("{peer:?}: closed {connection:?} for no reason");
                    seen.violations.push(line);
                }
                seen.held.entry(*peer).or_default().remove(connection);
            }
            _ => {}
        }
    }
}

/// Check 8, once quiet: both ends hold every link that is up, each by its
/// own number, and no node holds a connection whose link is down, or is
/// still dialing; each node's last readiness report of a peer is whether it
/// is ready; and no dial is left whose ending it was never told.
pub(super) fn links_agree(net: &Net, all: &[TestNode], ids: &[PeerId], causes: &[Causes]) {
    for link in net.links.values().filter(|link| link.up) {
        for (node, number) in link.ends {
            let (peer, _) = link.other(node);
            let i = ids.iter().position(|id| *id == node).expect("a node");
            assert!(
                all[i].state().p2p.holds_connection(peer, number),
                "{node:?} does not hold {number:?} with {peer:?}, which is up"
            );
        }
    }
    for (i, me) in ids.iter().enumerate() {
        let p2p = &all[i].state().p2p;
        for peer in ids.iter().filter(|peer| *peer != me) {
            for connection in p2p.connections(*peer) {
                let up = net.link_of(*me, connection).is_some_and(|link| {
                    let link = &net.links[&link];
                    link.up && link.number_at(*me) == connection
                });
                assert!(
                    up,
                    "{me:?} holds {connection:?} with {peer:?}, which is down"
                );
            }
            assert!(!p2p.is_dialing(*peer), "{me:?} is still dialing {peer:?}");
            let reported = causes[i].0.lock().unwrap().reported.get(peer).copied();
            assert!(
                !reported.is_some_and(|(_, dialed)| dialed),
                "{me:?} dialed {peer:?} and was never told how it ended"
            );
            let told = reported.is_some_and(|(ready, _)| ready);
            assert_eq!(
                told,
                p2p.is_ready(*peer),
                "{me:?} was last told {peer:?} ready {told}; it is ready {}",
                p2p.is_ready(*peer)
            );
        }
    }
}
