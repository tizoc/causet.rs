//! Simulated connections, transport and server Services.

use super::*;

// ---------------------------------------------------------------------------
// The network

/// One connection between two nodes, known at each end by that end's own
/// number: the dialer first, then the end that accepted it (numbered once
/// it is).
pub(super) struct Link {
    pub(super) ends: [(PeerId, ConnectionId); 2],
    pub(super) up: bool,
}

impl Link {
    /// The other end, as `me` sees it: the peer, and its own number.
    pub(super) fn other(&self, me: PeerId) -> (PeerId, ConnectionId) {
        let [first, second] = self.ends;
        if first.0 == me { second } else { first }
    }

    pub(super) fn number_at(&self, node: PeerId) -> ConnectionId {
        let [first, second] = self.ends;
        if first.0 == node { first.1 } else { second.1 }
    }
}

pub(super) enum Msg {
    ToP2p {
        to: PeerId,
        event: TransportEvent<Req, Resp>,
    },
    Served {
        reply: Reply<Served>,
        event: Served,
    },
    /// A dial connects: the end dialed accepts it.
    Establish {
        link: u64,
        dial: DialId,
    },
}

pub(super) struct Net {
    pub(super) world: Rng,
    pub(super) clock: ManualClock,
    pub(super) nodes: BTreeMap<PeerId, (EventSink, Mount<NodeP2p>)>,
    pub(super) links: BTreeMap<u64, Link>,
    pub(super) next_link: u64,
    /// Each node's own count of its connections.
    pub(super) numbers: BTreeMap<PeerId, u64>,
    /// The link each node's connection is.
    pub(super) by_end: BTreeMap<(PeerId, ConnectionId), u64>,
    /// In order on each link, each way: `(link, from)`.
    pub(super) flight: InFlight<Msg, (u64, PeerId)>,
    /// Callbacks delivered, for replaying stale ones.
    pub(super) delivered: Vec<(PeerId, TransportEvent<Req, Resp>)>,
    /// The requests each node received, by link and wire ID: an answer goes
    /// back on its request's link.
    pub(super) requests_in: BTreeSet<(u64, PeerId, WireId)>,
    /// Dials in flight that fail when due: the pair was cut off.
    pub(super) cut: BTreeSet<u64>,
    pub(super) violations: Vec<String>,
}

#[derive(Clone)]
pub(super) struct Shared(pub(super) Arc<Mutex<Net>>);

impl Net {
    pub(super) fn number(&mut self, node: PeerId) -> ConnectionId {
        let next = self.numbers.entry(node).or_default();
        *next += 1;
        ConnectionId(*next)
    }

    /// A callback to `to`, from the far end of `link`, after a link delay,
    /// in order.
    pub(super) fn deliver(
        &mut self,
        link: u64,
        from: PeerId,
        to: PeerId,
        event: TransportEvent<Req, Resp>,
    ) {
        let at = self.clock.get() + MS + self.world.below(20) * MS;
        self.flight
            .send_in_order((link, from), at, Msg::ToP2p { to, event });
    }

    pub(super) fn link_of(&self, node: PeerId, connection: ConnectionId) -> Option<u64> {
        self.by_end.get(&(node, connection)).copied()
    }

    pub(super) fn is_up(&self, node: PeerId, connection: ConnectionId) -> bool {
        self.link_of(node, connection)
            .is_some_and(|link| self.links[&link].up)
    }

    /// The dialed end accepts the link, numbering it now: both ends hear
    /// it connected.
    pub(super) fn establish(&mut self, link: u64, dial: DialId) {
        let (dialer, number) = self.links[&link].ends[0];
        let accepter = self.links[&link].ends[1].0;
        let accepted = self.number(accepter);
        let established = self.links.get_mut(&link).expect("a link");
        established.ends[1].1 = accepted;
        established.up = true;
        self.by_end.insert((accepter, accepted), link);
        let answer = TransportEvent::Connected {
            peer: accepter,
            connection: number,
            dial: Some(dial),
        };
        self.deliver(link, accepter, dialer, answer);
        let opened = TransportEvent::Connected {
            peer: dialer,
            connection: accepted,
            dial: None,
        };
        self.deliver(link, dialer, accepter, opened);
    }

    /// The network opens another connection between `a` and `b` on its
    /// own: each end numbers it, and hears it opened by the other.
    pub(super) fn open_extra(&mut self, a: PeerId, b: PeerId) {
        let link = self.next_link;
        self.next_link += 1;
        let (at_a, at_b) = (self.number(a), self.number(b));
        let opened = Link {
            ends: [(a, at_a), (b, at_b)],
            up: true,
        };
        self.links.insert(link, opened);
        self.by_end.insert((a, at_a), link);
        self.by_end.insert((b, at_b), link);
        let to_a = TransportEvent::Connected {
            peer: b,
            connection: at_a,
            dial: None,
        };
        self.deliver(link, b, a, to_a);
        let to_b = TransportEvent::Connected {
            peer: a,
            connection: at_b,
            dial: None,
        };
        self.deliver(link, a, b, to_b);
    }

    /// `from` sends `to` six requests at once, spread over their links, as
    /// a peer that does not keep to one request at a time would (wire IDs
    /// from 1,000, past any its P2P uses here); and, on every link, answers
    /// nobody asked for, under wire IDs 0 to 2. Those carry no value: one on
    /// the connection and wire ID of `to`'s request out is, to P2P, that
    /// request's answer.
    pub(super) fn flood(&mut self, from: PeerId, to: PeerId) {
        let links = self.links_between(from, to);
        if links.is_empty() {
            return;
        }
        for n in 0..6u32 {
            let link = links[n as usize % links.len()];
            let (_, theirs) = self.links[&link].other(from);
            let request = Req {
                key: 10 + u64::from(n),
                tag: 1_000 + u64::from(n),
            };
            let message = Wire::Request {
                wire: WireId(1_000 + n),
                request,
            };
            let received = TransportEvent::Received {
                peer: from,
                connection: theirs,
                message,
            };
            self.deliver(link, from, to, received);
        }
        for link in links {
            let (_, theirs) = self.links[&link].other(from);
            for wire in 0..3 {
                let message = Wire::Response {
                    wire: WireId(wire),
                    response: None,
                };
                let received = TransportEvent::Received {
                    peer: from,
                    connection: theirs,
                    message,
                };
                self.deliver(link, from, to, received);
            }
        }
    }

    /// The links up between `a` and `b`.
    pub(super) fn links_between(&self, a: PeerId, b: PeerId) -> Vec<u64> {
        self.links
            .iter()
            .filter(|(_, link)| {
                let ends = [link.ends[0].0, link.ends[1].0];
                link.up && ends.contains(&a) && ends.contains(&b)
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Every dial between `a` and `b` still in flight fails when due.
    pub(super) fn cut_dials(&mut self, a: PeerId, b: PeerId) {
        for (id, link) in &self.links {
            let [(dialer, _), (dialed, accepted)] = link.ends;
            let between = [dialer, dialed] == [a, b] || [dialer, dialed] == [b, a];
            if between && !link.up && accepted == ConnectionId(0) {
                self.cut.insert(*id);
            }
        }
    }

    /// The link breaks: both ends hear it closed.
    pub(super) fn break_link(&mut self, link: u64) {
        let Some(broken) = self.links.get_mut(&link).filter(|l| l.up) else {
            return;
        };
        broken.up = false;
        let [(a, at_a), (b, at_b)] = broken.ends;
        let to_a = TransportEvent::Closed {
            peer: b,
            connection: at_a,
        };
        self.deliver(link, b, a, to_a);
        let to_b = TransportEvent::Closed {
            peer: a,
            connection: at_b,
        };
        self.deliver(link, a, b, to_b);
    }

    /// A callback delivered before, delivered again now: with `responses`,
    /// only an old response, the stale callback that could answer a current
    /// request reusing its wire ID. Otherwise also an old message, close or
    /// failure, or a dial's failure; never a `Connected`, which the
    /// transport reports once.
    pub(super) fn replay_stale(&mut self, pick: u64, responses: bool) {
        let stale: Vec<(PeerId, TransportEvent<Req, Resp>)> = self
            .delivered
            .iter()
            .filter(|(to, event)| match event {
                TransportEvent::Received {
                    connection,
                    message: Wire::Response { .. },
                    ..
                } => !self.is_up(*to, *connection),
                _ if responses => false,
                TransportEvent::Received { connection, .. }
                | TransportEvent::Closed { connection, .. }
                | TransportEvent::SendFailed { connection, .. } => !self.is_up(*to, *connection),
                TransportEvent::DialFailed { .. } => true,
                TransportEvent::Connected { .. } => false,
            })
            .cloned()
            .collect();
        if stale.is_empty() {
            return;
        }
        let (to, event) = stale[pick as usize % stale.len()].clone();
        let at = self.clock.get() + MS;
        self.flight.send_at(at, Msg::ToP2p { to, event });
    }
}

impl Shared {
    pub(super) fn net(&self) -> MutexGuard<'_, Net> {
        self.0.lock().unwrap()
    }
}

impl World for Shared {
    fn next_time(&self) -> Option<Timestamp> {
        self.net().flight.next_time()
    }

    fn advance(&mut self, now: Timestamp) {
        let mut net = self.net();
        for msg in net.flight.due(now) {
            match msg {
                Msg::ToP2p { to, event } => {
                    if let TransportEvent::Received {
                        connection,
                        message: Wire::Request { wire, .. },
                        ..
                    } = &event
                        && let Some(link) = net.link_of(to, *connection)
                    {
                        net.requests_in.insert((link, to, *wire));
                    }
                    net.delivered.push((to, event.clone()));
                    let (sink, mount) = &net.nodes[&to];
                    drop(sink.try_send_to_as_worker(mount, event));
                }
                Msg::Served { reply, event } => drop(reply.try_send_as_worker(event)),
                Msg::Establish { link, dial } if net.cut.remove(&link) => {
                    let [(dialer, _), (peer, _)] = net.links[&link].ends;
                    net.deliver(
                        link,
                        peer,
                        dialer,
                        TransportEvent::DialFailed { peer, dial },
                    );
                }
                Msg::Establish { link, dial } => net.establish(link, dial),
            }
        }
    }
}

/// One node's transport and server, on the shared network.
pub(super) struct Port {
    pub(super) me: PeerId,
    pub(super) net: Shared,
}

impl Transport<Req, Resp> for Port {
    /// Numbered now; connected 1 to 30 ms later, one in ten failing.
    fn dial(&mut self, peer: PeerId, dial: DialId) {
        let mut net = self.net.net();
        let me = self.me;
        if !net.nodes.contains_key(&peer) || net.world.chance(1, 10) {
            let at = net.clock.get() + MS;
            let event = TransportEvent::DialFailed { peer, dial };
            net.flight.send_at(at, Msg::ToP2p { to: me, event });
            return;
        }
        let link = net.next_link;
        net.next_link += 1;
        let number = net.number(me);
        let dialing = Link {
            ends: [(me, number), (peer, ConnectionId(0))],
            up: false,
        };
        net.links.insert(link, dialing);
        net.by_end.insert((me, number), link);
        let at = net.clock.get() + MS + net.world.below(30) * MS;
        net.flight.send_at(at, Msg::Establish { link, dial });
    }

    /// Lost one in thirty; failed at once one in thirty, and always on a
    /// connection that is not up.
    fn send(&mut self, peer: PeerId, connection: ConnectionId, message: Wire<Req, Resp>) {
        let mut net = self.net.net();
        let me = self.me;
        let sent = Sent::of(&message);
        let link = net.link_of(me, connection);
        if let (Sent::Response(wire), Some(on)) = (sent, link)
            && !net.requests_in.contains(&(on, me, wire))
        {
            let line = format!("{me:?} answered {wire:?} on {connection:?}, not its request's");
            net.violations.push(line);
        }
        let up = net.is_up(me, connection);
        if !up || net.world.chance(1, 30) {
            let failed = TransportEvent::SendFailed {
                peer,
                connection,
                sent,
            };
            match link {
                Some(link) => net.deliver(link, peer, me, failed),
                None => {
                    let at = net.clock.get() + MS;
                    let msg = Msg::ToP2p {
                        to: me,
                        event: failed,
                    };
                    net.flight.send_at(at, msg);
                }
            }
            return;
        }
        if net.world.chance(1, 30) {
            return;
        }
        let link = link.expect("a link up");
        let (_, theirs) = net.links[&link].other(me);
        let received = TransportEvent::Received {
            peer: me,
            connection: theirs,
            message,
        };
        net.deliver(link, me, peer, received);
    }

    fn close(&mut self, peer: PeerId, connection: ConnectionId) {
        let mut net = self.net.net();
        let me = self.me;
        let Some(link) = net.link_of(me, connection) else {
            return;
        };
        let closed = net.links.get_mut(&link).expect("a link");
        if !closed.up {
            return;
        }
        closed.up = false;
        let (_, theirs) = closed.other(me);
        let event = TransportEvent::Closed {
            peer: me,
            connection: theirs,
        };
        net.deliver(link, me, peer, event);
    }
}

impl Server for Port {
    /// Answers after up to 3 s, past a disconnect sometimes; one in five
    /// after 4 to 8 s, past the requester's timeout; one in ten never.
    fn serve(&mut self, token: ReplyToken, request: Req, reply: Reply<Served>) {
        let mut net = self.net.net();
        if net.world.chance(1, 10) {
            return;
        }
        let after = if net.world.chance(1, 5) {
            4_000 + net.world.below(4_000)
        } else {
            net.world.below(3_000)
        };
        let at = net.clock.get() + MS + after * MS;
        let event = Served {
            token,
            response: Some(Resp::of(self.me, &request)),
        };
        net.flight.send_at(at, Msg::Served { reply, event });
    }
}
