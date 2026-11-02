//! An acceptance scenario: two Stores on a simulated network, a reply delayed
//! past its timeout, a restart that refuses the old route and leaves an old
//! request to the application's recovery policy, and the same outcome from
//! the same seed. Then a focused transition test from a validated fixture,
//! and a composite on the real Store.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use causet::hosting::Lane;
use causet::*;
use causet_test::{Driven, Group, NoWorld, Node, Rng, Scenario, Stop, World};

const MS: u64 = 1_000_000;
const TIMEOUT: Duration = Duration::from_millis(50);
const START: Timestamp = 1_000 * MS;

// ---------------------------------------------------------------------------
// Messages

#[derive(Debug, Action)]
#[causet(type = "client.action")]
enum ClientAction {
    Ask { n: u64 },
    Answered { request: RequestId, value: u64 },
    TimedOut { request: RequestId },
}

#[derive(Debug, Effect)]
#[causet(type = "client.effect")]
enum ClientEffect {
    Send { request: RequestId, n: u64 },
    Arm { request: RequestId },
}

/// The server's answer, as the network delivers it.
#[derive(Debug, Event)]
#[causet(type = "client.answer")]
struct Answer {
    request: RequestId,
    value: u64,
}

#[derive(Debug, Action)]
#[causet(type = "server.action", kind = "server.serve")]
struct Serve {
    request: RequestId,
    n: u64,
}

#[derive(Debug, Effect)]
#[causet(type = "server.effect", kind = "server.respond")]
struct Respond {
    request: RequestId,
    value: u64,
}

/// A client's question, as the network delivers it.
#[derive(Debug, Event)]
#[causet(type = "server.question")]
struct Question {
    request: RequestId,
    n: u64,
}

// ---------------------------------------------------------------------------
// The client: asks for squares, times each request out.

#[derive(Clone, Debug, PartialEq)]
struct ClientState {
    /// This run's session: the requests it makes carry it.
    session: SessionId,
    pending: BTreeMap<RequestId, u64>,
    answered: Vec<(u64, u64)>,
    timed_out: Vec<u64>,
    /// Requests of an earlier run, given up on recovery.
    abandoned: Vec<u64>,
    /// Every request that ended (answered, timed out or given up): each ends
    /// once. Kept by request, since two requests may ask the same number.
    settled: BTreeSet<RequestId>,
    /// Answers for no pending request: late, or for a request given up.
    stale_answers: u32,
    /// Timeouts for a request already answered.
    stale_timeouts: u32,
}

impl ClientState {
    fn new(session: SessionId) -> Self {
        Self {
            session,
            pending: BTreeMap::new(),
            answered: Vec::new(),
            timed_out: Vec::new(),
            abandoned: Vec::new(),
            settled: BTreeSet::new(),
            stale_answers: 0,
            stale_timeouts: 0,
        }
    }

    /// The application's recovery policy: the old run's requests are given
    /// up, so an answer to one, however it arrives, finds nothing pending.
    fn recovered(mut self, session: SessionId) -> Self {
        self.abandoned.extend(self.pending.values());
        self.settled.extend(self.pending.keys());
        self.pending.clear();
        self.session = session;
        self
    }

    /// Requests made in every run so far: pending or ended.
    fn made(&self) -> usize {
        self.pending.len() + self.settled.len()
    }

    fn invariants(&self) -> Result<(), String> {
        if let Some(request) = self.pending.keys().find(|r| r.session != self.session) {
            return Err(format!("{request:?} is pending from another run"));
        }
        if let Some((n, value)) = self.answered.iter().find(|(n, value)| *value != n * n) {
            return Err(format!("{n} answered {value}"));
        }
        if let Some(request) = self.pending.keys().find(|r| self.settled.contains(r)) {
            return Err(format!("{request:?} is both pending and ended"));
        }
        let ended = self.answered.len() + self.timed_out.len() + self.abandoned.len();
        if ended != self.settled.len() {
            return Err(format!(
                "{ended} endings for {} requests: one ended twice",
                self.settled.len()
            ));
        }
        Ok(())
    }
}

struct ClientServices {
    net: Net,
    timer: Timer,
}

struct Client;

impl Component for Client {
    type State = ClientState;
    type Service = ClientServices;
    type Action = ClientAction;
    type Effect = ClientEffect;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: ClientAction, _: &ActionMeta) {
        match action {
            ClientAction::Ask { n } => {
                let request = ctx.new_request();
                ctx.state_mut().pending.insert(request, n);
                let mut out = ctx.into_emitter();
                out.emit_effect(ClientEffect::Send { request, n });
                out.emit_effect(ClientEffect::Arm { request });
            }
            ClientAction::Answered { request, value } => {
                let state = ctx.state_mut();
                match state.pending.remove(&request) {
                    Some(n) => {
                        state.answered.push((n, value));
                        state.settled.insert(request);
                    }
                    None => state.stale_answers += 1,
                }
            }
            ClientAction::TimedOut { request } => {
                let state = ctx.state_mut();
                match state.pending.remove(&request) {
                    Some(n) => {
                        state.timed_out.push(n);
                        state.settled.insert(request);
                    }
                    None => state.stale_timeouts += 1,
                }
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: ClientEffect, _: &EffectMeta) {
        match effect {
            ClientEffect::Send { request, n } => ctx.service().net.ask(request, n),
            ClientEffect::Arm { request } => {
                let reply = ctx.reply::<TimerFired>();
                ctx.service().timer.after(TIMEOUT, reply, request);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Answer>();
        r.event::<TimerFired>();
    }
}

impl Adapts<Answer> for Client {
    fn adapt(answer: Answer, port: &mut ActionPort<'_, Self>) {
        port.emit_action(ClientAction::Answered {
            request: answer.request,
            value: answer.value,
        });
    }
}

impl Adapts<TimerFired> for Client {
    fn adapt(fired: TimerFired, port: &mut ActionPort<'_, Self>) {
        port.emit_action(ClientAction::TimedOut {
            request: fired.request,
        });
    }
}

// ---------------------------------------------------------------------------
// The server: squares.

#[derive(Clone, Debug, Default, PartialEq)]
struct ServerState {
    served: u64,
}

struct ServerServices {
    net: Net,
}

struct Server;

impl Component for Server {
    type State = ServerState;
    type Service = ServerServices;
    type Action = Serve;
    type Effect = Respond;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, Serve { request, n }: Serve, _: &ActionMeta) {
        ctx.state_mut().served += 1;
        ctx.into_emitter().emit_effect(Respond {
            request,
            value: n * n,
        });
    }

    fn handle(
        mut ctx: EffectContext<'_, Self>,
        Respond { request, value }: Respond,
        _: &EffectMeta,
    ) {
        ctx.service().net.answer(request, value);
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Question>();
    }
}

impl Adapts<Question> for Server {
    fn adapt(question: Question, port: &mut ActionPort<'_, Self>) {
        port.emit_action(Serve {
            request: question.request,
            n: question.n,
        });
    }
}

// ---------------------------------------------------------------------------
// The network: the application's own model. A message travels for a seeded
// delay and is delivered to the address it was sent to, as a network thread
// would deliver it: into the worker lane. If that address is gone (the client
// restarted), the delivery is refused, and the network resends to the
// client's current address.

#[derive(Clone)]
struct Net(Arc<Mutex<Network>>);

struct Network {
    clock: ManualClock,
    rng: Rng,
    seq: u64,
    in_flight: Vec<Flight>,
    client: Option<(EventSink, Mount<Client>)>,
    server: Option<(EventSink, Mount<Server>)>,
    /// Extra delay for the next answer: the test's lever.
    slow_next_answer: Option<Duration>,
    /// Deliveries refused at an old address.
    refused: u32,
    /// Resent to the current address after a refusal.
    rerouted: u32,
}

struct Flight {
    at: Timestamp,
    seq: u64,
    message: Message,
}

enum Message {
    Question {
        request: RequestId,
        n: u64,
        to: (EventSink, Mount<Server>),
    },
    Answer {
        request: RequestId,
        value: u64,
        to: (EventSink, Mount<Client>),
    },
}

impl Net {
    /// A network whose delays are drawn from `rng`.
    fn new(rng: Rng, clock: &ManualClock) -> Self {
        Self(Arc::new(Mutex::new(Network {
            clock: clock.clone(),
            rng,
            seq: 0,
            in_flight: Vec::new(),
            client: None,
            server: None,
            slow_next_answer: None,
            refused: 0,
            rerouted: 0,
        })))
    }

    fn attach_client(&self, node: &Node<ClientState, ClientServices>) {
        self.0.lock().unwrap().client = Some((node.sink().clone(), node.mount(&root::<Client>())));
    }

    fn attach_server(&self, node: &Node<ServerState, ServerServices>) {
        self.0.lock().unwrap().server = Some((node.sink().clone(), node.mount(&root::<Server>())));
    }

    fn slow_next_answer(&self, by: Duration) {
        self.0.lock().unwrap().slow_next_answer = Some(by);
    }

    fn counters(&self) -> (u32, u32) {
        let net = self.0.lock().unwrap();
        (net.refused, net.rerouted)
    }

    fn ask(&self, request: RequestId, n: u64) {
        let mut net = self.0.lock().unwrap();
        let to = net.server.clone().expect("a server");
        net.send(Message::Question { request, n, to }, Duration::ZERO);
    }

    fn answer(&self, request: RequestId, value: u64) {
        let mut net = self.0.lock().unwrap();
        let to = net.client.clone().expect("a client");
        let extra = net.slow_next_answer.take().unwrap_or_default();
        net.send(Message::Answer { request, value, to }, extra);
    }
}

impl Network {
    fn send(&mut self, message: Message, extra: Duration) {
        let delay = self.rng.between(MS, 5 * MS) + extra.as_nanos() as u64;
        let at = self.clock.get() + delay;
        self.in_flight.push(Flight {
            at,
            seq: self.seq,
            message,
        });
        self.seq += 1;
        self.in_flight.sort_by_key(|f| (f.at, f.seq));
    }
}

impl World for Net {
    fn next_time(&self) -> Option<Timestamp> {
        self.0.lock().unwrap().in_flight.first().map(|f| f.at)
    }

    fn advance(&mut self, now: Timestamp) {
        let mut net = self.0.lock().unwrap();
        let due: Vec<Flight> = {
            let split = net.in_flight.iter().take_while(|f| f.at <= now).count();
            net.in_flight.drain(..split).collect()
        };
        for flight in due {
            match flight.message {
                Message::Question {
                    request,
                    n,
                    to: (sink, mount),
                } => {
                    sink.try_send_to_as_worker(&mount, Question { request, n })
                        .expect("the server is up");
                }
                Message::Answer {
                    request,
                    value,
                    to: (sink, mount),
                } => {
                    match sink.try_send_to_as_worker(&mount, Answer { request, value }) {
                        Ok(()) => {}
                        Err(SendError::Closed(answer)) => {
                            // The old route is refused; a valid new route
                            // carries the same old request.
                            net.refused += 1;
                            let (sink, mount) = net.client.clone().expect("a client");
                            sink.try_send_to_as_worker(&mount, answer)
                                .expect("the current client is up");
                            net.rerouted += 1;
                        }
                        Err(e) => panic!("unexpected {e:?}"),
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The scenario

#[derive(Debug, PartialEq)]
struct Outcome {
    log: Vec<String>,
    client: ClientState,
    server: ServerState,
    /// (refused at the old address, resent to the new one)
    network: (u32, u32),
}

type ClientNode = Node<ClientState, ClientServices>;
type ServerNode = Node<ServerState, ServerServices>;

fn client_node(scenario: &Scenario, net: &Net, state: ClientState) -> ClientNode {
    let (timer, control) = Timer::simulated(&scenario.clock);
    let session = state.session;
    Node::new::<Client>(
        "client",
        state,
        ClientServices {
            net: net.clone(),
            timer,
        },
        session,
        &scenario.clock,
    )
    .with_timer(control)
    .check("client", ClientState::invariants)
}

/// The client restarted with the State its recovery policy keeps.
fn restart(client: ClientNode, scenario: &Scenario, net: &Net, session: SessionId) -> ClientNode {
    let (timer, control) = Timer::simulated(&scenario.clock);
    let kept = client.state().clone().recovered(session);
    let client = client
        .restart(
            kept,
            ClientServices {
                net: net.clone(),
                timer,
            },
            session,
        )
        .with_timer(control);
    net.attach_client(&client);
    client
}

/// Runs until nothing is held or ahead; fails on a stop of any other kind.
fn settle(
    scenario: &mut Scenario,
    client: &mut ClientNode,
    server: &mut ServerNode,
    net: &mut Net,
) {
    let mut nodes: [&mut dyn Driven; 2] = [client, server];
    let stop = scenario.run_until(&mut nodes, net, 1_000, None, |_| false);
    assert_eq!(stop, Stop::Quiet, "{}", scenario.pending(&nodes, net));
}

fn run(seed: u64) -> Outcome {
    let mut scenario = Scenario::new(seed, START);
    let mut net = Net::new(Rng::new(seed), &scenario.clock);
    let mut client = client_node(&scenario, &net, ClientState::new(SessionId(1)));
    let mut server = Node::new::<Server>(
        "server",
        ServerState::default(),
        ServerServices { net: net.clone() },
        SessionId(2),
        &scenario.clock,
    );
    net.attach_client(&client);
    net.attach_server(&server);
    let ask = |client: &mut ClientNode, n| {
        let mount = client.mount(&root::<Client>());
        client.dispatch(&mount, ClientAction::Ask { n })
    };

    // 1. Answered in time.
    ask(&mut client, 1);
    settle(&mut scenario, &mut client, &mut server, &mut net);
    assert_eq!(client.state().answered, [(1, 1)]);
    // Settling ran on to the timeout, which found the request answered.
    assert_eq!(client.state().stale_timeouts, 1);

    // 2. The answer comes after the timeout: the timeout wins, the late
    // answer is ignored.
    net.slow_next_answer(2 * TIMEOUT);
    ask(&mut client, 2);
    settle(&mut scenario, &mut client, &mut server, &mut net);
    assert_eq!(client.state().timed_out, [2]);
    assert_eq!(client.state().stale_answers, 1);

    // 3. The client restarts while the answer to 3 is in flight to its old
    // address.
    ask(&mut client, 3);
    {
        let mut nodes: [&mut dyn Driven; 2] = [&mut client, &mut server];
        let served = |nodes: &[&mut dyn Driven]| {
            nodes[1]
                .as_any()
                .downcast_ref::<ServerNode>()
                .expect("the server")
                .state()
                .served
                == 3
        };
        let stop = scenario.run_until(&mut nodes, &mut net, 100, None, served);
        assert_eq!(stop, Stop::Reached, "{}", scenario.pending(&nodes, &net));
    }
    client = restart(client, &scenario, &net, SessionId(3));
    settle(&mut scenario, &mut client, &mut server, &mut net);
    // The old route refused the answer; through the new route it reached a
    // run that gave the request up, so it was ignored.
    assert_eq!(net.counters(), (1, 1));
    assert_eq!(client.state().abandoned, [3]);
    assert_eq!(client.state().stale_answers, 2);

    // 4. The new run works.
    ask(&mut client, 4);
    settle(&mut scenario, &mut client, &mut server, &mut net);
    assert_eq!(client.state().answered, [(1, 1), (4, 16)]);
    assert_eq!(client.state().stale_timeouts, 2);

    // An invariant across both Stores: every question reached the server once.
    assert_eq!(server.state().served, 4);
    client.state().invariants().unwrap();

    let (client, server, network) = (
        client.state().clone(),
        server.state().clone(),
        net.counters(),
    );
    Outcome {
        log: scenario.into_log(),
        client,
        server,
        network,
    }
}

#[test]
fn the_scenario_repeats_from_its_seed() {
    let first = run(7);
    let second = run(7);
    assert_eq!(first, second, "the same seed made different decisions");
}

#[test]
fn every_seed_reaches_the_same_outcome_by_its_own_timing() {
    // Request IDs carry their execution's time, which differs by seed: the
    // requests that ended are compared by count.
    let untimed = |o: &Outcome| {
        let mut client = o.client.clone();
        let settled = std::mem::take(&mut client.settled).len();
        (client, settled, o.server.clone(), o.network)
    };
    let outcomes: Vec<Outcome> = (1..=5).map(run).collect();
    for outcome in &outcomes[1..] {
        assert_eq!(untimed(outcome), untimed(&outcomes[0]));
    }
    // The network's delays differ by seed, and so do the steps' times.
    assert!(outcomes.iter().any(|o| o.log != outcomes[0].log));
}

// ---------------------------------------------------------------------------
// A focused transition test from a validated fixture: a client mid-request,
// whose pending request, armed timer and session agree with its State.

/// Builds a client State with requests already pending, and checks the
/// domain's invariants before any test uses it.
struct ClientFixture {
    state: ClientState,
    next_index: u32,
}

impl ClientFixture {
    fn new(session: SessionId) -> Self {
        Self {
            state: ClientState::new(session),
            next_index: 0,
        }
    }

    /// A request made by execution `execution` of this run.
    fn pending(mut self, execution: ExecutionId, n: u64) -> (Self, RequestId) {
        let request = RequestId {
            session: self.state.session,
            execution,
            index: self.next_index,
        };
        self.next_index += 1;
        self.state.pending.insert(request, n);
        (self, request)
    }

    fn build(self) -> Result<ClientState, String> {
        self.state.invariants()?;
        Ok(self.state)
    }
}

#[test]
fn a_transition_from_a_fixture_mid_request() {
    let clock = ManualClock::new(START);
    let session = SessionId(10);
    let made_by = ExecutionId(START - 1);
    let (fixture, request) = ClientFixture::new(session).pending(made_by, 7);
    let state = fixture.build().expect("a valid fixture");
    let net = Net::new(Rng::new(0), &clock);
    let (timer, control) = Timer::simulated(&clock);
    let mut client = Node::new::<Client>(
        "client",
        state,
        ClientServices { net, timer },
        session,
        &clock,
    )
    .with_timer(control)
    .check("client", ClientState::invariants);
    let mount = client.mount(&root::<Client>());

    // The Service work the fixture implies: the request's timeout is armed,
    // answering through a reply route of the Effect that armed it.
    let reply = client
        .sink()
        .reply_for::<Client, TimerFired>(&mount, made_by)
        .expect("a registered Event");
    // The fixture arms it on a simulated Timer of its own.
    let (fixture_timer, fixture_control) = Timer::simulated(&clock);
    fixture_timer.after(TIMEOUT, reply, request);

    // The transition under test: the answer arrives.
    client
        .sink()
        .try_send_to_as_worker(&mount, Answer { request, value: 49 })
        .unwrap();
    let [held] = client.held().try_into().expect("one held Event");
    assert_eq!(
        (held.kind, held.path.as_ref(), held.session, held.lane),
        ("client.answer", "/", session, Lane::Worker)
    );
    client.deliver(held.id);
    assert_eq!(client.state().answered, [(7, 49)]);
    assert!(client.state().pending.is_empty());

    // Later, the timeout fires for a request no longer pending: ignored.
    clock.advance(TIMEOUT.as_nanos() as u64);
    assert_eq!(fixture_control.fire_due(), 1);
    let [fired] = client.held().try_into().expect("the timeout");
    client.deliver(fired.id);
    assert_eq!(client.state().stale_timeouts, 1);
    assert!(client.state().timed_out.is_empty());
}

// ---------------------------------------------------------------------------
// A composite on the real Store: the guide's shop, its cart and payment
// children and its gateway all real, the gateway's answer held until
// delivered.

#[test]
fn a_composite_runs_its_real_children() {
    use causet::guide::shop::*;
    let clock = ManualClock::new(START);
    let services = ShopServices {
        gateway: Box::new(ApproveUpTo(100)),
    };
    let mut shop = Node::from_protocol(
        "shop",
        protocol(),
        ShopState::default(),
        services,
        SessionId(20),
        &clock,
    )
    .check("paid within the gateway's limit", |s: &ShopState| {
        if s.payments.paid.iter().all(|&p| p <= 100) {
            Ok(())
        } else {
            Err(format!("paid {:?}", s.payments.paid))
        }
    });
    let cart = shop.mount(&root::<ShopRoot>().child(CART));
    shop.dispatch(
        &cart,
        CartAction::Add {
            item: "tea".into(),
            price: 4,
        },
    );
    shop.dispatch(&cart, CartAction::Checkout);

    // The cart's Output, the root's Pay and the payments child's Charge all
    // ran; the gateway answered at once, and its answer is held.
    assert_eq!(shop.state().orders, 1);
    assert_eq!(shop.state().payments.pending.len(), 1);
    let [charged] = shop.held().try_into().expect("the gateway's answer");
    assert_eq!(
        (charged.kind, charged.path.as_ref()),
        ("payments.charged", "/payments")
    );
    assert!(
        shop.inspect::<Charged, bool>(charged.id, |c| c.approved)
            .unwrap()
    );

    shop.deliver(charged.id);
    assert_eq!(shop.state().payments.paid, [4]);

    let mut nodes: [&mut dyn Driven; 1] = [&mut shop];
    let mut scenario = Scenario::new(0, START);
    assert_eq!(
        scenario.run_until(&mut nodes, &mut NoWorld, 10, None, |_| false),
        Stop::Quiet
    );
}

// ---------------------------------------------------------------------------
// Generated runs: the same two Stores, driven by steps drawn from one stream
// of choices, which also sets the network's delays.

/// A failure planted in a generated run, for shrinking to find.
#[derive(Clone, Copy, PartialEq)]
enum Planted {
    None,
    /// The client claims it never times out, which a slow enough answer
    /// breaks.
    NeverTimesOut,
    /// A progress property posing as an invariant:
    /// the server served every question asked, which fails while one is in
    /// flight.
    ServedEveryQuestion,
}

/// One generated run, with `planted` failures.
fn generated(rng: &mut Rng, planted: Planted) {
    let mut scenario = Scenario::with_rng(rng.clone(), START);
    let mut net = Net::new(rng.clone(), &scenario.clock);

    // Across both Stores, while work is in flight and the client restarts:
    // the server never served more questions than the client has made.
    let pair = Group::new("pair");
    pair.check("served at most what was asked", |views, _| {
        match (views.get::<usize>("client"), views.get::<u64>("server")) {
            (Some(&made), Some(&served)) if served as usize > made => {
                Err(format!("served {served} of {made}"))
            }
            _ => Ok(()),
        }
    });
    if planted == Planted::ServedEveryQuestion {
        pair.check("served every question", |views, _| {
            match (views.get::<usize>("client"), views.get::<u64>("server")) {
                (Some(&made), Some(&served)) if served as usize != made => {
                    Err(format!("served {served} of {made}"))
                }
                _ => Ok(()),
            }
        });
    }

    let mut client = client_node(&scenario, &net, ClientState::new(SessionId(1)))
        .publish(&pair, ClientState::made);
    if planted == Planted::NeverTimesOut {
        client = client.check("never times out", |s: &ClientState| {
            match s.timed_out.first() {
                Some(n) => Err(format!("{n} timed out")),
                None => Ok(()),
            }
        });
    }
    let mut server = Node::new::<Server>(
        "server",
        ServerState::default(),
        ServerServices { net: net.clone() },
        SessionId(2),
        &scenario.clock,
    )
    .publish(&pair, |s: &ServerState| s.served);
    net.attach_client(&client);
    net.attach_server(&server);
    let (mut asks, mut session) = (0, 2);
    while rng.more() {
        match rng.below(4) {
            0 => {
                let mut nodes: [&mut dyn Driven; 2] = [&mut client, &mut server];
                scenario.step(&mut nodes, &mut net);
            }
            1 => {
                let mount = client.mount(&root::<Client>());
                client.dispatch(&mount, ClientAction::Ask { n: rng.below(10) });
                asks += 1;
            }
            2 => net.slow_next_answer(Duration::from_millis(rng.below(100))),
            _ => {
                session += 1;
                client = restart(client, &scenario, &net, SessionId(session));
            }
        }
    }
    let mut nodes: [&mut dyn Driven; 2] = [&mut client, &mut server];
    let stop = scenario.run_until(&mut nodes, &mut net, 10_000, None, |_| false);
    assert_eq!(stop, Stop::Quiet, "{}", scenario.pending(&nodes, &net));

    // Across both Stores, once quiet: every question reached the server, and
    // every request ended one way.
    let state = client.state();
    assert_eq!(server.state().served, asks);
    assert!(state.pending.is_empty());
    assert_eq!(
        state.answered.len() + state.timed_out.len() + state.abandoned.len(),
        asks as usize
    );
}

#[test]
fn generated_runs_keep_every_invariant() {
    causet_test::check(causet_test::Config::default(), |rng| {
        generated(rng, Planted::None)
    });
}

#[test]
fn a_planted_failure_shrinks_to_a_slow_answer_after_one_question() {
    let found = causet_test::search(causet_test::Config::default(), |rng| {
        generated(rng, Planted::NeverTimesOut)
    })
    .expect("a timeout");
    assert_eq!(found.key, "node client: invariant \"never times out\"");
    assert!(found.choices.len() < found.original, "{found:?}");

    // What is left, choice by choice: a step (more), Ask, n = 0, the
    // question's network delay (the shortest, 1 ms); a step, slow the next
    // answer, by 49 ms; no more steps; the answer's own delay (1 ms). The
    // answer then arrives 1 + 1 + 49 = 51 ms in, past the 50 ms timeout; by
    // 48 it would arrive at 50 ms, delivered before the timer that fires at
    // the same time, and answered in time.
    assert_eq!(found.choices, [1, 1, 0, 0, 1, 2, 49, 0, 0], "{found:?}");

    // The list repeats the failure.
    let replayed = std::panic::catch_unwind(|| {
        causet_test::replay(&found.choices, |rng| generated(rng, Planted::NeverTimesOut))
    });
    assert!(replayed.is_err());
}

#[test]
fn progress_posing_as_an_invariant_fails_while_a_question_is_in_flight() {
    let found = causet_test::search(causet_test::Config::default(), |rng| {
        generated(rng, Planted::ServedEveryQuestion)
    })
    .expect("a question in flight");
    assert_eq!(found.key, "group pair: invariant \"served every question\"");
    // A step, Ask, n = 0, and the question's network delay (drawn as it is
    // sent): the client has made a request the server has not seen yet.
    assert_eq!(found.choices, [1, 1, 0, 0], "{found:?}");
}
