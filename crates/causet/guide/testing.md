The causet guide's Rust examples for its testing page, run as doc tests
(`cargo test -p causet --doc`). The guide's pages are in its own
repository and show each block here by the name above it.

<!-- snippet: one-node -->
```
use causet::guide::shop::*;
use causet::{ManualClock, SessionId};
use causet_test::Node;

let clock = ManualClock::new(1_000);
let services = ShopServices { gateway: Box::new(ApproveUpTo(100)) };
let mut shop = Node::from_protocol("shop", protocol(), ShopState::default(), services, SessionId(1), &clock)
    .check("every order is paid, declined or pending", |s: &ShopState| {
        let p = &s.payments;
        let ended = p.paid.len() + p.declined.len() + p.pending.len();
        if ended as u64 == s.orders { Ok(()) } else { Err(format!("{} orders, {ended} charges", s.orders)) }
    });
let cart = shop.mount(&root::<ShopRoot>().child(CART));

shop.dispatch(&cart, CartAction::Add { item: "tea".into(), price: 4 });
shop.dispatch(&cart, CartAction::Checkout);
// The gateway answered at once, but its answer is held.
assert_eq!(shop.held().len(), 1);
assert_eq!(shop.state().payments.pending.len(), 1);
shop.deliver_next();
assert_eq!(shop.state().payments.paid, [4]);
```

<!-- snippet: shared-world -->
```
use std::sync::{Arc, Mutex};

use causet::guide::shop::*;
use causet::{ManualClock, SessionId};
use causet_test::{Config, Driven, Group, InFlight, Node, Rng, Scenario, Stop, Swarm, World, check, search};

const MS: u64 = 1_000_000;
const BUDGET: u64 = 200;

/// The bank every shop's gateway reaches.
struct Bank {
    /// The world's own stream: the answers' delays.
    world: Rng,
    clock: ManualClock,
    flight: InFlight<(Reply<Charged>, Charged)>,
    approved: u64,
    /// The planted bug: approvals are not counted.
    forgets: bool,
}

/// The gateway of every shop, and the test's world.
#[derive(Clone)]
struct Shared(Arc<Mutex<Bank>>);

impl Gateway for Shared {
    fn charge(&mut self, request: RequestId, amount: u64, reply: Reply<Charged>) {
        let mut bank = self.0.lock().unwrap();
        let approved = bank.approved + amount <= BUDGET;
        if approved && !bank.forgets {
            bank.approved += amount;
        }
        let at = bank.clock.get() + bank.world.between(1, 50) * MS;
        bank.flight.send_at(at, (reply, Charged { request, approved }));
    }
}

impl World for Shared {
    fn next_time(&self) -> Option<Timestamp> {
        self.0.lock().unwrap().flight.next_time()
    }
    fn advance(&mut self, now: Timestamp) {
        let due = self.0.lock().unwrap().flight.due(now);
        for (reply, answer) in due {
            reply.try_send_as_worker(answer).expect("the held bus takes it");
        }
    }
}

#[derive(Clone, Copy)]
enum Step { Deliver, Add, Checkout }

/// The nodes, as a scenario drives them.
fn driven(nodes: &mut [Node<ShopState, ShopServices>]) -> Vec<&mut dyn Driven> {
    nodes.iter_mut().map(|n| n as &mut dyn Driven).collect()
}

fn shops(rng: &mut Rng, forgets: bool) {
    let mut scenario = Scenario::with_rng(rng.clone(), 1_000 * MS);
    let bank = Bank { world: rng.split(), clock: scenario.clock.clone(), flight: InFlight::new(), approved: 0, forgets };
    let mut world = Shared(Arc::new(Mutex::new(bank)));
    let group = Group::new("shops");
    group.check("paid within the budget", |views, _| {
        let paid: u64 = views.nodes().map(|node| *views.get::<u64>(node).unwrap()).sum();
        if paid <= BUDGET { Ok(()) } else { Err(format!("{paid} paid")) }
    });
    let protocol = protocol();
    let mut nodes: Vec<_> = (0..rng.between(2, 3))
        .map(|i| {
            let services = ShopServices { gateway: Box::new(world.clone()) };
            Node::from_protocol(&format!("shop {i}"), protocol.clone(), ShopState::default(), services, SessionId(1), &scenario.clock)
                .publish(&group, |s: &ShopState| s.payments.paid.iter().sum::<u64>())
        })
        .collect();
    let cart = nodes[0].mount(&root::<ShopRoot>().child(CART));

    let swarm = Swarm::new(rng, &[(Step::Deliver, 3), (Step::Add, 2), (Step::Checkout, 1)]);
    while rng.more() {
        let shop = rng.below(nodes.len() as u64) as usize;
        match swarm.pick(rng) {
            Step::Deliver => drop(scenario.step(&mut driven(&mut nodes), &mut world)),
            Step::Add => drop(nodes[shop].dispatch(&cart, CartAction::Add { item: "tea".into(), price: rng.between(1, 90) })),
            Step::Checkout => drop(nodes[shop].dispatch(&cart, CartAction::Checkout)),
        }
    }
    let stop = scenario.run_until(&mut driven(&mut nodes), &mut world, 1_000, None, |_| false);
    assert_eq!(stop, Stop::Quiet);
    for node in &nodes {
        assert!(node.state().payments.pending.is_empty(), "{}: a charge never answered", node.name());
    }
}

check(Config::default(), |rng| shops(rng, false));
let found = search(Config::default(), |rng| shops(rng, true)).expect("the planted bug is found");
assert_eq!(found.key, r#"group shops: invariant "paid within the budget""#);
assert!(found.choices.len() < found.original);
```

<!-- snippet: check-with -->
```
use causet::guide::shop::*;
use causet::{ManualClock, SessionId};
use causet::invariants::InvariantStep;
use causet_test::{Config, Node, search};

/// A shop takes one payment, then restarts from what it keeps.
fn pay_then_restart(keeps: bool) {
    let services = || ShopServices { gateway: Box::new(ApproveUpTo(100)) };
    // The invariant's memory: what was paid at its last step.
    let mut before = 0;
    let mut shop = Node::from_protocol("shop", protocol(), ShopState::default(), services(), SessionId(1), &ManualClock::new(1_000))
        .check_with("paid only by answers, never forgotten", move |s: &ShopState, step: &InvariantStep<'_>| {
            let paid: u64 = s.payments.paid.iter().sum();
            let was = std::mem::replace(&mut before, paid);
            let ok = match step {
                // A fixture may start with payments made.
                InvariantStep::Added => true,
                InvariantStep::Settled { kind: "payments.charged", .. } => paid >= was,
                InvariantStep::Settled { .. } | InvariantStep::Restored => paid == was,
            };
            if ok { Ok(()) } else { Err(format!("paid {was}, then {paid}")) }
        });
    let cart = shop.mount(&root::<ShopRoot>().child(CART));
    shop.dispatch(&cart, CartAction::Add { item: "tea".into(), price: 4 });
    shop.dispatch(&cart, CartAction::Checkout);
    shop.deliver_next();
    let kept = if keeps { shop.state().clone() } else { ShopState::default() };
    shop.restart(kept, services(), SessionId(2));
}

pay_then_restart(true);
let found = search(Config::default(), |_| pay_then_restart(false)).expect("the forgotten payment is found");
assert_eq!(found.key, r#"node shop: invariant "paid only by answers, never forgotten""#);
```

<!-- snippet: group-steps -->
```
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use causet::guide::shop::*;
use causet::{ManualClock, SessionId};
use causet_test::{Config, Group, GroupStep, Node, search};

/// Shop a is paid 4 and closes; shop b restarts, then is paid `price`.
/// Returns the steps the group's check ran at.
fn two_shops(price: u64) -> Vec<String> {
    let group = Group::new("shops");
    let steps = Rc::new(RefCell::new(Vec::new()));
    let log = Rc::clone(&steps);
    // The check's memory: each shop's paid total, kept after the shop has gone.
    let mut last = BTreeMap::new();
    group.check("paid within 10, gone shops included", move |views, step| {
        log.borrow_mut().push(match step {
            GroupStep::Added => "added".to_owned(),
            GroupStep::Published { node } => format!("{node} published"),
            GroupStep::Settled { node } => format!("{node} settled"),
            GroupStep::Restarted { node } => format!("{node} restarted"),
        });
        for node in views.nodes() {
            last.insert(node.to_owned(), *views.get::<u64>(node).unwrap());
        }
        let paid: u64 = last.values().sum();
        if paid <= 10 { Ok(()) } else { Err(format!("{paid} paid")) }
    });
    let (protocol, clock) = (protocol(), ManualClock::new(1_000));
    let services = || ShopServices { gateway: Box::new(ApproveUpTo(100)) };
    let open = |name: &str| {
        Node::from_protocol(name, protocol.clone(), ShopState::default(), services(), SessionId(1), &clock)
            .publish(&group, |s: &ShopState| s.payments.paid.iter().sum::<u64>())
    };
    let cart = protocol.resolve(&root::<ShopRoot>().child(CART)).unwrap();
    let pay = |shop: &mut Node<ShopState, ShopServices>, price| {
        shop.dispatch(&cart, CartAction::Add { item: "tea".into(), price });
        shop.dispatch(&cart, CartAction::Checkout);
        shop.deliver_next();
    };
    let (mut a, b) = (open("a"), open("b"));
    pay(&mut a, 4);
    // A dropped node's view leaves its groups, and no check runs.
    drop(a);
    let state = b.state().clone();
    let mut b = b.restart(state, services(), SessionId(2));
    pay(&mut b, price);
    steps.take()
}

let steps = two_shops(5);
assert_eq!(steps[..4], ["added", "a published", "b published", "a settled"]);
assert_eq!(steps[6..], ["b restarted", "b settled", "b settled", "b settled"]);
let found = search(Config::default(), |_| drop(two_shops(7))).expect("a's 4 is remembered");
assert_eq!(found.key, r#"group shops: invariant "paid within 10, gone shops included""#);
```

<!-- snippet: choose-order -->
```
use causet::guide::shop::*;
use causet::SessionId;
use causet::hosting::HeldEvent;
use causet_test::{Driven, Node, NoWorld, Rng, Scenario};

fn driven(nodes: &mut [Node<ShopState, ShopServices>]) -> Vec<&mut dyn Driven> {
    nodes.iter_mut().map(|n| n as &mut dyn Driven).collect()
}

let mut scenario = Scenario::new(1, 1_000);
let protocol = protocol();
let mut nodes: Vec<_> = (0..2)
    .map(|i| {
        let services = ShopServices { gateway: Box::new(ApproveUpTo(100)) };
        Node::from_protocol(&format!("shop {i}"), protocol.clone(), ShopState::default(), services, SessionId(1), &scenario.clock)
    })
    .collect();
let cart = nodes[0].mount(&root::<ShopRoot>().child(CART));
for (shop, price) in [(0, 4), (0, 5), (1, 6)] {
    nodes[shop].dispatch(&cart, CartAction::Add { item: "tea".into(), price });
    nodes[shop].dispatch(&cart, CartAction::Checkout);
}
scenario.log.push("test: three checkouts, every answer held".into());
let pending = scenario.pending(&driven(&mut nodes), &NoWorld);
assert!(pending.starts_with("at 1000:\n  shop 0: 2 held, next timer None\n"));

// Any held Event of any node, chosen by the scenario's generator.
let mut any = |held: &[(usize, HeldEvent)], rng: &mut Rng| rng.below(held.len() as u64) as usize;
while scenario.step_with(&mut driven(&mut nodes), &mut NoWorld, &mut any).is_some() {}
// Shop 1 first, then shop 0's second answer before its first.
assert_eq!(nodes[0].state().payments.paid, [5, 4]);
assert_eq!(
    scenario.report(2),
    "scenario seed 1 failed after 4 steps; the last 2:\n     3. shop 0: deliver payments.charged to /payments\n     4. shop 0: deliver payments.charged to /payments"
);
let log = scenario.into_log();
assert_eq!(log[..2], ["test: three checkouts, every answer held", "shop 1: deliver payments.charged to /payments"]);
```

<!-- snippet: queue-order -->
```
use causet::guide::shop::*;
use causet::SessionId;
use causet::hosting::HeldEvent;
use causet_test::{Driven, Node, NoWorld, Rng, Scenario, next_in_queue_order};

/// Any node that holds something, chosen by the generator; within it, the
/// Event its bus would run next.
fn any_node(held: &[(usize, HeldEvent)], rng: &mut Rng) -> usize {
    let node = held[rng.below(held.len() as u64) as usize].0;
    let first = held.iter().position(|(n, _)| *n == node).unwrap();
    let own: Vec<HeldEvent> = held[first..].iter().take_while(|(n, _)| *n == node).map(|(_, e)| e.clone()).collect();
    first + next_in_queue_order(&own).unwrap()
}

for seed in 0..20 {
    let mut scenario = Scenario::new(seed, 1_000);
    let protocol = protocol();
    let mut nodes: Vec<_> = (0..2)
        .map(|i| {
            let services = ShopServices { gateway: Box::new(ApproveUpTo(100)) };
            Node::from_protocol(&format!("shop {i}"), protocol.clone(), ShopState::default(), services, SessionId(1), &scenario.clock)
        })
        .collect();
    let cart = nodes[0].mount(&root::<ShopRoot>().child(CART));
    for (shop, price) in [(0, 4), (0, 5), (1, 6)] {
        nodes[shop].dispatch(&cart, CartAction::Add { item: "tea".into(), price });
        nodes[shop].dispatch(&cart, CartAction::Checkout);
    }
    let mut driven: Vec<&mut dyn Driven> = nodes.iter_mut().map(|n| n as &mut dyn Driven).collect();
    while scenario.step_with(&mut driven, &mut NoWorld, &mut any_node).is_some() {}
    // Whichever shop goes first, each settles its own answers in order.
    assert_eq!(nodes[0].state().payments.paid, [4, 5]);
}
```

<!-- snippet: mid-workflow -->
```
use causet::guide::shop::*;
use causet::{Child, ExecutionId, ManualClock, SessionId};
use causet::hosting::Lane;
use causet_test::Node;

/// A gateway that takes a charge and answers only when the test says.
struct Silent;
impl Gateway for Silent {
    fn charge(&mut self, _: RequestId, _: u64, _: Reply<Charged>) {}
}

// As if a checkout of 30 had run: its Effect, execution 7, made the request,
// and the charge is with the gateway.
let request = RequestId { session: SessionId(1), execution: ExecutionId(7), index: 0 };
let mut state = ShopState { orders: 1, ..ShopState::default() };
state.payments.pending.insert(request, 30);
let services = ShopServices { gateway: Box::new(Silent) };
let mut shop = Node::from_protocol("shop", protocol(), state, services, SessionId(1), &ManualClock::new(1_000));
let payments = shop.try_mount(&root::<ShopRoot>().child(PAYMENTS)).expect("mounted");
assert!(shop.try_mount(&root::<ShopRoot>().child(Child::<ShopRoot, Payments>::new("refunds"))).is_err());

// The Reply that Effect would have handed the gateway. The gateway answers
// twice, from what stands in for its worker thread.
let reply = shop.sink().reply_for::<Payments, Charged>(&payments, ExecutionId(7)).expect("payments adapts Charged");
for _ in 0..2 {
    reply.try_send_as_worker(Charged { request, approved: true }).unwrap();
}
let held = shop.held();
assert_eq!((held[0].answers, held[0].lane, &*held[0].path), (Some(ExecutionId(7)), Lane::Worker, "/payments"));
assert_eq!(shop.inspect(held[1].id, |c: &Charged| c.approved), Some(true));
// The second answer is lost on the way; the first settles the charge.
assert!(shop.discard(held[1].id));
shop.deliver(held[0].id);
assert_eq!(shop.state().payments.paid, [30]);
assert!(shop.held().is_empty() && shop.state().payments.pending.is_empty());
```

<!-- snippet: held-bus -->
```
use std::sync::Arc;

use causet::guide::shop::*;
use causet::hosting::EventBus;
use causet::SessionId;

let protocol = protocol();
let (_sink, inbox, held) = EventBus::held(&protocol, 64, SessionId(1));
let services = ShopServices { gateway: Box::new(ApproveUpTo(100)) };
let mut store = Store::new(Arc::clone(&protocol), ShopState::default(), services, inbox, StoreOptions::default()).unwrap();
let cart = protocol.resolve(&root::<ShopRoot>().child(CART)).unwrap();
for price in [4, 200] {
    store.dispatch_to(&cart, CartAction::Add { item: "tea".into(), price }).unwrap();
    store.dispatch_to(&cart, CartAction::Checkout).unwrap();
}
// Both answers are held: a drain finds nothing to run.
assert_eq!(store.drain_events(16).unwrap(), 0);
let [approved, declined]: [_; 2] = held.list().try_into().unwrap();
assert_eq!(held.inspect(declined.id, |c: &Charged| c.approved), Some(false));
assert!(held.discard(declined.id));
held.release(approved.id).unwrap();
assert_eq!(store.drain_events(16).unwrap(), 1);
assert_eq!(store.state().payments.paid, [4]);
// The declined charge's answer was lost: it is still pending.
assert_eq!(store.state().payments.pending.len(), 1);
```
