The causet guide's Rust examples for its overview page, run as doc tests
(`cargo test -p causet --doc`). The guide's pages are in its own
repository and show each block here by the name above it.

<!-- snippet: checkout -->
```
use causet::guide::shop::*;

let mut shop = Shop::open(StoreOptions::default());
shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
assert_eq!(shop.store.state().payments.pending.len(), 1); // sent, not answered yet
shop.store.drain_events(16).unwrap();                     // the gateway's answer runs
assert_eq!(shop.store.state().payments.paid, [4]);
```

<!-- snippet: reducer -->
```rust,ignore
fn reduce(mut ctx: ComponentContext<'_, Self>, action: PaymentsAction, _: &ActionMeta) {
    match action {
        PaymentsAction::Charge { amount } => {
            let request = ctx.new_request();
            ctx.state_mut().pending.insert(request, amount);
            ctx.into_emitter().emit_effect(PaymentsEffect::Charge { request, amount });
        }
        PaymentsAction::Settled { request, approved } => {
            let state = ctx.state_mut();
            // A duplicate, or an answer after a timeout, finds nothing: it is stale.
            if let Some(amount) = state.pending.take(request) {
                if approved { state.paid.push(amount) } else { state.declined.push(amount) }
            }
        }
    }
}
```

<!-- snippet: deliver-out-of-order -->
```
use causet::{ManualClock, SessionId, root};
use causet::guide::shop::{ApproveUpTo, Charged, Gateway, Payments, PaymentsAction, PaymentsState};
use causet_test::Node;

let gateway: Box<dyn Gateway> = Box::new(ApproveUpTo(100));
let clock = ManualClock::new(0);
let mut node = Node::new::<Payments>("shop", PaymentsState::default(), gateway, SessionId(1), &clock);
let payments = node.mount(&root::<Payments>());
node.dispatch(&payments, PaymentsAction::Charge { amount: 30 });
node.dispatch(&payments, PaymentsAction::Charge { amount: 200 });

// The second answer arrives first; each still settles its own charge.
assert!(node.deliver_where(|charged: &Charged| !charged.approved));
assert_eq!(node.state().declined, [200]);
node.deliver_next();
assert_eq!(node.state().paid, [30]);
```

## Find bugs automatically

<!-- snippet: find-bug -->
```rust
use causet::{ManualClock, Reply, RequestId, SessionId, root};
use causet::guide::shop::{Charged, Gateway, Payments, PaymentsAction, PaymentsState};
use causet_test::{Config, Node, Rng, check, replay, search};

struct Budget { remaining: u64, forgets: bool }
impl Gateway for Budget {
    fn charge(&mut self, request: RequestId, amount: u64, reply: Reply<Charged>) {
        let approved = amount <= self.remaining;
        // The planted bug: earlier approvals are forgotten.
        if approved && !self.forgets { self.remaining -= amount; }
        reply.try_send(Charged { request, approved }).unwrap();
    }
}

fn spending(rng: &mut Rng, forgets: bool) {
    let gateway: Box<dyn Gateway> = Box::new(Budget { remaining: 100, forgets });
    let mut node = Node::new::<Payments>("shop", PaymentsState::default(), gateway, SessionId(1), &ManualClock::new(0))
        .check("paid within budget", |s: &PaymentsState| {
            if s.paid.iter().sum::<u64>() <= 100 { Ok(()) } else { Err("overspent".into()) }
        });
    let mount = node.mount(&root::<Payments>());
    while rng.more() {
        if rng.chance(1, 2) {
            node.dispatch(&mount, PaymentsAction::Charge { amount: rng.between(1, 100) });
        } else {
            node.deliver_next();
        }
    }
    while node.deliver_next().is_some() {}
}

check(Config::default(), |rng| spending(rng, false)); // The fixed gateway passes.
let found = search(Config::default(), |rng| spending(rng, true)).unwrap();
assert_eq!(found.key, r#"node shop: invariant "paid within budget""#);
assert!(found.choices.len() < found.original);
println!("seed {}: {}\nshrunk {} choices to {}\nchoices {:?}",
    found.seed, found.key, found.original, found.choices.len(), found.choices);
// Replay must reproduce the planted failure.
assert!(std::panic::catch_unwind(|| replay(&found.choices, |rng| spending(rng, true))).is_err());
```
