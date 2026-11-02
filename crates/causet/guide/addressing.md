The causet guide's Rust examples for its addressing page, run as doc tests
(`cargo test -p causet --doc`). The guide's pages are in its own
repository and show each block here by the name above it.

<!-- snippet: resolve-mount -->
```
use causet::guide::shop::*;
use causet::root;

let protocol = protocol();
let cart: Mount<Cart> = protocol.resolve(&root::<ShopRoot>().child(CART)).unwrap();
assert_eq!(protocol.path_of(&cart), "/cart");
// A Mount<Cart> accepts only CartActions: the type comes from the slot.
```

<!-- snippet: foreign-mount -->
```
use causet::guide::shop::*;
use causet::{DispatchError, root};

let mut shop = Shop::open(StoreOptions::default());
let elsewhere = protocol(); // frozen separately: another Protocol
let foreign = elsewhere.resolve(&root::<ShopRoot>().child(CART)).unwrap();
let refused = shop.store.dispatch_to(&foreign, CartAction::Checkout);
assert_eq!(refused, Err(DispatchError::ForeignMount));
```

<!-- snippet: pair -->
```
use causet::*;

#[derive(Debug, Action)]
#[causet(type = "counter.action", kind = "counter.inc")]
struct Inc;

/// A reusable leaf, mounted twice below.
struct Counter;

impl Component for Counter {
    type State = u32;
    type Action = Inc;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, _: Inc, _: &ActionMeta) {
        *ctx.state_mut() += 1;
    }
}

struct Pair;

const LEFT: Child<Pair, Counter> = Child::new("left");
const RIGHT: Child<Pair, Counter> = Child::new("right");

#[derive(Default)]
struct PairState {
    open: bool,
    left: u32,
    /// Absent until created: a fallible lens, `right?`.
    right: Option<u32>,
    left_refused: u32,
}

#[derive(Debug, Action)]
#[causet(type = "pair.action")]
enum PairAction { Open, AddRight, Bump }

/// The parent's guard on LEFT: whatever the route, nothing reaches the left
/// counter while the pair is closed.
fn only_when_open(parent: &PairState, _: &Inc, _now: Timestamp) -> bool {
    parent.open
}

impl Component for Pair {
    type State = PairState;
    type Action = PairAction;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: PairAction, _: &ActionMeta) {
        match action {
            PairAction::Open => ctx.state_mut().open = true,
            PairAction::AddRight => ctx.state_mut().right = Some(0),
            PairAction::Bump => {
                // Inline: runs now, and the parent learns the outcome.
                if ctx.reduce_child(LEFT, Inc) == ChildOutcome::Refused {
                    ctx.state_mut().left_refused += 1;
                }
                // Queued: runs after this reducer, as its own execution.
                ctx.into_emitter().to_child(RIGHT, Inc);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(LEFT, lens!(PairState => left)).admit(only_when_open).bind();
        r.child(RIGHT, lens!(PairState => right?)).bind();
    }
}

let (mut store, _sink) = Store::start::<Pair>(PairState::default(), (), StoreOptions::default()).unwrap();
let pair = store.mount(&root::<Pair>()).unwrap();
let left = store.mount(&root::<Pair>().child(LEFT)).unwrap();
let right = store.mount(&root::<Pair>().child(RIGHT)).unwrap();

// Closed, and no right counter yet: both calls are refused.
store.dispatch_to(&pair, PairAction::Bump).unwrap();
assert_eq!((store.state().left, store.state().right, store.state().left_refused), (0, None, 1));
// The guard holds on every route, not only through the parent's reducer.
assert_eq!(store.dispatch_to(&left, Inc), Ok(DispatchOutcome::Disabled));

store.dispatch_to(&pair, PairAction::Open).unwrap();
store.dispatch_to(&pair, PairAction::AddRight).unwrap();
store.dispatch_to(&pair, PairAction::Bump).unwrap();
store.dispatch_to(&right, Inc).unwrap();
assert_eq!((store.state().left, store.state().right), (1, Some(2)));

// One component, two mounts, two identities.
assert_eq!(store.protocol().path_of(&left), "/left");
assert_ne!(store.protocol().instance_of(&left), store.protocol().instance_of(&right));
```

<!-- snippet: duplicate-answer -->
```
use causet::guide::shop::*;

let mut shop = Shop::open(StoreOptions::default());
shop.store.dispatch_to(&shop.payments, PaymentsAction::Charge { amount: 5 }).unwrap();
let request = shop.store.state().payments.pending.iter().next().unwrap().0;

// A duplicate answer arrives from outside, for the same request.
shop.sink.try_send_to(&shop.payments, Charged { request, approved: true }).unwrap();
shop.store.drain_events(16).unwrap();
// The gateway's answer and the duplicate both ran; only the first matched.
assert_eq!(shop.store.state().payments.paid, [5]);
```

<!-- snippet: send-unadapted -->
```compile_fail,E0277
use causet::guide::shop::*;

let shop = Shop::open(StoreOptions::default());
let request = todo!();
shop.sink.try_send_to(&shop.cart, Charged { request, approved: true }).unwrap();
```

<!-- snippet: wallet -->
```
use causet::*;

/// Pays out; the balance it must not exceed is kept by its parent.
struct Sender;

#[derive(Debug, Default)]
struct SenderState {
    sent: Vec<u64>,
    refused: Vec<u64>,
}

/// `affordable` is context: the parent read it, the child cannot.
#[derive(Debug, Action)]
#[causet(type = "sender.action", kind = "sender.send")]
struct Send {
    amount: u64,
    affordable: bool,
}

#[derive(Debug, Output)]
#[causet(type = "sender.output", kind = "sender.spent")]
struct Spent {
    amount: u64,
}

impl Component for Sender {
    type State = SenderState;
    type Action = Send;
    type Output = Spent;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, Send { amount, affordable }: Send, _: &ActionMeta) {
        if !affordable {
            ctx.state_mut().refused.push(amount);
            return;
        }
        ctx.state_mut().sent.push(amount);
        ctx.into_emitter().emit_output(Spent { amount });
    }
}

struct Wallet;

#[derive(Debug, Default)]
struct WalletState {
    balance: u64,
    sender: SenderState,
}

#[derive(Debug, Action)]
#[causet(type = "wallet.action")]
enum WalletAction {
    Pay { amount: u64 },
    Debit { amount: u64 },
}

const SENDER: Child<Wallet, Sender> = Child::new("sender");

impl Component for Wallet {
    type State = WalletState;
    type Action = WalletAction;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: WalletAction, _: &ActionMeta) {
        match action {
            WalletAction::Pay { amount } => {
                // The parent reads what the child cannot, and passes it on.
                let affordable = ctx.state().balance >= amount;
                ctx.reduce_child(SENDER, Send { amount, affordable });
            }
            WalletAction::Debit { amount } => ctx.state_mut().balance -= amount,
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(SENDER, lens!(WalletState => sender)).on_output(debit).bind();
    }
}

/// The child's request, as the parent's Action.
fn debit(_: Child<Wallet, Sender>, Spent { amount }: Spent, port: &mut ActionPort<'_, Wallet>) {
    port.emit_action(WalletAction::Debit { amount });
}

let state = WalletState { balance: 10, ..WalletState::default() };
let (mut store, _sink) = Store::start::<Wallet>(state, (), StoreOptions::default()).unwrap();
let wallet = store.mount(&root::<Wallet>()).unwrap();

store.dispatch_to(&wallet, WalletAction::Pay { amount: 7 }).unwrap();
store.dispatch_to(&wallet, WalletAction::Pay { amount: 7 }).unwrap(); // 3 left
assert_eq!(store.state().sender.sent, [7]);
assert_eq!(store.state().sender.refused, [7]);
assert_eq!(store.state().balance, 3);
```
