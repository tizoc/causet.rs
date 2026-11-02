The causet guide's Rust examples for its running page, run as doc tests
(`cargo test -p causet --doc`). The guide's pages are in its own
repository and show each block here by the name above it.

<!-- snippet: start -->
```
use causet::guide::shop::*;

let services = ShopServices { gateway: Box::new(ApproveUpTo(100)) };
let (mut store, _sink) =
    Store::start::<ShopRoot>(ShopState::default(), services, StoreOptions::default())
        .expect("a valid registration");
let cart = store.mount(&root::<ShopRoot>().child(CART)).unwrap();

store.dispatch_to(&cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
assert_eq!(store.state().cart.total, 4);
```

<!-- snippet: assemble -->
```
use std::sync::Arc;
use causet::guide::shop::*;
use causet::hosting::EventBus;

/// The shop, assembled by hand: a Protocol, its bus, a Store and its mounts.
fn open(gateway: Box<dyn Gateway>, worker_capacity: usize) -> Shop {
    let mut registry = Registry::<ShopState, ShopServices>::new();
    registry.root::<ShopRoot>(); // registers the whole tree
    let protocol = registry.freeze().expect("a valid registration");

    let (sink, inbox) = EventBus::new(&protocol, worker_capacity);
    let services = ShopServices { gateway };
    let store = Store::new(
        Arc::clone(&protocol), ShopState::default(), services, inbox, StoreOptions::default(),
    )
    .expect("the bus is this Protocol's");

    let cart = protocol.resolve(&root::<ShopRoot>().child(CART)).unwrap();
    let payments = protocol.resolve(&root::<ShopRoot>().child(PAYMENTS)).unwrap();
    Shop { store, sink, cart, payments }
}

let mut shop = open(Box::new(ApproveUpTo(100)), 64);
shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
shop.store.drain_events(16).unwrap();
assert_eq!(shop.store.state().payments.paid, [4]);
```

<!-- snippet: event-readiness -->
```
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use causet::guide::shop::*;

let mut shop = Shop::open(StoreOptions::default());
let ready = shop.store.readiness();
let pending = Arc::new(AtomicBool::new(false));
let wake = pending.clone();
// A host integration would schedule its driver or unpark its thread here.
let mut subscription = ready.subscribe(move || { wake.store(true, Ordering::SeqCst); }).unwrap();
shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
assert!(pending.load(Ordering::SeqCst));
assert!(shop.store.state().payments.paid.is_empty());
shop.store.drain_events(64).unwrap();
assert_eq!(shop.store.state().payments.paid, [4]);
subscription.cancel();

```

<!-- snippet: blocking-readiness -->
```
use std::time::Duration;
use causet::guide::shop::*;
use causet::hosting::EventAvailability;

let mut shop = Shop::open(StoreOptions::default());
let ready = shop.store.readiness();
assert_eq!(ready.wait_timeout(Duration::ZERO), None);
shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
assert_eq!(ready.wait_timeout(Duration::from_secs(1)), Some(EventAvailability::Events));
assert!(shop.store.state().payments.paid.is_empty());
shop.store.drain_events(64).unwrap();
assert_eq!(shop.store.state().payments.paid, [4]);
```

<!-- snippet: registration-problems -->
```
use causet::guide::shop::*;
use causet::{Problem, RegistrationError};

/// A root that gets its wiring wrong three ways.
struct Miswired;

const FIRST: Child<Miswired, Cart> = Child::new("cart");
const SECOND: Child<Miswired, Cart> = Child::new("cart");
const PAY: Child<Miswired, Payments> = Child::new("payments");

impl Component for Miswired {
    type State = ShopState;
    type Action = ShopAction;
    type Output = Never;
    no_effects!(ShopServices);

    fn reduce(_: ComponentContext<'_, Self>, _: ShopAction, _: &ActionMeta) {}

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        // The cart reports Outputs, and nothing lowers them.
        r.child(FIRST, lens!(ShopState => cart)).bind();
        // A second slot with the same name.
        r.child(SECOND, lens!(ShopState => cart)).bind();
        // Payments has Effects, and no Service lens reaches it.
        r.child(PAY, lens!(ShopState => payments)).bind();
    }
}

let mut registry = Registry::<ShopState, ShopServices>::new();
registry.root::<Miswired>();
let Err(RegistrationError(problems)) = registry.freeze() else { panic!("frozen") };
assert_eq!(problems, [
    Problem::NoLowerer { path: "/cart".into() },
    Problem::SlotBoundTwice { path: "/cart".into() },
    Problem::NoServiceProjection { path: "/payments".into(), at: "/payments".into() },
]);
```

<!-- snippet: store-errors -->
```
use std::sync::Arc;
use causet::guide::shop::*;
use causet::hosting::EventBus;
use causet::{DispatchError, ExecutionId, SendError, root};

// A bus made for one Protocol, given to a Store of another.
let (_sink, inbox) = EventBus::new(&protocol(), 64);
let services = ShopServices { gateway: Box::new(ApproveUpTo(100)) };
let refused = Store::new(protocol(), ShopState::default(), services, inbox, StoreOptions::default());
assert_eq!(refused.err(), Some(DispatchError::ForeignBus));

// A Mount resolved against another Protocol.
let mut shop = Shop::open(StoreOptions::default());
let foreign = protocol().resolve(&root::<ShopRoot>().child(CART)).unwrap();
let refused = shop.store.dispatch_to(&foreign, CartAction::Checkout);
assert_eq!(refused, Err(DispatchError::ForeignMount));

/// A gateway whose code panics.
struct Down;

impl Gateway for Down {
    fn charge(&mut self, _: RequestId, _: u64, _: Reply<Charged>) {
        panic!("the gateway is down");
    }
}

// A panic faults the Store: this call and every later one say why...
let shop = protocol();
let (sink, inbox) = EventBus::new(&shop, 64);
let services = ShopServices { gateway: Box::new(Down) };
let mut store =
    Store::new(Arc::clone(&shop), ShopState::default(), services, inbox, StoreOptions::default())
        .unwrap();
let payments = shop.resolve(&root::<ShopRoot>().child(PAYMENTS)).unwrap();
let faulted = Err(DispatchError::Faulted("the gateway is down".into()));
assert_eq!(store.dispatch_to(&payments, PaymentsAction::Charge { amount: 1 }), faulted);
assert_eq!(store.drain_events(16).err(), faulted.err());

// ...and its bus is closed, as if it had been dropped.
let request = RequestId { session: store.session(), execution: ExecutionId(1), index: 0 };
let late = Charged { request, approved: true };
assert!(matches!(sink.try_send_to(&payments, late), Err(SendError::Closed(_))));
```

<!-- snippet: wrong-root-types -->
```compile_fail
use causet::guide::shop::*;
use causet::hosting::EventBus;

// A Protocol is typed by its root State and Services: a Store of other
// types does not compile.
let shop = protocol(); // Arc<Protocol<ShopState, ShopServices>>
let (_sink, inbox) = EventBus::new(&shop, 64);
let store = Store::new(shop, 0_u32, (), inbox, StoreOptions::default());
```

<!-- snippet: driver-loop -->
```
# use std::sync::Arc;
# use causet::guide::shop::*;
# use causet::hosting::EventBus;
# fn open(gateway: Box<dyn Gateway>, worker_capacity: usize) -> Shop {
#     let mut registry = Registry::<ShopState, ShopServices>::new();
#     registry.root::<ShopRoot>();
#     let protocol = registry.freeze().unwrap();
#     let (sink, inbox) = EventBus::new(&protocol, worker_capacity);
#     let services = ShopServices { gateway };
#     let store = Store::new(Arc::clone(&protocol), ShopState::default(), services, inbox,
#         StoreOptions::default()).unwrap();
#     let cart = protocol.resolve(&root::<ShopRoot>().child(CART)).unwrap();
#     let payments = protocol.resolve(&root::<ShopRoot>().child(PAYMENTS)).unwrap();
#     Shop { store, sink, cart, payments }
# }
use std::thread;
use std::time::{Duration, Instant};

/// Answers each charge from a thread of its own.
struct OnThreads;

impl Gateway for OnThreads {
    fn charge(&mut self, request: RequestId, amount: u64, reply: Reply<Charged>) {
        // The reply moves to the worker thread, which answers when it is done.
        thread::spawn(move || {
            let answer = Charged { request, approved: amount <= 100 };
            // Waits while the worker lane is full; fails once the Store is gone or faulted.
            let _ = reply.send_timeout(answer, Duration::from_secs(5));
        });
    }
}

// A worker lane of 2, for 5 charges: some answers wait for room.
let mut shop = open(Box::new(OnThreads), 2);
for amount in [10, 20, 30, 40, 500] {
    shop.store.dispatch_to(&shop.payments, PaymentsAction::Charge { amount }).unwrap();
}

// The driver loop: drain what has arrived, and rest briefly when nothing has.
let ready = shop.store.readiness();
let deadline = Instant::now() + Duration::from_secs(10);
while !shop.store.state().payments.pending.is_empty() {
    assert!(Instant::now() < deadline, "the gateway never answered");
    if shop.store.drain_events(64).unwrap() == 0 {
        ready.wait_timeout(deadline.saturating_duration_since(Instant::now()));
    }
}

// The threads answered in any order.
let mut paid = shop.store.state().payments.paid.clone();
paid.sort();
assert_eq!(paid, [10, 20, 30, 40]);
assert_eq!(shop.store.state().payments.declined, [500]);
```

<!-- snippet: worker-lane-full -->
```
# use std::sync::Arc;
# use causet::guide::shop::*;
# use causet::hosting::EventBus;
# fn open(gateway: Box<dyn Gateway>, worker_capacity: usize) -> Shop {
#     let mut registry = Registry::<ShopState, ShopServices>::new();
#     registry.root::<ShopRoot>();
#     let protocol = registry.freeze().unwrap();
#     let (sink, inbox) = EventBus::new(&protocol, worker_capacity);
#     let services = ShopServices { gateway };
#     let store = Store::new(Arc::clone(&protocol), ShopState::default(), services, inbox,
#         StoreOptions::default()).unwrap();
#     let cart = protocol.resolve(&root::<ShopRoot>().child(CART)).unwrap();
#     let payments = protocol.resolve(&root::<ShopRoot>().child(PAYMENTS)).unwrap();
#     Shop { store, sink, cart, payments }
# }
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use causet::SendError;

/// Keeps each charge's reply, for a worker to answer later.
#[derive(Clone, Default)]
struct Parked(Arc<Mutex<Vec<(RequestId, Reply<Charged>)>>>);

impl Gateway for Parked {
    fn charge(&mut self, request: RequestId, _amount: u64, reply: Reply<Charged>) {
        self.0.lock().unwrap().push((request, reply));
    }
}

let parked = Parked::default();
let mut shop = open(Box::new(parked.clone()), 1); // room for one Event
shop.store.dispatch_to(&shop.payments, PaymentsAction::Charge { amount: 10 }).unwrap();
shop.store.dispatch_to(&shop.payments, PaymentsAction::Charge { amount: 20 }).unwrap();
let replies = std::mem::take(&mut *parked.0.lock().unwrap());

// A worker answers both, while the Store drains nothing.
thread::spawn(move || {
    let [(first, a), (second, b)]: [_; 2] = replies.try_into().ok().unwrap();
    assert!(a.try_send(Charged { request: first, approved: true }).is_ok());
    // The lane is full: the Event is handed back, to retry, drop or report.
    let Err(SendError::Full(answer)) = b.try_send(Charged { request: second, approved: true })
    else { panic!("sent") };
    // Waiting for room gives up after its timeout.
    let waited = b.send_timeout(answer, Duration::from_millis(10));
    assert!(matches!(waited, Err(SendError::Full(_))));
})
.join()
.unwrap();

assert_eq!(shop.store.drain_events(16).unwrap(), 1);
assert_eq!(shop.store.state().payments.paid, [10]);
assert_eq!(shop.store.state().payments.pending.len(), 1); // its answer was never sent
```

<!-- snippet: send-front -->
```
# use std::sync::Arc;
# use causet::guide::shop::*;
# use causet::hosting::EventBus;
# fn open(gateway: Box<dyn Gateway>, worker_capacity: usize) -> Shop {
#     let mut registry = Registry::<ShopState, ShopServices>::new();
#     registry.root::<ShopRoot>();
#     let protocol = registry.freeze().unwrap();
#     let (sink, inbox) = EventBus::new(&protocol, worker_capacity);
#     let services = ShopServices { gateway };
#     let store = Store::new(Arc::clone(&protocol), ShopState::default(), services, inbox,
#         StoreOptions::default()).unwrap();
#     let cart = protocol.resolve(&root::<ShopRoot>().child(CART)).unwrap();
#     let payments = protocol.resolve(&root::<ShopRoot>().child(PAYMENTS)).unwrap();
#     Shop { store, sink, cart, payments }
# }
/// Answers at once, at the head of its lane: on the Store's thread, as here,
/// ahead of everything waiting in the Store lane.
struct Urgent;

impl Gateway for Urgent {
    fn charge(&mut self, request: RequestId, _amount: u64, reply: Reply<Charged>) {
        reply.send_front(Charged { request, approved: true }).expect("the Store is open");
    }
}

let mut shop = open(Box::new(Urgent), 64);
shop.store.dispatch_to(&shop.payments, PaymentsAction::Charge { amount: 1 }).unwrap();
shop.store.dispatch_to(&shop.payments, PaymentsAction::Charge { amount: 2 }).unwrap();
shop.store.drain_events(16).unwrap();
// The second answer went ahead of the first.
assert_eq!(shop.store.state().payments.paid, [2, 1]);
```

<!-- snippet: shutdown -->
```
# use std::sync::Arc;
# use causet::guide::shop::*;
# use causet::hosting::EventBus;
# fn open(gateway: Box<dyn Gateway>, worker_capacity: usize) -> Shop {
#     let mut registry = Registry::<ShopState, ShopServices>::new();
#     registry.root::<ShopRoot>();
#     let protocol = registry.freeze().unwrap();
#     let (sink, inbox) = EventBus::new(&protocol, worker_capacity);
#     let services = ShopServices { gateway };
#     let store = Store::new(Arc::clone(&protocol), ShopState::default(), services, inbox,
#         StoreOptions::default()).unwrap();
#     let cart = protocol.resolve(&root::<ShopRoot>().child(CART)).unwrap();
#     let payments = protocol.resolve(&root::<ShopRoot>().child(PAYMENTS)).unwrap();
#     Shop { store, sink, cart, payments }
# }
# use std::sync::Mutex;
# #[derive(Clone, Default)]
# struct Parked(Arc<Mutex<Vec<(RequestId, Reply<Charged>)>>>);
# impl Gateway for Parked {
#     fn charge(&mut self, request: RequestId, _amount: u64, reply: Reply<Charged>) {
#         self.0.lock().unwrap().push((request, reply));
#     }
# }
use causet::SendError;

let parked = Parked::default();
let mut shop = open(Box::new(parked.clone()), 64);
shop.store.dispatch_to(&shop.payments, PaymentsAction::Charge { amount: 10 }).unwrap();
let (request, reply) = parked.0.lock().unwrap().pop().unwrap();

let Shop { store, sink, payments, .. } = shop;
drop(store); // closes the bus

// Every later send fails, on every thread, and hands the Event back.
let answer = Charged { request, approved: true };
assert!(matches!(reply.try_send(answer), Err(SendError::Closed(_))));
let answer = Charged { request, approved: true };
assert!(matches!(sink.try_send_to(&payments, answer), Err(SendError::Closed(_))));
```
