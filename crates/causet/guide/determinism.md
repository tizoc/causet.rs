The causet guide's Rust examples for its determinism page, run as doc tests
(`cargo test -p causet --doc`). The guide's pages are in its own
repository and show each block here by the name above it.

<!-- snippet: repeat-run -->
```
use causet::guide::shop::*;
use causet::ManualClock;
use causet::observers::TraceLog;

let run = || {
    let mut shop = Shop::open(StoreOptions::with_clock(ManualClock::new(1_000)));
    shop.store.add_observer(TraceLog::new());
    shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
    shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
    shop.store.drain_events(16).unwrap();
    let trace: Vec<_> = shop.store.observer::<TraceLog>().unwrap().entries().iter()
        .map(|e| (e.id, e.causal, e.kind, e.path.clone()))
        .collect();
    (trace, shop.store.state().clone())
};
// Two runs, the same executions in the same order, the same end. (Nothing
// compared here holds a request ID, so the sessions do not show.)
assert_eq!(run(), run());
```

<!-- snippet: pending-session -->
```
use causet::guide::shop::*;
use causet::ManualClock;

let pending = || {
    let mut shop = Shop::open(StoreOptions::with_clock(ManualClock::new(1_000)));
    shop.store.dispatch_to(&shop.payments, PaymentsAction::Charge { amount: 7 }).unwrap();
    shop.store.state().payments.pending.iter().next().unwrap().0 // the answer is not drained
};
let (first, second) = (pending(), pending());
assert_ne!(first.session, second.session); // each run's own
assert_eq!((first.execution, first.index), (second.execution, second.index));
```

<!-- snippet: stamps -->
```
use causet::*;

#[derive(Debug, Action)]
#[causet(type = "stamp.action", kind = "stamp.now")]
struct Now;

/// Keeps the time of every Action it reduced.
struct Stamps;

impl Component for Stamps {
    type State = Vec<Timestamp>;
    type Action = Now;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, _: Now, meta: &ActionMeta) {
        ctx.state_mut().push(meta.time); // the execution's time, never the system clock's
    }
}

let clock = ManualClock::new(1_000);
let options = StoreOptions::with_clock(clock.clone());
let (mut store, _sink) = Store::start::<Stamps>(Vec::<Timestamp>::new(), (), options).unwrap();
let stamps = store.mount(&root::<Stamps>()).unwrap();

store.dispatch_to(&stamps, Now).unwrap();
store.dispatch_to(&stamps, Now).unwrap(); // the clock has not moved
clock.set(5_000);
store.dispatch_to(&stamps, Now).unwrap();
clock.set(10); // a clock that steps back
store.dispatch_to(&stamps, Now).unwrap();
assert_eq!(*store.state(), [1_000, 1_000, 5_000, 5_000]);
```
