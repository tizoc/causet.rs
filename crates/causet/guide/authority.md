The causet guide's Rust examples for its authority page, run as doc tests
(`cargo test -p causet --doc`). The guide's pages are in its own
repository and show each block here by the name above it.

<!-- snippet: harness -->
```
use causet::guide::shop::*;
use causet::{Emission, Harness};

let mut harness = Harness::<Payments>::new();
let mut state = PaymentsState::default();
let emissions = harness.reduce(&mut state, PaymentsAction::Charge { amount: 30 });

// It remembered the request, then emitted the Effect that carries it.
let (request, amount) = state.pending.iter().next().unwrap();
assert_eq!(*amount, 30);
assert!(matches!(
    emissions.as_slice(),
    [Emission::Effect(PaymentsEffect::Charge { request: r, amount: 30 })] if *r == request
));
```

<!-- snippet: state-after-emit -->
```compile_fail,E0382
use causet::guide::shop::*;

fn reduce(mut ctx: ComponentContext<'_, Cart>) {
    let mut out = ctx.into_emitter();
    ctx.state_mut().total = 0; // `ctx` was consumed by `into_emitter`
    out.emit_output(CartOutput::CheckedOut { total: 0 });
}
```

<!-- snippet: is-enabled -->
```
use causet::guide::shop::*;
use causet::Harness;

let mut harness = Harness::<Cart>::new();
let empty = CartState::default();
assert!(!harness.is_enabled(&empty, &CartAction::Checkout));
```

<!-- snippet: handler-reads-state -->
```compile_fail,E0599
use causet::guide::shop::*;

fn handle(mut ctx: EffectContext<'_, Payments>) {
    ctx.state_mut(); // handlers read State; only reducers change it
}
```

<!-- snippet: port-emits-actions -->
```compile_fail,E0599
use causet::guide::shop::*;

fn adapt(event: Charged, port: &mut ActionPort<'_, Payments>) {
    port.emit_effect(PaymentsEffect::Charge { request: event.request, amount: 1 });
}
```

<!-- snippet: dispatch-from-outside -->
```
use causet::guide::shop::*;

let mut shop = Shop::open(StoreOptions::default());
let outcome = shop.store.dispatch_to(&shop.payments, PaymentsAction::Charge { amount: 7 }).unwrap();
assert_eq!(outcome, DispatchOutcome::Executed);
```
