//! Start here: one checkout, then the same Payments component in a simulation.
//! Run with `cargo run -p causet --example shop`.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use causet::ManualClock;
use causet::SessionId;
use causet::guide::shop::*;
use causet_test::Node;

fn main() {
    let mut shop = Shop::open(StoreOptions::default());
    let signalled = Arc::new(AtomicBool::new(false));
    let wake = signalled.clone();
    // A host integration would schedule its driver or unpark its thread.
    let mut subscription = shop
        .store
        .readiness()
        .subscribe(move || {
            wake.store(true, Ordering::SeqCst);
        })
        .unwrap();
    shop.store
        .dispatch_to(
            &shop.cart,
            CartAction::Add {
                item: "tea".into(),
                price: 4,
            },
        )
        .unwrap();
    shop.store
        .dispatch_to(&shop.cart, CartAction::Checkout)
        .unwrap();
    let payments = &shop.store.state().payments;
    assert_eq!(payments.pending.len(), 1);
    assert!(signalled.load(Ordering::SeqCst));
    assert!(payments.paid.is_empty());
    println!(
        "before delivery: pending={}, paid={:?}",
        payments.pending.len(),
        payments.paid
    );

    assert_eq!(shop.store.drain_events(16).unwrap(), 1);
    let payments = &shop.store.state().payments;
    assert!(payments.pending.is_empty());
    assert_eq!(payments.paid, [4]);
    println!(
        "after delivery: pending={}, paid={:?}",
        payments.pending.len(),
        payments.paid
    );

    subscription.cancel();

    // The same component, with replies held until the test chooses their order.
    let clock = ManualClock::new(0);
    let gateway: Box<dyn Gateway> = Box::new(ApproveUpTo(100));
    let mut node = Node::new::<Payments>(
        "payments",
        PaymentsState::default(),
        gateway,
        SessionId(1),
        &clock,
    );
    let payments = node.mount(&root::<Payments>());
    node.dispatch(&payments, PaymentsAction::Charge { amount: 30 });
    node.dispatch(&payments, PaymentsAction::Charge { amount: 200 });
    assert!(node.deliver_where::<Charged>(|answer| !answer.approved));
    assert_eq!(node.state().declined, [200]);
    node.deliver_next();
    assert_eq!(node.state().paid, [30]);
    assert!(node.state().pending.is_empty());
    println!(
        "simulation: declined={:?}, paid={:?}",
        node.state().declined,
        node.state().paid
    );
}

#[test]
fn checkout_and_reordered_replies() {
    main();
}
