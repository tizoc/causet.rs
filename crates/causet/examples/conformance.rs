//! Traces of the guide's shop, printed one fact per line, for a port in
//! another language to compare against: the same scenario must make the
//! same executions, in the same order, with the same identities.
//!
//! `cargo run -p causet --example conformance > shop.txt`

use causet::ManualClock;
use causet::Origin;
use causet::guide::shop::*;
use causet::observers::{RefusedBy, SentFrom, TraceLog};

fn origin(o: &Option<Origin>) -> String {
    match o {
        None => "-".into(),
        Some(o) => format!(
            "{}/{:016x}/{}/{}",
            o.causal.0, o.component.0, o.emission_ordinal, o.lowering_ordinal
        ),
    }
}

fn sent(s: &SentFrom) -> String {
    match s {
        SentFrom::Root => "root".into(),
        SentFrom::Effect(at) => format!("effect {}", at.0),
        SentFrom::Queued(o) => format!("queued {}", origin(&Some(*o))),
        SentFrom::Inline(at) => format!("inline {}", at.0),
    }
}

fn by(b: RefusedBy) -> &'static str {
    match b {
        RefusedBy::RootGuard => "root-guard",
        RefusedBy::Guard => "guard",
        RefusedBy::Component => "component",
        RefusedBy::Absent => "absent",
    }
}

fn print_trace(name: &str, shop: &Shop) {
    let log = shop.store.observer::<TraceLog>().expect("a trace log");
    println!("scenario {name}");
    for e in log.entries() {
        println!(
            "exec {} time {} {:?} {} {} causal {} origin {} answers {} depth {}",
            e.id.0,
            e.time,
            e.category,
            e.path,
            e.kind,
            e.causal.0,
            origin(&e.origin),
            e.answers.map_or("-".into(), |a| a.0.to_string()),
            e.depth
        );
    }
    for r in log.refusals() {
        println!(
            "refused {} {} {} by {} at {}",
            r.path,
            r.kind,
            sent(&r.sent),
            by(r.by),
            r.time
        );
    }
    for i in log.inline() {
        println!("inline {} {} within {}", i.path, i.kind, i.within.0);
    }
    let s = shop.store.state();
    println!(
        "state paid {:?} declined {:?} pending {} orders {} cart {:?} {}",
        s.payments.paid,
        s.payments.declined,
        s.payments.pending.len(),
        s.orders,
        s.cart.items,
        s.cart.total
    );
}

fn open() -> Shop {
    let mut shop = Shop::open(StoreOptions::with_clock(ManualClock::new(1_000)));
    shop.store.add_observer(TraceLog::new());
    shop
}

fn add(shop: &mut Shop, item: &str, price: u64) {
    let action = CartAction::Add {
        item: item.into(),
        price,
    };
    shop.store.dispatch_to(&shop.cart, action).unwrap();
}

fn main() {
    // One order, paid.
    let mut shop = open();
    add(&mut shop, "tea", 4);
    shop.store
        .dispatch_to(&shop.cart, CartAction::Checkout)
        .unwrap();
    shop.store.drain_events(16).unwrap();
    print_trace("one-order", &shop);

    // Two orders before any answer, one declined; an empty checkout refused.
    let mut shop = open();
    add(&mut shop, "tea", 4);
    add(&mut shop, "pot", 60);
    shop.store
        .dispatch_to(&shop.cart, CartAction::Checkout)
        .unwrap();
    shop.store
        .dispatch_to(&shop.cart, CartAction::Checkout)
        .unwrap();
    add(&mut shop, "kettle", 150);
    shop.store
        .dispatch_to(&shop.cart, CartAction::Checkout)
        .unwrap();
    shop.store.drain_events(1).unwrap();
    add(&mut shop, "cup", 3);
    shop.store.drain_events(16).unwrap();
    print_trace("declined-and-refused", &shop);
}
