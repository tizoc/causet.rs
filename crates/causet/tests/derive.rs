//! The message derives: type names, default and overridden kinds, nested
//! kinds, and logging.

// The messages carry fields only to be realistic; the tests read their kinds.
#![allow(dead_code)]

use causet::{Action, Effect, Event, Message, Output};

#[derive(Debug, Action)]
#[causet(type = "shop.action")]
enum ShopAction {
    Open,
    AddItem(String),
    SetPrice {
        cents: u64,
    },
    #[causet(kind = "shop.checkout_started")]
    Checkout,
    #[causet(nested)]
    Cart(CartAction),
}

#[derive(Debug, Action)]
#[causet(type = "cart.action")]
enum CartAction {
    Clear,
}

#[derive(Debug, Effect)]
#[causet(type = "shop.effect", prefix = "shop.effect")]
enum ShopEffect {
    ChargeCard,
}

#[derive(Debug, Event)]
#[causet(type = "shop.paid")]
struct Paid {
    cents: u64,
}

#[derive(Debug, Output)]
#[causet(type = "shop.output", kind = "shop.sold")]
struct Sold;

#[test]
fn enum_kinds_default_to_the_type_prefix_and_the_variant_in_snake_case() {
    assert_eq!(ShopAction::TYPE, "shop.action");
    assert_eq!(ShopAction::Open.kind(), "shop.open");
    assert_eq!(ShopAction::AddItem("tea".into()).kind(), "shop.add_item");
    assert_eq!(ShopAction::SetPrice { cents: 1 }.kind(), "shop.set_price");
}

#[test]
fn overrides_win_over_the_defaults() {
    assert_eq!(ShopAction::Checkout.kind(), "shop.checkout_started");
    assert_eq!(ShopEffect::ChargeCard.kind(), "shop.effect.charge_card");
    assert_eq!(Sold.kind(), "shop.sold");
}

#[test]
fn a_struct_kind_defaults_to_its_type() {
    assert_eq!(Paid { cents: 1 }.kind(), "shop.paid");
}

#[test]
fn a_nested_variant_reports_the_inner_message_kind() {
    assert_eq!(ShopAction::Cart(CartAction::Clear).kind(), "cart.clear");
}

#[test]
fn every_category_gets_its_marker() {
    fn action<A: causet::Action>() -> &'static str {
        A::TYPE
    }
    fn effect<E: causet::Effect>() -> &'static str {
        E::TYPE
    }
    fn event<E: causet::Event>() -> &'static str {
        E::TYPE
    }
    fn output<O: causet::Output>() -> &'static str {
        O::TYPE
    }
    assert_eq!(
        [
            action::<ShopAction>(),
            action::<CartAction>(),
            effect::<ShopEffect>(),
            event::<Paid>(),
            output::<Sold>()
        ],
        [
            "shop.action",
            "cart.action",
            "shop.effect",
            "shop.paid",
            "shop.output"
        ]
    );
}

// ---------------------------------------------------------------------------
// Logging: levels and fields chosen by `#[causet(log(...))]`.

#[derive(Debug, Action)]
#[causet(type = "peer.action", log(level = "trace"))]
enum PeerAction {
    Ping,
    #[causet(log(level = "warn", fields(peer, n = count, display(addr))))]
    Dropped {
        peer: u32,
        count: u64,
        addr: String,
        secret: Vec<u8>,
    },
    #[causet(log(fields(peer)))]
    Joined {
        peer: u32,
    },
    #[causet(nested)]
    Inner(InnerAction),
}

#[derive(Debug, Action)]
#[causet(type = "inner.action")]
enum InnerAction {
    #[causet(log(level = "error", fields(code = display(code))))]
    Failed { code: u16 },
}

#[derive(Debug, Event)]
#[causet(type = "peer.seen", log(level = "info", fields(peer)))]
struct Seen {
    peer: u32,
    at: u64,
}

/// The fields a message selects, `Debug` ones quoted as `{:?}` gives them.
#[derive(Default)]
struct Collect(Vec<String>);

impl causet::observers::FieldVisitor for Collect {
    fn debug(&mut self, name: &'static str, value: &dyn std::fmt::Debug) {
        self.0.push(format!("{name}={value:?}"));
    }
    fn display(&mut self, name: &'static str, value: &dyn std::fmt::Display) {
        self.0.push(format!("{name}:{value}"));
    }
}

fn fields(message: &impl Message) -> Vec<String> {
    let mut collect = Collect::default();
    message.log_fields(&mut collect);
    collect.0
}

#[test]
fn log_levels_come_from_the_variant_then_the_type_then_debug() {
    use causet::LogLevel;
    assert_eq!(PeerAction::Ping.log_level(), LogLevel::Trace);
    let dropped = PeerAction::Dropped {
        peer: 7,
        count: 3,
        addr: "10.0.0.1".into(),
        secret: vec![1],
    };
    assert_eq!(dropped.log_level(), LogLevel::Warn);
    assert_eq!(PeerAction::Joined { peer: 1 }.log_level(), LogLevel::Trace);
    assert_eq!(Seen { peer: 1, at: 2 }.log_level(), LogLevel::Info);
    assert_eq!(ShopAction::Open.log_level(), LogLevel::Debug);
}

#[test]
fn log_fields_are_the_selected_ones_by_debug_or_display() {
    let dropped = PeerAction::Dropped {
        peer: 7,
        count: 3,
        addr: "10.0.0.1".into(),
        secret: vec![1],
    };
    assert_eq!(fields(&dropped), ["peer=7", "n=3", "addr:10.0.0.1"]);
    assert_eq!(fields(&PeerAction::Joined { peer: 1 }), ["peer=1"]);
    assert!(fields(&PeerAction::Ping).is_empty());
    assert_eq!(fields(&Seen { peer: 4, at: 9 }), ["peer=4"]);
    assert!(fields(&ShopAction::AddItem("tea".into())).is_empty());
}

#[test]
fn a_nested_variant_logs_as_its_inner_message() {
    let failed = PeerAction::Inner(InnerAction::Failed { code: 503 });
    assert_eq!(failed.log_level(), causet::LogLevel::Error);
    assert_eq!(fields(&failed), ["code:503"]);
}
