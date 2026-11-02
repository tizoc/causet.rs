//! The example application the guide's examples run: a shop whose cart
//! checks out through a payment component. Read its source; every piece of
//! causet appears once.
//!
//! ```text
//! /            ShopRoot  root: State is the whole ShopState, Service the whole ShopServices
//! /cart        Cart      no Effects; reports CheckedOut to its parent as an Output
//! /payments    Payments  charges through the Gateway Service, answered by a Charged Event
//! ```
//!
//! [`Shop`] is not a component: it is a running shop, its Store and mounts.
//!
//! A checkout flows through every category:
//!
//! ```text
//! CartAction::Checkout -> Cart reducer -> Output CheckedOut
//!   -> ShopRoot's lowerer -> ShopAction::Pay -> ShopRoot reducer -> to_child(PAYMENTS, Charge)
//!   -> Payments reducer -> Effect Charge -> Gateway -> Event Charged
//!   -> drain_events -> Payments adapter -> PaymentsAction::Settled -> Payments reducer
//! ```
//!
//! ```
//! use causet::guide::shop::*;
//!
//! let mut shop = Shop::open(StoreOptions::default());
//! shop.store.dispatch_to(&shop.cart, CartAction::Add { item: "tea".into(), price: 4 }).unwrap();
//! shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
//! shop.store.drain_events(16).unwrap();
//! assert_eq!(shop.store.state().payments.paid, [4]);
//! ```

use std::sync::Arc;

pub use crate::{
    Action, ActionMeta, ActionPort, Adapts, Child, Component, ComponentContext, ComponentRegistry,
    DispatchOutcome, Effect, EffectContext, EffectMeta, Event, EventSink, Mount, Never, Output,
    Pending, Protocol, Registry, Reply, RequestId, Store, StoreOptions, Timestamp, lens,
    no_effects, root,
};

// ---------------------------------------------------------------------------
// Cart: a leaf with no Effects.

/// Collects items and checks out.
pub struct Cart;

/// What the cart holds: its State.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CartState {
    /// Item names, in the order they were added.
    pub items: Vec<String>,
    /// The sum of their prices.
    pub total: u64,
}

/// What the cart can be asked to do: its Actions.
#[derive(Debug, Action)]
#[causet(type = "cart.action")]
pub enum CartAction {
    /// Kind `cart.add`.
    Add {
        /// The item's name.
        item: String,
        /// Its price, added to the total.
        price: u64,
    },
    /// Kind `cart.checkout`. Refused while the cart is empty.
    Checkout,
}

/// What the cart tells its parent.
#[derive(Debug, Output)]
#[causet(type = "cart.output")]
pub enum CartOutput {
    /// Kind `cart.checked_out`. The cart was emptied at checkout.
    CheckedOut {
        /// What the emptied cart cost, to be charged.
        total: u64,
    },
}

impl Component for Cart {
    type State = CartState;
    type Action = CartAction;
    type Output = CartOutput;
    no_effects!();

    fn is_enabled(state: &CartState, action: &CartAction, _now: Timestamp) -> bool {
        !matches!(action, CartAction::Checkout) || !state.items.is_empty()
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: CartAction, _: &ActionMeta) {
        match action {
            CartAction::Add { item, price } => {
                let cart = ctx.state_mut();
                cart.items.push(item);
                cart.total += price;
            }
            CartAction::Checkout => {
                let total = std::mem::take(ctx.state_mut()).total;
                ctx.into_emitter()
                    .emit_output(CartOutput::CheckedOut { total });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Payments: Effects, a Service and an Event.

/// Charges through the `Gateway` and records what was paid.
pub struct Payments;

/// Every charge, by where it stands: the payments' State.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PaymentsState {
    /// Charges sent and not answered yet, by request: the first answer to
    /// each takes it, and any after it is stale.
    pub pending: Pending<u64>,
    /// Amounts the gateway approved, in the order their answers arrived.
    pub paid: Vec<u64>,
    /// Amounts the gateway declined, in the same order.
    pub declined: Vec<u64>,
}

/// Starting a charge and settling one: the payments' Actions.
#[derive(Debug, Action)]
#[causet(type = "payments.action")]
pub enum PaymentsAction {
    /// Kind `payments.charge`. Records the charge as pending and emits the
    /// Effect that sends it.
    Charge {
        /// The amount to charge.
        amount: u64,
    },
    /// Kind `payments.settled`. The gateway's answer, made from a `Charged`
    /// Event by the adapter.
    Settled {
        /// The charge this answers.
        request: RequestId,
        /// Whether the gateway approved it.
        approved: bool,
    },
}

/// The command the reducer gives the `Gateway`: the payments' Effect.
#[derive(Debug, Effect)]
#[causet(type = "payments.effect")]
pub enum PaymentsEffect {
    /// Kind `payments.charge`. Run by the Effect handler, which hands the
    /// gateway a `Reply` for the answer.
    Charge {
        /// Identifies the charge, so its answer can be matched.
        request: RequestId,
        /// The amount to charge.
        amount: u64,
    },
}

/// The gateway's answer, published through the Event bus.
#[derive(Debug, Event)]
#[causet(type = "payments.charged")]
pub struct Charged {
    /// The charge answered, as the Effect named it.
    pub request: RequestId,
    /// Whether the charge went through.
    pub approved: bool,
}

/// The Payments capability: anything that can charge and answer later.
pub trait Gateway: Send {
    /// Charges `amount` and answers with a `Charged` for `request` through
    /// `reply`, now or later, from any thread.
    fn charge(&mut self, request: RequestId, amount: u64, reply: Reply<Charged>);
}

/// Approves charges up to a limit, answering at once on the Store's thread.
/// A real gateway would answer from a worker thread through the same `Reply`.
pub struct ApproveUpTo(pub u64);

impl Gateway for ApproveUpTo {
    fn charge(&mut self, request: RequestId, amount: u64, reply: Reply<Charged>) {
        let approved = amount <= self.0;
        reply
            .try_send(Charged { request, approved })
            .expect("the Store lane is never full");
    }
}

impl Component for Payments {
    type State = PaymentsState;
    type Service = Box<dyn Gateway>;
    type Action = PaymentsAction;
    type Effect = PaymentsEffect;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: PaymentsAction, _: &ActionMeta) {
        match action {
            PaymentsAction::Charge { amount } => {
                // Made during mutation, so it can be both remembered and sent.
                let request = ctx.new_request();
                ctx.state_mut().pending.insert(request, amount);
                ctx.into_emitter()
                    .emit_effect(PaymentsEffect::Charge { request, amount });
            }
            PaymentsAction::Settled { request, approved } => {
                let state = ctx.state_mut();
                // An answer to no pending request is stale: ignored.
                if let Some(amount) = state.pending.take(request) {
                    if approved {
                        state.paid.push(amount)
                    } else {
                        state.declined.push(amount)
                    }
                }
            }
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: PaymentsEffect, _: &EffectMeta) {
        match effect {
            PaymentsEffect::Charge { request, amount } => {
                let reply = ctx.reply::<Charged>();
                ctx.service().charge(request, amount, reply);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Charged>();
    }
}

impl Adapts<Charged> for Payments {
    fn adapt(event: Charged, port: &mut ActionPort<'_, Self>) {
        port.emit_action(PaymentsAction::Settled {
            request: event.request,
            approved: event.approved,
        });
    }
}

// ---------------------------------------------------------------------------
// Shop: the root.

/// The root component: its State is the whole application State.
pub struct ShopRoot;

/// The root's slot for the cart, mounted at `/cart`.
pub const CART: Child<ShopRoot, Cart> = Child::new("cart");
/// The root's slot for payments, mounted at `/payments`.
pub const PAYMENTS: Child<ShopRoot, Payments> = Child::new("payments");

/// The whole application State: each child's State in the field its lens
/// names, beside the root's own.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShopState {
    /// The cart's State, reached through the `CART` slot's lens.
    pub cart: CartState,
    /// The payments' State, reached through the `PAYMENTS` slot's lens.
    pub payments: PaymentsState,
    /// Checkouts the root has sent on to payments.
    pub orders: u64,
}

/// The whole application Service: each child's Service in the field its
/// lens names.
pub struct ShopServices {
    /// The payments' Service, lent to the `PAYMENTS` mount.
    pub gateway: Box<dyn Gateway>,
}

/// The root's Actions.
#[derive(Debug, Action)]
#[causet(type = "shop.action")]
pub enum ShopAction {
    /// Kind `shop.pay`. Lowered from the cart's `CheckedOut`; counts the order
    /// and sends payments a charge.
    Pay {
        /// The amount to charge.
        total: u64,
    },
}

impl Component for ShopRoot {
    type State = ShopState;
    type Action = ShopAction;
    type Output = Never;
    no_effects!(ShopServices);

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: ShopAction, _: &ActionMeta) {
        match action {
            ShopAction::Pay { total } => {
                ctx.state_mut().orders += 1;
                ctx.into_emitter()
                    .to_child(PAYMENTS, PaymentsAction::Charge { amount: total });
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(CART, lens!(ShopState => cart))
            .on_output(lower_cart)
            .bind();
        r.child(PAYMENTS, lens!(ShopState => payments))
            .service(lens!(ShopServices => gateway))
            .bind();
    }
}

/// The parent decides what a child's Output means.
fn lower_cart(
    _slot: Child<ShopRoot, Cart>,
    output: CartOutput,
    port: &mut ActionPort<'_, ShopRoot>,
) {
    match output {
        CartOutput::CheckedOut { total } => port.emit_action(ShopAction::Pay { total }),
    }
}

// ---------------------------------------------------------------------------
// Wiring

/// The frozen Protocol: built once, shared by every Store of the shop.
pub fn protocol() -> Arc<Protocol<ShopState, ShopServices>> {
    let mut registry = Registry::<ShopState, ShopServices>::new();
    registry.root::<ShopRoot>();
    registry.freeze().expect("a valid protocol")
}

/// A running shop: its Store, the bus's sending side and its two mounts.
pub struct Shop {
    /// Holds the shop's State and Services and runs its Actions.
    pub store: Store<ShopState, ShopServices>,
    /// Sends Events to a mount from outside the Store, from any thread.
    pub sink: EventSink,
    /// Where `CartAction`s are dispatched.
    pub cart: Mount<Cart>,
    /// Where `PaymentsAction`s are dispatched and `Charged` Events sent.
    pub payments: Mount<Payments>,
}

impl Shop {
    /// A shop whose gateway approves charges up to 100.
    pub fn open(options: StoreOptions) -> Self {
        let services = ShopServices {
            gateway: Box::new(ApproveUpTo(100)),
        };
        let (store, sink) = Store::start::<ShopRoot>(ShopState::default(), services, options)
            .expect("a valid protocol");
        let cart = store
            .mount(&root::<ShopRoot>().child(CART))
            .expect("mounted");
        let payments = store
            .mount(&root::<ShopRoot>().child(PAYMENTS))
            .expect("mounted");
        Self {
            store,
            sink,
            cart,
            payments,
        }
    }
}
