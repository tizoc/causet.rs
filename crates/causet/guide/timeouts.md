The causet guide's Rust examples for its timeouts page, run as doc tests
(`cargo test -p causet --doc`). The guide's pages are in its own
repository and show each block here by the name above it.

<!-- snippet: host-driven-timer -->
```
use causet::{SystemClock, Timer};

let (timer, control) = Timer::driven(SystemClock::new());
// Put `timer` in Services. After each root input, arm the host's wakeup for:
let next = control.next_deadline();
// When the host wakes, publish due Events, then drain the Store as usual:
control.fire_due();
# assert_eq!(next, None);
# assert_eq!(control.pending(), 0);
```

<!-- snippet: quotes -->
```
use std::time::Duration;

use causet::*;

/// How long to wait for an answer, and how many times to ask.
const TIMEOUT: Duration = Duration::from_millis(100);
const ATTEMPTS: u32 = 2;

/// A supplier's answer.
#[derive(Debug, Event)]
#[causet(type = "quote.answer")]
struct Quoted {
    request: RequestId,
    price: u64,
}

/// Anything that can quote a price and answer later, from any thread.
trait Supplier: Send {
    fn ask(&mut self, request: RequestId, item: String, reply: Reply<Quoted>);
}

struct QuoteServices {
    supplier: Box<dyn Supplier>,
    timer: Timer,
}

/// What an answer needs: the item asked about, and which attempt this is.
#[derive(Debug)]
struct Attempt {
    item: String,
    attempt: u32,
}

#[derive(Debug, Default)]
struct QuoteState {
    waiting: Pending<Attempt>,
    prices: Vec<(String, u64)>,
    gave_up: Vec<String>,
    /// Answers and timers for requests no longer pending.
    stale: u32,
}

#[derive(Debug, Action)]
#[causet(type = "quote.action")]
enum QuoteAction {
    Ask { item: String },
    Answered { request: RequestId, price: u64 },
    TimedOut { request: RequestId },
}

#[derive(Debug, Effect)]
#[causet(type = "quote.effect")]
enum QuoteEffect {
    /// Sends the request to the supplier.
    Send { request: RequestId, item: String },
    /// Arms a timer pinned to the same request.
    Arm { request: RequestId },
}

struct Quotes;

impl Component for Quotes {
    type State = QuoteState;
    type Service = QuoteServices;
    type Action = QuoteAction;
    type Effect = QuoteEffect;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: QuoteAction, _: &ActionMeta) {
        let (item, attempt) = match action {
            QuoteAction::Ask { item } => (item, 1),
            QuoteAction::Answered { request, price } => {
                let state = ctx.state_mut();
                match state.waiting.take(request) {
                    Some(Attempt { item, .. }) => state.prices.push((item, price)),
                    // Too late: it timed out, and was asked again.
                    None => state.stale += 1,
                }
                return;
            }
            QuoteAction::TimedOut { request } => {
                let state = ctx.state_mut();
                match state.waiting.take(request) {
                    Some(Attempt { item, attempt }) if attempt < ATTEMPTS => (item, attempt + 1),
                    Some(Attempt { item, .. }) => {
                        state.gave_up.push(item);
                        return;
                    }
                    // Answered in time: the timer is stale.
                    None => {
                        state.stale += 1;
                        return;
                    }
                }
            }
        };
        // Every attempt is a new request: an answer to an earlier one is stale.
        let request = ctx.new_request();
        ctx.state_mut().waiting.insert(request, Attempt { item: item.clone(), attempt });
        let mut out = ctx.into_emitter();
        out.emit_effect(QuoteEffect::Send { request, item });
        out.emit_effect(QuoteEffect::Arm { request });
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: QuoteEffect, _: &EffectMeta) {
        match effect {
            QuoteEffect::Send { request, item } => {
                let reply = ctx.reply::<Quoted>();
                ctx.service().supplier.ask(request, item, reply);
            }
            QuoteEffect::Arm { request } => {
                let reply = ctx.reply::<TimerFired>();
                ctx.service().timer.after(TIMEOUT, reply, request);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Quoted>();
        r.event::<TimerFired>();
    }
}

impl Adapts<Quoted> for Quotes {
    fn adapt(Quoted { request, price }: Quoted, port: &mut ActionPort<'_, Self>) {
        port.emit_action(QuoteAction::Answered { request, price });
    }
}

impl Adapts<TimerFired> for Quotes {
    fn adapt(fired: TimerFired, port: &mut ActionPort<'_, Self>) {
        port.emit_action(QuoteAction::TimedOut { request: fired.request });
    }
}
```

<!-- snippet: quotes-timeout -->
```
# use std::time::Duration;
#
# use causet::*;
#
# /// How long to wait for an answer, and how many times to ask.
# const TIMEOUT: Duration = Duration::from_millis(100);
# const ATTEMPTS: u32 = 2;
#
# /// A supplier's answer.
# #[derive(Debug, Event)]
# #[causet(type = "quote.answer")]
# struct Quoted {
#     request: RequestId,
#     price: u64,
# }
#
# /// Anything that can quote a price and answer later, from any thread.
# trait Supplier: Send {
#     fn ask(&mut self, request: RequestId, item: String, reply: Reply<Quoted>);
# }
#
# struct QuoteServices {
#     supplier: Box<dyn Supplier>,
#     timer: Timer,
# }
#
# /// What an answer needs: the item asked about, and which attempt this is.
# #[derive(Debug)]
# struct Attempt {
#     item: String,
#     attempt: u32,
# }
#
# #[derive(Debug, Default)]
# struct QuoteState {
#     waiting: Pending<Attempt>,
#     prices: Vec<(String, u64)>,
#     gave_up: Vec<String>,
#     /// Answers and timers for requests no longer pending.
#     stale: u32,
# }
#
# #[derive(Debug, Action)]
# #[causet(type = "quote.action")]
# enum QuoteAction {
#     Ask { item: String },
#     Answered { request: RequestId, price: u64 },
#     TimedOut { request: RequestId },
# }
#
# #[derive(Debug, Effect)]
# #[causet(type = "quote.effect")]
# enum QuoteEffect {
#     /// Sends the request to the supplier.
#     Send { request: RequestId, item: String },
#     /// Arms a timer pinned to the same request.
#     Arm { request: RequestId },
# }
#
# struct Quotes;
#
# impl Component for Quotes {
#     type State = QuoteState;
#     type Service = QuoteServices;
#     type Action = QuoteAction;
#     type Effect = QuoteEffect;
#     type Output = Never;
#
#     fn reduce(mut ctx: ComponentContext<'_, Self>, action: QuoteAction, _: &ActionMeta) {
#         let (item, attempt) = match action {
#             QuoteAction::Ask { item } => (item, 1),
#             QuoteAction::Answered { request, price } => {
#                 let state = ctx.state_mut();
#                 match state.waiting.take(request) {
#                     Some(Attempt { item, .. }) => state.prices.push((item, price)),
#                     // Too late: it timed out, and was asked again.
#                     None => state.stale += 1,
#                 }
#                 return;
#             }
#             QuoteAction::TimedOut { request } => {
#                 let state = ctx.state_mut();
#                 match state.waiting.take(request) {
#                     Some(Attempt { item, attempt }) if attempt < ATTEMPTS => (item, attempt + 1),
#                     Some(Attempt { item, .. }) => {
#                         state.gave_up.push(item);
#                         return;
#                     }
#                     // Answered in time: the timer is stale.
#                     None => {
#                         state.stale += 1;
#                         return;
#                     }
#                 }
#             }
#         };
#         // Every attempt is a new request: an answer to an earlier one is stale.
#         let request = ctx.new_request();
#         ctx.state_mut().waiting.insert(request, Attempt { item: item.clone(), attempt });
#         let mut out = ctx.into_emitter();
#         out.emit_effect(QuoteEffect::Send { request, item });
#         out.emit_effect(QuoteEffect::Arm { request });
#     }
#
#     fn handle(mut ctx: EffectContext<'_, Self>, effect: QuoteEffect, _: &EffectMeta) {
#         match effect {
#             QuoteEffect::Send { request, item } => {
#                 let reply = ctx.reply::<Quoted>();
#                 ctx.service().supplier.ask(request, item, reply);
#             }
#             QuoteEffect::Arm { request } => {
#                 let reply = ctx.reply::<TimerFired>();
#                 ctx.service().timer.after(TIMEOUT, reply, request);
#             }
#         }
#     }
#
#     fn register(r: &mut ComponentRegistry<'_, Self>) {
#         r.event::<Quoted>();
#         r.event::<TimerFired>();
#     }
# }
#
# impl Adapts<Quoted> for Quotes {
#     fn adapt(Quoted { request, price }: Quoted, port: &mut ActionPort<'_, Self>) {
#         port.emit_action(QuoteAction::Answered { request, price });
#     }
# }
#
# impl Adapts<TimerFired> for Quotes {
#     fn adapt(fired: TimerFired, port: &mut ActionPort<'_, Self>) {
#         port.emit_action(QuoteAction::TimedOut { request: fired.request });
#     }
# }
#
use causet_test::Node;

const MS: u64 = 1_000_000;

/// Answers at once. A test node holds every Event until the test delivers
/// it, so the test decides when the answer arrives.
struct AtOnce;

impl Supplier for AtOnce {
    fn ask(&mut self, request: RequestId, _item: String, reply: Reply<Quoted>) {
        reply.try_send_as_worker(Quoted { request, price: 7 }).expect("held");
    }
}

let clock = ManualClock::new(1_000 * MS);
let (timer, control) = Timer::simulated(&clock);
let services = QuoteServices { supplier: Box::new(AtOnce), timer };
let mut node = Node::new::<Quotes>("quotes", QuoteState::default(), services, SessionId(1), &clock)
    .with_timer(control);
let quotes = node.mount(&root::<Quotes>());

node.dispatch(&quotes, QuoteAction::Ask { item: "tea".into() });
assert_eq!(node.next_deadline(), Some(1_100 * MS));

// 100 ms pass before the answer is delivered: the timer fires first.
clock.advance(100 * MS);
assert_eq!(node.fire_timers(), 1);
assert!(node.deliver_where::<TimerFired>(|_| true));
// It asked again, as a new request with a timer of its own.
assert_eq!(node.state().waiting.values().map(|a| a.attempt).collect::<Vec<_>>(), [2]);
assert_eq!(node.next_deadline(), Some(1_200 * MS));

// The first answer arrives late, and is stale. The second is taken.
node.deliver_next();
assert_eq!(node.state().stale, 1);
node.deliver_next();
assert_eq!(node.state().prices, [("tea".to_string(), 7)]);

// The second timer fires after its answer, and finds nothing.
clock.advance(100 * MS);
assert_eq!(node.fire_timers(), 1);
node.deliver_next();
assert_eq!(node.state().stale, 2);
assert!(node.state().waiting.is_empty());
```

<!-- snippet: pending-stale -->
```
use causet::{ExecutionId, Pending, RequestId, SessionId};

// Request IDs as a reducer's `new_request` makes them.
let request = |index| RequestId { session: SessionId(1), execution: ExecutionId(1), index };
let mut pending = Pending::new();

// The first of the answer and the timer takes the request; the second is stale.
pending.insert(request(0), "tea");
assert_eq!(pending.take(request(0)), Some("tea"));
assert_eq!(pending.take(request(0)), None);

// Cancelled: its answer, when it comes, is stale.
pending.insert(request(1), "coffee");
pending.cancel(request(1));
assert_eq!(pending.take(request(1)), None);

// Order 7 ended: so did every request made for it.
pending.insert(request(2), "order 7");
pending.insert(request(3), "order 8");
pending.retain(|_, work| *work != "order 7");
assert_eq!(pending.take(request(2)), None);

// A new search replaces the one in flight.
pending.insert(request(4), "first search");
pending.clear();
pending.insert(request(5), "second search");
assert_eq!(pending.take(request(4)), None);
assert_eq!(pending.len(), 1);
```

<!-- snippet: lookups -->
```
use std::time::Duration;

use causet::*;

const MS: Timestamp = 1_000_000;

/// The directory's answer.
#[derive(Debug, Event)]
#[causet(type = "lookup.answer")]
struct Listing {
    request: RequestId,
    number: u64,
}

/// Anything that can look a name up and answer later, from any thread.
trait Directory: Send {
    fn find(&mut self, request: RequestId, name: String, reply: Reply<Listing>);
}

struct LookupServices {
    directory: Box<dyn Directory>,
    timer: Timer,
}

#[derive(Debug, Default)]
struct LookupState {
    /// Each lookup's name, until its deadline.
    lookups: Pending<String>,
    /// The one timer armed: its own request, and the deadline it is for.
    armed: Option<(RequestId, Timestamp)>,
    found: Vec<(String, u64)>,
    expired: Vec<String>,
}

#[derive(Debug, Action)]
#[causet(type = "lookup.action")]
enum LookupAction {
    Find { name: String, wait_ms: u64 },
    Answered { request: RequestId, number: u64 },
    Sweep { timer: RequestId },
}

#[derive(Debug, Effect)]
#[causet(type = "lookup.effect")]
enum LookupEffect {
    Ask { request: RequestId, name: String },
    /// Arms the one timer, `delay` nanoseconds from now.
    Arm { timer: RequestId, delay: Timestamp },
}

struct Lookups;

impl Component for Lookups {
    type State = LookupState;
    type Service = LookupServices;
    type Action = LookupAction;
    type Effect = LookupEffect;
    type Output = Never;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: LookupAction, meta: &ActionMeta) {
        let now = meta.time;
        let mut ask = None;
        match action {
            LookupAction::Find { name, wait_ms } => {
                let request = ctx.new_request();
                ctx.state_mut().lookups.insert_until(request, name.clone(), now + wait_ms * MS);
                ask = Some(LookupEffect::Ask { request, name });
            }
            LookupAction::Answered { request, number } => {
                let state = ctx.state_mut();
                // Stale once expired: the deadline came first.
                if let Some(name) = state.lookups.take(request) {
                    state.found.push((name, number));
                }
            }
            LookupAction::Sweep { timer } => {
                let state = ctx.state_mut();
                // A timer replaced by an earlier one is stale.
                if state.armed.is_some_and(|(armed, _)| armed == timer) {
                    state.armed = None;
                    let due = state.lookups.expire(now);
                    state.expired.extend(due.into_iter().map(|(_, name)| name));
                }
            }
        }
        // One timer, for the earliest deadline: armed when none is, or when
        // a deadline comes before the one it is armed for.
        let earliest = ctx.state().lookups.next_deadline();
        let arm = match (earliest, ctx.state().armed) {
            (Some(at), None) => Some(at),
            (Some(at), Some((_, armed_at))) if at < armed_at => Some(at),
            _ => None,
        };
        let arm = arm.map(|at| {
            let timer = ctx.new_request();
            ctx.state_mut().armed = Some((timer, at));
            LookupEffect::Arm { timer, delay: at.saturating_sub(now) }
        });
        let mut out = ctx.into_emitter();
        for effect in ask.into_iter().chain(arm) {
            out.emit_effect(effect);
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: LookupEffect, _: &EffectMeta) {
        match effect {
            LookupEffect::Ask { request, name } => {
                let reply = ctx.reply::<Listing>();
                ctx.service().directory.find(request, name, reply);
            }
            LookupEffect::Arm { timer, delay } => {
                let reply = ctx.reply::<TimerFired>();
                ctx.service().timer.after(Duration::from_nanos(delay), reply, timer);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Listing>();
        r.event::<TimerFired>();
    }
}

impl Adapts<Listing> for Lookups {
    fn adapt(Listing { request, number }: Listing, port: &mut ActionPort<'_, Self>) {
        port.emit_action(LookupAction::Answered { request, number });
    }
}

impl Adapts<TimerFired> for Lookups {
    fn adapt(fired: TimerFired, port: &mut ActionPort<'_, Self>) {
        port.emit_action(LookupAction::Sweep { timer: fired.request });
    }
}
```

<!-- snippet: lookups-run -->
```
# use std::time::Duration;
#
# use causet::*;
#
# const MS: Timestamp = 1_000_000;
#
# /// The directory's answer.
# #[derive(Debug, Event)]
# #[causet(type = "lookup.answer")]
# struct Listing {
#     request: RequestId,
#     number: u64,
# }
#
# /// Anything that can look a name up and answer later, from any thread.
# trait Directory: Send {
#     fn find(&mut self, request: RequestId, name: String, reply: Reply<Listing>);
# }
#
# struct LookupServices {
#     directory: Box<dyn Directory>,
#     timer: Timer,
# }
#
# #[derive(Debug, Default)]
# struct LookupState {
#     /// Each lookup's name, until its deadline.
#     lookups: Pending<String>,
#     /// The one timer armed: its own request, and the deadline it is for.
#     armed: Option<(RequestId, Timestamp)>,
#     found: Vec<(String, u64)>,
#     expired: Vec<String>,
# }
#
# #[derive(Debug, Action)]
# #[causet(type = "lookup.action")]
# enum LookupAction {
#     Find { name: String, wait_ms: u64 },
#     Answered { request: RequestId, number: u64 },
#     Sweep { timer: RequestId },
# }
#
# #[derive(Debug, Effect)]
# #[causet(type = "lookup.effect")]
# enum LookupEffect {
#     Ask { request: RequestId, name: String },
#     /// Arms the one timer, `delay` nanoseconds from now.
#     Arm { timer: RequestId, delay: Timestamp },
# }
#
# struct Lookups;
#
# impl Component for Lookups {
#     type State = LookupState;
#     type Service = LookupServices;
#     type Action = LookupAction;
#     type Effect = LookupEffect;
#     type Output = Never;
#
#     fn reduce(mut ctx: ComponentContext<'_, Self>, action: LookupAction, meta: &ActionMeta) {
#         let now = meta.time;
#         let mut ask = None;
#         match action {
#             LookupAction::Find { name, wait_ms } => {
#                 let request = ctx.new_request();
#                 ctx.state_mut().lookups.insert_until(request, name.clone(), now + wait_ms * MS);
#                 ask = Some(LookupEffect::Ask { request, name });
#             }
#             LookupAction::Answered { request, number } => {
#                 let state = ctx.state_mut();
#                 // Stale once expired: the deadline came first.
#                 if let Some(name) = state.lookups.take(request) {
#                     state.found.push((name, number));
#                 }
#             }
#             LookupAction::Sweep { timer } => {
#                 let state = ctx.state_mut();
#                 // A timer replaced by an earlier one is stale.
#                 if state.armed.is_some_and(|(armed, _)| armed == timer) {
#                     state.armed = None;
#                     let due = state.lookups.expire(now);
#                     state.expired.extend(due.into_iter().map(|(_, name)| name));
#                 }
#             }
#         }
#         // One timer, for the earliest deadline: armed when none is, or when
#         // a deadline comes before the one it is armed for.
#         let earliest = ctx.state().lookups.next_deadline();
#         let arm = match (earliest, ctx.state().armed) {
#             (Some(at), None) => Some(at),
#             (Some(at), Some((_, armed_at))) if at < armed_at => Some(at),
#             _ => None,
#         };
#         let arm = arm.map(|at| {
#             let timer = ctx.new_request();
#             ctx.state_mut().armed = Some((timer, at));
#             LookupEffect::Arm { timer, delay: at.saturating_sub(now) }
#         });
#         let mut out = ctx.into_emitter();
#         for effect in ask.into_iter().chain(arm) {
#             out.emit_effect(effect);
#         }
#     }
#
#     fn handle(mut ctx: EffectContext<'_, Self>, effect: LookupEffect, _: &EffectMeta) {
#         match effect {
#             LookupEffect::Ask { request, name } => {
#                 let reply = ctx.reply::<Listing>();
#                 ctx.service().directory.find(request, name, reply);
#             }
#             LookupEffect::Arm { timer, delay } => {
#                 let reply = ctx.reply::<TimerFired>();
#                 ctx.service().timer.after(Duration::from_nanos(delay), reply, timer);
#             }
#         }
#     }
#
#     fn register(r: &mut ComponentRegistry<'_, Self>) {
#         r.event::<Listing>();
#         r.event::<TimerFired>();
#     }
# }
#
# impl Adapts<Listing> for Lookups {
#     fn adapt(Listing { request, number }: Listing, port: &mut ActionPort<'_, Self>) {
#         port.emit_action(LookupAction::Answered { request, number });
#     }
# }
#
# impl Adapts<TimerFired> for Lookups {
#     fn adapt(fired: TimerFired, port: &mut ActionPort<'_, Self>) {
#         port.emit_action(LookupAction::Sweep { timer: fired.request });
#     }
# }
#
use causet_test::Node;

/// Answers at once; the test node holds the answers.
struct AtOnce;

impl Directory for AtOnce {
    fn find(&mut self, request: RequestId, name: String, reply: Reply<Listing>) {
        let number = name.len() as u64;
        reply.try_send_as_worker(Listing { request, number }).expect("held");
    }
}

let clock = ManualClock::new(1_000 * MS);
let (timer, control) = Timer::simulated(&clock);
let services = LookupServices { directory: Box::new(AtOnce), timer };
let mut node = Node::new::<Lookups>("lookups", LookupState::default(), services, SessionId(1), &clock)
    .with_timer(control);
let lookups = node.mount(&root::<Lookups>());

node.dispatch(&lookups, LookupAction::Find { name: "ada".into(), wait_ms: 300 });
assert_eq!(node.next_deadline(), Some(1_300 * MS));
// An earlier deadline: a new timer, and the first one is stale.
node.dispatch(&lookups, LookupAction::Find { name: "bob".into(), wait_ms: 100 });
node.dispatch(&lookups, LookupAction::Find { name: "cy".into(), wait_ms: 200 });
assert_eq!(node.next_deadline(), Some(1_100 * MS));

// Only cy's answer arrives in time.
let answers = node.held();
node.deliver(answers[2].id);
assert_eq!(node.state().found, [("cy".to_string(), 2)]);

// At 1,100 ms bob expires, and the timer is armed again, for ada.
clock.advance(100 * MS);
assert_eq!(node.fire_timers(), 1);
assert!(node.deliver_where::<TimerFired>(|_| true));
assert_eq!(node.state().expired, ["bob"]);
assert_eq!(node.next_deadline(), Some(1_300 * MS));

// At 1,300 ms two timers fire: the stale first one finds nothing.
clock.advance(200 * MS);
assert_eq!(node.fire_timers(), 2);
while node.deliver_where::<TimerFired>(|_| true) {}
assert_eq!(node.state().expired, ["bob", "ada"]);

// The answers for ada and bob arrive late, and are stale.
node.deliver(answers[0].id);
node.deliver(answers[1].id);
assert_eq!(node.state().found, [("cy".to_string(), 2)]);
assert!(node.state().lookups.is_empty());
```
