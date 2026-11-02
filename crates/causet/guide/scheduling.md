The causet guide's Rust examples for its scheduling page, run as doc tests
(`cargo test -p causet --doc`). The guide's pages are in its own
repository and show each block here by the name above it.

<!-- snippet: fifo-order -->
```
use causet::*;
use causet::observers::TraceLog;

#[derive(Debug, Action)]
#[causet(type = "steps.action")]
enum Step { Start, A, AChild, C, FromEffect }

#[derive(Debug, Effect)]
#[causet(type = "steps.effect", kind = "steps.b")]
struct B;

struct Steps;

impl Component for Steps {
    type State = ();
    type Service = ();
    type Action = Step;
    type Effect = B;
    type Output = Never;

    fn reduce(ctx: ComponentContext<'_, Self>, step: Step, _: &ActionMeta) {
        let mut out = ctx.into_emitter();
        match step {
            Step::Start => {
                out.emit_action(Step::A);
                out.emit_effect(B);
                out.emit_action(Step::C);
            }
            Step::A => out.emit_action(Step::AChild),
            Step::AChild | Step::C | Step::FromEffect => {}
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, _: B, _: &EffectMeta) {
        ctx.dispatch(Step::FromEffect);
    }
}

let (mut store, _sink) = Store::start::<Steps>((), (), StoreOptions::default()).unwrap();
let steps = store.mount(&root::<Steps>()).unwrap();

store.add_observer(TraceLog::new());
store.dispatch_to(&steps, Step::Start).unwrap();

let trace = store.observer::<TraceLog>().unwrap().entries();
let kinds: Vec<_> = trace.iter().map(|e| e.kind).collect();
assert_eq!(kinds, ["steps.start", "steps.a", "steps.a_child", "steps.b", "steps.from_effect", "steps.c"]);
// Every execution has its own ID, strictly increasing...
assert!(trace.windows(2).all(|w| w[0].id < w[1].id));
// ...and names the execution that caused it.
assert_eq!(trace[2].causal, trace[1].id); // AChild was emitted by A
assert_eq!(trace[4].causal, trace[3].id); // FromEffect was dispatched by B
```

<!-- snippet: refused-at-root -->
```
use causet::guide::shop::*;

let mut shop = Shop::open(StoreOptions::default());
// An empty cart cannot check out.
let outcome = shop.store.dispatch_to(&shop.cart, CartAction::Checkout).unwrap();
assert_eq!(outcome, DispatchOutcome::Disabled);
assert_eq!(shop.store.executions(), 0);
```

<!-- snippet: root-guard -->
```
# use causet::*;
# use causet::hosting::EventBus;
#
# #[derive(Debug, Default)]
# struct SyncState { asked: Vec<u32> }
#
# #[derive(Debug, Action)]
# #[causet(type = "sync.action")]
# enum SyncAction { Request { peer: u32 } }
#
# struct Sync;
#
# impl Component for Sync {
#     type State = SyncState;
#     type Action = SyncAction;
#     type Output = Never;
#     no_effects!();
#
#     fn reduce(mut ctx: ComponentContext<'_, Self>, action: SyncAction, _: &ActionMeta) {
#         let SyncAction::Request { peer } = action;
#         ctx.state_mut().asked.push(peer);
#     }
# }
#
# #[derive(Debug, Default)]
# struct ChainState { sync: SyncState }
#
# #[derive(Debug, Action)]
# #[causet(type = "chain.action")]
# enum ChainAction { Synced }
#
# struct Chain;
#
# const SYNC: Child<Chain, Sync> = Child::new("sync");
#
# impl Component for Chain {
#     type State = ChainState;
#     type Action = ChainAction;
#     type Output = Never;
#     no_effects!();
#
#     fn reduce(_: ComponentContext<'_, Self>, _: ChainAction, _: &ActionMeta) {}
#
#     fn register(r: &mut ComponentRegistry<'_, Self>) {
#         r.child(SYNC, lens!(ChainState => sync)).bind();
#     }
# }
#
# #[derive(Debug, Default)]
# struct NodeState { peers: Vec<u32>, chain: ChainState }
#
# #[derive(Debug, Action)]
# #[causet(type = "node.action")]
# enum NodeAction { Connect(u32) }
#
# struct Node;
#
# const CHAIN: Child<Node, Chain> = Child::new("chain");
#
# impl Component for Node {
#     type State = NodeState;
#     type Action = NodeAction;
#     type Output = Never;
#     no_effects!();
#
#     fn reduce(mut ctx: ComponentContext<'_, Self>, action: NodeAction, _: &ActionMeta) {
#         let NodeAction::Connect(peer) = action;
#         ctx.state_mut().peers.push(peer);
#     }
#
#     fn register(r: &mut ComponentRegistry<'_, Self>) {
#         r.child(CHAIN, lens!(NodeState => chain)).bind();
#     }
# }
#
let mut registry = Registry::<NodeState, ()>::new();
registry.root::<Node>();
// Sync, two levels down, may ask only a peer the node is connected to.
registry.admit(
    root::<Node>().child(CHAIN).child(SYNC),
    |node, _sync, action, _now| match action {
        SyncAction::Request { peer } => node.peers.contains(peer),
    },
);
let protocol = registry.freeze().unwrap();
let state = NodeState { peers: vec![1, 2], ..NodeState::default() };
let (mut store, _sink) = Store::open(&protocol, state, (), StoreOptions::default());
let sync = store.mount(&root::<Node>().child(CHAIN).child(SYNC)).unwrap();

let asked = store.dispatch_to(&sync, SyncAction::Request { peer: 1 }).unwrap();
assert_eq!(asked, DispatchOutcome::Executed);
let refused = store.dispatch_to(&sync, SyncAction::Request { peer: 9 }).unwrap();
assert_eq!(refused, DispatchOutcome::Disabled);
assert_eq!(store.state().chain.sync.asked, [1]);
```

<!-- snippet: drain-events -->
```
use causet::guide::shop::*;

let mut shop = Shop::open(StoreOptions::default());
shop.store.dispatch_to(&shop.payments, PaymentsAction::Charge { amount: 500 }).unwrap();
// The gateway has answered; the answer waits on the bus until drained.
assert_eq!(shop.store.state().payments.pending.len(), 1);
assert_eq!(shop.store.drain_events(16).unwrap(), 1);
assert_eq!(shop.store.state().payments.declined, [500]);
```

<!-- snippet: fault -->
```
use causet::*;

#[derive(Debug, Action)]
#[causet(type = "fragile.action")]
enum Fragile { Fine, Break }

struct Glass;

impl Component for Glass {
    type State = u32;
    type Action = Fragile;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: Fragile, _: &ActionMeta) {
        *ctx.state_mut() += 1;
        if let Fragile::Break = action {
            panic!("broke");
        }
    }
}

let (mut store, _sink) = Store::start::<Glass>(0u32, (), StoreOptions::default()).unwrap();
let glass = store.mount(&root::<Glass>()).unwrap();

assert_eq!(store.dispatch_to(&glass, Fragile::Break), Err(DispatchError::Faulted("broke".into())));
assert_eq!(store.dispatch_to(&glass, Fragile::Fine), Err(DispatchError::Faulted("broke".into())));
assert_eq!(*store.state(), 1); // the reducer's change before the panic stays
assert_eq!(store.fault(), Some("broke"));
```

<!-- snippet: bug-report -->
```
use causet::*;
use causet::observers::{BugLog, Phase};

#[derive(Default)]
struct Shelf {
    titles: Vec<String>,
    /// Positions in `titles`, by title.
    index: std::collections::BTreeMap<String, usize>,
}

impl Shelf {
    /// A helper with no context: the report finds its Store by itself.
    fn take(&mut self, title: &str) -> Option<String> {
        let at = self.index.remove(title)?;
        let Some(found) = self.titles.get(at) else {
            bug_condition!(title = title, at = at; "the index names a title the shelf does not have");
            return None; // the fallback: as if it were not there
        };
        Some(found.clone())
    }
}

#[derive(Debug, Action)]
#[causet(type = "shelf.action", kind = "shelf.take")]
struct Take(String);

struct Shelves;

impl Component for Shelves {
    type State = Shelf;
    type Action = Take;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, Take(title): Take, _: &ActionMeta) {
        ctx.state_mut().take(&title);
    }
}

let mut broken = Shelf::default();
broken.index.insert("Dune".into(), 3);
let (mut store, _sink) = Store::start::<Shelves>(broken, (), StoreOptions::default()).unwrap();
store.add_observer(BugLog::new());
let shelves = store.mount(&root::<Shelves>()).unwrap();

// The Store keeps running: the fallback ran.
assert_eq!(store.dispatch_to(&shelves, Take("Dune".into())), Ok(DispatchOutcome::Executed));
let [report] = store.observer::<BugLog>().unwrap().reports() else { panic!("one report") };
let place = report.place.as_ref().unwrap();
assert_eq!((place.phase, place.path.as_ref()), (Phase::Reducer, "/"));
assert_eq!(report.fields[1], ("at".into(), "3".into()));
```
