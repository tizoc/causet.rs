//! A composite component mounted twice.
//!
//! A `Warehouse` holds a `Shelf` and an optional overflow `Shelf`; the
//! application mounts it as `/north` and `/south`, so the shelves are
//! `/north/shelf`, `/south/shelf` and `/south/overflow`. It shows:
//!
//! - inline child calls (`reduce_child`), in the parent's own execution, and
//!   queued ones (`to_child`), as executions of their own;
//! - a parent returning early, before calling its child at all;
//! - a child refusing an inline call, and the parent reading the outcome;
//! - an unavailable child: the overflow shelf before it exists;
//! - a parent admission guard: `/south` is closed until it opens;
//! - Outputs lowered by one function for both shelves, and by one function
//!   for both warehouses, which tells them apart by slot.
//!
//! The run is deterministic: run twice, it makes the same executions and
//! ends with the same State (it makes no requests, so no session identity
//! shows in it).
//!
//! `cargo run -p causet --example composite`

// An example program: its public items are not an API.
#![allow(missing_docs)]

use std::collections::BTreeMap;

use causet::observers::TraceLog;
use causet::{
    Action, ActionMeta, ActionPort, Child, ChildOutcome, Component, ComponentContext,
    ComponentRegistry, DispatchOutcome, ManualClock, Never, Output, Store, StoreOptions, Timestamp,
    lens, no_effects, root,
};

// ---------------------------------------------------------------------------
// Shelf: a leaf with no Effects.

pub struct Shelf;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShelfState {
    pub stock: BTreeMap<String, u32>,
}

#[derive(Debug, Action)]
#[causet(type = "shelf.action")]
pub enum ShelfAction {
    Add { item: String, qty: u32 },
    Take { item: String, qty: u32 },
}

#[derive(Debug, Output)]
#[causet(type = "shelf.output")]
pub enum ShelfOutput {
    Low { item: String, left: u32 },
}

impl Component for Shelf {
    type State = ShelfState;
    type Action = ShelfAction;
    type Output = ShelfOutput;
    no_effects!();

    /// Taking more than is on the shelf is refused before the reducer runs.
    fn is_enabled(state: &ShelfState, action: &ShelfAction, _: Timestamp) -> bool {
        match action {
            ShelfAction::Add { .. } => true,
            ShelfAction::Take { item, qty } => state.stock.get(item).is_some_and(|n| n >= qty),
        }
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: ShelfAction, _: &ActionMeta) {
        match action {
            ShelfAction::Add { item, qty } => {
                *ctx.state_mut().stock.entry(item).or_default() += qty
            }
            ShelfAction::Take { item, qty } => {
                let left = {
                    let n = ctx
                        .state_mut()
                        .stock
                        .get_mut(&item)
                        .expect("admitted, so stocked");
                    *n -= qty;
                    *n
                };
                if left < 2 {
                    ctx.into_emitter()
                        .emit_output(ShelfOutput::Low { item, left });
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Warehouse: the composite.

pub struct Warehouse;

const SHELF: Child<Warehouse, Shelf> = Child::new("shelf");
const OVERFLOW: Child<Warehouse, Shelf> = Child::new("overflow");

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WarehouseState {
    pub shelf: ShelfState,
    /// `None` until opened: the `overflow` child is unavailable until then.
    pub overflow: Option<ShelfState>,
    pub received: u32,
    pub backorders: Vec<String>,
    pub spills_refused: u32,
}

#[derive(Debug, Action)]
#[causet(type = "warehouse.action")]
pub enum WarehouseAction {
    Receive { item: String, qty: u32 },
    Restock { item: String, qty: u32 },
    Ship { item: String, qty: u32 },
    Spill { item: String, qty: u32 },
    OpenOverflow,
    Low { item: String },
}

#[derive(Debug, Output)]
#[causet(type = "warehouse.output")]
pub enum WarehouseOutput {
    Reorder { item: String },
}

/// One lowerer for both shelves.
fn lower_shelf(_: Child<Warehouse, Shelf>, out: ShelfOutput, port: &mut ActionPort<'_, Warehouse>) {
    let ShelfOutput::Low { item, .. } = out;
    port.emit_action(WarehouseAction::Low { item });
}

impl Component for Warehouse {
    type State = WarehouseState;
    type Action = WarehouseAction;
    type Output = WarehouseOutput;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: WarehouseAction, _: &ActionMeta) {
        match action {
            WarehouseAction::Receive { item, qty } => {
                if qty == 0 {
                    return; // before the child: nothing reaches the shelf
                }
                ctx.reduce_child(SHELF, ShelfAction::Add { item, qty });
                ctx.state_mut().received += qty; // after the child, same execution
            }
            WarehouseAction::Restock { item, qty } => {
                ctx.into_emitter()
                    .to_child(SHELF, ShelfAction::Add { item, qty });
            }
            WarehouseAction::Ship { item, qty } => {
                if ctx.reduce_child(
                    SHELF,
                    ShelfAction::Take {
                        item: item.clone(),
                        qty,
                    },
                ) == ChildOutcome::Refused
                {
                    ctx.state_mut().backorders.push(item);
                }
            }
            WarehouseAction::Spill { item, qty } => {
                if ctx.reduce_child(OVERFLOW, ShelfAction::Add { item, qty })
                    == ChildOutcome::Refused
                {
                    ctx.state_mut().spills_refused += 1;
                }
            }
            WarehouseAction::OpenOverflow => ctx.state_mut().overflow = Some(ShelfState::default()),
            WarehouseAction::Low { item } => ctx
                .into_emitter()
                .emit_output(WarehouseOutput::Reorder { item }),
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(SHELF, lens!(WarehouseState => shelf))
            .on_output(lower_shelf)
            .bind();
        r.child(OVERFLOW, lens!(WarehouseState => overflow?))
            .on_output(lower_shelf)
            .bind();
    }
}

// ---------------------------------------------------------------------------
// The application: two warehouses.

pub struct App;

const NORTH: Child<App, Warehouse> = Child::new("north");
const SOUTH: Child<App, Warehouse> = Child::new("south");

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AppState {
    pub north: WarehouseState,
    pub south: WarehouseState,
    pub south_open: bool,
    pub reorders: Vec<(String, String)>,
}

#[derive(Debug, Action)]
#[causet(type = "app.action")]
pub enum AppAction {
    OpenSouth,
    Reorder { site: String, item: String },
}

/// One lowerer for both warehouses: the slot says which one asked.
fn lower_warehouse(
    slot: Child<App, Warehouse>,
    out: WarehouseOutput,
    port: &mut ActionPort<'_, App>,
) {
    let WarehouseOutput::Reorder { item } = out;
    port.emit_action(AppAction::Reorder {
        site: slot.name().into(),
        item,
    });
}

/// `/south` takes nothing, on any route, until it opens.
fn south_is_open(app: &AppState, _: &WarehouseAction, _: Timestamp) -> bool {
    app.south_open
}

impl Component for App {
    type State = AppState;
    type Action = AppAction;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: AppAction, _: &ActionMeta) {
        match action {
            AppAction::OpenSouth => ctx.state_mut().south_open = true,
            AppAction::Reorder { site, item } => ctx.state_mut().reorders.push((site, item)),
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(NORTH, lens!(AppState => north))
            .on_output(lower_warehouse)
            .bind();
        r.child(SOUTH, lens!(AppState => south))
            .admit(south_is_open)
            .on_output(lower_warehouse)
            .bind();
    }
}

// ---------------------------------------------------------------------------
// The run.

pub struct Run {
    pub live: AppState,
    /// Each execution's ID, path and kind.
    pub trace: Vec<(u64, String, &'static str)>,
    /// What each dispatch from outside came to.
    pub outcomes: Vec<DispatchOutcome>,
}

pub fn run() -> Result<Run, String> {
    // A clock that moves only when told, so two runs see the same times.
    let options = StoreOptions::with_clock(ManualClock::new(1_000));
    let (mut store, _sink) =
        Store::start::<App>(AppState::default(), (), options).map_err(|e| format!("{e}"))?;
    store.add_observer(TraceLog::new());

    let app = store.mount(&root::<App>()).map_err(|e| format!("{e:?}"))?;
    let north = store
        .mount(&root::<App>().child(NORTH))
        .map_err(|e| format!("{e:?}"))?;
    let south = store
        .mount(&root::<App>().child(SOUTH))
        .map_err(|e| format!("{e:?}"))?;
    let item = |name: &str| name.to_string();
    let mut outcomes = Vec::new();
    let mut to = |store: &mut Store<AppState, ()>, mount, action| -> Result<(), String> {
        outcomes.push(
            store
                .dispatch_to(mount, action)
                .map_err(|e| format!("{e:?}"))?,
        );
        Ok(())
    };

    to(
        &mut store,
        &north,
        WarehouseAction::Receive {
            item: item("apple"),
            qty: 5,
        },
    )?; // inline
    to(
        &mut store,
        &north,
        WarehouseAction::Receive {
            item: item("pear"),
            qty: 0,
        },
    )?; // returns early
    to(
        &mut store,
        &north,
        WarehouseAction::Restock {
            item: item("pear"),
            qty: 2,
        },
    )?; // queued
    to(
        &mut store,
        &north,
        WarehouseAction::Ship {
            item: item("apple"),
            qty: 4,
        },
    )?; // leaves 1: reorder
    to(
        &mut store,
        &north,
        WarehouseAction::Ship {
            item: item("apple"),
            qty: 9,
        },
    )?; // refused: backorder
    to(
        &mut store,
        &south,
        WarehouseAction::Receive {
            item: item("nuts"),
            qty: 3,
        },
    )?; // closed
    store
        .dispatch_to(&app, AppAction::OpenSouth)
        .map_err(|e| format!("{e:?}"))?;
    to(
        &mut store,
        &south,
        WarehouseAction::Receive {
            item: item("nuts"),
            qty: 3,
        },
    )?;
    to(
        &mut store,
        &south,
        WarehouseAction::Spill {
            item: item("nuts"),
            qty: 1,
        },
    )?; // no overflow yet
    to(&mut store, &south, WarehouseAction::OpenOverflow)?;
    to(
        &mut store,
        &south,
        WarehouseAction::Spill {
            item: item("nuts"),
            qty: 1,
        },
    )?;

    let live = store.state().clone();
    let trace = store
        .observer::<TraceLog>()
        .expect("installed")
        .entries()
        .iter()
        .map(|e| (e.id.0, e.path.to_string(), e.kind))
        .collect();
    Ok(Run {
        live,
        trace,
        outcomes,
    })
}

fn main() {
    let run = run().unwrap_or_else(|e| panic!("{e}"));
    println!("executions:");
    for (id, path, kind) in &run.trace {
        println!("  {id} {path:<16} {kind}");
    }
    println!("north: {:?}", run.live.north);
    println!("south: {:?}", run.live.south);
    println!("reorders: {:?}", run.live.reorders);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_composite_run_is_repeatable() {
        let run = run().unwrap();
        let (north, south) = (&run.live.north, &run.live.south);

        assert_eq!(
            north.shelf.stock,
            BTreeMap::from([("apple".into(), 1), ("pear".into(), 2)])
        );
        assert_eq!(north.received, 5, "the early return received nothing");
        assert_eq!(north.backorders, ["apple"]);
        assert_eq!(
            run.live.reorders,
            [("north".to_string(), "apple".to_string())]
        );
        assert_eq!(south.shelf.stock, BTreeMap::from([("nuts".into(), 3)]));
        assert_eq!(south.spills_refused, 1);
        assert_eq!(
            south.overflow.as_ref().map(|o| o.stock.clone()),
            Some(BTreeMap::from([("nuts".into(), 1)]))
        );

        // `/south` refused its first Receive at the guard.
        assert_eq!(run.outcomes[5], DispatchOutcome::Disabled);
        assert!(
            run.outcomes
                .iter()
                .enumerate()
                .all(|(i, o)| i == 5 || *o == DispatchOutcome::Executed)
        );

        let paths = |kind: &str| {
            run.trace
                .iter()
                .filter(|(_, _, k)| *k == kind)
                .map(|(_, p, _)| p.as_str())
                .collect::<Vec<_>>()
        };
        // Inline calls are part of their parent's execution; the queued
        // Restock is the only shelf execution of its own.
        assert_eq!(paths("shelf.add"), ["/north/shelf"]);
        assert_eq!(paths("warehouse.low"), ["/north"]);
        assert_eq!(paths("app.reorder"), ["/"]);

        // Run again: the same executions, at the same times, the same end.
        let again = super::run().unwrap();
        assert_eq!((again.trace, again.live), (run.trace, run.live));
    }
}
