//! Review round 6: an observer fault faults the Store at its own boundary;
//! clock faults while starting a synchronous Effect child fault it too;
//! Stats keeps a name and its prefix apart.

mod common;

use std::sync::Arc;

use causet::InstanceId;
use causet::observers::Stats;
use causet::observers::{BusDepths, Category, Execution, MessageView, Observer};
use causet::*;
use common::*;

fn options(clock: impl Clock + 'static) -> StoreOptions {
    StoreOptions::with_clock(clock)
}

struct FinishPanics;

impl Observer for FinishPanics {
    fn action_finish(&mut self, _: &Execution<'_>) {
        panic!("observer broke");
    }
}

#[test]
fn an_observer_fault_faults_the_store_at_its_boundary() {
    let mut f = fixture_with(Box::new(Inline), 4, options(ManualClock::new(1000)));
    f.store.add_observer(FinishPanics);
    assert_eq!(
        f.store.dispatch_to(&f.counter, Inc),
        Err(DispatchError::Faulted("observer broke".into()))
    );
    assert_eq!(f.store.state().counter.n, 1);
}

struct ChildStartPanics;

impl Observer for ChildStartPanics {
    fn action_start(&mut self, exec: &Execution<'_>, _: &dyn MessageView) {
        if exec.kind == "downloader.note" {
            panic!("child start observer broke");
        }
    }
}

#[test]
fn a_child_start_observer_fault_stops_before_the_child_runs() {
    let mut f = fixture_with(Box::new(Inline), 4, options(ManualClock::new(1000)));
    f.store.add_observer(ChildStartPanics);
    assert_eq!(
        f.store.dispatch_to(&f.a, DownloaderAction::Probe),
        Err(DispatchError::Faulted("child start observer broke".into()))
    );
    let live = f.store.state().clone();
    assert!(live.a.notes.is_empty());
}

struct FailsFourth {
    reads: u64,
}

impl Clock for FailsFourth {
    fn now(&mut self) -> Timestamp {
        self.reads += 1;
        assert!(self.reads < 4, "clock stopped");
        1000 + self.reads
    }
}

#[test]
fn a_clock_fault_starting_a_synchronous_effect_child_faults() {
    let mut f = fixture_with(Box::new(Inline), 4, options(FailsFourth { reads: 0 }));
    // Anchor, root Probe, its Effect, then the admitted synchronous Note(1).
    assert_eq!(
        f.store.dispatch_to(&f.a, DownloaderAction::Probe),
        Err(DispatchError::Faulted("clock stopped".into()))
    );
}

#[test]
fn statistics_distinguish_static_names_sharing_a_start_address() {
    let full: &'static str = "task.run";
    let prefix: &'static str = &full[..4];
    assert_eq!(full.as_ptr(), prefix.as_ptr());
    assert_ne!(full, prefix);
    let path: Arc<str> = Arc::from("/");
    let mut stats = Stats::new();
    for (time, kind) in [(10, full), (20, prefix), (30, full)] {
        let meta = ExecMeta {
            id: ExecutionId(time),
            depth: 1,
            time,
            origin: None,
        };
        let exec = Execution {
            category: Category::Action,
            path: &path,
            instance: InstanceId(1),
            message: "task.action",
            kind,
            meta: &meta,
            answers: None,
        };
        stats.action_start(&exec, &Inc);
    }
    let snapshot = stats.snapshot();
    let kinds = &snapshot.actions["task.action"];
    assert_eq!(kinds.len(), 2, "distinct kind names were merged: {kinds:?}");
    assert_eq!(kinds["task.run"].calls(), 1);
    assert_eq!(kinds["task"].calls(), 1);
}

struct EffectFinishPanics;

impl Observer for EffectFinishPanics {
    fn effect_finish(&mut self, _: &Execution<'_>) {
        panic!("effect finish observer broke");
    }
}

#[test]
fn an_effect_finish_observer_fault_comes_after_the_synchronous_children() {
    let mut f = fixture_with(Box::new(Inline), 4, options(ManualClock::new(1000)));
    f.store.add_observer(EffectFinishPanics);
    assert!(f.store.dispatch_to(&f.a, DownloaderAction::Probe).is_err());
    let live = f.store.state().clone();
    assert_eq!(
        live.a.notes,
        [1],
        "the child ran before the Effect finished"
    );
}

struct BusPanics;

impl Observer for BusPanics {
    fn bus(&mut self, _: &BusDepths) {
        panic!("bus observer broke");
    }
}

#[test]
fn a_bus_observer_fault_faults_the_store() {
    let mut f = fixture_with(Box::new(Inline), 4, options(ManualClock::new(1000)));
    f.store.dispatch_to(&f.counter, Inc).unwrap();
    f.store.add_observer(BusPanics);
    assert_eq!(
        f.store.drain_events(16),
        Err(DispatchError::Faulted("bus observer broke".into()))
    );
}
