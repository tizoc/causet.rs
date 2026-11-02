//! Review round 5: the bus closes before Services drop; a fault at the clock
//! (a panicking clock) records something replay can consume;
//! replay checks a failed admission candidate's time against the clock rule.

mod common;

use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use causet::*;
use common::*;

fn options(clock: impl Clock + 'static) -> StoreOptions {
    StoreOptions::with_clock(clock)
}

struct FailingClock {
    reads: usize,
}

impl Clock for FailingClock {
    fn now(&mut self) -> Timestamp {
        self.reads += 1;
        assert!(self.reads < 3, "clock stopped");
        1000 + self.reads as u64
    }
}

#[test]
fn a_panicking_clock_faults_the_store() {
    let mut f = fixture_with(Box::new(Inline), 1, options(FailingClock { reads: 0 }));
    f.store.dispatch_to(&f.counter, Inc).unwrap();
    assert!(matches!(
        f.store.dispatch_to(&f.counter, Inc),
        Err(DispatchError::Faulted(_))
    ));
}

type Worker = JoinHandle<Result<(), SendError<DownloadEvent>>>;
type WorkerOutcome = Option<Result<(), SendError<DownloadEvent>>>;

struct JoiningService {
    worker: Arc<Mutex<Option<Worker>>>,
    outcome: Arc<Mutex<WorkerOutcome>>,
}

impl DownloadService for JoiningService {
    fn fetch(&mut self, _: RequestId, _: String, _: Reply<DownloadEvent>) {
        unreachable!("no request is needed for the shutdown test");
    }
}

impl Drop for JoiningService {
    fn drop(&mut self) {
        let worker = self.worker.lock().unwrap().take().unwrap();
        *self.outcome.lock().unwrap() = Some(worker.join().unwrap());
    }
}

fn pushed(note: &str) -> DownloadEvent {
    DownloadEvent::Pushed { note: note.into() }
}

#[test]
fn the_bus_closes_before_a_service_joins_its_worker() {
    let worker = Arc::new(Mutex::new(None));
    let outcome = Arc::new(Mutex::new(None));
    let service = JoiningService {
        worker: Arc::clone(&worker),
        outcome: Arc::clone(&outcome),
    };
    let f = fixture(Box::new(service), 1);
    // No Store input has run, so this fills the bounded worker lane.
    f.sink.try_send_to(&f.a, pushed("first")).unwrap();
    let sink = f.sink.clone();
    let mount = f.a;
    *worker.lock().unwrap() = Some(thread::spawn(move || {
        sink.send_timeout_to(&mount, pushed("second"), Duration::from_millis(100))
    }));

    drop(f);
    assert_eq!(
        *outcome.lock().unwrap(),
        Some(Err(SendError::Closed(pushed("second"))))
    );
}

/// Fails on its third reading: the anchor, then a root Action, then the next
/// execution to start.
struct FailsThird {
    reads: u64,
}

impl Clock for FailsThird {
    fn now(&mut self) -> Timestamp {
        self.reads += 1;
        assert!(self.reads < 3, "clock stopped");
        1000 + self.reads
    }
}

#[test]
fn a_clock_failure_inside_an_execution_faults_the_store() {
    let mut f = fixture_with(Box::new(Inline), 1, options(FailsThird { reads: 0 }));
    // Probe runs at 1002; its Effect's clock read fails.
    let Err(DispatchError::Faulted(reason)) = f.store.dispatch_to(&f.a, DownloaderAction::Probe)
    else {
        panic!("the clock faults the Store");
    };
    assert_eq!(reason, "clock stopped");
}
