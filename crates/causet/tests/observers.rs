//! Observers on the downloads scenario: payload views at start only, Events
//! and synchronous children in the lifecycle, faults and bus depths.

mod common;

use std::thread;

use causet::SendError;
use causet::observers::{BusDepths, Execution, FaultInfo, MessageView, Observer, TraceLog};
use common::*;

/// Every callback as one line, payloads in their `Debug` form.
#[derive(Default)]
struct Calls(Vec<String>);

impl Observer for Calls {
    fn action_start(&mut self, exec: &Execution<'_>, payload: &dyn MessageView) {
        self.0.push(format!(
            "action_start {} {} {payload:?}",
            exec.path, exec.kind
        ));
    }
    fn action_finish(&mut self, exec: &Execution<'_>) {
        self.0.push(format!("action_finish {}", exec.kind));
    }
    fn action_settled(&mut self, exec: &Execution<'_>) {
        self.0.push(format!("action_settled {}", exec.kind));
    }
    fn effect_start(&mut self, exec: &Execution<'_>, payload: &dyn MessageView) {
        self.0.push(format!(
            "effect_start {} {} {payload:?}",
            exec.path, exec.kind
        ));
    }
    fn effect_finish(&mut self, exec: &Execution<'_>) {
        self.0.push(format!("effect_finish {}", exec.kind));
    }
    fn event_start(&mut self, exec: &Execution<'_>, payload: &dyn MessageView) {
        let answers = exec.answers.is_some();
        self.0.push(format!(
            "event_start {} {} answers={answers} {payload:?}",
            exec.path, exec.kind
        ));
    }
    fn event_finish(&mut self, exec: &Execution<'_>) {
        self.0.push(format!("event_finish {}", exec.kind));
    }
    fn event_settled(&mut self, exec: &Execution<'_>) {
        self.0.push(format!("event_settled {}", exec.kind));
    }
    fn fault(&mut self, fault: &FaultInfo<'_>) {
        self.0.push(format!(
            "fault {:?} at={} {}",
            fault.phase,
            fault.at.is_some(),
            fault.message
        ));
    }
    fn bus(&mut self, depths: &BusDepths) {
        self.0.push(format!(
            "bus {} {} {}",
            depths.store_lane, depths.worker_lane, depths.rejected_full
        ));
    }
}

fn calls(f: &Fixture) -> Vec<String> {
    f.store.observer::<Calls>().unwrap().0.clone()
}

#[test]
fn a_request_and_its_reply_reach_observers_in_lifecycle_order() {
    let mut f = fixture(Box::new(Inline), 4);
    f.store.add_observer(Calls::default());

    f.store
        .dispatch_to(&f.a, DownloaderAction::Fetch("alpha".into()))
        .unwrap();
    f.store.drain_events(16).unwrap();

    assert_eq!(
        calls(&f).iter().map(|c| redact(c)).collect::<Vec<_>>(),
        [
            r#"action_start /a downloader.fetch Fetch("alpha")"#,
            "action_finish downloader.fetch",
            r#"effect_start /a downloader.effect.fetch Fetch { request: R, url: "alpha" }"#,
            "effect_finish downloader.effect.fetch",
            "action_settled downloader.fetch",
            // The Inline service replied on the Store lane.
            "bus 1 0 0",
            "event_start /a download.done answers=true Done { request: R, bytes: 5 }",
            "event_finish download.done",
            "action_start /a downloader.completed Completed { request: R, bytes: 5 }",
            "action_finish downloader.completed",
            r#"action_start /a downloader.finished Finished { url: "alpha", bytes: 5 }"#,
            "action_finish downloader.finished",
            "action_settled downloader.finished",
            "action_settled downloader.completed",
            "event_settled download.done",
        ]
    );
}

/// Replaces each `RequestId { .. }`, whose IDs are clock times, with `R`.
fn redact(line: &str) -> String {
    let mut out = String::new();
    let mut rest = line;
    while let Some(start) = rest.find("RequestId {") {
        out.push_str(&rest[..start]);
        out.push('R');
        let end = rest[start..].find('}').expect("closing brace") + start + 1;
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

#[test]
fn synchronous_effect_children_run_inside_their_effect() {
    let mut f = fixture(Box::new(Inline), 4);
    f.store.add_observer(Calls::default());

    f.store.dispatch_to(&f.a, DownloaderAction::Probe).unwrap();

    // Note(0) is refused: no callback. Note(1) runs whole before the Effect
    // finishes.
    assert_eq!(
        calls(&f),
        [
            "action_start /a downloader.probe Probe",
            "action_finish downloader.probe",
            "effect_start /a downloader.effect.probe Probe",
            "action_start /a downloader.note Note(1)",
            "action_finish downloader.note",
            "action_settled downloader.note",
            "effect_finish downloader.effect.probe",
            "action_settled downloader.probe",
        ]
    );
}

#[test]
fn a_fault_reaches_observers_with_its_phase_and_reason() {
    let mut f = fixture(Box::new(Inline), 4);
    f.store.add_observer(Calls::default());

    assert!(f.store.dispatch_to(&f.a, DownloaderAction::Crash).is_err());
    assert_eq!(
        calls(&f),
        [
            "action_start /a downloader.crash Crash",
            "fault Reducer at=true downloader crashed"
        ]
    );

    // An admission panic belongs to no execution.
    let mut f = fixture(Box::new(Inline), 4);
    f.store.add_observer(Calls::default());
    assert!(
        f.store
            .dispatch_to(&f.a, DownloaderAction::Explode)
            .is_err()
    );
    assert_eq!(calls(&f), ["fault Admission at=false admission exploded"]);
}

#[test]
fn a_drain_reports_lane_depths_and_full_rejections() {
    let mut f = fixture(Box::new(Inline), 1);
    f.store.add_observer(Calls::default());
    let (sink, a) = (f.sink.clone(), f.a);
    let pushed = |note: &str| DownloadEvent::Pushed { note: note.into() };

    let rejected = thread::spawn(move || {
        sink.try_send_to(&a, pushed("first")).unwrap();
        let full = sink.try_send_to(&a, pushed("second"));
        assert_eq!(full, Err(SendError::Full(pushed("second"))));
    });
    rejected.join().unwrap();

    assert_eq!(f.store.drain_events(16), Ok(1));
    assert_eq!(f.store.drain_events(16), Ok(0));
    let bus: Vec<_> = calls(&f)
        .into_iter()
        .filter(|c| c.starts_with("bus"))
        .collect();
    // Depths are taken before draining; the count resets once reported.
    assert_eq!(bus, ["bus 0 1 1", "bus 0 0 0"]);
}

#[test]
fn a_panicking_observer_faults_the_store() {
    struct Panics;
    impl Observer for Panics {
        fn action_finish(&mut self, _: &Execution<'_>) {
            panic!("observer broke");
        }
    }
    let mut f = fixture(Box::new(Inline), 4);
    f.store.add_observer(Panics);
    f.store.add_observer(TraceLog::new());

    let error = f.store.dispatch_to(&f.counter, Inc).unwrap_err();
    assert_eq!(
        error,
        causet::DispatchError::Faulted("observer broke".into())
    );
    // It ran inside the root input: the Counter's Action had started.
    assert_eq!(f.store.observer::<TraceLog>().unwrap().entries().len(), 1);
}

/// An observer reads a payload as its own type: the Actions it knows, read
/// exactly, and nothing for the others.
#[test]
fn an_observer_reads_a_payload_as_its_type() {
    use causet::guide::shop::*;
    use causet::observers::{Execution, MessageView, Observer};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Prices(Arc<Mutex<Vec<u64>>>);

    impl Observer for Prices {
        fn action_start(&mut self, _: &Execution<'_>, payload: &dyn MessageView) {
            if let Some(CartAction::Add { price, .. }) = payload.downcast_ref::<CartAction>() {
                self.0.lock().unwrap().push(*price);
            }
            // Another message type is not a CartAction.
            if payload.downcast_ref::<PaymentsAction>().is_some() {
                assert!(payload.downcast_ref::<CartAction>().is_none());
            }
        }
    }

    let prices = Arc::new(Mutex::new(Vec::new()));
    let mut shop = Shop::open(StoreOptions::default());
    shop.store.add_observer(Prices(Arc::clone(&prices)));
    for price in [3, 4] {
        shop.store
            .dispatch_to(
                &shop.cart,
                CartAction::Add {
                    item: "tea".into(),
                    price,
                },
            )
            .unwrap();
    }
    shop.store
        .dispatch_to(&shop.cart, CartAction::Checkout)
        .unwrap();
    shop.store.drain_events(16).unwrap();
    assert_eq!(*prices.lock().unwrap(), [3, 4]);
}
