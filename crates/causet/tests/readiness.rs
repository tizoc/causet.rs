//! Synchronous notifications, cancellation and native waits on the real bus.
mod common;

use causet::SessionId;
use causet::hosting::{
    EventAvailability, EventBus, EventReadiness, ReadinessInUse, ReadinessSubscription,
};
use causet::{Store, StoreOptions, root};
use common::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

fn pushed() -> DownloadEvent {
    DownloadEvent::Pushed {
        note: "ready".into(),
    }
}
fn count(counter: &AtomicUsize) -> usize {
    counter.load(Ordering::SeqCst)
}

/// A subscription that counts its notifications.
fn counting(ready: &EventReadiness) -> (Arc<AtomicUsize>, ReadinessSubscription) {
    let wakes = Arc::new(AtomicUsize::new(0));
    let counter = wakes.clone();
    let subscription = ready
        .subscribe(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    (wakes, subscription)
}

/// A `wait_timeout` blocked on another thread: it has started, and is still
/// waiting a moment later.
fn waiting(ready: &EventReadiness) -> JoinHandle<(Option<EventAvailability>, Duration)> {
    let ready = ready.clone();
    let start = Arc::new(Barrier::new(2));
    let started = start.clone();
    let waiter = thread::spawn(move || {
        started.wait();
        let at = Instant::now();
        (ready.wait_timeout(Duration::from_secs(10)), at.elapsed())
    });
    start.wait();
    thread::sleep(Duration::from_millis(10));
    assert!(!waiter.is_finished(), "nothing is ready yet");
    waiter
}

/// What a blocked waiter returned once woken, well before its timeout.
fn woken(waiter: JoinHandle<(Option<EventAvailability>, Duration)>) -> Option<EventAvailability> {
    let (ready, waited) = waiter.join().unwrap();
    assert!(waited < Duration::from_secs(2), "wakes before timeout");
    ready
}

#[test]
fn notifications_persist_coalesce_and_never_consume_input() {
    let mut f = fixture(Box::new(Inline), 4);
    let ready = f.store.readiness();
    let wakes = Arc::new(AtomicUsize::new(0));
    let count_wakes = wakes.clone();
    let _subscription = ready
        .subscribe(move || {
            count_wakes.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    assert_eq!(ready.current(), None);
    for _ in 0..2 {
        f.sink.try_send_to(&f.a, pushed()).unwrap();
    }
    assert_eq!(count(&wakes), 1);
    assert!(f.store.state().a.pushed.is_empty());
    assert_eq!(ready.current(), Some(EventAvailability::Events));
    assert_eq!(f.store.drain_events(1), Ok(1));
    f.sink.try_send_to(&f.a, pushed()).unwrap();
    assert_eq!(count(&wakes), 1, "still nonempty");
    assert_eq!(f.store.drain_events(4), Ok(2));
    f.sink.try_send_to(&f.a, pushed()).unwrap();
    assert_eq!(count(&wakes), 2, "new batch, same registration");
    drop(f);
    assert_eq!(count(&wakes), 3, "closure notifies too");
    assert_eq!(ready.current(), Some(EventAvailability::Closed));
}

#[test]
fn only_one_driver_registers_and_old_cancellation_cannot_remove_its_replacement() {
    let mut f = fixture(Box::new(Inline), 2);
    let ready = f.store.readiness();
    let mut first = ready.subscribe(|| {}).unwrap();
    assert_eq!(ready.subscribe(|| {}).unwrap_err(), ReadinessInUse);
    first.cancel();
    let wakes = Arc::new(AtomicUsize::new(0));
    let counter = wakes.clone();
    let _replacement = ready
        .subscribe(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    first.cancel();
    drop(first);
    f.sink.try_send_to(&f.a, pushed()).unwrap();
    assert_eq!(count(&wakes), 1);
    assert_eq!(f.store.drain_events(1), Ok(1));
    f.sink.try_send_to(&f.a, pushed()).unwrap();
    assert_eq!(count(&wakes), 2);
}

#[test]
fn registration_with_existing_input_is_immediate_and_drop_unregisters() {
    let f = fixture(Box::new(Inline), 2);
    f.sink.try_send_to(&f.a, pushed()).unwrap();
    let wakes = Arc::new(AtomicUsize::new(0));
    let counter = wakes.clone();
    let subscription = f
        .store
        .readiness()
        .subscribe(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    assert_eq!(count(&wakes), 1);
    drop(subscription);
    drop(f);
    assert_eq!(count(&wakes), 1);
}

#[test]
fn store_lane_replies_notify_and_bad_callbacks_cannot_fault_the_store() {
    let mut f = fixture(Box::new(Inline), 0);
    let ready = f.store.readiness();
    let _subscription = ready.subscribe(|| panic!("bad host notifier")).unwrap();
    f.store
        .dispatch_to(&f.a, DownloaderAction::Fetch("a".into()))
        .unwrap();
    assert_eq!(ready.current(), Some(EventAvailability::Events));
    assert_eq!(f.store.fault(), None);
    assert_eq!(f.store.drain_events(1), Ok(1));
}

#[test]
fn callbacks_and_capture_drops_run_outside_the_bus_lock() {
    struct Capture {
        ready: causet::hosting::EventReadiness,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Capture {
        fn drop(&mut self) {
            let _ = self.ready.current();
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    let f = fixture(Box::new(Inline), 2);
    let ready = f.store.readiness();
    let drops = Arc::new(AtomicUsize::new(0));
    let capture = Arc::new(Capture {
        ready: ready.clone(),
        drops: drops.clone(),
    });
    let weak = Arc::downgrade(&capture);
    let mut subscription = ready
        .subscribe(move || {
            assert_eq!(capture.ready.current(), Some(EventAvailability::Events));
        })
        .unwrap();
    f.sink.try_send_to(&f.a, pushed()).unwrap();
    assert!(weak.upgrade().is_some());
    subscription.cancel();
    assert!(
        weak.upgrade().is_none(),
        "cancellation releases captures even with token alive"
    );
    assert_eq!(count(&drops), 1);
}

#[test]
fn closure_releases_the_callback_even_while_the_subscription_survives() {
    let p = protocol();
    let (_, inbox) = EventBus::new(&p, 1);
    let ready = inbox.readiness();
    let capture = Arc::new(());
    let weak = Arc::downgrade(&capture);
    let _subscription = ready
        .subscribe(move || {
            let _ = &capture;
        })
        .unwrap();
    drop(inbox);
    assert!(weak.upgrade().is_none());
    let wakes = Arc::new(AtomicUsize::new(0));
    let counter = wakes.clone();
    let _closed_subscription = ready
        .subscribe(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    assert_eq!(count(&wakes), 1);
}

#[test]
fn unused_inbox_store_fault_and_refused_inbox_all_notify_closure() {
    let p = protocol();
    let (_, inbox) = EventBus::new(&p, 1);
    let ready = inbox.readiness();
    let _subscription = ready.subscribe(|| {}).unwrap();
    drop(inbox);
    assert_eq!(ready.current(), Some(EventAvailability::Closed));
    let mut f = fixture(Box::new(Inline), 1);
    let ready = f.store.readiness();
    let wakes = Arc::new(AtomicUsize::new(0));
    let counter = wakes.clone();
    let _subscription = ready
        .subscribe(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    assert!(f.store.dispatch_to(&f.a, DownloaderAction::Crash).is_err());
    assert_eq!(count(&wakes), 1);
    assert_eq!(ready.current(), Some(EventAvailability::Closed));
    let (_, inbox) = EventBus::new(&p, 1);
    let ready = inbox.readiness();
    let result = Store::new(
        protocol(),
        AppState::default(),
        AppServices {
            downloads: Box::new(Inline),
        },
        inbox,
        StoreOptions::default(),
    );
    assert!(result.is_err());
    assert_eq!(ready.current(), Some(EventAvailability::Closed));
}

#[test]
fn held_events_notify_only_when_released() {
    let p = protocol();
    let a = p.resolve(&root::<App>().child(A)).unwrap();
    let (sink, inbox, held) = EventBus::held(&p, 1, SessionId(1));
    let ready = inbox.readiness();
    let wakes = Arc::new(AtomicUsize::new(0));
    let counter = wakes.clone();
    let _subscription = ready
        .subscribe(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    sink.try_send_to(&a, pushed()).unwrap();
    held.discard(held.list()[0].id);
    assert_eq!(count(&wakes), 0);
    assert_eq!(ready.current(), None);
    sink.try_send_to(&a, pushed()).unwrap();
    held.release(held.list()[0].id).unwrap();
    assert_eq!(count(&wakes), 1);
    assert_eq!(ready.current(), Some(EventAvailability::Events));
    drop(inbox);
    assert_eq!(count(&wakes), 2);
}

#[test]
fn racing_registration_with_publication_never_loses_the_wakeup() {
    let mut f = fixture(Box::new(Inline), 1);
    let ready = f.store.readiness();
    for _ in 0..256 {
        let start = Arc::new(Barrier::new(2));
        let (sink, a, barrier) = (f.sink.clone(), f.a, start.clone());
        let worker = thread::spawn(move || {
            barrier.wait();
            sink.try_send_to_as_worker(&a, pushed()).unwrap();
        });
        let wakes = Arc::new(AtomicUsize::new(0));
        let counter = wakes.clone();
        start.wait();
        let subscription = ready
            .subscribe(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        worker.join().unwrap();
        assert_eq!(count(&wakes), 1);
        assert_eq!(f.store.drain_events(1), Ok(1));
        drop(subscription);
    }
}

#[test]
fn native_wait_times_out_and_wakes_without_consuming_input() {
    let mut f = fixture(Box::new(Inline), 1);
    let ready = f.store.readiness();
    assert_eq!(ready.wait_timeout(Duration::ZERO), None);
    let (sink, a) = (f.sink.clone(), f.a);
    let worker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        sink.try_send_to_as_worker(&a, pushed()).unwrap();
    });
    let started = Instant::now();
    assert_eq!(
        ready.wait_timeout(Duration::from_secs(10)),
        Some(EventAvailability::Events)
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "wakes before timeout"
    );
    worker.join().unwrap();
    assert!(f.store.state().a.pushed.is_empty());
    assert_eq!(f.store.drain_events(1), Ok(1));
    assert_eq!(ready.wait_timeout(Duration::from_millis(2)), None);
}

#[test]
fn native_wait_wakes_on_closure() {
    let (_, inbox) = EventBus::new(&protocol(), 1);
    let ready = inbox.readiness();
    let worker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        drop(inbox);
    });
    let started = Instant::now();
    assert_eq!(
        ready.wait_timeout(Duration::from_secs(10)),
        Some(EventAvailability::Closed)
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    worker.join().unwrap();
}

#[test]
fn releasing_a_held_event_wakes_a_blocked_wait_and_notifies() {
    let p = protocol();
    let a = p.resolve(&root::<App>().child(A)).unwrap();
    let (sink, inbox, held) = EventBus::held(&p, 1, SessionId(1));
    let ready = inbox.readiness();
    let (wakes, _subscription) = counting(&ready);
    sink.try_send_to(&a, pushed()).unwrap();
    let waiter = waiting(&ready);
    assert_eq!(count(&wakes), 0, "a held Event is not ready");
    held.release(held.list()[0].id).unwrap();
    assert_eq!(woken(waiter), Some(EventAvailability::Events));
    assert_eq!(count(&wakes), 1);
    assert_eq!(ready.current(), Some(EventAvailability::Events));
}

#[test]
fn a_store_fault_wakes_a_blocked_wait_with_closed_and_notifies() {
    let mut f = fixture(Box::new(Inline), 1);
    let ready = f.store.readiness();
    let (wakes, _subscription) = counting(&ready);
    let waiter = waiting(&ready);
    assert!(f.store.dispatch_to(&f.a, DownloaderAction::Crash).is_err());
    assert_eq!(woken(waiter), Some(EventAvailability::Closed));
    assert_eq!(count(&wakes), 1);
}

#[test]
fn a_store_lane_reply_during_a_root_input_wakes_a_blocked_wait_and_notifies() {
    // With no worker capacity, the Inline reply can only take the Store lane.
    let mut f = fixture(Box::new(Inline), 0);
    let ready = f.store.readiness();
    let (wakes, _subscription) = counting(&ready);
    let waiter = waiting(&ready);
    f.store
        .dispatch_to(&f.a, DownloaderAction::Fetch("a".into()))
        .unwrap();
    assert_eq!(woken(waiter), Some(EventAvailability::Events));
    assert_eq!(count(&wakes), 1);
    assert_eq!(f.store.drain_events(1), Ok(1));
    assert_eq!(f.store.state().a.done, [("a".to_string(), 1)]);
}
