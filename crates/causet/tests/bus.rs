//! The Event bus's worker lane: bounded, waiting senders woken by the drain
//! or by the Store closing. A fault closes the bus as a drop does, and
//! `send_front` puts the Event at the head of its thread's lane.

mod common;

use std::thread;
use std::time::{Duration, Instant};

use causet::hosting::{EventBus, ReleaseError};
use causet::{DispatchError, SendError, Store, StoreOptions, root};
use causet::{ExecutionId, SessionId};
use common::*;

fn on_worker<R: Send + 'static>(f: impl FnOnce() -> R + Send + 'static) -> R {
    thread::spawn(f).join().unwrap()
}

fn pushed(note: &str) -> DownloadEvent {
    DownloadEvent::Pushed { note: note.into() }
}

#[test]
fn a_waiting_sender_wakes_when_the_store_drains() {
    let mut f = fixture(Box::new(Inline), 1);
    let (sink, a) = (f.sink.clone(), f.a);
    thread::spawn({
        let sink = sink.clone();
        move || sink.try_send_to(&a, pushed("first")).unwrap()
    })
    .join()
    .unwrap();

    // The lane is full; this sender waits instead of failing.
    let waiting = thread::spawn(move || {
        let started = Instant::now();
        let result = sink.send_timeout_to(&a, pushed("second"), Duration::from_secs(10));
        (result, started.elapsed())
    });
    thread::sleep(Duration::from_millis(50));
    assert_eq!(f.store.drain_events(1), Ok(1));

    let (result, waited) = waiting.join().unwrap();
    assert_eq!(result, Ok(()));
    assert!(
        waited < Duration::from_secs(5),
        "woken by the drain, not by the timeout ({waited:?})"
    );
    assert_eq!(f.store.drain_events(16), Ok(1));
    assert_eq!(f.store.state().a.pushed, ["first", "second"]);
}

#[test]
fn send_timeout_gives_up_after_its_bound() {
    let f = fixture(Box::new(Inline), 1);
    let (sink, a) = (f.sink.clone(), f.a);
    let (result, waited) = thread::spawn(move || {
        sink.try_send_to(&a, pushed("first")).unwrap();
        let started = Instant::now();
        let result = sink.send_timeout_to(&a, pushed("second"), Duration::from_millis(50));
        (result, started.elapsed())
    })
    .join()
    .unwrap();
    assert_eq!(result, Err(SendError::Full(pushed("second"))));
    assert!(waited >= Duration::from_millis(50), "{waited:?}");
}

#[test]
fn dropping_the_store_wakes_a_waiting_sender_with_closed() {
    let f = fixture(Box::new(Inline), 1);
    let (sink, a) = (f.sink.clone(), f.a);
    sink.try_send_to(&a, pushed("first")).unwrap();
    let waiting =
        thread::spawn(move || sink.send_timeout_to(&a, pushed("second"), Duration::from_secs(10)));
    thread::sleep(Duration::from_millis(50));
    drop(f);
    assert_eq!(
        waiting.join().unwrap(),
        Err(SendError::Closed(pushed("second")))
    );
}

#[test]
fn a_fault_wakes_a_waiting_sender_with_closed() {
    let mut f = fixture(Box::new(Inline), 1);
    let (sink, a) = (f.sink.clone(), f.a);
    sink.try_send_to(&a, pushed("first")).unwrap();
    let waiting =
        thread::spawn(move || sink.send_timeout_to(&a, pushed("second"), Duration::from_secs(10)));
    thread::sleep(Duration::from_millis(50));
    assert!(f.store.dispatch_to(&f.a, DownloaderAction::Crash).is_err());
    assert_eq!(
        waiting.join().unwrap(),
        Err(SendError::Closed(pushed("second")))
    );
}

#[test]
fn a_fault_closes_the_bus_on_every_thread() {
    let mut f = fixture(Box::new(Inline), 4);
    let reply = f.sink.reply_for(&f.a, ExecutionId(1)).unwrap();
    // Queued in both lanes when the Store faults.
    f.store.dispatch_to(&f.counter, Inc).unwrap();
    f.sink.try_send_to(&f.a, pushed("store")).unwrap();
    let (sink, a) = (f.sink.clone(), f.a);
    on_worker(move || sink.try_send_to(&a, pushed("worker")).unwrap());

    assert_eq!(
        f.store.dispatch_to(&f.a, DownloaderAction::Crash),
        Err(DispatchError::Faulted("downloader crashed".into()))
    );

    // Every send fails from now on: from this thread, from another, through
    // a Reply, and to a lane chosen outright.
    let late = || pushed("late");
    assert_eq!(
        f.sink.try_send_to(&f.a, late()),
        Err(SendError::Closed(late()))
    );
    assert_eq!(
        f.sink.try_send_to_as_worker(&f.a, late()),
        Err(SendError::Closed(late()))
    );
    assert_eq!(reply.try_send(late()), Err(SendError::Closed(late())));
    assert_eq!(reply.send_front(late()), Err(SendError::Closed(late())));
    let (sink, a, worker_reply) = (f.sink.clone(), f.a, reply.clone());
    on_worker(move || {
        assert_eq!(sink.try_send_to(&a, late()), Err(SendError::Closed(late())));
        assert_eq!(
            sink.send_timeout_to(&a, late(), Duration::from_secs(5)),
            Err(SendError::Closed(late()))
        );
        assert_eq!(
            worker_reply.try_send(late()),
            Err(SendError::Closed(late()))
        );
        assert_eq!(
            worker_reply.send_front(late()),
            Err(SendError::Closed(late()))
        );
    });
    // What was queued never runs.
    assert!(matches!(
        f.store.drain_events(16),
        Err(DispatchError::Faulted(_))
    ));
    assert!(f.store.state().a.pushed.is_empty());
}

#[test]
fn a_fault_discards_what_a_held_bus_holds() {
    let p = protocol();
    let a = p.resolve(&root::<App>().child(A)).unwrap();
    let (sink, inbox, held) = EventBus::held(&p, 4, SessionId(1));
    let services = AppServices {
        downloads: Box::new(Inline),
    };
    let mut store = Store::new(
        p,
        AppState::default(),
        services,
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    sink.try_send_to(&a, pushed("held")).unwrap();
    let id = held.list()[0].id;
    assert!(store.dispatch_to(&a, DownloaderAction::Crash).is_err());
    assert!(held.is_empty());
    assert_eq!(held.release(id), Err(ReleaseError::Closed));
    assert_eq!(
        sink.try_send_to(&a, pushed("late")),
        Err(SendError::Closed(pushed("late")))
    );
}

#[test]
fn send_front_from_the_store_thread_goes_to_the_head_of_the_store_lane() {
    let mut f = fixture(Box::new(Inline), 4);
    let reply = f.sink.reply_for(&f.a, ExecutionId(1)).unwrap();
    f.store.dispatch_to(&f.counter, Inc).unwrap();
    let (sink, a) = (f.sink.clone(), f.a);
    on_worker(move || sink.try_send_to(&a, pushed("worker")).unwrap());
    f.sink.try_send_to(&f.a, pushed("back")).unwrap();
    reply.send_front(pushed("front")).unwrap();
    f.store.drain_events(16).unwrap();
    assert_eq!(f.store.state().a.pushed, ["front", "back", "worker"]);
}

#[test]
fn send_front_from_another_thread_goes_to_the_head_of_the_worker_lane() {
    let mut f = fixture(Box::new(Inline), 3);
    let reply = f.sink.reply_for(&f.a, ExecutionId(1)).unwrap();
    f.store.dispatch_to(&f.counter, Inc).unwrap();
    f.sink.try_send_to(&f.a, pushed("store")).unwrap();
    let (sink, a) = (f.sink.clone(), f.a);
    on_worker(move || {
        sink.try_send_to(&a, pushed("first")).unwrap();
        sink.try_send_to(&a, pushed("second")).unwrap();
        reply.send_front(pushed("front")).unwrap();
        // The worker lane's capacity holds for a front send too: at capacity
        // it fails at once, never waiting.
        assert_eq!(
            reply.send_front(pushed("over")),
            Err(SendError::Full(pushed("over")))
        );
    });
    f.store.drain_events(16).unwrap();
    // The Store lane still runs first.
    assert_eq!(
        f.store.state().a.pushed,
        ["store", "front", "first", "second"]
    );
}
