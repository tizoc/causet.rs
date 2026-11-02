//! The downloads scenario (`common`): Effects, a shared Service, and replies
//! as Events through `drain_events`.

mod common;

use std::thread;

use causet::observers::{Category, TraceLog};
use causet::{
    ActionMeta, Child, Component, ComponentContext, ComponentRegistry, DispatchError,
    EffectContext, EffectMeta, Never, Registry, SendError, lens,
};
use common::*;

#[test]
fn request_round_trip_through_a_shared_threaded_service() {
    let mut f = fixture(Box::new(Threaded { duplicate: false }), 16);

    f.store
        .dispatch_to(&f.a, DownloaderAction::Fetch("alpha".into()))
        .unwrap();
    f.store
        .dispatch_to(&f.b, DownloaderAction::Fetch("bravo-long".into()))
        .unwrap();
    drain_until(&mut f.store, |s| {
        !s.a.done.is_empty() && !s.b.done.is_empty()
    });

    let s = f.store.state();
    assert_eq!(s.a.done, vec![("alpha".to_string(), 5)]);
    assert_eq!(s.b.done, vec![("bravo-long".to_string(), 10)]);
    assert!(s.a.pending.is_empty() && s.b.pending.is_empty());
    assert!(s.a.stale.is_empty() && s.b.stale.is_empty());

    // Each reply is a root Event execution at the mount that asked, and its
    // route names the Effect that issued it.
    let trace = f.store.observer::<TraceLog>().unwrap().entries();
    for path in ["/a", "/b"] {
        let effect = trace
            .iter()
            .find(|e| e.category == Category::Effect && &*e.path == path)
            .expect("the fetch Effect");
        let event = trace
            .iter()
            .find(|e| e.category == Category::Event && &*e.path == path)
            .expect("the reply Event");
        assert_eq!(event.answers, Some(effect.id));
        assert_eq!(event.causal, event.id, "an Event is a root input");
    }
}

#[test]
fn duplicate_reply_is_ignored_as_stale() {
    let mut f = fixture(Box::new(Threaded { duplicate: true }), 16);

    f.store
        .dispatch_to(&f.a, DownloaderAction::Fetch("alpha".into()))
        .unwrap();
    drain_until(&mut f.store, |s| {
        s.a.done.len() == 1 && s.a.stale.len() == 1
    });

    let s = f.store.state();
    assert_eq!(s.a.done.len(), 1);
    assert_eq!(s.a.stale.len(), 1);
}

#[test]
fn store_lane_is_unbounded_and_drained_before_the_worker_lane() {
    let mut f = fixture(Box::new(Inline), 1);

    // Replies sent on the Store's thread: three, with a worker capacity of 1.
    for url in ["one", "two", "three"] {
        f.store
            .dispatch_to(&f.a, DownloaderAction::Fetch(url.into()))
            .unwrap();
    }
    // A worker thread fills the bounded lane and gets its Event back.
    let (sink, a) = (f.sink.clone(), f.a);
    let results = thread::spawn(move || {
        let first = sink.try_send_to(&a, DownloadEvent::Pushed { note: "w1".into() });
        let second = sink.try_send_to(&a, DownloadEvent::Pushed { note: "w2".into() });
        (first, second)
    })
    .join()
    .unwrap();
    assert_eq!(results.0, Ok(()));
    assert_eq!(
        results.1,
        Err(SendError::Full(DownloadEvent::Pushed { note: "w2".into() }))
    );

    assert_eq!(f.store.drain_events(16), Ok(4));
    let kinds: Vec<_> = f
        .store
        .observer::<TraceLog>()
        .unwrap()
        .entries()
        .iter()
        .filter(|e| e.category == Category::Event)
        .map(|e| e.kind)
        .collect();
    assert_eq!(
        kinds,
        vec![
            "download.done",
            "download.done",
            "download.done",
            "download.pushed"
        ]
    );
    assert_eq!(f.store.state().a.done.len(), 3);
    assert_eq!(f.store.state().a.pushed, vec!["w1"]);
}

#[test]
fn refused_synchronous_child_leaves_no_ordinal_gap() {
    let mut f = fixture(Box::new(Inline), 4);

    f.store.dispatch_to(&f.a, DownloaderAction::Probe).unwrap();

    let trace = f.store.observer::<TraceLog>().unwrap().entries();
    let kinds: Vec<_> = trace.iter().map(|e| (e.category, e.kind)).collect();
    assert_eq!(
        kinds,
        vec![
            (Category::Action, "downloader.probe"),
            (Category::Effect, "downloader.effect.probe"),
            (Category::Action, "downloader.note"),
        ]
    );
    let note = trace[2].origin.expect("an Effect child");
    assert_eq!(note.causal, trace[1].id);
    assert_eq!(
        note.emission_ordinal, 0,
        "the refused attempt claimed nothing"
    );
    assert_eq!(f.store.state().a.notes, vec![1]);
}

#[test]
fn reply_for_an_unregistered_event_faults() {
    let mut f = fixture(Box::new(Inline), 4);

    let Err(DispatchError::Faulted(message)) =
        f.store.dispatch_to(&f.a, DownloaderAction::BadReply)
    else {
        panic!("expected a fault");
    };
    assert!(
        message.contains("/a never registered Event ping"),
        "{message}"
    );
}

#[test]
fn leaf_without_effects_needs_no_service_binding() {
    let mut f = fixture(Box::new(Inline), 4);
    f.store.dispatch_to(&f.counter, Inc).unwrap();
    assert_eq!(f.store.state().counter.n, 1);
}

#[test]
fn effectful_child_without_a_service_projection_fails_freeze() {
    struct Unwired;
    impl Component for Unwired {
        type State = AppState;
        type Service = AppServices;
        type Action = Noop;
        type Effect = Never;
        type Output = Never;
        fn reduce(_: ComponentContext<'_, Self>, _: Noop, _: &ActionMeta) {}
        fn handle(_: EffectContext<'_, Self>, effect: Never, _: &EffectMeta) {
            match effect {}
        }
        fn register(r: &mut ComponentRegistry<'_, Self>) {
            r.child(Child::<Unwired, Downloader>::new("a"), lens!(AppState => a))
                .bind();
        }
    }
    let mut registry = Registry::<AppState, AppServices>::new();
    registry.root::<Unwired>();
    let error = registry.freeze().err().expect("must fail");
    assert!(
        error.0.contains(&causet::Problem::NoServiceProjection {
            path: "/a".into(),
            at: "/a".into()
        }),
        "{error}"
    );
}

#[test]
fn closed_bus_hands_the_event_back() {
    let f = fixture(Box::new(Inline), 4);
    let (sink, a) = (f.sink.clone(), f.a);
    drop(f);
    let result = thread::spawn(move || {
        sink.try_send_to(
            &a,
            DownloadEvent::Pushed {
                note: "late".into(),
            },
        )
    })
    .join()
    .unwrap();
    assert_eq!(
        result,
        Err(SendError::Closed(DownloadEvent::Pushed {
            note: "late".into()
        }))
    );
}
