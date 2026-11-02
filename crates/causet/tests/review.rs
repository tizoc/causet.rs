//! Regressions from the independent review of the prototype, plus the
//! `no_effects!(Service)` form it suggested.

mod common;

use std::thread;

use causet::hosting::EventBus;
use causet::{
    ActionMeta, Child, Component, ComponentContext, ComponentRegistry, DispatchError, Never,
    Registry, SendError, Store, StoreOptions, lens, no_effects, root,
};
use common::*;

#[test]
fn closed_store_lane_returns_the_event_on_its_own_thread() {
    let mut f = fixture(Box::new(Inline), 4);
    f.store.drain_events(0).unwrap(); // this thread now owns the Store lane
    let (sink, a) = (f.sink.clone(), f.a);
    drop(f);
    let late = || DownloadEvent::Pushed {
        note: "late".into(),
    };
    assert_eq!(sink.try_send_to(&a, late()), Err(SendError::Closed(late())));
}

#[test]
fn closed_store_returns_the_event_on_another_thread() {
    let f = fixture(Box::new(Inline), 4);
    let (sink, a) = (f.sink.clone(), f.a);
    drop(f);
    let late = || DownloadEvent::Pushed {
        note: "late".into(),
    };
    let result = thread::spawn(move || sink.try_send_to(&a, late()))
        .join()
        .unwrap();
    assert_eq!(result, Err(SendError::Closed(late())));
}

#[test]
fn event_sink_rejects_a_foreign_protocol_mount() {
    let foreign = fixture(Box::new(Inline), 4);
    let mut local = fixture(Box::new(Inline), 4);
    let event = || DownloadEvent::Pushed {
        note: "foreign mount".into(),
    };

    let sent = local.sink.try_send_to(&foreign.a, event());

    assert_eq!(sent, Err(SendError::ForeignMount(event())));
    assert_eq!(local.store.drain_events(1), Ok(0));
    assert!(local.store.state().a.pushed.is_empty());
}

#[test]
fn a_store_refuses_a_bus_made_for_another_protocol() {
    let first = protocol();
    let second = protocol();
    let (_sink, inbox) = EventBus::new(&first, 4);
    let services = AppServices {
        downloads: Box::new(Inline),
    };
    let result = Store::new(
        second,
        AppState::default(),
        services,
        inbox,
        StoreOptions::default(),
    );
    assert!(matches!(result, Err(DispatchError::ForeignBus)));
}

#[test]
fn freeze_rejects_a_child_registered_but_never_bound() {
    struct Unbound;
    impl Component for Unbound {
        type State = AppState;
        type Action = Noop;
        type Output = Never;
        no_effects!();
        fn reduce(_: ComponentContext<'_, Self>, _: Noop, _: &ActionMeta) {}
        fn register(r: &mut ComponentRegistry<'_, Self>) {
            let slot = Child::<Unbound, Counter>::new("counter");
            // `SlotBinding` is `#[must_use]`; `let _` silences the warning, and
            // `freeze()` still has to catch the missing `.bind()`.
            let _ = r.child(slot, lens!(AppState => counter));
        }
    }
    let mut registry = Registry::<AppState, ()>::new();
    registry.root::<Unbound>();
    let error = registry.freeze().err().expect("must fail");
    assert!(
        error.0.contains(&causet::Problem::ChildNeverBound {
            path: "/counter".into()
        }),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains("/counter was registered but never bound")
    );
}

// A composite with no Effects whose child has some: `no_effects!(Service)`
// carries the child's Service through it.

struct Hub;

const DOWNLOADS: Child<Hub, Downloader> = Child::new("downloads");

#[derive(Default, Debug)]
struct HubState {
    downloads: DownloaderState,
}

struct HubServices {
    downloads: Box<dyn DownloadService>,
}

impl Component for Hub {
    type State = HubState;
    type Action = Noop;
    type Output = Never;
    no_effects!(HubServices);

    fn reduce(_: ComponentContext<'_, Self>, _: Noop, _: &ActionMeta) {}

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(DOWNLOADS, lens!(HubState => downloads))
            .service(lens!(HubServices => downloads))
            .bind();
    }
}

#[test]
fn no_effects_with_a_service_carries_a_childs_service() {
    let mut registry = Registry::<HubState, HubServices>::new();
    registry.root::<Hub>();
    let p = registry.freeze().expect("the child's Service is reachable");
    let (_sink, inbox) = EventBus::new(&p, 4);
    let services = HubServices {
        downloads: Box::new(Inline),
    };
    let mut store = Store::new(
        p.clone(),
        HubState::default(),
        services,
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    let downloads = p.resolve(&root::<Hub>().child(DOWNLOADS)).unwrap();

    store
        .dispatch_to(&downloads, DownloaderAction::Fetch("alpha".into()))
        .unwrap();
    store.drain_events(16).unwrap();

    assert_eq!(store.state().downloads.done, vec![("alpha".to_string(), 5)]);
}
