//! Review round 4: the inbox closes its bus when dropped, unused or rejected.

mod common;

use causet::hosting::EventBus;
use causet::*;
use common::*;

fn pushed() -> DownloadEvent {
    DownloadEvent::Pushed {
        note: "late".into(),
    }
}

#[test]
fn dropping_an_unused_inbox_closes_its_bus() {
    let p = protocol();
    let a = p.resolve(&root::<App>().child(A)).unwrap();
    let (sink, inbox) = EventBus::new(&p, 1);
    drop(inbox);
    assert_eq!(
        sink.try_send_to(&a, pushed()),
        Err(SendError::Closed(pushed()))
    );
}

#[test]
fn rejecting_a_foreign_bus_closes_its_inbox() {
    let first = protocol();
    let second = protocol();
    let a = first.resolve(&root::<App>().child(A)).unwrap();
    let (sink, inbox) = EventBus::new(&first, 1);
    let services = AppServices {
        downloads: Box::new(Inline),
    };
    assert!(matches!(
        Store::new(
            second,
            AppState::default(),
            services,
            inbox,
            StoreOptions::default()
        ),
        Err(DispatchError::ForeignBus)
    ));
    assert_eq!(
        sink.try_send_to(&a, pushed()),
        Err(SendError::Closed(pushed()))
    );
}
