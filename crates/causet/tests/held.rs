//! The held bus: every accepted send waits until the test releases or
//! discards it; a held send never fails for room, a release into a full
//! worker lane does; closing the Store drops what is held.

use std::fmt;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use causet::guide::shop::*;
use causet::hosting::EventBus;
use causet::hosting::{HeldEvents, HeldId, Lane, ReleaseError};
use causet::*;

struct Held {
    store: Store<ShopState, ShopServices>,
    sink: EventSink,
    held: HeldEvents,
    payments: Mount<Payments>,
}

fn held(worker_capacity: usize) -> Held {
    let p = protocol();
    let (sink, inbox, held) = EventBus::held(&p, worker_capacity, SessionId(9));
    let services = ShopServices {
        gateway: Box::new(ApproveUpTo(100)),
    };
    let options = StoreOptions::with_clock(ManualClock::new(1_000));
    let store = Store::new(p.clone(), ShopState::default(), services, inbox, options).unwrap();
    Held {
        store,
        sink,
        held,
        payments: p.resolve(&root::<ShopRoot>().child(PAYMENTS)).unwrap(),
    }
}

/// A gateway's answer sent from another thread, which takes the worker lane.
fn from_a_worker(f: &Held, index: u32) {
    let (sink, payments) = (f.sink.clone(), f.payments);
    let request = RequestId {
        session: SessionId(9),
        execution: ExecutionId(1),
        index,
    };
    thread::spawn(move || {
        sink.try_send_to(
            &payments,
            Charged {
                request,
                approved: true,
            },
        )
        .unwrap()
    })
    .join()
    .unwrap();
}

#[test]
fn held_events_wait_and_are_delivered_in_the_order_released() {
    let mut f = held(4);
    f.store
        .dispatch_to(&f.payments, PaymentsAction::Charge { amount: 1 })
        .unwrap();
    f.store
        .dispatch_to(&f.payments, PaymentsAction::Charge { amount: 2 })
        .unwrap();

    // Both answers are held, in send order, with where and what they answer.
    let list = f.held.list();
    assert_eq!(list.len(), 2);
    assert!(list[0].id < list[1].id);
    for event in &list {
        assert_eq!(
            (event.kind, event.path.as_ref(), event.lane),
            ("payments.charged", "/payments", Lane::Store)
        );
        assert_eq!(event.session, SessionId(9));
        assert!(event.answers.is_some());
    }
    assert_eq!(
        f.store.drain_events(16),
        Ok(0),
        "nothing reaches a lane while held"
    );

    // Released in the opposite order, they are delivered in that order.
    f.held.release(list[1].id).unwrap();
    f.held.release(list[0].id).unwrap();
    assert_eq!(f.held.release(list[0].id), Err(ReleaseError::NotHeld));
    assert_eq!(f.store.drain_events(16), Ok(2));
    assert_eq!(f.store.state().payments.paid, [2, 1]);
}

#[test]
fn a_held_send_never_fails_for_room_but_a_release_into_a_full_lane_does() {
    let mut f = held(1);
    from_a_worker(&f, 0);
    from_a_worker(&f, 1); // past the lane's capacity, still accepted
    let list = f.held.list();
    assert_eq!(
        list.iter().map(|e| e.lane).collect::<Vec<_>>(),
        [Lane::Worker, Lane::Worker]
    );

    f.held.release(list[0].id).unwrap();
    assert_eq!(f.held.release(list[1].id), Err(ReleaseError::Full));
    assert_eq!(f.held.len(), 1, "a refused release stays held");
    assert_eq!(f.store.drain_events(1), Ok(1));
    f.held.release(list[1].id).unwrap();
    assert_eq!(f.store.drain_events(16), Ok(1));
}

#[test]
fn a_discarded_answer_is_lost_and_inspect_reads_without_releasing() {
    let mut f = held(4);
    f.store
        .dispatch_to(&f.payments, PaymentsAction::Charge { amount: 500 })
        .unwrap();
    let [answer] = f.held.list().try_into().unwrap();
    assert_eq!(
        f.held.inspect::<Charged, _>(answer.id, |c| c.approved),
        Some(false)
    );
    assert_eq!(
        f.held.inspect::<TimerFired, _>(answer.id, |_| ()),
        None,
        "another Event type"
    );
    assert_eq!(f.held.len(), 1);

    assert!(f.held.discard(answer.id));
    assert!(!f.held.discard(answer.id));
    assert_eq!(f.store.drain_events(16), Ok(0));
    assert_eq!(
        f.store.state().payments.pending.len(),
        1,
        "the request waits for an answer that never comes"
    );
}

#[test]
fn closing_the_store_drops_what_is_held() {
    let f = held(4);
    from_a_worker(&f, 0);
    let [answer] = f.held.list().try_into().unwrap();
    let Held {
        store,
        sink,
        held,
        payments,
    } = f;
    drop(store);
    assert!(held.is_empty());
    assert_eq!(held.release(answer.id), Err(ReleaseError::Closed));
    let request = RequestId {
        session: SessionId(9),
        execution: ExecutionId(1),
        index: 1,
    };
    assert!(matches!(
        sink.try_send_to(
            &payments,
            Charged {
                request,
                approved: true
            }
        ),
        Err(SendError::Closed(_))
    ));
}

/// An Event whose drop sends another Event on its own bus.
#[derive(Event)]
#[causet(type = "held.resend")]
struct Resend {
    on_drop: Option<(EventSink, Mount<Resender>)>,
}

impl fmt::Debug for Resend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Resend")
    }
}

impl Drop for Resend {
    fn drop(&mut self) {
        if let Some((sink, mount)) = self.on_drop.take() {
            sink.try_send_to(&mount, Resend { on_drop: None }).unwrap();
        }
    }
}

struct Resender;

#[derive(Debug, Action)]
#[causet(type = "held.resender.action")]
struct Resent;

impl Component for Resender {
    type State = ();
    type Action = Resent;
    type Output = Never;
    no_effects!();

    fn reduce(_: ComponentContext<'_, Self>, _: Resent, _: &ActionMeta) {}

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Resend>();
    }
}

impl Adapts<Resend> for Resender {
    fn adapt(_: Resend, _: &mut ActionPort<'_, Self>) {}
}

#[test]
fn discarding_an_event_whose_drop_sends_on_the_same_bus_returns() {
    let mut registry = Registry::new();
    registry.root::<Resender>();
    let p = registry.freeze().unwrap();
    let mount = p.resolve(&root::<Resender>()).unwrap();
    let (sink, inbox, held) = EventBus::held(&p, 1, SessionId(9));
    let on_drop = Some((sink.clone(), mount));
    sink.try_send_to(&mount, Resend { on_drop }).unwrap();
    let [resend] = held.list().try_into().unwrap();

    // Discarded on another thread, so that a deadlock fails the test instead
    // of hanging it. The inbox goes with it: dropping the inbox takes the bus
    // lock, which this thread's unwinding must not wait for.
    let (done, discarded) = mpsc::channel();
    let discarding = held.clone();
    thread::spawn(move || {
        let found = discarding.discard(resend.id);
        done.send((found, inbox)).unwrap();
    });
    let (found, inbox) = discarded
        .recv_timeout(Duration::from_secs(5))
        .expect("discard returns");
    assert!(found);

    // The drop's send was accepted, and is held like any other.
    let [resent] = held.list().try_into().unwrap();
    assert!(resent.id > resend.id);
    assert_eq!(
        held.inspect::<Resend, _>(resent.id, |r| r.on_drop.is_none()),
        Some(true)
    );
    drop(inbox);
}

/// `inspect` runs its closure without the bus lock: the closure may send on
/// the bus, list what is held and release another Event. The Event it reads
/// is out of the held list meanwhile, and back in its place afterwards.
#[test]
fn an_inspect_closure_may_use_the_bus() {
    let mut registry = Registry::new();
    registry.root::<Resender>();
    let p = registry.freeze().unwrap();
    let mount = p.resolve(&root::<Resender>()).unwrap();
    let (sink, inbox, held) = EventBus::held(&p, 4, SessionId(9));
    sink.try_send_to(&mount, Resend { on_drop: None }).unwrap();
    sink.try_send_to(&mount, Resend { on_drop: None }).unwrap();
    let [first, second] = held.list().try_into().unwrap();

    // On another thread, so that a deadlock fails the test instead of
    // hanging it; the inbox goes with it, as in the discard test above.
    let (done, inspected) = mpsc::channel();
    let inspecting = held.clone();
    thread::spawn(move || {
        let seen = inspecting.inspect::<Resend, _>(first.id, |_| {
            sink.try_send_to(&mount, Resend { on_drop: None }).unwrap();
            let ids: Vec<HeldId> = inspecting.list().iter().map(|h| h.id).collect();
            let released = inspecting.release(second.id);
            (ids, released)
        });
        done.send((seen, inbox)).unwrap();
    });
    let (seen, inbox) = inspected
        .recv_timeout(Duration::from_secs(5))
        .expect("inspect returns");
    let (during, released) = seen.expect("the Event was held");
    assert_eq!(during.len(), 2, "the inspected Event is out meanwhile");
    assert!(!during.contains(&first.id));
    assert_eq!(released, Ok(()));
    let after: Vec<HeldId> = held.list().iter().map(|h| h.id).collect();
    assert_eq!(after.len(), 2);
    assert_eq!(
        after[0], first.id,
        "back in its place, before the later send"
    );
    drop(inbox);
}

#[test]
fn an_inspect_closure_that_panics_puts_the_event_back() {
    let mut registry = Registry::new();
    registry.root::<Resender>();
    let p = registry.freeze().unwrap();
    let mount = p.resolve(&root::<Resender>()).unwrap();
    let (sink, _inbox, held) = EventBus::held(&p, 4, SessionId(9));
    sink.try_send_to(&mount, Resend { on_drop: None }).unwrap();
    sink.try_send_to(&mount, Resend { on_drop: None }).unwrap();
    let before: Vec<HeldId> = held.list().iter().map(|h| h.id).collect();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        held.inspect::<Resend, ()>(before[0], |_| panic!("the closure fails"))
    }));
    assert!(panicked.is_err());
    let after: Vec<HeldId> = held.list().iter().map(|h| h.id).collect();
    assert_eq!(after, before);
}
