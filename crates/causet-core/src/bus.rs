//! Event routing, bounded worker ingress and held delivery.

use crate::{
    Adapts, Event, EventReadiness, ExecutionId, InstanceId, Mount, Payload, Protocol, ProtocolDefs,
    SessionId, readiness,
};
use std::any::TypeId;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::ThreadId;
#[cfg(not(target_family = "wasm"))]
use std::time::Duration;
use web_time::Instant;

// ---------------------------------------------------------------------------
// Event bus

/// An Event on its way to a mount.
pub struct Envelope {
    pub(super) session: SessionId,
    pub(super) instance: InstanceId,
    pub(super) event: TypeId,
    pub(super) kind: &'static str,
    pub(super) effect: Option<ExecutionId>,
    pub(super) payload: Payload,
}

/// Both lanes under one lock, so ordering, capacity and closing are decided
/// in one place.
pub(super) struct Lanes {
    /// The thread that last called the Store's `dispatch_to` or
    /// `drain_events`: its sends go to the Store lane. Moving the Store
    /// changes it only at the next such call.
    pub(super) owner: Option<ThreadId>,
    pub(super) store: VecDeque<Envelope>,
    pub(super) worker: VecDeque<Envelope>,
    pub(super) worker_capacity: usize,
    /// Set when the Store is dropped or faults: every later send fails, on
    /// any thread.
    pub(super) closed: bool,
    /// Sends refused for room since the last report to observers.
    pub(super) rejected_full: u64,
    /// `Some` on a held bus (`EventBus::held`): sends wait here, in send
    /// order, until a test releases or discards them.
    pub(super) held: Option<Held>,
    /// The host's one persistent driver wake target.
    pub(super) readiness: Option<readiness::Notifier>,
}

#[derive(Default)]
pub(super) struct Held {
    pub(super) next: u64,
    pub(super) events: Vec<HeldEntry>,
}

pub(super) struct HeldEntry {
    pub(super) id: HeldId,
    pub(super) lane: Lane,
    /// Sent with `send_front`: released to the head of its lane.
    pub(super) front: bool,
    pub(super) envelope: Envelope,
}

/// A held Event taken out for `HeldEvents::inspect`, put back in its place,
/// in id order, when dropped. On a bus closed meanwhile it is discarded
/// instead, after the lock is released, as `Bus::close` discards.
struct PutBack<'a> {
    bus: &'a Bus,
    entry: Option<HeldEntry>,
}

impl Drop for PutBack<'_> {
    fn drop(&mut self) {
        let Some(entry) = self.entry.take() else {
            return;
        };
        let discarded = {
            let mut lanes = self.bus.lock();
            if lanes.closed {
                Some(entry)
            } else if let Some(held) = lanes.held.as_mut() {
                let at = held.events.partition_point(|other| other.id < entry.id);
                held.events.insert(at, entry);
                None
            } else {
                Some(entry)
            }
        };
        drop(discarded);
    }
}

pub(super) struct Bus {
    pub(super) lanes: Mutex<Lanes>,
    /// Signalled when the worker lane gains room or the bus closes.
    pub(super) room: Condvar,
    /// Native drivers waiting for deliverable Events or closure.
    #[cfg(not(target_family = "wasm"))]
    pub(super) available: Condvar,
}

impl Bus {
    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, Lanes> {
        self.lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Store lane first, then the worker lane.
    pub(super) fn pop(&self) -> Option<Envelope> {
        let mut lanes = self.lock();
        if let Some(envelope) = lanes.store.pop_front() {
            return Some(envelope);
        }
        let envelope = lanes.worker.pop_front();
        if envelope.is_some() {
            self.room.notify_one();
        }
        envelope
    }

    /// Closes the bus for good: when its Store is dropped or faults. Queued
    /// and held Events are discarded, after the lock is released, so an
    /// Event whose drop sends finds the bus closed rather than deadlocking.
    pub(super) fn close(&self) {
        let (discarded, notify) = {
            let mut lanes = self.lock();
            lanes.closed = true;
            lanes.owner = None;
            let held = lanes
                .held
                .as_mut()
                .map(|held| std::mem::take(&mut held.events));
            let discarded = (
                std::mem::take(&mut lanes.store),
                std::mem::take(&mut lanes.worker),
                held,
            );
            (discarded, lanes.readiness.take())
        };
        self.room.notify_all();
        readiness::wake(self, notify);
        drop(discarded);
    }
}

/// Cloneable, `Send` sending side. Sends from the Store's own thread (Effect
/// handlers) go to the unbounded Store lane; sends from other threads go to
/// the bounded worker lane.
#[derive(Clone)]
pub struct EventSink {
    pub(super) session: SessionId,
    /// The Protocol this bus serves: it says which Mounts exist and which
    /// Events each of them accepts.
    pub(super) protocol: Arc<ProtocolDefs>,
    pub(super) bus: Arc<Bus>,
}

/// The receiving side, which the Store takes over. Dropping it, unused or
/// with its Store, closes the bus, and so does a fault of its Store: queued
/// Events are discarded, later sends fail with `Closed` on every thread, and
/// waiting senders wake.
pub struct EventInbox {
    pub(super) sink: EventSink,
}

impl EventInbox {
    /// A readiness handle the host can keep after giving this inbox to a
    /// Store. Waiting on it never drains input.
    pub fn readiness(&self) -> EventReadiness {
        EventReadiness {
            bus: Arc::clone(&self.sink.bus),
        }
    }
}

impl Drop for EventInbox {
    fn drop(&mut self) {
        self.sink.bus.close();
    }
}

/// Why an Event was not sent. Each variant hands the Event back.
#[derive(Debug, PartialEq, Eq)]
pub enum SendError<Ev> {
    /// The worker lane is at capacity: retry later, drop it, or report it.
    Full(Ev),
    /// The Store is gone or faulted (or its inbox was dropped).
    Closed(Ev),
    /// The Mount belongs to another Protocol than this bus.
    ForeignMount(Ev),
    /// The Mount's component never registered this Event type (`r.event`).
    Unregistered(Ev),
}

/// Makes the Event bus for one Store: `EventBus::new` returns the sending
/// side, which Services keep, and the inbox, which the Store takes.
pub struct EventBus;

impl EventBus {
    /// A bus for one Store of `protocol`, with a fresh session. Mounts of
    /// other Protocols are refused.
    #[allow(clippy::new_ret_no_self)]
    pub fn new<S, Svc>(
        protocol: &Arc<Protocol<S, Svc>>,
        worker_capacity: usize,
    ) -> (EventSink, EventInbox) {
        static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
        // Drawn from the top half, so sessions a test chooses never collide
        // with drawn ones.
        let session = SessionId((1 << 63) | NEXT_SESSION.fetch_add(1, Ordering::Relaxed));
        Self::build(protocol, worker_capacity, session, None)
    }

    /// A bus with the session given rather than a fresh one: two runs that
    /// give their Stores the same sessions make the same request IDs. Give
    /// a restarted Store, or any two Stores that exchange requests, sessions
    /// of their own.
    pub fn with_session<S, Svc>(
        protocol: &Arc<Protocol<S, Svc>>,
        worker_capacity: usize,
        session: SessionId,
    ) -> (EventSink, EventInbox) {
        Self::build(protocol, worker_capacity, session, None)
    }

    /// A held bus, for tests and simulations: every
    /// accepted send waits in a held set until the returned `HeldEvents`
    /// releases it into its lane or discards it. A held send never waits and
    /// never fails for room; a release into a full worker lane is refused.
    pub fn held<S, Svc>(
        protocol: &Arc<Protocol<S, Svc>>,
        worker_capacity: usize,
        session: SessionId,
    ) -> (EventSink, EventInbox, HeldEvents) {
        let (sink, inbox) = Self::build(protocol, worker_capacity, session, Some(Held::default()));
        let held = HeldEvents {
            bus: Arc::clone(&sink.bus),
            protocol: Arc::clone(&protocol.defs),
        };
        (sink, inbox, held)
    }

    fn build<S, Svc>(
        protocol: &Arc<Protocol<S, Svc>>,
        worker_capacity: usize,
        session: SessionId,
        held: Option<Held>,
    ) -> (EventSink, EventInbox) {
        let lanes = Lanes {
            owner: None,
            store: VecDeque::new(),
            worker: VecDeque::new(),
            worker_capacity,
            closed: false,
            rejected_full: 0,
            held,
            readiness: None,
        };
        let sink = EventSink {
            session,
            protocol: Arc::clone(&protocol.defs),
            bus: Arc::new(Bus {
                lanes: Mutex::new(lanes),
                room: Condvar::new(),
                #[cfg(not(target_family = "wasm"))]
                available: Condvar::new(),
            }),
        };
        (sink.clone(), EventInbox { sink })
    }
}

/// Where a send asks to go: the back or the head of its thread's lane, or the
/// worker lane whatever the thread.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Place {
    Back,
    Front,
    Worker,
}

/// A held Event's identity on its bus: increasing in send order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeldId(pub u64);

/// The lane an Event was sent to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    /// Sent from the Store's own thread (an Effect handler): drained first.
    Store,
    /// Sent from another thread: bounded.
    Worker,
}

/// A held Event, as a test sees it.
#[derive(Clone, Debug)]
pub struct HeldEvent {
    /// Its identity on the bus, to deliver or discard it by.
    pub id: HeldId,
    /// The destination mount's path.
    pub path: Arc<str>,
    /// The destination mount.
    pub instance: InstanceId,
    /// The Event's type name (`Message::TYPE`).
    pub event: &'static str,
    /// Its kind.
    pub kind: &'static str,
    /// The Effect it answers, if it is a reply.
    pub answers: Option<ExecutionId>,
    /// The session of the Store it was sent to.
    pub session: SessionId,
    /// The lane it was sent to, and goes to on release.
    pub lane: Lane,
    /// Sent to the head of its lane (`Reply::send_front`), where it goes on
    /// release.
    pub front: bool,
}

/// Why a held Event was not released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseError {
    /// No such Event is held: released or discarded already, or never sent.
    NotHeld,
    /// Its lane, the worker lane, is at capacity: drain first.
    Full,
    /// The Store is gone or faulted.
    Closed,
}

/// The test's handle on a held bus: list, inspect, release or discard what
/// is held. Cloneable; it never delivers anything itself: `Store::drain_events`
/// does, by the ordinary rules.
#[derive(Clone)]
pub struct HeldEvents {
    pub(super) bus: Arc<Bus>,
    pub(super) protocol: Arc<ProtocolDefs>,
}

impl HeldEvents {
    /// Every held Event, in send order.
    pub fn list(&self) -> Vec<HeldEvent> {
        let lanes = self.bus.lock();
        let Some(held) = &lanes.held else {
            return Vec::new();
        };
        held.events
            .iter()
            .map(|entry| {
                let e = &entry.envelope;
                let mount = self
                    .protocol
                    .mount_by_instance(e.instance)
                    .expect("a mount of this Protocol");
                HeldEvent {
                    id: entry.id,
                    path: Arc::clone(&self.protocol.mounts[mount].path),
                    instance: e.instance,
                    event: self.protocol.mounts[mount]
                        .events
                        .iter()
                        .find(|b| b.event == e.event)
                        .expect("a registered Event")
                        .name,
                    kind: e.kind,
                    answers: e.effect,
                    session: e.session,
                    lane: entry.lane,
                    front: entry.front,
                }
            })
            .collect()
    }

    /// How many Events are held.
    pub fn len(&self) -> usize {
        self.bus
            .lock()
            .held
            .as_ref()
            .map_or(0, |held| held.events.len())
    }

    /// Whether nothing is held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Calls `f` with the held Event `id`, if it is held and an `Ev`.
    ///
    /// `f` runs without the bus lock, so it may send on the bus, or release,
    /// discard and list other held Events. While it runs, the Event it is
    /// given is out of the held list: to `f`, and to any other thread, that
    /// Event is not held. It goes back to its place when `f` returns or
    /// panics, unless the bus has closed meanwhile, which discards it.
    pub fn inspect<Ev: Event, R>(&self, id: HeldId, f: impl FnOnce(&Ev) -> R) -> Option<R> {
        let entry = {
            let mut lanes = self.bus.lock();
            let held = lanes.held.as_mut()?;
            let at = held.events.iter().position(|entry| entry.id == id)?;
            if !held.events[at].envelope.payload.is::<Ev>() {
                return None;
            }
            held.events.remove(at)
        };
        let back = PutBack {
            bus: &self.bus,
            entry: Some(entry),
        };
        let event = back
            .entry
            .as_ref()
            .and_then(|entry| entry.envelope.payload.downcast_ref::<Ev>())
            .expect("checked under the lock");
        Some(f(event))
    }

    /// Moves the held Event `id` into its lane, at the head if it was sent
    /// with `send_front`, where the next `drain_events` delivers it. A
    /// worker-lane Event is refused while that lane is full, and stays held.
    pub fn release(&self, id: HeldId) -> Result<(), ReleaseError> {
        let mut lanes = self.bus.lock();
        if lanes.closed {
            return Err(ReleaseError::Closed);
        }
        let was_empty = readiness::availability(&lanes).is_none();
        let worker_full = lanes.worker.len() >= lanes.worker_capacity;
        let held = lanes.held.as_mut().ok_or(ReleaseError::NotHeld)?;
        let at = held
            .events
            .iter()
            .position(|entry| entry.id == id)
            .ok_or(ReleaseError::NotHeld)?;
        if held.events[at].lane == Lane::Worker && worker_full {
            return Err(ReleaseError::Full);
        }
        let entry = held.events.remove(at);
        match (entry.lane, entry.front) {
            (Lane::Store, true) => lanes.store.push_front(entry.envelope),
            (Lane::Store, false) => lanes.store.push_back(entry.envelope),
            (Lane::Worker, true) => lanes.worker.push_front(entry.envelope),
            (Lane::Worker, false) => lanes.worker.push_back(entry.envelope),
        }
        let notify = if was_empty {
            lanes.readiness.clone()
        } else {
            None
        };
        drop(lanes);
        if was_empty {
            readiness::wake(&self.bus, notify);
        }
        Ok(())
    }

    /// Drops the held Event `id`, as a lost message. False if it was not held.
    pub fn discard(&self, id: HeldId) -> bool {
        let discarded = {
            let mut lanes = self.bus.lock();
            let Some(held) = lanes.held.as_mut() else {
                return false;
            };
            let Some(at) = held.events.iter().position(|entry| entry.id == id) else {
                return false;
            };
            held.events.remove(at)
        };
        // Dropped after the lock is released, as in `Bus::close`: an Event
        // whose drop sends on this bus must not deadlock.
        drop(discarded);
        true
    }
}

impl EventSink {
    /// The session of the Store this bus belongs to.
    pub fn session(&self) -> SessionId {
        self.session
    }

    /// An input that answers no Effect, sent to a mount directly.
    /// `C: Adapts<Ev>` is checked by the compiler; the Mount's Protocol and the
    /// registered binding here, before anything is queued.
    pub fn try_send_to<C: Adapts<Ev>, Ev: Event>(
        &self,
        mount: &Mount<C>,
        event: Ev,
    ) -> Result<(), SendError<Ev>> {
        self.try_send_placed(mount, event, Place::Back)
    }

    /// `try_send_to` as from a worker thread, whatever thread it is on: the
    /// worker lane, failing with `Full` when it is at capacity, never
    /// waiting. For a simulated Service that stands in for a thread but runs
    /// on the Store's (a network model, say), so its Events keep a worker's
    /// priority and backpressure.
    pub fn try_send_to_as_worker<C: Adapts<Ev>, Ev: Event>(
        &self,
        mount: &Mount<C>,
        event: Ev,
    ) -> Result<(), SendError<Ev>> {
        self.try_send_placed(mount, event, Place::Worker)
    }

    fn try_send_placed<C: Adapts<Ev>, Ev: Event>(
        &self,
        mount: &Mount<C>,
        event: Ev,
        place: Place,
    ) -> Result<(), SendError<Ev>> {
        if mount.protocol != self.protocol.id {
            return Err(SendError::ForeignMount(event));
        }
        if !self.protocol.mounts[mount.index]
            .events
            .iter()
            .any(|b| b.event == TypeId::of::<Ev>())
        {
            return Err(SendError::Unregistered(event));
        }
        self.send(self.envelope(mount.instance, None, event), place, None)
    }

    /// A reply route to `mount`, as if its Effect `effect` had made it: for a
    /// test fixture that starts mid-workflow, so the Service work it sets up
    /// (a timer already armed, say) answers through an ordinary `Reply`.
    /// `None` for a Mount of another Protocol or an unregistered Event type.
    pub fn reply_for<C: Adapts<Ev>, Ev: Event>(
        &self,
        mount: &Mount<C>,
        effect: ExecutionId,
    ) -> Option<Reply<Ev>> {
        if mount.protocol != self.protocol.id
            || !self.protocol.mounts[mount.index]
                .events
                .iter()
                .any(|b| b.event == TypeId::of::<Ev>())
        {
            return None;
        }
        let route = ReplyRoute {
            session: self.session,
            instance: mount.instance,
            effect,
            _event: PhantomData,
        };
        Some(Reply {
            sink: self.clone(),
            route,
        })
    }

    /// `try_send_to`, but from a worker thread it waits up to `timeout` for
    /// room in the worker lane before failing with `Full`. Native targets
    /// only: WebAssembly hosts use `try_send_to_as_worker` and retry through
    /// their event loop instead of blocking it.
    #[cfg(not(target_family = "wasm"))]
    pub fn send_timeout_to<C: Adapts<Ev>, Ev: Event>(
        &self,
        mount: &Mount<C>,
        event: Ev,
        timeout: Duration,
    ) -> Result<(), SendError<Ev>> {
        if mount.protocol != self.protocol.id {
            return Err(SendError::ForeignMount(event));
        }
        if !self.protocol.mounts[mount.index]
            .events
            .iter()
            .any(|b| b.event == TypeId::of::<Ev>())
        {
            return Err(SendError::Unregistered(event));
        }
        self.send(
            self.envelope(mount.instance, None, event),
            Place::Back,
            Some(Instant::now() + timeout),
        )
    }

    fn envelope<Ev: Event>(
        &self,
        instance: InstanceId,
        effect: Option<ExecutionId>,
        event: Ev,
    ) -> Envelope {
        Envelope {
            session: self.session,
            instance,
            event: TypeId::of::<Ev>(),
            kind: event.kind(),
            effect,
            payload: Payload::new(event),
        }
    }

    /// Store-thread sends never wait and never fail for room. Worker sends wait
    /// for room until `deadline`, or fail at once without one. `Place::Front`
    /// puts the Event at the head of its thread's lane: the Store lane on the
    /// Store's thread, the worker lane (capacity permitting) on any other.
    fn send<Ev: Event>(
        &self,
        envelope: Envelope,
        place: Place,
        deadline: Option<Instant>,
    ) -> Result<(), SendError<Ev>> {
        let front = place == Place::Front;
        let back = |e: Envelope| e.payload.take::<Ev>().expect("Event type");
        let mut lanes = self.bus.lock();
        loop {
            if lanes.closed {
                return Err(SendError::Closed(back(envelope)));
            }
            let on_store_thread = lanes.owner == Some(thread::current().id());
            let lane = if on_store_thread && place != Place::Worker {
                Lane::Store
            } else {
                Lane::Worker
            };
            if let Some(held) = &mut lanes.held {
                let id = HeldId(held.next);
                held.next += 1;
                held.events.push(HeldEntry {
                    id,
                    lane,
                    front,
                    envelope,
                });
                return Ok(());
            }
            let was_empty = readiness::availability(&lanes).is_none();
            if lane == Lane::Store {
                if front {
                    lanes.store.push_front(envelope);
                } else {
                    lanes.store.push_back(envelope);
                }
                let notify = if was_empty {
                    lanes.readiness.clone()
                } else {
                    None
                };
                drop(lanes);
                if was_empty {
                    readiness::wake(&self.bus, notify);
                }
                return Ok(());
            }
            if lanes.worker.len() < lanes.worker_capacity {
                if front {
                    lanes.worker.push_front(envelope);
                } else {
                    lanes.worker.push_back(envelope);
                }
                let notify = if was_empty {
                    lanes.readiness.clone()
                } else {
                    None
                };
                drop(lanes);
                if was_empty {
                    readiness::wake(&self.bus, notify);
                }
                return Ok(());
            }
            let Some(deadline) = deadline else {
                lanes.rejected_full += 1;
                return Err(SendError::Full(back(envelope)));
            };
            let now = Instant::now();
            if now >= deadline {
                lanes.rejected_full += 1;
                return Err(SendError::Full(back(envelope)));
            }
            lanes = self
                .bus
                .room
                .wait_timeout(lanes, deadline - now)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
    }
}

/// The typed reply route, as data.
pub struct ReplyRoute<Ev> {
    /// The Store run that issued it.
    pub session: SessionId,
    /// The mount whose Effect made it, which receives the answer.
    pub instance: InstanceId,
    /// The Effect this answers, for the trace; not for request matching.
    pub effect: ExecutionId,
    pub(super) _event: PhantomData<fn() -> Ev>,
}

// Implemented by hand: deriving would require `Ev: Clone + Copy + Debug`.
impl<Ev> Clone for ReplyRoute<Ev> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<Ev> Copy for ReplyRoute<Ev> {}
impl<Ev> Debug for ReplyRoute<Ev> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyRoute")
            .field("session", &self.session)
            .field("instance", &self.instance)
            .field("effect", &self.effect)
            .finish()
    }
}

/// A live reply handle a Service can keep and send from any thread.
pub struct Reply<Ev> {
    pub(super) sink: EventSink,
    pub(super) route: ReplyRoute<Ev>,
}

impl<Ev> Clone for Reply<Ev> {
    fn clone(&self) -> Self {
        Self {
            sink: self.sink.clone(),
            route: self.route,
        }
    }
}

impl<Ev: Event> Reply<Ev> {
    /// Where this reply goes, as data.
    pub fn route(&self) -> ReplyRoute<Ev> {
        self.route
    }

    /// Sends the answer without waiting: on the Store's thread to the Store
    /// lane, which is never full; from any other thread to the worker lane,
    /// failing with `Full` when it is at capacity.
    pub fn try_send(&self, event: Ev) -> Result<(), SendError<Ev>> {
        let envelope = self
            .sink
            .envelope(self.route.instance, Some(self.route.effect), event);
        self.sink.send(envelope, Place::Back, None)
    }

    /// `try_send` as from a worker thread, whatever thread it is on: the
    /// worker lane, failing with `Full` when it is at capacity, never
    /// waiting. For a simulated Service that stands in for a thread but runs
    /// on the Store's, such as the simulated timer, so its answers keep a
    /// worker's priority and backpressure.
    pub fn try_send_as_worker(&self, event: Ev) -> Result<(), SendError<Ev>> {
        let envelope = self
            .sink
            .envelope(self.route.instance, Some(self.route.effect), event);
        self.sink.send(envelope, Place::Worker, None)
    }

    /// Sends the answer to the head of its lane, ahead of everything queued
    /// there, without waiting: on the Store's thread the head of the Store
    /// lane, which is never full; from any other thread the head of the
    /// worker lane, failing with `Full` when it is at capacity.
    pub fn send_front(&self, event: Ev) -> Result<(), SendError<Ev>> {
        let envelope = self
            .sink
            .envelope(self.route.instance, Some(self.route.effect), event);
        self.sink.send(envelope, Place::Front, None)
    }

    /// For worker threads: waits up to `timeout` for room in the worker lane,
    /// woken as soon as the Store drains one, or fails with `Full`. Native
    /// targets only; WebAssembly hosts retry `try_send_as_worker` from their
    /// event loop instead of blocking.
    #[cfg(not(target_family = "wasm"))]
    pub fn send_timeout(&self, event: Ev, timeout: Duration) -> Result<(), SendError<Ev>> {
        let envelope = self
            .sink
            .envelope(self.route.instance, Some(self.route.effect), event);
        self.sink
            .send(envelope, Place::Back, Some(Instant::now() + timeout))
    }
}
