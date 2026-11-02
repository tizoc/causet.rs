//! Synchronous, level-triggered Event readiness for a host's driver.

use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::sync::PoisonError;
#[cfg(not(target_family = "wasm"))]
use std::time::{Duration, Instant};

use crate::{Bus, Lanes};

/// What a host can currently deliver from a Store's Event bus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventAvailability {
    /// Deliverable Events exist. Checking readiness neither consumes nor reserves them.
    Events,
    /// The inbox or Store was dropped, or the Store faulted.
    Closed,
}

/// The bus already has a live driver notification registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadinessInUse;

impl fmt::Display for ReadinessInUse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the Event bus already has a readiness subscription")
    }
}

impl std::error::Error for ReadinessInUse {}

pub(super) type Notifier = Arc<dyn Fn() + Send + Sync>;

/// Cloneable readiness for one Event bus, available from its inbox or Store.
/// The host owns its loop; readiness never runs the Store. Held Events become
/// ready only when released. No executor or Future is involved.
#[derive(Clone)]
pub struct EventReadiness {
    pub(super) bus: Arc<Bus>,
}

impl EventReadiness {
    /// The current readiness, or `None` while open and empty.
    pub fn current(&self) -> Option<EventAvailability> {
        availability(&self.bus.lock())
    }

    /// Installs the bus's one persistent driver notification. It runs on the
    /// notifying thread, outside the queue lock, when an empty bus gains
    /// deliverable Events and when it closes. Registration also checks for
    /// existing readiness and notifies immediately. Bursts may coalesce.
    ///
    /// The callback must only signal the host's driver, which rechecks readiness
    /// before draining; it must not run the Store. Ordinary unwinding panics in
    /// notifications are ignored. Cancel or drop the returned subscription to
    /// unregister it; an already-issued notification may still run.
    /// A closed bus notifies immediately and retains no callback.
    ///
    /// ```
    /// # fn setup(ready: causet_core::EventReadiness) {
    /// let driver = std::thread::current();
    /// let _subscription = ready.subscribe(move || driver.unpark()).unwrap();
    /// // The host drains bounded batches and rechecks readiness before parking.
    /// # }
    /// ```
    pub fn subscribe(
        &self,
        notify: impl Fn() + Send + Sync + 'static,
    ) -> Result<ReadinessSubscription, ReadinessInUse> {
        let notify: Notifier = Arc::new(notify);
        let mut lanes = self.bus.lock();
        if lanes.readiness.is_some() {
            return Err(ReadinessInUse);
        }
        let current = availability(&lanes);
        let active = !lanes.closed;
        if active {
            lanes.readiness = Some(Arc::clone(&notify));
        }
        drop(lanes);
        if current.is_some() {
            call(Some(notify));
        }
        Ok(ReadinessSubscription {
            bus: active.then(|| Arc::clone(&self.bus)),
        })
    }

    /// Blocks a dedicated native driver until Events, closure or the timeout.
    /// Returns `None` on timeout. The predicate and sleep share the queue lock,
    /// so arrivals cannot be missed. Spurious wakeups are retried within the
    /// same timeout. It neither consumes input nor needs a subscription.
    /// Event-loop and WebAssembly hosts use `subscribe` instead.
    #[cfg(not(target_family = "wasm"))]
    pub fn wait_timeout(&self, timeout: Duration) -> Option<EventAvailability> {
        // The deadline is fixed once, here: `Condvar::wait_timeout_while`
        // reports a poisoned lock at its first wakeup, even a stale one, and
        // would end the wait early. A deadline past what `Instant` can hold
        // (`Duration::MAX`) waits for Events or closure alone.
        let deadline = Instant::now().checked_add(timeout);
        let mut lanes = self.bus.lock();
        loop {
            if let Some(ready) = availability(&lanes) {
                return Some(ready);
            }
            lanes = match deadline {
                None => self
                    .bus
                    .available
                    .wait(lanes)
                    .unwrap_or_else(PoisonError::into_inner),
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return None;
                    }
                    self.bus
                        .available
                        .wait_timeout(lanes, left)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
            };
        }
    }
}

/// The bus's driver registration. Cancel or drop it to release its callback.
#[must_use = "dropping the subscription unregisters the driver notification"]
pub struct ReadinessSubscription {
    bus: Option<Arc<Bus>>,
}

impl ReadinessSubscription {
    /// Unregisters this callback; idempotent. Queued Events and the Store are
    /// unaffected. An already-issued notification may still run.
    pub fn cancel(&mut self) {
        if let Some(bus) = self.bus.take() {
            let retired = bus.lock().readiness.take();
            // A callback's captures can run destructors: release them unlocked.
            drop(retired);
        }
    }
}

impl Drop for ReadinessSubscription {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl fmt::Debug for ReadinessSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadinessSubscription")
            .field("active", &self.bus.is_some())
            .finish()
    }
}

pub(super) fn availability(lanes: &Lanes) -> Option<EventAvailability> {
    if lanes.closed {
        Some(EventAvailability::Closed)
    } else if !lanes.store.is_empty() || !lanes.worker.is_empty() {
        Some(EventAvailability::Events)
    } else {
        None
    }
}

pub(super) fn wake(bus: &Bus, notify: Option<Notifier>) {
    #[cfg(not(target_family = "wasm"))]
    bus.available.notify_all();
    #[cfg(target_family = "wasm")]
    let _ = bus;
    call(notify);
}

fn call(notify: Option<Notifier>) {
    if let Some(notify) = notify {
        crate::ignoring_panics(|| {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| {
                notify();
                drop(notify);
            }));
        });
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use std::any::TypeId;
    use std::collections::VecDeque;
    use std::sync::{Condvar, Mutex};
    use std::thread;

    use super::*;
    use crate::{Envelope, InstanceId, Payload, SessionId};

    /// An open, empty bus whose queue lock a panic has poisoned.
    fn poisoned_bus() -> Arc<Bus> {
        let bus = Arc::new(Bus {
            lanes: Mutex::new(Lanes {
                owner: None,
                store: VecDeque::new(),
                worker: VecDeque::new(),
                worker_capacity: 1,
                closed: false,
                rejected_full: 0,
                held: None,
                readiness: None,
            }),
            room: Condvar::new(),
            available: Condvar::new(),
        });
        let poisoned = panic::catch_unwind(AssertUnwindSafe(|| {
            let _lanes = bus.lock();
            panic!("poisons the queue lock");
        }));
        assert!(poisoned.is_err());
        assert!(bus.lanes.is_poisoned());
        bus
    }

    /// Wakes the bus's waiters with nothing to deliver, as a stale or
    /// spurious wakeup does, every millisecond until `done` (five seconds at
    /// most).
    fn stale_wakeups_until(bus: &Bus, done: impl Fn() -> bool) {
        let give_up = Instant::now() + Duration::from_secs(5);
        while !done() && Instant::now() < give_up {
            bus.available.notify_all();
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn stale_wakeups_on_a_poisoned_lock_wait_on_until_an_event_arrives() {
        let bus = poisoned_bus();
        let ready = EventReadiness {
            bus: Arc::clone(&bus),
        };
        let waiter = thread::spawn(move || ready.wait_timeout(Duration::MAX));
        let started = Instant::now();
        stale_wakeups_until(&bus, || started.elapsed() >= Duration::from_millis(50));
        assert!(!waiter.is_finished(), "a stale wakeup ended the wait");

        // An Event arrives, queued and announced as a send does.
        bus.lock().worker.push_back(Envelope {
            session: SessionId(1),
            instance: InstanceId(1),
            event: TypeId::of::<()>(),
            kind: "test",
            effect: None,
            payload: Payload::new(()),
        });
        wake(&bus, None);
        assert_eq!(waiter.join().unwrap(), Some(EventAvailability::Events));
    }

    #[test]
    fn stale_wakeups_on_a_poisoned_lock_wait_on_until_the_timeout() {
        let bus = poisoned_bus();
        let ready = EventReadiness {
            bus: Arc::clone(&bus),
        };
        let timeout = Duration::from_millis(100);
        let waiter = thread::spawn(move || {
            let started = Instant::now();
            (ready.wait_timeout(timeout), started.elapsed())
        });
        stale_wakeups_until(&bus, || waiter.is_finished());
        let (result, waited) = waiter.join().unwrap();
        assert_eq!(result, None);
        assert!(
            waited >= timeout,
            "timed out after {waited:?}, before its {timeout:?}"
        );
    }
}
