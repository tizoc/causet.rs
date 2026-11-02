//! The timer Service. A handler asks for a `TimerFired` after a delay; it
//! arrives through the bus like any reply. Timers keep their own monotonic
//! clock, as Services may: reducers only ever see the Event. A host-driven
//! timer (`Timer::driven`) works without threads on any target. A simulated
//! timer (`Timer::simulated`) uses the same mechanism on a `ManualClock`.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fmt;
#[cfg(not(target_family = "wasm"))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(target_family = "wasm"))]
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Mutex, PoisonError};
#[cfg(not(target_family = "wasm"))]
use std::thread::{self, JoinHandle};
use std::time::Duration;
#[cfg(not(target_family = "wasm"))]
use std::time::Instant;

use crate::{Clock, Event, LogLevel, ManualClock, Message, Reply, RequestId, SendError, Timestamp};

/// A timer's deadline passed. `request` is whatever the reducer pinned it to;
/// a reply for a request no longer pending is stale and ignored, which is how
/// a timeout is cancelled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerFired {
    /// The request the timer was set for.
    pub request: RequestId,
}

impl Message for TimerFired {
    const TYPE: &'static str = "causet.timer_fired";
    fn kind(&self) -> &'static str {
        Self::TYPE
    }
    fn log_level(&self) -> LogLevel {
        LogLevel::Trace
    }
}
impl Event for TimerFired {}

struct Pending {
    reply: Reply<TimerFired>,
    request: RequestId,
}

#[cfg(not(target_family = "wasm"))]
enum Command {
    After(Instant, Pending),
    Stop,
}

/// The timer Service: a native background thread, or host-driven deadlines
/// on an injected clock. Host-driven timers also work on WebAssembly.
/// Dropping it drops timers that have not fired (and stops a native thread).
pub struct Timer {
    inner: Inner,
}

enum Inner {
    #[cfg(not(target_family = "wasm"))]
    Threaded {
        commands: Sender<Command>,
        /// Set on drop, so a timer waiting for room on a full lane gives up.
        stopping: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    },
    Driven(Arc<Mutex<Driven>>),
}

impl Timer {
    /// Starts the timer thread. Available on native targets; WebAssembly
    /// hosts use `Timer::driven` with their own event loop.
    #[cfg(not(target_family = "wasm"))]
    pub fn new() -> Self {
        let (commands, inbox) = mpsc::channel();
        let stopping = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&stopping);
        let thread = thread::Builder::new()
            .name("causet-timer".into())
            .spawn(move || run(inbox, &stop))
            .expect("spawn the timer thread");
        Self {
            inner: Inner::Threaded {
                commands,
                stopping,
                thread: Some(thread),
            },
        }
    }

    /// A timer on `clock`, which the Stores of a simulation share, and its
    /// control: timers fire only when the control fires them, once the clock
    /// has reached their deadline. Services use the `Timer` as before.
    pub fn simulated(clock: &ManualClock) -> (Self, TimerControl) {
        Self::driven(clock.clone())
    }

    /// A timer on `clock`, driven by its host instead of a background
    /// thread. The host reads `TimerControl::next_deadline`, arranges its
    /// own wakeup, and calls `TimerControl::fire_due` when due. Call it on
    /// the driver thread and recheck the deadline after each root input,
    /// which may have scheduled an earlier timer.
    pub fn driven(clock: impl Clock + 'static) -> (Self, TimerControl) {
        let shared = Arc::new(Mutex::new(Driven {
            clock: Box::new(clock),
            timers: Timers::default(),
        }));
        (
            Self {
                inner: Inner::Driven(Arc::clone(&shared)),
            },
            TimerControl { shared },
        )
    }

    /// Sends `TimerFired { request }` through `reply` once `delay` has
    /// passed. Timers due at the same instant fire in the order they were set.
    pub fn after(&self, delay: Duration, reply: Reply<TimerFired>, request: RequestId) {
        match &self.inner {
            #[cfg(not(target_family = "wasm"))]
            Inner::Threaded { commands, .. } => {
                let at = Instant::now() + delay;
                // The thread outlives every `after` call: it stops only on drop.
                let _ = commands.send(Command::After(at, Pending { reply, request }));
            }
            Inner::Driven(shared) => {
                let mut sim = shared.lock().unwrap_or_else(PoisonError::into_inner);
                let nanos = Timestamp::try_from(delay.as_nanos()).unwrap_or(Timestamp::MAX);
                let deadline = sim.clock.now().saturating_add(nanos);
                sim.timers.push(deadline, Pending { reply, request });
            }
        }
    }
}

struct Driven {
    clock: Box<dyn Clock>,
    timers: Timers<Timestamp>,
}

/// A simulated or host-driven timer's control. It never starts a thread.
#[derive(Clone)]
pub struct TimerControl {
    shared: Arc<Mutex<Driven>>,
}

impl TimerControl {
    /// The earliest deadline set and not yet fired, on the clock's scale.
    pub fn next_deadline(&self) -> Option<Timestamp> {
        let sim = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
        sim.timers.queue.peek().map(|Reverse((at, _))| *at)
    }

    /// How many timers are set and not yet fired.
    pub fn pending(&self) -> usize {
        self.shared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .timers
            .pending
            .len()
    }

    /// Fires every timer whose deadline the clock has reached, in deadline
    /// order and first set first at equal deadlines, by sending its
    /// `TimerFired` through its `Reply` into the worker lane, as the threaded
    /// timer does from its thread, whichever thread calls this. A timer whose
    /// lane is full stays set, and so do the ones after it. Returns how many
    /// fired.
    pub fn fire_due(&self) -> usize {
        let mut fired = 0;
        loop {
            let due = {
                let mut sim = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
                let now = sim.clock.now();
                match sim.timers.queue.peek() {
                    Some(&Reverse((at, seq))) if at <= now => {
                        sim.timers.queue.pop();
                        Some((
                            at,
                            seq,
                            sim.timers.pending.remove(&seq).expect("a queued timer"),
                        ))
                    }
                    _ => None,
                }
            };
            let Some((at, seq, timer)) = due else {
                return fired;
            };
            // As the threaded timer does, from its own thread.
            match timer.reply.try_send_as_worker(TimerFired {
                request: timer.request,
            }) {
                Err(SendError::Full(_)) => {
                    let mut sim = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
                    sim.timers.queue.push(Reverse((at, seq)));
                    sim.timers.pending.insert(seq, timer);
                    return fired;
                }
                // Sent, or the Store it answers is gone.
                _ => fired += 1,
            }
        }
    }
}

impl fmt::Debug for TimerControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimerControl")
            .field("pending", &self.pending())
            .finish()
    }
}

#[cfg(not(target_family = "wasm"))]
impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Timer")
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        match &mut self.inner {
            #[cfg(not(target_family = "wasm"))]
            Inner::Threaded {
                commands,
                stopping,
                thread,
            } => {
                stopping.store(true, Ordering::Relaxed);
                let _ = commands.send(Command::Stop);
                if let Some(thread) = thread.take() {
                    let _ = thread.join();
                }
            }
            // A control kept by the test fires nothing more.
            Inner::Driven(shared) => {
                let mut sim = shared.lock().unwrap_or_else(PoisonError::into_inner);
                sim.timers.queue.clear();
                sim.timers.pending.clear();
            }
        }
    }
}

#[cfg(not(target_family = "wasm"))]
fn run(inbox: Receiver<Command>, stopping: &AtomicBool) {
    let mut timers = Timers::<Instant>::default();
    loop {
        let command = match timers.queue.peek() {
            None => inbox.recv().map_err(|_| RecvTimeoutError::Disconnected),
            Some(Reverse((at, _))) => {
                inbox.recv_timeout(at.saturating_duration_since(Instant::now()))
            }
        };
        match command {
            Ok(Command::After(at, timer)) => timers.push(at, timer),
            Ok(Command::Stop) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {}
        }
        // Every timer set so far is queued before the next due one is chosen,
        // and again after each firing, which can wait on a full lane: a timer
        // set meanwhile with an earlier deadline fires first.
        loop {
            if !timers.take_waiting(&inbox) {
                return;
            }
            match timers.queue.peek() {
                Some(&Reverse((at, seq))) if at <= Instant::now() => {
                    timers.queue.pop();
                    let timer = timers.pending.remove(&seq).expect("a queued timer");
                    fire(timer, stopping);
                }
                _ => break,
            }
        }
    }
}

/// Deadlines in order: an `Instant` on the thread, a clock reading when
/// simulated. The sequence number keeps equal deadlines first in, first out.
struct Timers<T> {
    queue: BinaryHeap<Reverse<(T, u64)>>,
    pending: HashMap<u64, Pending>,
    next_seq: u64,
}

impl<T: Ord> Default for Timers<T> {
    fn default() -> Self {
        Self {
            queue: BinaryHeap::new(),
            pending: HashMap::new(),
            next_seq: 0,
        }
    }
}

impl<T: Ord> Timers<T> {
    fn push(&mut self, at: T, timer: Pending) {
        self.queue.push(Reverse((at, self.next_seq)));
        self.pending.insert(self.next_seq, timer);
        self.next_seq += 1;
    }
}

#[cfg(not(target_family = "wasm"))]
impl Timers<Instant> {
    /// Queues the commands already sent. False once the Timer is stopping.
    fn take_waiting(&mut self, inbox: &Receiver<Command>) -> bool {
        loop {
            match inbox.try_recv() {
                Ok(Command::After(at, timer)) => self.push(at, timer),
                Ok(Command::Stop) | Err(TryRecvError::Disconnected) => return false,
                Err(TryRecvError::Empty) => return true,
            }
        }
    }
}

/// Waits for room on a full worker lane, which holds back the timers due
/// after it, until the Timer drops. A closed bus means the Store is gone or faulted.
#[cfg(not(target_family = "wasm"))]
fn fire(timer: Pending, stopping: &AtomicBool) {
    let mut event = TimerFired {
        request: timer.request,
    };
    while !stopping.load(Ordering::Relaxed) {
        match timer.reply.send_timeout(event, Duration::from_millis(10)) {
            Ok(())
            | Err(SendError::Closed(_) | SendError::ForeignMount(_) | SendError::Unregistered(_)) =>
            {
                return;
            }
            Err(SendError::Full(back)) => event = back,
        }
    }
}
