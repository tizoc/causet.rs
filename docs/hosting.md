# Hosting a Store

A Store runs synchronously on its caller's thread. The host owns the driver:
when to drain Events, process RPC and control inputs, and check deadlines.

## Moving to the driver thread

`Store<S, Svc>` is `Send` when `S` and `Svc` are `Send`, including types that
are not `Sync`. Construct it on the host thread and move it into a dedicated
driver, or construct it inside the driver. A Store with non-`Send` State or
Services stays local. A Store is not `Sync`; every dispatch and drain needs
exclusive access, and each root input runs to completion.

The Store lane belongs to the thread that last called `dispatch_to` or
`drain_events`: each call records its thread before it runs any input. An
Effect's synchronous replies therefore keep their Store-lane priority even
when the Store has moved since its previous input.

Moving a Store does not move its lane. Until the Store's first `dispatch_to`
or `drain_events` on the driver thread, sends from the thread it last ran on
still go to the Store lane: unbounded, past the worker capacity, and delivered
ahead of queued worker Events. A Store that has not run yet has no Store lane,
so every send is a worker send. A host that moves a Store after running it
should call `drain_events` once on the driver thread before other threads
send, or have those threads send as workers (`try_send_to_as_worker`,
`try_send_as_worker`).

## Waiting for Events

`inbox.readiness()` returns a cloneable handle before the Store takes the
inbox; `store.readiness()` obtains the same readiness afterwards. The handle
can outlive the Store.

Readiness is synchronous. `ready.current()` returns
`Some(EventAvailability::Events)`, `Some(EventAvailability::Closed)`, or
`None` while open and empty. It never reserves or drains input.

`ready.subscribe(notify)` installs one persistent wake target for the
Store's driver. A second active registration returns `ReadinessInUse`.
Registration checks readiness under the queue lock too, notifying immediately
if input already waits or the bus is closed. An open bus retains the
callback until its subscription is cancelled/dropped or the bus closes.
A closed bus notifies immediately and retains no callback.

Both lanes notify when an empty bus gains deliverable Events. Bursts may
coalesce; held sends notify only on release, and closure notifies too.
The callback runs outside the queue lock, on the notifying thread. It must
only signal the host's driver (unpark its thread or schedule an event-loop
turn), never run the Store inline. Ordinary unwinding panics in the callback
are isolated from input delivery. Dropping callback captures happens outside
the lock too. An already-issued notification may run after cancellation.

On a dedicated native thread, `ready.wait_timeout(duration)` blocks on a
condition variable until Events, closure or the timeout (`None`). Checking
and sleeping use the same queue lock and tolerate spurious wakeups without
extending the timeout. It needs no notification registration. No Future,
executor or async syntax is involved. Event-loop hosts use notifications.

After a bounded drain, check readiness again before sleeping. A partially
drained queue remains ready and does not need another notification.

Keep fairness in the host: drain a bounded number, return to other ready
inputs, and then wait again. A host that also limits elapsed drain time can
call `drain_events(1)` in its own bounded loop. Waiting adds no scheduler
execution and changes no ordering or admission rules.

## Host-driven timers and WebAssembly

Native applications can use `Timer::new()` for a background timer thread.
On any target, `Timer::driven(clock)` returns a Timer Service and its
`TimerControl`. The component keeps calling the same `after` method; the host
uses `control.next_deadline()` to arrange its next wakeup and
`control.fire_due()` to publish the due Events. Recheck the deadline after
each root input, which may have armed an earlier timer. Drive the control on
the Store's driver thread. Full worker lanes retain the remaining timers in
deadline order; drain Events and fire again when there is room.

`Timer::simulated(&manual_clock)` is the same mechanism on a shared manual
clock. Neither driven nor simulated timers spawn a thread, and dropping the
Timer cancels its pending alarms even if a control handle survives it.

For `wasm32-unknown-unknown`, `SystemClock` uses `web-time`'s browser clocks
instead of the unsupported standard-library clock reads. This requires a
wasm-bindgen host with `Date` and `performance` (a browser window/worker, or
Node). The host can use its event loop for readiness and timer deadlines.
Blocking waits (`wait_timeout`), sends (`send_timeout`, `send_timeout_to`) and the background timer
constructor/default are native-only; WebAssembly callers use the
nonblocking sends and host-driven timers.

An event-loop callback may run on the Store's thread even though it stands
for an external worker. Use `try_send_to_as_worker` or `try_send_as_worker`
for such input to preserve worker priority and bounded capacity. A `Full`
result returns the Event so the host can retry through its own event loop.

WebAssembly panics use the target's panic strategy: on the ordinary aborting
target, a panic traps instead of becoming a caught Store fault. With unwinding
enabled, native targets retain the ordinary Store-fault contract.

## Validation

CI runs the native workspace tests, doctests, formatting and Clippy on Rust
1.90, compiles the library for iOS simulator and Android, and executes
`crates/causet/tests/web.rs` as WebAssembly under Node 24. To run that lane
locally, install `wasm32-unknown-unknown` and the wasm-bindgen CLI version
matching the lockfile (currently 0.2.129), then run:

```sh
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  cargo test --release --locked -p causet --target wasm32-unknown-unknown --test web
```
