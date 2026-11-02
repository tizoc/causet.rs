# Runtime internals

`lib.rs` is the public facade. Private modules follow the runtime boundaries:

| Module | Responsibility |
| --- | --- |
| `ids`, `messages`, `metadata` | Protocol data and execution identity. |
| `components` | Component contracts, child slots and lenses. |
| `execution` | A single execution's FIFO and counters. |
| `context` | Reducer capabilities and Action-only adapter ports. |
| `harness` | A component reducer without a mounted Store. |
| `effects` | Effect capabilities and panic-boundary helpers. |
| `bus`, `readiness` | Event routing, backpressure, held delivery and wakeups. |
| `protocol` | Erased bindings, registration and frozen mount paths. |
| `store` | Scheduling, fault boundaries, invariants and Store ownership. |
| `clock`, `timer`, `pending` | Time sources, timer Service and request tables. |
| `observe`, `bug`, `payload` | Observation contracts, bug reports and erased messages. |

Public names remain re-exported from the crate root. Implementation details
shared by modules are visible only inside the crate; splitting the source
does not add packages or new runtime layers. Start at `Store::dispatch_to`
or `Store::drain_events`, follow the scheduler in `store`, and then the
capabilities it gives a handler. Framework integration tests live under
`crates/causet/tests` and exercise the facade applications use.
