# Reusable P2P component

A component generic over its request and response messages. It owns
connection generations, wire request identities, pending requests and
timeouts. Its host owns application policy and chooses how its Outputs
affect the rest of the application.

Start with [`src/lib.rs`](src/lib.rs): protocol types, State, admission,
reduction, transport Effects, and invariants. Then read the
[P2P node](../p2p-node/README.md) that mounts it.

Run the host contract tests from the repository root:

```sh
cargo test -p p2p-node --test contracts
```

The tests exercise refusals, reconnects and stale transport callbacks.
Transport is a Service supplied by the application; the example tests use
a simulated network and need no real sockets.
