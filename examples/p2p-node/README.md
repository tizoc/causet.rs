# P2P node

A host for the [reusable P2P component](../p2p/README.md), with fetching and
serving consumers. It demonstrates parent-owned admission policy, request
routes and reply tokens without putting application policy into P2P.

Read [`src/lib.rs`](src/lib.rs) for the root and its child binding,
then the contract tests and generated simulation in `tests/`.
The simulation is split into [support/network.rs](tests/support/network.rs),
[support/checks.rs](tests/support/checks.rs), and [generated.rs](tests/generated.rs).

From the repository root:

```sh
cargo test -p p2p-node --test contracts
cargo test -p p2p-node
```

The full suite generates two to four nodes, changes policy, loses and
delays replies, disconnects and reconnects peers, and checks identities
and eventual outcomes. Success is a passing test suite; no server remains
running. The transport and server Services are simulated.
