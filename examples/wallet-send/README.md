# Wallet send on a light node

A larger reference application combining bounded input discovery,
snapshot-pinned requests, retries, cancellation, proving and hydration.
It is useful after the shop and P2P examples.

Read in this order:

1. [`src/state.rs`](src/state.rs): the send lifecycle and its transition helpers.
2. [`src/wallet_send.rs`](src/wallet_send.rs): the component and its ordered emissions.
3. [`src/node.rs`](src/node.rs): context, child integration and I/O ownership.
4. `tests/`: focused transitions, deadlines, probe lifecycle, then the
   simulated world and generated scenarios.

From the repository root:

```sh
cargo test -p wallet-send --test transitions
cargo test -p wallet-send
```

The generated scenarios run a light node and up to three full peers,
including faulty, late and restarting peers. Ledger data and Services are
modelled for the example; it does not connect to a blockchain or run a real
prover. Success is a passing test suite.
