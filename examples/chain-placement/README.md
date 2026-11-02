# Chain placement projection

An advanced projection example: derive where produced blocks landed from
reports that arrive late and out of order. This exercises the projection
and reporting model rather than the Action/Effect component lifecycle.

Start with [`src/record.rs`](src/record.rs) for the input records,
[`src/classify.rs`](src/classify.rs) for the pure classifier, then
[`src/projection.rs`](src/projection.rs) for incremental state and
its bounded context. The tests compare against recomputing from scratch,
check restarts and pin the edge cases generation found.

From the repository root:

```sh
cargo test -p chain-placement
```

Success is a passing test suite. Reports and chains are generated in
memory; no node or external log service is needed.
