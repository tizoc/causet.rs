//! Regressions from the third independent review: the reviewer's
//! reproductions, with exact expected outcomes.

mod common;

use causet::*;
use common::*;

#[test]
fn external_unregistered_events_are_rejected_before_enqueueing() {
    let mut f = fixture(Box::new(Inline), 4);
    // Downloader implements Adapts<Ping>, but never registers Ping.
    let sent = f.sink.try_send_to(&f.a, Ping);
    let drained = f.store.drain_events(1);
    assert!(
        matches!(sent, Err(SendError::Unregistered(Ping))),
        "{sent:?}"
    );
    assert_eq!(drained, Ok(0), "nothing was queued");
}
