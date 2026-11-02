//! `InFlight`: a simulated world's messages in flight.

use std::collections::BTreeMap;

use causet_test::{Config, InFlight, Rng, check};

#[test]
fn in_flight_delivers_by_time_then_by_sending() {
    let mut flight: InFlight<&str> = InFlight::new();
    assert_eq!(flight.next_time(), None);
    flight.send_at(20, "c");
    flight.send_at(10, "a");
    flight.send_at(10, "b");
    flight.send_at(30, "d");
    assert_eq!(flight.next_time(), Some(10));
    assert_eq!(flight.due(9), Vec::<&str>::new());
    assert_eq!(flight.due(20), ["a", "b", "c"]);
    assert_eq!(flight.len(), 1);
    assert_eq!(flight.due(u64::MAX), ["d"]);
    assert!(flight.is_empty());
}

#[test]
fn in_flight_keeps_a_channel_in_order_whatever_the_delays() {
    let mut flight: InFlight<&str, u8> = InFlight::new();
    assert_eq!(flight.send_in_order(1, 50, "first"), 50);
    // Drawn earlier, it still comes after the channel's last message.
    assert_eq!(flight.send_in_order(1, 10, "second"), 51);
    // Another channel, and a message on none, are not held back.
    assert_eq!(flight.send_in_order(2, 10, "other"), 10);
    flight.send_at(20, "unordered");
    assert_eq!(flight.due(51), ["other", "unordered", "first", "second"]);
}

/// Regression: after a message due at the end of time, the next on
/// its channel overflowed `last + 1` (a panic, or in release a message
/// delivered before the one sent ahead of it).
#[test]
fn a_channel_at_the_end_of_time_keeps_its_order() {
    let mut flight: InFlight<&str, u8> = InFlight::new();
    assert_eq!(flight.send_in_order(1, u64::MAX, "first"), u64::MAX);
    assert_eq!(flight.send_in_order(1, 0, "second"), u64::MAX);
    assert_eq!(flight.send_in_order(1, 7, "third"), u64::MAX);
    assert_eq!(flight.due(u64::MAX), ["first", "second", "third"]);
}

/// Regression: a channel's first message was due one after the time
/// asked, as if a message had gone before it at zero.
#[test]
fn a_channel_s_first_message_is_due_when_asked() {
    let mut flight: InFlight<&str, u8> = InFlight::new();
    assert_eq!(flight.send_in_order(1, 0, "first"), 0);
    assert_eq!(flight.send_in_order(1, 0, "second"), 1);
    assert_eq!(flight.send_in_order(2, 0, "other"), 0);
    assert_eq!(flight.due(0), ["first", "other"]);
}

#[test]
fn in_flight_shows_what_it_holds_in_order() {
    let mut flight: InFlight<&str> = InFlight::new();
    flight.send_at(5, "b");
    flight.send_at(3, "a");
    let held: Vec<_> = flight.iter().collect();
    assert_eq!(held, [(3, &"a"), (5, &"b")]);
    assert!(format!("{flight:?}").contains("\"a\""));
}

/// Generated sends on three channels and on none, and deliveries, at times
/// drawn near both ends of time and between. Checked: each delivery is
/// every message due by then, in order of when it is due and then of
/// sending; a message off a channel is due when asked; on a channel, never
/// before it was asked, its first when asked, one asked after the last
/// message's time when asked, and in the order sent.
fn generated(rng: &mut Rng) {
    let time = |rng: &mut Rng| match rng.below(4) {
        0 => rng.below(4),
        1 => u64::MAX - rng.below(4),
        _ => rng.below(1_000),
    };
    let mut flight: InFlight<usize, u64> = InFlight::new();
    // By message: when it is due.
    let mut due_at: Vec<u64> = Vec::new();
    let mut last: BTreeMap<u64, u64> = BTreeMap::new();
    let mut pending: Vec<usize> = Vec::new();
    while rng.more() {
        let at = time(rng);
        match rng.below(3) {
            0 => {
                flight.send_at(at, due_at.len());
                due_at.push(at);
            }
            1 => {
                let channel = rng.below(3);
                let due = flight.send_in_order(channel, at, due_at.len());
                assert!(due >= at, "due at {due}, asked for {at}");
                match last.get(&channel) {
                    None => assert_eq!(due, at, "a channel's first message"),
                    Some(&previous) => {
                        assert!(due >= previous, "due at {due} before {previous}");
                        if at > previous {
                            assert_eq!(due, at, "asked after the last, at {previous}");
                        }
                    }
                }
                last.insert(channel, due);
                due_at.push(due);
            }
            _ => {
                let mut expected: Vec<usize> = pending
                    .iter()
                    .copied()
                    .filter(|m| due_at[*m] <= at)
                    .collect();
                expected.sort_by_key(|m| (due_at[*m], *m));
                assert_eq!(flight.due(at), expected, "due at {at}");
                pending.retain(|m| due_at[*m] > at);
                continue;
            }
        }
        pending.push(due_at.len() - 1);
        assert_eq!(flight.next_time(), pending.iter().map(|m| due_at[*m]).min());
    }
    let mut rest = pending;
    rest.sort_by_key(|m| (due_at[*m], *m));
    assert_eq!(flight.due(u64::MAX), rest);
    assert!(flight.is_empty());
}

#[test]
fn generated_in_flight_delivers_in_order() {
    check(Config::default(), generated);
}
