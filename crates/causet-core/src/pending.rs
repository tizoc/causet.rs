//! Requests a component waits on: what it needs to handle each answer, and
//! when it stops waiting.

use std::collections::BTreeMap;

use crate::{RequestId, Timestamp};

/// The requests a component waits on, kept in its State: for each, what it
/// needs to handle the answer, and an optional deadline.
///
/// The first of the answer and the deadline to arrive takes the request
/// ([`take`](Pending::take)); whatever comes after finds nothing and is
/// stale, which is how a reducer tells a late answer, a duplicate, or a
/// timer for a request already answered from the one it waits for.
///
/// Deadlines are kept two ways:
///
/// - one timer per request, pinned to its ID: the timer's Event calls
///   `take` like an answer does, and whichever comes second is stale;
/// - one timer for the earliest deadline ([`next_deadline`](Pending::next_deadline)):
///   when it fires, [`expire`](Pending::expire) takes every request whose
///   deadline has come, and the next timer is armed for what is left.
///
/// Requests are kept in ID order, which is the order they were made in a
/// Store's run.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(bound(
        serialize = "T: serde::Serialize",
        deserialize = "T: serde::Deserialize<'de>"
    ))
)]
pub struct Pending<T> {
    #[cfg_attr(feature = "serde", serde(with = "entries"))]
    entries: BTreeMap<RequestId, Entry<T>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Entry<T> {
    value: T,
    deadline: Option<Timestamp>,
}

impl<T> Default for Pending<T> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
}

impl<T> Pending<T> {
    /// No request waited on.
    pub fn new() -> Self {
        Self::default()
    }

    /// Waits on `request`, with `value`, until its answer is taken. A request
    /// already waited on is replaced.
    pub fn insert(&mut self, request: RequestId, value: T) {
        self.entries.insert(
            request,
            Entry {
                value,
                deadline: None,
            },
        );
    }

    /// Waits on `request`, with `value`, until its answer is taken or
    /// `deadline` comes.
    pub fn insert_until(&mut self, request: RequestId, value: T, deadline: Timestamp) {
        self.entries.insert(
            request,
            Entry {
                value,
                deadline: Some(deadline),
            },
        );
    }

    /// An answer, or a timer pinned to `request`, arrived: the request's
    /// value, now no longer waited on. `None` when it is stale: taken
    /// already, expired, cancelled, or never waited on here.
    ///
    /// It does not compare a timer's firing time with the deadline: a
    /// threaded timer keeps its own clock, and a skew between that clock and
    /// the Store's must not drop a real timeout.
    pub fn take(&mut self, request: RequestId) -> Option<T> {
        self.entries.remove(&request).map(|entry| entry.value)
    }

    /// Stops waiting on `request` without an answer; its value, if it was
    /// waited on. A later answer is stale.
    pub fn cancel(&mut self, request: RequestId) -> Option<T> {
        self.take(request)
    }

    /// Stops waiting on every request `keep` rejects, such as every request
    /// of a piece of work that has ended; later answers to them are stale.
    pub fn retain(&mut self, mut keep: impl FnMut(RequestId, &T) -> bool) {
        self.entries
            .retain(|&request, entry| keep(request, &entry.value));
    }

    /// Stops waiting on every request, such as when a new one replaces the
    /// one in flight: `clear`, then [`insert`](Pending::insert) the new
    /// request, and the old answer is stale.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Takes every request whose deadline has come by `now`, in deadline
    /// order, then request order.
    pub fn expire(&mut self, now: Timestamp) -> Vec<(RequestId, T)> {
        let mut due: Vec<(Timestamp, RequestId)> = self
            .entries
            .iter()
            .filter_map(|(&request, entry)| {
                entry
                    .deadline
                    .filter(|&deadline| deadline <= now)
                    .map(|deadline| (deadline, request))
            })
            .collect();
        due.sort();
        due.into_iter()
            .filter_map(|(_, request)| self.take(request).map(|value| (request, value)))
            .collect()
    }

    /// The earliest deadline of the requests waited on, for one timer armed
    /// at a time.
    pub fn next_deadline(&self) -> Option<Timestamp> {
        self.entries
            .values()
            .filter_map(|entry| entry.deadline)
            .min()
    }

    /// The deadline of `request`, if it is waited on and has one.
    pub fn deadline(&self, request: RequestId) -> Option<Timestamp> {
        self.entries.get(&request).and_then(|entry| entry.deadline)
    }

    /// Whether `request` is waited on.
    pub fn contains(&self, request: RequestId) -> bool {
        self.entries.contains_key(&request)
    }

    /// The value of `request`, if it is waited on.
    pub fn get(&self, request: RequestId) -> Option<&T> {
        self.entries.get(&request).map(|entry| &entry.value)
    }

    /// The value of `request`, to change in place, if it is waited on.
    pub fn get_mut(&mut self, request: RequestId) -> Option<&mut T> {
        self.entries.get_mut(&request).map(|entry| &mut entry.value)
    }

    /// How many requests are waited on.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether none is.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The values of the requests waited on, in request order.
    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.entries.values().map(|entry| &entry.value)
    }

    /// The requests waited on and their values, in request order.
    pub fn iter(&self) -> impl Iterator<Item = (RequestId, &T)> {
        self.entries
            .iter()
            .map(|(&request, entry)| (request, &entry.value))
    }
}

/// The entries as a list of pairs: JSON and formats like it allow only
/// string keys.
#[cfg(feature = "serde")]
mod entries {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::Entry;
    use crate::RequestId;

    pub(super) fn serialize<T: Serialize, S: Serializer>(
        entries: &BTreeMap<RequestId, Entry<T>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(entries.iter())
    }

    pub(super) fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<RequestId, Entry<T>>, D::Error> {
        Ok(Vec::<(RequestId, Entry<T>)>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::Pending;
    use crate::{ExecutionId, RequestId, SessionId};

    fn request(index: u32) -> RequestId {
        RequestId {
            session: SessionId(1),
            execution: ExecutionId(1),
            index,
        }
    }

    #[test]
    fn the_first_to_arrive_takes_the_request_and_the_second_is_stale() {
        let mut pending = Pending::new();
        pending.insert(request(0), "charge");
        assert_eq!(pending.take(request(0)), Some("charge"));
        // The timer for it, or a duplicate answer.
        assert_eq!(pending.take(request(0)), None);
        assert_eq!(pending.take(request(9)), None, "never waited on here");
    }

    #[test]
    fn expire_takes_what_is_due_in_deadline_order() {
        let mut pending = Pending::new();
        pending.insert_until(request(0), "late", 300);
        pending.insert_until(request(1), "first", 100);
        pending.insert(request(2), "no deadline");
        pending.insert_until(request(3), "also first", 100);
        assert_eq!(pending.next_deadline(), Some(100));
        assert_eq!(pending.expire(99), []);
        assert_eq!(
            pending.expire(200),
            [(request(1), "first"), (request(3), "also first")]
        );
        assert_eq!(pending.next_deadline(), Some(300));
        assert_eq!(pending.len(), 2);
        assert_eq!(
            pending.take(request(1)),
            None,
            "an answer after its deadline is stale"
        );
    }

    #[test]
    fn a_cancelled_request_is_stale_and_its_value_can_change_while_it_waits() {
        let mut pending = Pending::new();
        pending.insert_until(request(0), 1, 50);
        *pending.get_mut(request(0)).unwrap() += 1;
        assert_eq!(pending.get(request(0)), Some(&2));
        assert_eq!(pending.deadline(request(0)), Some(50));
        pending.insert(request(1), 7);
        pending.retain(|_, &value| value != 7);
        assert_eq!(
            pending.take(request(1)),
            None,
            "a request retain dropped is stale"
        );
        assert_eq!(pending.cancel(request(0)), Some(2));
        assert!(pending.is_empty());
        assert_eq!(pending.next_deadline(), None);
    }

    #[test]
    fn a_new_request_replaces_the_one_in_flight() {
        let mut pending = Pending::new();
        pending.insert_until(request(0), "first search", 100);
        pending.clear();
        pending.insert(request(1), "second search");
        assert_eq!(pending.take(request(0)), None, "the old answer is stale");
        assert_eq!(pending.next_deadline(), None);
        assert_eq!(pending.take(request(1)), Some("second search"));
    }
}
