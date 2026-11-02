//! Incremental equals from scratch on generated logs: each id's newest value
//! by event time, with records arriving late and out of order. Kept by event
//! time, it passes; overwritten in arrival order, the check finds it and
//! shrinks it to its story: two records of one id, the newer arriving first.

use std::collections::BTreeMap;

use causet_projection::{Keyed, LogEntry, Mismatch, Projection, check_incremental};
use causet_test::{Config, Rng, search};

#[derive(Clone, Debug)]
struct Rec {
    time: u64,
    id: u64,
    value: u64,
}

impl LogEntry for Rec {
    fn time_ms(&self) -> u64 {
        self.time
    }
}

/// An id's newest value, by event time.
#[derive(Clone, Debug, PartialEq)]
struct Newest {
    key: String,
    time: u64,
    value: u64,
}

impl Keyed for Newest {
    fn key(&self) -> &str {
        &self.key
    }
}

fn key(id: u64) -> String {
    format!("id {id}")
}

/// Each id's newest record; `by_event_time` false overwrites with every
/// record, in arrival order: the mistake to find.
struct Latest {
    by_event_time: bool,
    newest: BTreeMap<u64, (u64, u64)>,
}

impl Latest {
    fn new(by_event_time: bool) -> Self {
        Self {
            by_event_time,
            newest: BTreeMap::new(),
        }
    }
}

impl Projection<Rec> for Latest {
    type Output = Newest;

    fn name(&self) -> &str {
        "latest"
    }

    fn process(&mut self, record: &Rec, out: &mut Vec<Newest>) {
        let older = self
            .newest
            .get(&record.id)
            .is_some_and(|&(time, _)| time > record.time);
        if self.by_event_time && older {
            return;
        }
        self.newest.insert(record.id, (record.time, record.value));
        out.push(Newest {
            key: key(record.id),
            time: record.time,
            value: record.value,
        });
    }
}

/// Every record, kept: the definition, recomputed from scratch.
#[derive(Default)]
struct History(Vec<Rec>);

impl Projection<Rec> for History {
    type Output = Newest;

    fn name(&self) -> &str {
        "history"
    }

    fn process(&mut self, record: &Rec, _: &mut Vec<Newest>) {
        self.0.push(record.clone());
    }
}

impl History {
    /// The newest record of the key's id; of two at one time, the later
    /// to arrive.
    fn newest(&self, key: &str) -> Option<Newest> {
        let record = self
            .0
            .iter()
            .enumerate()
            .filter(|(_, r)| self::key(r.id) == key)
            .max_by_key(|&(arrived, r)| (r.time, arrived))?
            .1;
        Some(Newest {
            key: key.to_owned(),
            time: record.time,
            value: record.value,
        })
    }
}

/// Records of a few ids as time goes on, each arriving after a delay: the
/// log is in arrival order.
fn generate(rng: &mut Rng) -> Vec<Rec> {
    let (mut time, mut arrivals) = (0, Vec::new());
    while rng.more() {
        time += rng.below(10);
        let record = Rec {
            time,
            id: rng.below(3),
            value: rng.below(100),
        };
        let arrival = time + rng.below(30);
        arrivals.push((arrival, arrivals.len(), record));
    }
    arrivals.sort_by_key(|&(arrival, step, _)| (arrival, step));
    arrivals.into_iter().map(|(_, _, record)| record).collect()
}

fn check(by_event_time: bool, log: &[Rec]) -> Result<(), Mismatch<Newest>> {
    check_incremental(
        Latest::new(by_event_time),
        History::default(),
        log,
        |_, history, key| history.newest(key),
    )
}

fn quick() -> Config {
    Config {
        runs: 300,
        ..Config::default()
    }
}

#[test]
fn a_projection_kept_by_event_time_equals_its_recomputation() {
    causet_test::check(quick(), |rng| {
        let log = generate(rng);
        check(true, &log).unwrap();
    });
}

#[test]
fn a_projection_overwritten_in_arrival_order_is_found_and_shrunk() {
    let found = search(quick(), |rng| {
        let log = generate(rng);
        if let Err(mismatch) = check(false, &log) {
            panic!("{mismatch:?}");
        }
    })
    .expect("arrival order loses the newest value");
    let mut log = Vec::new();
    causet_test::replay(&found.choices, |rng| log = generate(rng));
    assert_eq!(log.len(), 2, "{found:?}: {log:?}");
    assert!(
        log[0].time > log[1].time,
        "the newer arrives first: {log:?}"
    );
}

#[test]
fn a_mismatch_names_the_record_the_key_and_both_outputs() {
    let log = [
        Rec {
            time: 5,
            id: 1,
            value: 50,
        },
        Rec {
            time: 2,
            id: 1,
            value: 20,
        },
    ];
    let mismatch = check(false, &log).unwrap_err();
    assert_eq!(mismatch.record, 1);
    assert_eq!(mismatch.key, "id 1");
    assert_eq!(mismatch.stored.value, 20);
    assert_eq!(mismatch.recomputed.value, 50);
    assert_eq!(
        format!("{mismatch:?}"),
        "after record 1, id 1 is stored as Newest { key: \"id 1\", time: 2, value: 20 }, \
         but every record so far gives Newest { key: \"id 1\", time: 5, value: 50 }"
    );
}

#[test]
fn a_key_no_longer_maintained_is_skipped() {
    let log = [
        Rec {
            time: 5,
            id: 1,
            value: 50,
        },
        Rec {
            time: 2,
            id: 1,
            value: 20,
        },
    ];
    let skip_all = check_incremental(Latest::new(false), History::default(), &log, |_, _, _| None);
    assert_eq!(skip_all, Ok(()));
}
