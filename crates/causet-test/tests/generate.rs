//! Generated runs and shrinking: the stream of choices, failure keys, the
//! limits, and what `check` reports.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};

use causet::*;
use causet_test::{Config, Node, Rng, Swarm, check, replay, search};

// ---------------------------------------------------------------------------
// A number that two invariants watch, and that can fault or report a bug.

#[derive(Debug, Action)]
#[causet(type = "number.action", kind = "number.set")]
struct Set(u64);

struct Number;

impl Component for Number {
    type State = u64;
    type Action = Set;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, Set(n): Set, _: &ActionMeta) {
        if n == 14 {
            panic!("fourteen");
        }
        if n == 8 {
            bug_condition!(n = n; "eight is unlucky");
        }
        *ctx.state_mut() = n;
    }
}

fn number() -> Node<u64, ()> {
    Node::new::<Number>("number", 0, (), SessionId(1), &ManualClock::new(1_000))
        .check("high", |&n| {
            if n >= 50 {
                Err(format!("{n} is high"))
            } else {
                Ok(())
            }
        })
        .check("odd", |&n| {
            if n % 2 == 1 {
                Err(format!("{n} is odd"))
            } else {
                Ok(())
            }
        })
}

fn set(node: &mut Node<u64, ()>, n: u64) {
    let mount = node.mount(&root::<Number>());
    node.dispatch(&mount, Set(n));
}

fn quick() -> Config {
    Config {
        runs: 200,
        ..Config::default()
    }
}

// ---------------------------------------------------------------------------
// The stream of choices

#[test]
fn a_replayed_list_is_clamped_to_each_bound_then_zero() {
    let mut drawn = Vec::new();
    replay(&[7, 99, 3], |rng| {
        drawn.extend([
            rng.below(10),
            rng.below(5),
            rng.below(1),
            rng.below(4),
            rng.below(4),
        ])
    });
    // 99 clamps to 4; a draw from one value is no choice; past the end, 0.
    assert_eq!(drawn, [7, 4, 0, 3, 0]);
}

#[test]
fn a_choice_of_zero_is_the_simplest_answer() {
    replay(&[], |rng| {
        assert!(!rng.more());
        assert!(!rng.chance(99, 100));
        assert_eq!(*rng.pick(&["first", "second"]), "first");
        assert_eq!(rng.between(5, 9), 5);
    });
}

#[test]
fn a_split_takes_one_choice_and_records_nothing() {
    replay(&[5, 2], |rng| {
        let mut world = rng.split();
        let drawn: Vec<u64> = (0..3).map(|_| world.below(1_000)).collect();
        assert_eq!(rng.below(10), 2, "the world's draws are not the run's");
        assert_eq!(rng.choices().unwrap(), [5, 2]);
        assert_eq!(world.choices(), None);
        // The world is the seed's: replaying the choice replays it.
        let mut same = Rng::new(5);
        let again: Vec<u64> = (0..3).map(|_| same.below(1_000)).collect();
        assert_eq!(drawn, again);
    });
}

#[test]
fn the_world_does_not_shift_the_steps() {
    let steps = |world_draws: usize| {
        let mut rng = Rng::new(9);
        let mut world = rng.split();
        let mut steps = Vec::new();
        for _ in 0..5 {
            steps.push(rng.below(100));
            for _ in 0..world_draws {
                world.below(100);
            }
        }
        steps
    };
    assert_eq!(steps(0), steps(7));
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Send,
    Deliver,
    Restart,
}

const KINDS: [(Kind, u64); 3] = [(Kind::Send, 2), (Kind::Deliver, 1), (Kind::Restart, 1)];

#[test]
fn a_swarm_of_zero_choices_has_every_kind_and_picks_the_first() {
    replay(&[], |rng| {
        let swarm = Swarm::new(rng, &KINDS);
        assert_eq!(
            swarm.kinds().collect::<Vec<_>>(),
            [Kind::Send, Kind::Deliver, Kind::Restart]
        );
        assert_eq!(swarm.pick(rng), Kind::Send);
    });
}

#[test]
fn a_swarm_picks_only_the_kinds_it_drew() {
    replay(&[1, 0, 1, 7, 3], |rng| {
        let swarm = Swarm::new(rng, &KINDS);
        assert_eq!(swarm.kinds().collect::<Vec<_>>(), [Kind::Deliver]);
        assert_eq!(swarm.pick(rng), Kind::Deliver);
        assert_eq!(swarm.pick(rng), Kind::Deliver);
    });
}

#[test]
fn a_swarm_with_every_kind_off_has_them_all() {
    replay(&[1, 1, 1], |rng| {
        let swarm = Swarm::new(rng, &KINDS);
        assert_eq!(swarm.kinds().count(), 3);
    });
}

#[test]
fn a_swarm_picks_by_weight() {
    replay(&[], |rng| {
        let swarm = Swarm::new(rng, &KINDS);
        let mut seeded = Rng::new(4);
        let sends = (0..4_000)
            .filter(|_| swarm.pick(&mut seeded) == Kind::Send)
            .count();
        assert!((1_800..2_200).contains(&sends), "{sends} of 4,000");
    });
}

/// Regression: weights whose total overflows a `u64` are refused
/// when the swarm is made, not wrapped or panicked on when it picks.
#[test]
#[should_panic(expected = "a swarm's weights must total at most u64::MAX")]
fn a_swarm_refuses_weights_that_overflow() {
    replay(&[], |rng| {
        Swarm::new(rng, &[(0u8, 1u64 << 63), (1u8, 1u64 << 63)]);
    });
}

/// Regression: the largest total that fits reaches its last kind.
#[test]
fn a_swarm_of_the_largest_total_picks_its_last_kind() {
    replay(&[0, 0, u64::MAX - 1], |rng| {
        let swarm = Swarm::new(rng, &[(0u8, u64::MAX - 1), (1u8, 1)]);
        assert_eq!(swarm.pick(rng), 1);
    });
}

#[test]
fn clones_draw_from_one_stream() {
    let mut a = Rng::new(3);
    let mut b = a.clone();
    let (first, second) = (a.next_u64(), b.next_u64());
    let mut fresh = Rng::new(3);
    assert_eq!((first, second), (fresh.next_u64(), fresh.next_u64()));
}

/// The same seed draws the same stream in every port, a split generator's
/// included: these numbers are the OCaml and TypeScript ports' too. Each
/// row: a seed, its first three `next_u64`, the seed its `split` draws, that
/// child's two `next_u64`, `below(10)` and `below(1000)`, and the parent's
/// next `next_u64` after the split.
#[test]
fn a_seed_draws_the_same_stream_in_every_port() {
    #[allow(clippy::type_complexity)]
    let rows: [(u64, [u64; 3], u64, [u64; 4], u64); 5] = [
        (
            0,
            [
                184_964_832_154_816,
                6_929_580_258_060_647,
                1_228_259_715_532_165,
            ],
            3_299_258_355_452_336,
            [1_556_177_596_879_896, 3_079_948_183_007_717, 8, 526],
            7_187_964_147_299_700,
        ),
        (
            1,
            [
                2_865_243_701_272_905,
                3_251_949_076_935_262,
                5_527_171_857_800_482,
            ],
            429_432_007_478_425,
            [4_151_588_442_903_439, 1_578_455_540_222_732, 3, 225],
            7_693_114_567_407_942,
        ),
        (
            42,
            [
                6_529_064_058_451_075,
                4_471_935_826_588_234,
                5_180_173_260_398_988,
            ],
            8_129_784_920_598_100,
            [6_074_493_261_542_752, 4_401_473_988_561_057, 8, 573],
            7_978_443_526_906_943,
        ),
        (
            3_735_928_559,
            [
                8_929_200_111_742_449,
                6_872_159_228_071_956,
                8_932_636_711_779_373,
            ],
            1_915_845_255_195_443,
            [8_611_885_385_275_428, 5_148_353_159_110_923, 9, 640],
            7_467_217_096_916_731,
        ),
        (
            9_007_199_254_740_991,
            [
                7_124_478_592_440_004,
                1_111_579_265_331_998,
                1_373_957_957_192_600,
            ],
            4_568_600_538_254_545,
            [4_916_215_589_202_146, 5_472_395_411_161_787, 4, 847],
            7_729_296_003_959_000,
        ),
    ];
    for (seed, nexts, split_seed, child, after) in rows {
        let mut rng = Rng::new(seed);
        assert_eq!(nexts, [(); 3].map(|()| rng.next_u64()), "seed {seed}");
        let mut world = rng.split();
        assert_eq!(world.seed(), Some(split_seed), "seed {seed}");
        let drawn = [
            world.next_u64(),
            world.next_u64(),
            world.below(10),
            world.below(1000),
        ];
        assert_eq!(drawn, child, "seed {seed}");
        assert_eq!(rng.next_u64(), after, "seed {seed}");
    }
}

// ---------------------------------------------------------------------------
// Shrinking keeps to one failure

#[test]
fn shrinking_does_not_trade_one_invariant_for_another() {
    // One draw, as the property draws it, picks the first failure: a seed
    // whose first run sets a high number. Shrinking lowers it, and 49 would
    // still fail, but as "odd": the key keeps it at the smallest high one.
    let seed = (0..).find(|&s| Rng::new(s).below(100) >= 50).unwrap();
    let found = search(Config { seed, ..quick() }, |rng| {
        set(&mut number(), rng.below(100))
    })
    .unwrap();
    assert_eq!(
        (found.seed, found.key.as_str()),
        (seed, "node number: invariant \"high\"")
    );
    assert_eq!(found.choices, [50]);
}

#[test]
fn a_fault_is_keyed_by_where_the_code_panicked() {
    let found = search(quick(), |rng| {
        set(&mut number(), 2 * rng.below(10) + 14 * rng.below(2))
    })
    .unwrap();
    assert!(
        found.key.starts_with("node number: fault at ") && found.key.contains("generate.rs:"),
        "{}",
        found.key
    );
    // The run sets 14 either as 2 * 7 + 0 or as 2 * 0 + 14. From [7, 0] no
    // single edit reaches the smaller [0, 1]; the two-choice pass does.
    assert_eq!(found.choices, [0, 1], "{found:?}");
}

// A handler that catches the panic of its dispatch, then panics with the
// same message elsewhere: the fault is the first panic.

#[derive(Debug, Action)]
#[causet(type = "catcher.action")]
enum Catch {
    Start,
    Boom,
}

#[derive(Debug, Effect)]
#[causet(type = "catcher.effect", kind = "catcher.run")]
struct Run;

struct Catcher;

fn boom() -> ! {
    panic!("boom")
}
const BOOM_LINE: u32 = line!() - 2;

impl Component for Catcher {
    type State = ();
    type Service = ();
    type Action = Catch;
    type Effect = Run;
    type Output = Never;

    fn reduce(ctx: ComponentContext<'_, Self>, action: Catch, _: &ActionMeta) {
        match action {
            Catch::Start => ctx.into_emitter().emit_effect(Run),
            Catch::Boom => boom(),
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, Run: Run, _: &EffectMeta) {
        let caught =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| ctx.dispatch(Catch::Boom)));
        assert!(caught.is_err());
        panic!("boom");
    }
}

#[test]
fn a_fault_a_handler_caught_is_keyed_by_the_first_panic() {
    let found = search(quick(), |_| {
        let mut node = Node::new::<Catcher>("catcher", (), (), SessionId(1), &ManualClock::new(1));
        let mount = node.mount(&root::<Catcher>());
        node.dispatch(&mount, Catch::Start);
    })
    .unwrap();
    assert_eq!(
        found.key,
        format!("node catcher: fault at {}:{BOOM_LINE}", file!()),
    );
}

#[test]
fn an_unexpected_bug_report_fails_the_node() {
    let found = search(quick(), |rng| {
        set(&mut number(), [2, 4, 8][rng.below(3) as usize])
    })
    .unwrap();
    assert!(
        found.key.starts_with("node number: bug report at ") && found.key.contains("generate.rs:"),
        "{}",
        found.key
    );
    assert_eq!(found.choices, [2]);

    // Expected, it fails nothing, and the test reads it.
    let mut node = number().allow_bugs();
    set(&mut node, 8);
    assert_eq!(node.bug_reports().len(), 1);
}

#[test]
fn a_property_that_holds_finds_nothing() {
    assert_eq!(
        search(quick(), |rng| set(&mut number(), 10 * rng.below(5))),
        None
    );
}

// ---------------------------------------------------------------------------
// Limits

#[test]
#[should_panic(expected = "the generator rejects almost everything")]
fn a_generator_that_rejects_everything_fails_the_test() {
    check(
        Config {
            runs: 10,
            ..Config::default()
        },
        |rng| rng.reject(),
    );
}

#[test]
fn rejected_runs_are_neither_runs_nor_failures() {
    // Half the runs reject; the rest pass.
    assert_eq!(
        search(quick(), |rng| if rng.chance(1, 2) {
            rng.reject()
        }),
        None
    );
}

#[test]
fn a_run_that_does_not_end_fails_at_its_step_budget() {
    let found = search(
        Config {
            max_steps: 50,
            ..quick()
        },
        |_| {
            let mut node = number();
            loop {
                set(&mut node, 2);
            }
        },
    )
    .unwrap();
    assert_eq!(found.key, "step budget");
}

// ---------------------------------------------------------------------------
// check

#[test]
#[should_panic(expected = "invariant \"high\": 50 is high")]
fn check_fails_with_the_shrunk_run_s_own_panic() {
    let seed = (0..).find(|&s| Rng::new(s).below(100) >= 50).unwrap();
    check(Config { seed, ..quick() }, |rng| {
        set(&mut number(), rng.below(100))
    });
}

#[test]
#[should_panic(expected = "passed when replayed: the run is not repeatable")]
fn check_says_so_when_a_failure_does_not_repeat() {
    static FAILED: AtomicBool = AtomicBool::new(false);
    check(quick(), |_| {
        assert!(FAILED.swap(true, Ordering::Relaxed), "fails once only");
    });
}

// ---------------------------------------------------------------------------
// Determinism

thread_local! {
    /// Bumped by every making of a run: a run that reads it reads what its
    /// choices do not decide.
    static MADE: Cell<u64> = const { Cell::new(0) };
}

/// Sets the number once, or twice on every other making.
fn reads_a_counter(rng: &mut Rng) {
    let made = MADE.with(|m| m.replace(m.get() + 1));
    let mut node = number();
    let n = rng.below(10) * 2;
    set(&mut node, n);
    if made % 2 == 1 {
        set(&mut node, n);
    }
}

#[test]
fn a_run_made_differently_from_its_own_choices_is_not_deterministic() {
    MADE.with(|m| m.set(0));
    let config = Config {
        runs: 5,
        ..Config::default()
    };
    let found = search(config, reads_a_counter).expect("found");
    assert_eq!((found.key.as_str(), found.seed), ("not deterministic", 0));
    assert_eq!(
        found.detail.as_deref(),
        Some("the first made 1 executions, refusals and inline calls, the second 2")
    );
}

/// Sets the number from the counter, not from the choices: the same kind
/// every time, with another value on every making.
fn sends_a_counted_value(_: &mut Rng) {
    let made = MADE.with(|m| m.replace(m.get() + 1));
    set(&mut number(), (made % 2) * 2);
}

#[test]
fn the_same_kinds_with_other_values_are_not_deterministic() {
    MADE.with(|m| m.set(0));
    let config = Config {
        runs: 5,
        ..Config::default()
    };
    let found = search(config, sends_a_counted_value).expect("found");
    assert_eq!(found.key, "not deterministic");
    let detail = found.detail.expect("a detail");
    assert!(
        detail.starts_with("at step 1: first ") && detail.ends_with(": Set(2)"),
        "{detail}"
    );
}

/// Passes on its first making and rejects on the next.
fn rejects_when_made_again(rng: &mut Rng) {
    let made = MADE.with(|m| m.replace(m.get() + 1));
    set(&mut number(), 2);
    if made % 2 == 1 {
        rng.reject();
    }
}

#[test]
fn a_run_that_passes_then_is_rejected_is_not_deterministic() {
    MADE.with(|m| m.set(0));
    let config = Config {
        runs: 5,
        ..Config::default()
    };
    let found = search(config, rejects_when_made_again).expect("found");
    assert_eq!(found.key, "not deterministic");
    assert_eq!(
        found.detail.as_deref(),
        Some("the first passed and the second was rejected")
    );
}

#[test]
#[should_panic(expected = "is not deterministic: made twice from the same")]
fn check_fails_a_run_that_is_not_deterministic() {
    MADE.with(|m| m.set(0));
    check(
        Config {
            runs: 5,
            ..Config::default()
        },
        reads_a_counter,
    );
}

#[test]
fn the_determinism_check_can_be_turned_off() {
    MADE.with(|m| m.set(0));
    let config = Config {
        runs: 5,
        determinism: false,
        ..Config::default()
    };
    assert_eq!(search(config, reads_a_counter), None);
}
