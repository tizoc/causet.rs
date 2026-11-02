//! The harness's invariant checks: run on the starting State and between
//! root inputs, and a failure names the node and the root input after which
//! it was found.

use causet::guide::shop::*;
use causet::*;
use causet_test::Node;

fn shop(limit: u64) -> Node<ShopState, ShopServices> {
    let services = ShopServices {
        gateway: Box::new(ApproveUpTo(100)),
    };
    Node::from_protocol(
        "shop",
        protocol(),
        ShopState::default(),
        services,
        SessionId(1),
        &ManualClock::new(1_000),
    )
    .check("paid within limit", move |s: &ShopState| {
        match s.payments.paid.iter().find(|&&p| p > limit) {
            Some(p) => Err(format!("paid {p} over {limit}")),
            None => Ok(()),
        }
    })
}

#[test]
#[should_panic(
    expected = "node shop, after root input 4: invariant \"paid within limit\": paid 7 over 5"
)]
fn a_broken_invariant_is_reported_after_the_root_input_that_broke_it_not_later() {
    let mut shop = shop(5);
    let payments = shop.mount(&root::<ShopRoot>().child(PAYMENTS));
    shop.dispatch(&payments, PaymentsAction::Charge { amount: 3 }); // 1
    let [answer] = shop.held().try_into().unwrap();
    shop.deliver(answer.id); // 2
    shop.dispatch(&payments, PaymentsAction::Charge { amount: 7 }); // 3: the gateway approves, nothing paid yet
    let [answer] = shop.held().try_into().unwrap();
    shop.deliver(answer.id); // 4: paid
}

#[test]
#[should_panic(
    expected = "node shop, before any root input: invariant \"paid within limit\": paid 9 over 5"
)]
fn a_starting_state_is_checked_when_the_check_is_added() {
    let mut state = ShopState::default();
    state.payments.paid.push(9);
    let services = ShopServices {
        gateway: Box::new(ApproveUpTo(100)),
    };
    let _ = Node::from_protocol(
        "shop",
        protocol(),
        state,
        services,
        SessionId(1),
        &ManualClock::new(1_000),
    )
    .check("paid within limit", |s: &ShopState| {
        match s.payments.paid.iter().find(|&&p| p > 5) {
            Some(p) => Err(format!("paid {p} over 5")),
            None => Ok(()),
        }
    });
}

#[test]
fn a_scenario_reports_its_seed_and_last_steps() {
    use causet_test::{Driven, Scenario, Stop};

    let mut shop = shop(100);
    let cart = shop.mount(&root::<ShopRoot>().child(CART));
    shop.dispatch(
        &cart,
        CartAction::Add {
            item: "tea".into(),
            price: 4,
        },
    );
    shop.dispatch(&cart, CartAction::Checkout);
    let mut scenario = Scenario::new(42, 1_000);
    let mut nodes: [&mut dyn Driven; 1] = [&mut shop];
    assert_eq!(
        scenario.run_until(&mut nodes, &mut causet_test::NoWorld, 10, None, |_| false),
        Stop::Quiet
    );

    assert_eq!(scenario.seed(), Some(42));
    assert_eq!(
        scenario.report(5),
        "scenario seed 42 failed after 1 steps:\n     1. shop: deliver payments.charged to /payments"
    );
    scenario.log.extend((0..30).map(|n| format!("step {n}")));
    let report = scenario.report(2);
    assert!(
        report.starts_with("scenario seed 42 failed after 31 steps; the last 2:"),
        "{report}"
    );
    assert!(
        report.ends_with("    30. step 28\n    31. step 29"),
        "{report}"
    );
}

#[test]
#[should_panic(expected = "a failed check")]
fn a_scenario_alive_in_a_failing_test_lets_the_panic_through() {
    let _scenario = causet_test::Scenario::new(3, 1_000);
    panic!("a failed check");
}

#[test]
fn a_closed_stderr_leaves_the_original_failure_to_fail_the_test() {
    use std::process::{Command, Stdio};

    const CHILD: &str = "CAUSET_TEST_REPORT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let _scenario = causet_test::Scenario::new(17, 0);
        panic!("the original test failure");
    }
    // This test, again, in a child whose stderr is closed and not captured.
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "a_closed_stderr_leaves_the_original_failure_to_fail_the_test",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stderr.take());
    let status = child.wait().unwrap();
    assert_eq!(
        status.code(),
        Some(101),
        "the report aborted the test process: {status}"
    );
}
