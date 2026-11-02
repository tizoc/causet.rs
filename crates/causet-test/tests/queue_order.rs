//! The harness keeps production's queue order and the timer's lane and
//! lifecycle: the default scenario delivers held Events in the order the
//! lanes would run them, a simulated timer publishes into the worker lane
//! like the threaded one, and a dropped Timer fires nothing. A simulated
//! Service that stands in for a thread publishes as one too.

use std::thread;
use std::time::Duration;

use causet::hosting::{EventBus, Lane, ReleaseError};
use causet::*;
use causet_test::{Driven, NoWorld, Node, Scenario};

#[derive(Debug, Action)]
#[causet(type = "edge.action")]
enum Input {
    Prime,
    Note(&'static str),
}

#[derive(Debug, Event)]
#[causet(type = "edge.seen")]
struct Seen(&'static str);

struct Probe;

impl Component for Probe {
    type State = Vec<&'static str>;
    type Action = Input;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, input: Input, _: &ActionMeta) {
        if let Input::Note(note) = input {
            ctx.state_mut().push(note);
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<Seen>();
        r.event::<TimerFired>();
    }
}

impl Adapts<Seen> for Probe {
    fn adapt(Seen(note): Seen, port: &mut ActionPort<'_, Self>) {
        port.emit_action(Input::Note(note));
    }
}

impl Adapts<TimerFired> for Probe {
    fn adapt(_: TimerFired, port: &mut ActionPort<'_, Self>) {
        port.emit_action(Input::Note("timer"));
    }
}

fn node(scenario: &Scenario) -> (Node<Vec<&'static str>, ()>, Mount<Probe>) {
    let mut node = Node::new::<Probe>("probe", Vec::new(), (), SessionId(1), &scenario.clock);
    let mount = node.mount(&root::<Probe>());
    node.dispatch(&mount, Input::Prime);
    (node, mount)
}

fn from_worker(sink: EventSink, mount: Mount<Probe>) {
    thread::spawn(move || sink.try_send_to(&mount, Seen("worker")).unwrap())
        .join()
        .unwrap();
}

#[test]
fn default_scenario_preserves_store_lane_priority() {
    // First establish the production order for exactly this backlog.
    let mut registry = Registry::<Vec<&'static str>, ()>::new();
    registry.root::<Probe>();
    let proto = registry.freeze().unwrap();
    let mount = proto.resolve(&root::<Probe>()).unwrap();
    let (sink, inbox) = EventBus::new(&proto, 8);
    let mut production = Store::new(
        proto,
        Vec::<&'static str>::new(),
        (),
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    production.dispatch_to(&mount, Input::Prime).unwrap();
    from_worker(sink.clone(), mount);
    sink.try_send_to(&mount, Seen("store")).unwrap();
    production.drain_events(8).unwrap();
    assert_eq!(production.state(), &["store", "worker"]);

    let mut scenario = Scenario::new(1, 1_000);
    let (mut node, mount) = node(&scenario);
    from_worker(node.sink().clone(), mount);
    node.sink().try_send_to(&mount, Seen("store")).unwrap();
    assert_eq!(
        node.held().iter().map(|e| e.lane).collect::<Vec<_>>(),
        [Lane::Worker, Lane::Store]
    );
    let mut nodes: [&mut dyn Driven; 1] = [&mut node];
    scenario.step(&mut nodes, &mut NoWorld).unwrap();
    scenario.step(&mut nodes, &mut NoWorld).unwrap();
    assert_eq!(node.state(), production.state());
}

#[test]
fn default_scenario_preserves_front_insertion_order() {
    let mut scenario = Scenario::new(1, 1_000);
    let (mut node, mount) = node(&scenario);
    let reply = node
        .sink()
        .reply_for::<Probe, Seen>(&mount, ExecutionId(1))
        .unwrap();
    reply.send_front(Seen("first")).unwrap();
    reply.send_front(Seen("second")).unwrap();
    let mut nodes: [&mut dyn Driven; 1] = [&mut node];
    scenario.step(&mut nodes, &mut NoWorld).unwrap();
    scenario.step(&mut nodes, &mut NoWorld).unwrap();
    assert_eq!(node.state(), &["second", "first"]);
}

#[test]
fn dropping_a_simulated_timer_cancels_its_pending_alarms() {
    let scenario = Scenario::new(1, 1_000);
    let (node, mount) = node(&scenario);
    let reply = node
        .sink()
        .reply_for::<Probe, TimerFired>(&mount, ExecutionId(1))
        .unwrap();
    let (timer, control) = Timer::simulated(&scenario.clock);
    let request = RequestId {
        session: node.session(),
        execution: ExecutionId(1),
        index: 0,
    };
    timer.after(Duration::from_nanos(1), reply, request);
    drop(timer);
    scenario.clock.advance(1);
    assert_eq!(
        control.fire_due(),
        0,
        "a dropped Timer must never publish another alarm"
    );
    assert!(node.held().is_empty());
}

#[test]
fn simulated_timer_uses_the_worker_lane_like_the_threaded_timer() {
    let scenario = Scenario::new(1, 1_000);
    let (node, mount) = node(&scenario);
    let reply = node
        .sink()
        .reply_for::<Probe, TimerFired>(&mount, ExecutionId(1))
        .unwrap();
    let (timer, control) = Timer::simulated(&scenario.clock);
    let request = RequestId {
        session: node.session(),
        execution: ExecutionId(1),
        index: 0,
    };
    timer.after(Duration::ZERO, reply, request);
    assert_eq!(control.fire_due(), 1);
    assert_eq!(node.held()[0].lane, Lane::Worker);
}

#[test]
fn a_mixed_backlog_runs_in_production_order() {
    let mut registry = Registry::<Vec<&'static str>, ()>::new();
    registry.root::<Probe>();
    let proto = registry.freeze().unwrap();
    let mount = proto.resolve(&root::<Probe>()).unwrap();
    let (sink, inbox) = EventBus::new(&proto, 8);
    let mut production = Store::new(
        proto,
        Vec::<&'static str>::new(),
        (),
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    production.dispatch_to(&mount, Input::Prime).unwrap();
    let front = |sink: &EventSink, mount: &Mount<Probe>, note| {
        sink.reply_for::<Probe, Seen>(mount, ExecutionId(1))
            .unwrap()
            .send_front(Seen(note))
            .unwrap()
    };
    let backlog = |sink: &EventSink, mount: Mount<Probe>| {
        thread::spawn({
            let sink = sink.clone();
            move || sink.try_send_to(&mount, Seen("w1")).unwrap()
        })
        .join()
        .unwrap();
        sink.try_send_to(&mount, Seen("b1")).unwrap();
        front(sink, &mount, "f1");
        sink.try_send_to(&mount, Seen("b2")).unwrap();
        front(sink, &mount, "f2");
        thread::spawn({
            let sink = sink.clone();
            move || sink.try_send_to(&mount, Seen("w2")).unwrap()
        })
        .join()
        .unwrap();
    };
    backlog(&sink, mount);
    production.drain_events(16).unwrap();
    assert_eq!(production.state(), &["f2", "f1", "b1", "b2", "w1", "w2"]);

    let mut scenario = Scenario::new(1, 1_000);
    let (mut node, mount) = node(&scenario);
    backlog(&node.sink().clone(), mount);
    let mut nodes: [&mut dyn Driven; 1] = [&mut node];
    while scenario.step(&mut nodes, &mut NoWorld).is_some() {}
    assert_eq!(node.state(), production.state());
}

#[test]
fn a_front_send_from_a_worker_runs_in_production_order() {
    let mut registry = Registry::<Vec<&'static str>, ()>::new();
    registry.root::<Probe>();
    let proto = registry.freeze().unwrap();
    let mount = proto.resolve(&root::<Probe>()).unwrap();
    let (sink, inbox) = EventBus::new(&proto, 8);
    let mut production = Store::new(
        proto,
        Vec::<&'static str>::new(),
        (),
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    production.dispatch_to(&mount, Input::Prime).unwrap();
    let backlog = |sink: &EventSink, mount: Mount<Probe>| {
        sink.try_send_to(&mount, Seen("s1")).unwrap();
        let sink = sink.clone();
        thread::spawn(move || {
            let reply = sink
                .reply_for::<Probe, Seen>(&mount, ExecutionId(1))
                .unwrap();
            sink.try_send_to(&mount, Seen("w1")).unwrap();
            reply.send_front(Seen("f1")).unwrap();
            sink.try_send_to(&mount, Seen("w2")).unwrap();
            reply.send_front(Seen("f2")).unwrap();
        })
        .join()
        .unwrap();
    };
    backlog(&sink, mount);
    production.drain_events(16).unwrap();
    assert_eq!(production.state(), &["s1", "f2", "f1", "w1", "w2"]);

    let mut scenario = Scenario::new(1, 1_000);
    let (mut node, mount) = node(&scenario);
    backlog(&node.sink().clone(), mount);
    // The held bus records each front send in the worker lane, as a front.
    assert_eq!(
        node.held()
            .iter()
            .map(|e| (e.lane, e.front))
            .collect::<Vec<_>>(),
        [
            (Lane::Store, false),
            (Lane::Worker, false),
            (Lane::Worker, true),
            (Lane::Worker, false),
            (Lane::Worker, true),
        ]
    );
    let mut nodes: [&mut dyn Driven; 1] = [&mut node];
    while scenario.step(&mut nodes, &mut NoWorld).is_some() {}
    assert_eq!(node.state(), production.state());
}

#[test]
fn a_released_worker_front_send_goes_to_the_head_of_the_worker_lane() {
    let mut registry = Registry::<Vec<&'static str>, ()>::new();
    registry.root::<Probe>();
    let proto = registry.freeze().unwrap();
    let mount = proto.resolve(&root::<Probe>()).unwrap();
    let (sink, inbox, held) = EventBus::held(&proto, 2, SessionId(1));
    let mut store = Store::new(
        proto,
        Vec::<&'static str>::new(),
        (),
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    store.dispatch_to(&mount, Input::Prime).unwrap();
    thread::spawn(move || {
        let reply = sink
            .reply_for::<Probe, Seen>(&mount, ExecutionId(1))
            .unwrap();
        sink.try_send_to(&mount, Seen("back")).unwrap();
        reply.send_front(Seen("front")).unwrap();
        reply.send_front(Seen("over")).unwrap();
    })
    .join()
    .unwrap();
    let ids: Vec<_> = held.list().iter().map(|e| e.id).collect();
    held.release(ids[0]).unwrap();
    held.release(ids[1]).unwrap();
    // Released at the head, still bounded by the worker lane's capacity.
    assert_eq!(held.release(ids[2]), Err(ReleaseError::Full));
    store.drain_events(16).unwrap();
    assert_eq!(store.state(), &["front", "back"]);
}

#[test]
fn a_simulated_worker_keeps_the_worker_lane_on_the_driver_thread() {
    use causet_test::World;

    /// Stands in for a thread: publishes once the clock reaches `at`.
    struct WorkerWorld {
        sink: EventSink,
        mount: Mount<Probe>,
        at: Option<Timestamp>,
    }

    impl World for WorkerWorld {
        fn next_time(&self) -> Option<Timestamp> {
            self.at
        }

        fn advance(&mut self, now: Timestamp) {
            if self.at.is_some_and(|at| at <= now) {
                self.at = None;
                self.sink
                    .try_send_to_as_worker(&self.mount, Seen("external worker"))
                    .unwrap();
                let reply = self
                    .sink
                    .reply_for::<Probe, Seen>(&self.mount, ExecutionId(1))
                    .unwrap();
                reply.try_send_as_worker(Seen("worker reply")).unwrap();
            }
        }
    }

    let mut scenario = Scenario::new(1, 1_000);
    let (mut node, mount) = node(&scenario);
    let mut world = WorkerWorld {
        sink: node.sink().clone(),
        mount,
        at: Some(1_001),
    };
    let mut nodes: [&mut dyn Driven; 1] = [&mut node];
    scenario.step(&mut nodes, &mut world).unwrap();
    assert_eq!(
        node.held().iter().map(|e| e.lane).collect::<Vec<_>>(),
        [Lane::Worker, Lane::Worker]
    );
    // The plain sends from this thread, the Store's, take the Store lane.
    node.sink().try_send_to(&mount, Seen("store")).unwrap();
    assert_eq!(node.held()[2].lane, Lane::Store);
}

#[test]
fn worker_sends_from_the_store_thread_are_refused_when_the_lane_is_full() {
    let mut registry = Registry::<Vec<&'static str>, ()>::new();
    registry.root::<Probe>();
    let proto = registry.freeze().unwrap();
    let mount = proto.resolve(&root::<Probe>()).unwrap();
    let (sink, inbox) = EventBus::new(&proto, 1);
    let mut store = Store::new(
        proto,
        Vec::<&'static str>::new(),
        (),
        inbox,
        StoreOptions::default(),
    )
    .unwrap();
    store.dispatch_to(&mount, Input::Prime).unwrap();
    sink.try_send_to_as_worker(&mount, Seen("w1")).unwrap();
    assert!(matches!(
        sink.try_send_to_as_worker(&mount, Seen("w2")),
        Err(SendError::Full(_))
    ));
    sink.try_send_to(&mount, Seen("store")).unwrap(); // the Store lane is not bounded
    store.drain_events(8).unwrap();
    assert_eq!(store.state(), &["store", "w1"]);
}
