//! The downloads scenario, shared by several test files: a
//! `Downloader` with Effects mounted twice (`/a`, `/b`) sharing one download
//! Service, and a `Counter` leaf with no Effects.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use causet::hosting::EventBus;
use causet::observers::TraceLog;
use causet::{
    Action, ActionMeta, ActionPort, Adapts, Child, Component, ComponentContext, ComponentRegistry,
    DispatchOutcome, Effect, EffectContext, EffectMeta, Event, EventSink, Mount, Never, Protocol,
    Registry, Reply, RequestId, Store, StoreOptions, Timestamp, lens, no_effects, root,
};

// ---------------------------------------------------------------------------
// Messages

#[derive(Debug, PartialEq, Event)]
#[causet(type = "download.event")]
pub enum DownloadEvent {
    Done { request: RequestId, bytes: usize },
    Pushed { note: String },
}

/// Adapted by the Downloader, but never registered.
#[derive(Debug, Event)]
#[causet(type = "ping")]
pub struct Ping;

#[derive(Debug, Action)]
#[causet(type = "downloader.action")]
pub enum DownloaderAction {
    Fetch(String),
    Completed {
        request: RequestId,
        bytes: usize,
    },
    Finished {
        url: String,
        bytes: usize,
    },
    Probe,
    Note(u32),
    BadReply,
    Pushed(String),
    /// Panics in the reducer.
    Crash,
    /// Its Effect dispatches `Note(7)`, then panics in the handler.
    CrashInHandler,
    /// Panics in the enabling check.
    Explode,
}

#[derive(Debug, Effect)]
#[causet(type = "downloader.effect", prefix = "downloader.effect")]
pub enum DownloaderEffect {
    Fetch { request: RequestId, url: String },
    Probe,
    BadReply,
    CrashInHandler,
}

// ---------------------------------------------------------------------------
// The Service: a capability trait, held as `Box<dyn ...>`.

pub trait DownloadService: Send {
    fn fetch(&mut self, request: RequestId, url: String, reply: Reply<DownloadEvent>);
}

/// Replies from a worker thread, optionally twice.
pub struct Threaded {
    pub duplicate: bool,
}

impl DownloadService for Threaded {
    fn fetch(&mut self, request: RequestId, url: String, reply: Reply<DownloadEvent>) {
        let duplicate = self.duplicate;
        thread::spawn(move || {
            let done = || DownloadEvent::Done {
                request,
                bytes: url.len(),
            };
            reply
                .send_timeout(done(), Duration::from_secs(1))
                .expect("worker lane");
            if duplicate {
                reply
                    .send_timeout(done(), Duration::from_secs(1))
                    .expect("worker lane");
            }
        });
    }
}

/// Replies synchronously, on the Store's thread.
pub struct Inline;

impl DownloadService for Inline {
    fn fetch(&mut self, request: RequestId, url: String, reply: Reply<DownloadEvent>) {
        reply
            .try_send(DownloadEvent::Done {
                request,
                bytes: url.len(),
            })
            .expect("the Store lane is never full");
    }
}

// ---------------------------------------------------------------------------
// Downloader

pub struct Downloader;

#[derive(Clone, Default, Debug, PartialEq)]
pub struct DownloaderState {
    pub pending: BTreeMap<RequestId, String>,
    pub done: Vec<(String, usize)>,
    pub stale: Vec<RequestId>,
    pub notes: Vec<u32>,
    pub pushed: Vec<String>,
}

impl Component for Downloader {
    type State = DownloaderState;
    type Service = Box<dyn DownloadService>;
    type Action = DownloaderAction;
    type Effect = DownloaderEffect;
    type Output = Never;

    fn is_enabled(_: &DownloaderState, action: &DownloaderAction, _: Timestamp) -> bool {
        match action {
            DownloaderAction::Note(0) => false,
            DownloaderAction::Explode => panic!("admission exploded"),
            _ => true,
        }
    }

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: DownloaderAction, _: &ActionMeta) {
        match action {
            DownloaderAction::Fetch(url) => {
                let request = ctx.new_request();
                ctx.state_mut().pending.insert(request, url.clone());
                ctx.into_emitter()
                    .emit_effect(DownloaderEffect::Fetch { request, url });
            }
            DownloaderAction::Completed { request, bytes } => {
                let found = ctx.state_mut().pending.remove(&request);
                match found {
                    Some(url) => ctx
                        .into_emitter()
                        .emit_action(DownloaderAction::Finished { url, bytes }),
                    None => ctx.state_mut().stale.push(request),
                }
            }
            DownloaderAction::Finished { url, bytes } => ctx.state_mut().done.push((url, bytes)),
            DownloaderAction::Probe => ctx.into_emitter().emit_effect(DownloaderEffect::Probe),
            DownloaderAction::Note(n) => ctx.state_mut().notes.push(n),
            DownloaderAction::BadReply => {
                ctx.into_emitter().emit_effect(DownloaderEffect::BadReply)
            }
            DownloaderAction::Pushed(note) => ctx.state_mut().pushed.push(note),
            DownloaderAction::Crash => panic!("downloader crashed"),
            DownloaderAction::CrashInHandler => ctx
                .into_emitter()
                .emit_effect(DownloaderEffect::CrashInHandler),
            DownloaderAction::Explode => unreachable!("refused by its enabling check"),
        }
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: DownloaderEffect, _: &EffectMeta) {
        match effect {
            DownloaderEffect::Fetch { request, url } => {
                let reply = ctx.reply::<DownloadEvent>();
                ctx.service().fetch(request, url, reply);
            }
            DownloaderEffect::Probe => {
                assert_eq!(
                    ctx.dispatch(DownloaderAction::Note(0)),
                    DispatchOutcome::Disabled
                );
                assert_eq!(
                    ctx.dispatch(DownloaderAction::Note(1)),
                    DispatchOutcome::Executed
                );
            }
            DownloaderEffect::BadReply => {
                let _ = ctx.reply::<Ping>();
            }
            DownloaderEffect::CrashInHandler => {
                ctx.dispatch(DownloaderAction::Note(7));
                panic!("handler crashed");
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<DownloadEvent>();
    }
}

impl Adapts<DownloadEvent> for Downloader {
    fn adapt(event: DownloadEvent, port: &mut ActionPort<'_, Self>) {
        match event {
            DownloadEvent::Done { request, bytes } => {
                port.emit_action(DownloaderAction::Completed { request, bytes })
            }
            DownloadEvent::Pushed { note } => port.emit_action(DownloaderAction::Pushed(note)),
        }
    }
}

impl Adapts<Ping> for Downloader {
    fn adapt(_: Ping, _: &mut ActionPort<'_, Self>) {}
}

// ---------------------------------------------------------------------------
// Counter: a leaf with no Effects, so no Service binding.

pub struct Counter;

#[derive(Clone, Default, Debug, PartialEq)]
pub struct CounterState {
    pub n: u64,
}

#[derive(Debug, Action)]
#[causet(type = "counter.action", kind = "counter.inc")]
pub struct Inc;

impl Component for Counter {
    type State = CounterState;
    type Action = Inc;
    type Output = Never;
    no_effects!();

    fn reduce(mut ctx: ComponentContext<'_, Self>, _: Inc, _: &ActionMeta) {
        ctx.state_mut().n += 1;
    }
}

// ---------------------------------------------------------------------------
// App

pub struct App;

pub const A: Child<App, Downloader> = Child::new("a");
pub const B: Child<App, Downloader> = Child::new("b");
pub const COUNTER: Child<App, Counter> = Child::new("counter");

#[derive(Clone, Default, Debug, PartialEq)]
pub struct AppState {
    pub a: DownloaderState,
    pub b: DownloaderState,
    pub counter: CounterState,
}

pub struct AppServices {
    pub downloads: Box<dyn DownloadService>,
}

#[derive(Debug, Action)]
#[causet(type = "app.action", kind = "app.noop")]
pub struct Noop;

impl Component for App {
    type State = AppState;
    type Service = AppServices;
    type Action = Noop;
    type Effect = Never;
    type Output = Never;

    fn reduce(_: ComponentContext<'_, Self>, _: Noop, _: &ActionMeta) {}

    fn handle(_: EffectContext<'_, Self>, effect: Never, _: &EffectMeta) {
        match effect {}
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        // Both mounts project the same Service field: one shared Service.
        r.child(A, lens!(AppState => a))
            .service(lens!(AppServices => downloads))
            .bind();
        r.child(B, lens!(AppState => b))
            .service(lens!(AppServices => downloads))
            .bind();
        r.child(COUNTER, lens!(AppState => counter)).bind();
    }
}

// ---------------------------------------------------------------------------
// Fixture

pub struct Fixture {
    pub store: Store<AppState, AppServices>,
    pub sink: EventSink,
    pub a: Mount<Downloader>,
    pub b: Mount<Downloader>,
    pub counter: Mount<Counter>,
}

pub fn protocol() -> Arc<Protocol<AppState, AppServices>> {
    let mut registry = Registry::<AppState, AppServices>::new();
    registry.root::<App>();
    registry.freeze().expect("valid protocol")
}

pub fn fixture_with(
    downloads: Box<dyn DownloadService>,
    worker_capacity: usize,
    options: StoreOptions,
) -> Fixture {
    fixture_from(AppState::default(), downloads, worker_capacity, options)
}

pub fn fixture_from(
    state: AppState,
    downloads: Box<dyn DownloadService>,
    worker_capacity: usize,
    options: StoreOptions,
) -> Fixture {
    let p = protocol();
    let (sink, inbox) = EventBus::new(&p, worker_capacity);
    let mut store =
        Store::new(p.clone(), state, AppServices { downloads }, inbox, options).unwrap();
    store.add_observer(TraceLog::new());
    Fixture {
        store,
        sink,
        a: p.resolve(&root::<App>().child(A)).unwrap(),
        b: p.resolve(&root::<App>().child(B)).unwrap(),
        counter: p.resolve(&root::<App>().child(COUNTER)).unwrap(),
    }
}

pub fn fixture(downloads: Box<dyn DownloadService>, worker_capacity: usize) -> Fixture {
    fixture_with(downloads, worker_capacity, StoreOptions::default())
}

pub fn drain_until(store: &mut Store<AppState, AppServices>, done: impl Fn(&AppState) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done(store.state()) {
        store.drain_events(64).expect("no fault");
        assert!(Instant::now() < deadline, "timed out waiting for replies");
        thread::sleep(Duration::from_millis(1));
    }
}
