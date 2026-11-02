//! One `Downloader` component, mounted under two different parents, sharing
//! one threaded network Service and the timer Service.
//!
//! - `/library/fetch` is called with queued child sends, `/archive/fetch`
//!   inline; both report through Outputs their parents lower.
//! - Replies come back through `Reply` from worker threads. A request is
//!   pinned by its `RequestId`: a reply for one no longer pending (a
//!   duplicate, a late answer, the deadline of a finished request) is ignored.
//! - Each attempt sets a deadline with the timer; a failure or a timeout is
//!   retried once, then given up.
//!
//! `cargo run -p causet --example downloads`

// An example program: its public items are not an API.
#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::thread;
use std::time::{Duration, Instant};

use causet::observers::Stats;
use causet::{
    Action, ActionMeta, ActionPort, Adapts, Child, Component, ComponentContext, ComponentRegistry,
    Effect, EffectContext, EffectMeta, Event, Lens, Never, Output, Pending, Reply, RequestId,
    Store, StoreOptions, Timer, TimerFired, lens, no_effects, root,
};

const ATTEMPTS: u32 = 2;
const DEADLINE: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// The network: a capability trait, here simulated on worker threads.

#[derive(Debug, Event)]
#[causet(type = "net.reply")]
pub enum NetReply {
    Done {
        request: RequestId,
        bytes: usize,
    },
    #[causet(log(level = "warn", fields(reason)))]
    Failed {
        request: RequestId,
        reason: String,
    },
}

pub trait Network: Send {
    fn get(&mut self, request: RequestId, url: String, reply: Reply<NetReply>);
}

/// Behaves by the URL's first segment: `ok` answers in 5 ms, `dup` answers
/// twice, `fail` always fails, `flaky` fails its first attempt, `slow`
/// answers after 300 ms, well past the deadline.
#[derive(Default)]
struct SimulatedNetwork {
    attempts: BTreeMap<String, u32>,
}

impl Network for SimulatedNetwork {
    fn get(&mut self, request: RequestId, url: String, reply: Reply<NetReply>) {
        let attempt = self
            .attempts
            .entry(url.clone())
            .and_modify(|n| *n += 1)
            .or_insert(1);
        let behaviour = url.split('/').next().unwrap_or_default().to_string();
        let first = *attempt == 1;
        thread::spawn(move || {
            let done = || NetReply::Done {
                request,
                bytes: url.len() * 100,
            };
            let failed = || NetReply::Failed {
                request,
                reason: "connection reset".into(),
            };
            let wait = |ms| thread::sleep(Duration::from_millis(ms));
            let send = |reply_: NetReply| {
                // A closed bus means the Store is gone or faulted: nothing to answer.
                let _ = reply.send_timeout(reply_, Duration::from_secs(1));
            };
            match behaviour.as_str() {
                "dup" => {
                    wait(5);
                    send(done());
                    send(done());
                }
                "fail" => {
                    wait(5);
                    send(failed());
                }
                "flaky" if first => {
                    wait(5);
                    send(failed());
                }
                "slow" => {
                    wait(300);
                    send(done());
                }
                _ => {
                    wait(5);
                    send(done());
                }
            }
        });
    }
}

/// What every Downloader needs, shared by both mounts.
pub struct Downloads {
    pub net: Box<dyn Network>,
    pub timer: Timer,
}

// ---------------------------------------------------------------------------
// Downloader: fetches with a deadline and one retry.

pub struct Downloader;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Attempt {
    pub url: String,
    pub number: u32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DownloaderState {
    pub pending: Pending<Attempt>,
    /// Replies and deadlines for requests no longer pending.
    pub ignored: u32,
}

#[derive(Debug, Action)]
#[causet(type = "downloader.action")]
pub enum DownloaderAction {
    #[causet(log(level = "info", fields(url)))]
    Fetch {
        url: String,
    },
    Got {
        request: RequestId,
        bytes: usize,
    },
    Failed {
        request: RequestId,
    },
    TimedOut {
        request: RequestId,
    },
}

#[derive(Debug, Effect)]
#[causet(type = "downloader.effect", prefix = "downloader.effect")]
pub enum DownloaderEffect {
    Get { request: RequestId, url: String },
    Deadline { request: RequestId },
}

#[derive(Debug, Output)]
#[causet(type = "downloader.output")]
pub enum DownloaderOutput {
    Fetched { url: String, bytes: usize },
    GaveUp { url: String },
}

impl Component for Downloader {
    type State = DownloaderState;
    type Service = Downloads;
    type Action = DownloaderAction;
    type Effect = DownloaderEffect;
    type Output = DownloaderOutput;

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: DownloaderAction, _: &ActionMeta) {
        // Mutation first; what to emit is decided along the way.
        let (url, number) = match action {
            DownloaderAction::Fetch { url } => (url, 1),
            DownloaderAction::Got { request, bytes } => {
                match ctx.state_mut().pending.take(request) {
                    Some(attempt) => ctx.into_emitter().emit_output(DownloaderOutput::Fetched {
                        url: attempt.url,
                        bytes,
                    }),
                    None => ctx.state_mut().ignored += 1,
                }
                return;
            }
            DownloaderAction::Failed { request } | DownloaderAction::TimedOut { request } => {
                match ctx.state_mut().pending.take(request) {
                    Some(attempt) if attempt.number < ATTEMPTS => (attempt.url, attempt.number + 1),
                    Some(attempt) => {
                        ctx.into_emitter()
                            .emit_output(DownloaderOutput::GaveUp { url: attempt.url });
                        return;
                    }
                    None => {
                        ctx.state_mut().ignored += 1;
                        return;
                    }
                }
            }
        };
        let request = ctx.new_request();
        ctx.state_mut().pending.insert(
            request,
            Attempt {
                url: url.clone(),
                number,
            },
        );
        let mut out = ctx.into_emitter();
        out.emit_effect(DownloaderEffect::Get { request, url });
        out.emit_effect(DownloaderEffect::Deadline { request });
    }

    fn handle(mut ctx: EffectContext<'_, Self>, effect: DownloaderEffect, _: &EffectMeta) {
        match effect {
            DownloaderEffect::Get { request, url } => {
                let reply = ctx.reply::<NetReply>();
                ctx.service().net.get(request, url, reply);
            }
            DownloaderEffect::Deadline { request } => {
                let reply = ctx.reply::<TimerFired>();
                ctx.service().timer.after(DEADLINE, reply, request);
            }
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.event::<NetReply>();
        r.event::<TimerFired>();
    }
}

impl Adapts<NetReply> for Downloader {
    fn adapt(reply: NetReply, port: &mut ActionPort<'_, Self>) {
        match reply {
            NetReply::Done { request, bytes } => {
                port.emit_action(DownloaderAction::Got { request, bytes })
            }
            NetReply::Failed { request, .. } => {
                port.emit_action(DownloaderAction::Failed { request })
            }
        }
    }
}

impl Adapts<TimerFired> for Downloader {
    fn adapt(fired: TimerFired, port: &mut ActionPort<'_, Self>) {
        port.emit_action(DownloaderAction::TimedOut {
            request: fired.request,
        });
    }
}

// ---------------------------------------------------------------------------
// Library: borrows books through queued child sends.

pub struct Library;

const LIBRARY_FETCH: Child<Library, Downloader> = Child::new("fetch");

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LibraryState {
    pub fetch: DownloaderState,
    pub shelf: Vec<String>,
    pub missing: Vec<String>,
}

#[derive(Debug, Action)]
#[causet(type = "library.action")]
pub enum LibraryAction {
    Borrow { url: String },
    Shelve { url: String },
    Missing { url: String },
}

fn lower_library(
    _: Child<Library, Downloader>,
    out: DownloaderOutput,
    port: &mut ActionPort<'_, Library>,
) {
    match out {
        DownloaderOutput::Fetched { url, .. } => port.emit_action(LibraryAction::Shelve { url }),
        DownloaderOutput::GaveUp { url } => port.emit_action(LibraryAction::Missing { url }),
    }
}

impl Component for Library {
    type State = LibraryState;
    type Action = LibraryAction;
    type Output = Never;
    no_effects!(Downloads);

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: LibraryAction, _: &ActionMeta) {
        match action {
            LibraryAction::Borrow { url } => ctx
                .into_emitter()
                .to_child(LIBRARY_FETCH, DownloaderAction::Fetch { url }),
            LibraryAction::Shelve { url } => ctx.state_mut().shelf.push(url),
            LibraryAction::Missing { url } => ctx.state_mut().missing.push(url),
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(LIBRARY_FETCH, lens!(LibraryState => fetch))
            .service(Lens::identity())
            .on_output(lower_library)
            .bind();
    }
}

// ---------------------------------------------------------------------------
// Archive: stores pages, calling its Downloader inline.

pub struct Archive;

const ARCHIVE_FETCH: Child<Archive, Downloader> = Child::new("fetch");

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ArchiveState {
    pub fetch: DownloaderState,
    pub requested: u32,
    pub stored_bytes: usize,
    pub lost: Vec<String>,
}

#[derive(Debug, Action)]
#[causet(type = "archive.action")]
pub enum ArchiveAction {
    Keep { url: String },
    Stored { bytes: usize },
    Lost { url: String },
}

fn lower_archive(
    _: Child<Archive, Downloader>,
    out: DownloaderOutput,
    port: &mut ActionPort<'_, Archive>,
) {
    match out {
        DownloaderOutput::Fetched { bytes, .. } => {
            port.emit_action(ArchiveAction::Stored { bytes })
        }
        DownloaderOutput::GaveUp { url } => port.emit_action(ArchiveAction::Lost { url }),
    }
}

impl Component for Archive {
    type State = ArchiveState;
    type Action = ArchiveAction;
    type Output = Never;
    no_effects!(Downloads);

    fn reduce(mut ctx: ComponentContext<'_, Self>, action: ArchiveAction, _: &ActionMeta) {
        match action {
            ArchiveAction::Keep { url } => {
                // Inline: the child's request and Effects join this execution.
                ctx.reduce_child(ARCHIVE_FETCH, DownloaderAction::Fetch { url });
                ctx.state_mut().requested += 1;
            }
            ArchiveAction::Stored { bytes } => ctx.state_mut().stored_bytes += bytes,
            ArchiveAction::Lost { url } => ctx.state_mut().lost.push(url),
        }
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(ARCHIVE_FETCH, lens!(ArchiveState => fetch))
            .service(Lens::identity())
            .on_output(lower_archive)
            .bind();
    }
}

// ---------------------------------------------------------------------------
// The application.

pub struct App;

const LIBRARY: Child<App, Library> = Child::new("library");
const ARCHIVE: Child<App, Archive> = Child::new("archive");

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AppState {
    pub library: LibraryState,
    pub archive: ArchiveState,
}

impl AppState {
    fn settled(&self) -> bool {
        self.library.fetch.pending.is_empty() && self.archive.fetch.pending.is_empty()
    }

    fn ignored(&self) -> u32 {
        self.library.fetch.ignored + self.archive.fetch.ignored
    }
}

pub struct AppServices {
    pub downloads: Downloads,
}

impl Component for App {
    type State = AppState;
    type Action = Never;
    type Output = Never;
    no_effects!(AppServices);

    fn reduce(_: ComponentContext<'_, Self>, action: Never, _: &ActionMeta) {
        match action {}
    }

    fn register(r: &mut ComponentRegistry<'_, Self>) {
        r.child(LIBRARY, lens!(AppState => library))
            .service(lens!(AppServices => downloads))
            .bind();
        r.child(ARCHIVE, lens!(AppState => archive))
            .service(lens!(AppServices => downloads))
            .bind();
    }
}

// ---------------------------------------------------------------------------
// The run.

pub struct Run {
    pub live: AppState,
    pub executions: usize,
    pub stats: causet::observers::StatsSnapshot,
}

/// Every attempt sets a deadline, and every deadline fires, so the run is over
/// when nothing is pending and these many replies and deadlines were ignored:
/// `ok` 1 (its deadline), `dup` 2, and `flaky`, `slow` and `fail` 2 each.
const IGNORED: u32 = 9;

pub fn run() -> Result<Run, String> {
    let services = AppServices {
        downloads: Downloads {
            net: Box::new(SimulatedNetwork::default()),
            timer: Timer::new(),
        },
    };
    let (mut store, _sink) =
        Store::start::<App>(AppState::default(), services, StoreOptions::default())
            .map_err(|e| format!("{e}"))?;
    store.add_observer(Stats::new());

    let library = store
        .mount(&root::<App>().child(LIBRARY))
        .map_err(|e| format!("{e:?}"))?;
    let archive = store
        .mount(&root::<App>().child(ARCHIVE))
        .map_err(|e| format!("{e:?}"))?;
    for url in ["ok/rust-book", "dup/sicp", "fail/lost-scroll"] {
        store
            .dispatch_to(&library, LibraryAction::Borrow { url: url.into() })
            .map_err(|e| format!("{e:?}"))?;
    }
    for url in ["flaky/home-page", "slow/old-forum"] {
        store
            .dispatch_to(&archive, ArchiveAction::Keep { url: url.into() })
            .map_err(|e| format!("{e:?}"))?;
    }

    // The driver loop: deliver replies until the run is over.
    let ready = store.readiness();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !(store.state().settled() && store.state().ignored() == IGNORED) {
        if Instant::now() > deadline {
            return Err(format!("the run did not finish: {:#?}", store.state()));
        }
        if store.drain_events(64).map_err(|e| format!("{e:?}"))? == 0 {
            ready.wait_timeout(deadline.saturating_duration_since(Instant::now()));
        }
    }
    let live = store.state().clone();
    let executions = store.executions();
    let stats = store.observer::<Stats>().expect("installed").snapshot();
    Ok(Run {
        live,
        executions,
        stats,
    })
}

fn main() {
    let run = run().unwrap_or_else(|e| panic!("{e}"));
    let state = &run.live;
    println!("library shelf:   {:?}", state.library.shelf);
    println!("library missing: {:?}", state.library.missing);
    println!(
        "archive stored:  {} bytes, lost {:?}",
        state.archive.stored_bytes, state.archive.lost
    );
    println!("ignored replies and deadlines: {}", state.ignored());
    println!("{} executions", run.executions);
    for (ty, kinds) in &run.stats.actions {
        for (kind, ranges) in kinds {
            println!("  {ty} {kind}: {} calls", ranges.calls());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_downloads_run_settles_every_request() {
        let run = run().unwrap();
        let state = &run.live;

        // Worker threads answer in any order, so the shelf is sorted first.
        let mut shelf = state.library.shelf.clone();
        shelf.sort();
        assert_eq!(shelf, ["dup/sicp", "ok/rust-book"]);
        assert_eq!(state.library.missing, ["fail/lost-scroll"]);
        // `flaky` succeeded on its retry; `slow` timed out twice.
        assert_eq!(state.archive.stored_bytes, "flaky/home-page".len() * 100);
        assert_eq!(state.archive.lost, ["slow/old-forum"]);
        assert_eq!(state.archive.requested, 2);
        assert_eq!(state.ignored(), IGNORED);
        assert!(state.settled());

        let fetches = &run.stats.actions["downloader.action"];
        // The Library's 3 queued sends are executions of their own; the
        // Archive's 2 inline calls are part of its `archive.keep`.
        assert_eq!(fetches["downloader.fetch"].calls(), 3);
        assert_eq!(
            run.stats.actions["archive.action"]["archive.keep"].calls(),
            2
        );
    }
}
