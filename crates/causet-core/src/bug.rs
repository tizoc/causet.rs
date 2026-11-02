//! Bug reports: `bug_condition!` reports a state the code believes
//! impossible, and the caller carries on with its fallback. The same in every
//! build: nothing faults, nothing is cancelled or rolled back.
//!
//! While a Store runs a root input, reports wait in a buffer of the Store's
//! thread; the Store takes them at fixed points (after admission, a reducer,
//! an adapter, before and after an Effect handler's
//! synchronous dispatch, on a fault), so each has an exact place in the run,
//! and gives them to its observers. Elsewhere (a Service's thread, a plain
//! unit test), a report goes to the process's hook.

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use crate::{ExecutionId, InstanceId, Phase};

/// One bug report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BugReport {
    /// What was wrong, as the caller formatted it.
    pub message: String,
    /// The fields the caller named (`bug_condition!(request = id; "...")`),
    /// each by its `Debug` form.
    pub fields: Vec<(Cow<'static, str>, String)>,
    /// The source file of the `bug_condition!`.
    pub file: Cow<'static, str>,
    /// Its line.
    pub line: u32,
    /// Where in the Store it happened; `None` outside any Store.
    pub place: Option<BugPlace>,
}

/// Where in a Store's run a bug was reported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BugPlace {
    /// What was running: admission, a reducer (with the children it called
    /// inline and the lowerers it reached), an Effect handler, an adapter,
    /// an observer or the clock.
    pub phase: Phase,
    /// The execution it happened in. For admission, which runs before any
    /// execution exists, the execution that emitted or dispatched the
    /// candidate; `None` for an Action dispatched to the Store.
    pub at: Option<ExecutionId>,
    /// The mount whose code reported it.
    pub component: InstanceId,
    /// That mount's path.
    pub path: Arc<str>,
    /// Its position among all the reports of this Store's run, from 0.
    pub sequence: u64,
}

/// A report waiting for the Store, with the place it was tagged with as it
/// left an inline child, a guard or a lowerer, if it did.
pub(crate) struct Pending {
    pub(crate) message: String,
    pub(crate) fields: Vec<(&'static str, String)>,
    pub(crate) file: &'static str,
    pub(crate) line: u32,
    pub(crate) tag: Option<(Phase, usize)>,
}

thread_local! {
    /// How many scopes are open on this thread: more than 0 while a Store
    /// runs a root input here.
    static DEPTH: Cell<u32> = const { Cell::new(0) };
    /// The innermost scope's reports.
    static REPORTS: RefCell<Vec<Pending>> = const { RefCell::new(Vec::new()) };
}

/// Reports waiting in any thread's scope: one relaxed load tells the Store
/// that, almost always, there is nothing to take.
static PENDING: AtomicUsize = AtomicUsize::new(0);

type Hook = Box<dyn Fn(&BugReport) + Send + Sync>;

static HOOK: RwLock<Option<Hook>> = RwLock::new(None);

/// Where reports made outside any Store go: a Service's threads, helpers
/// called from plain tests. Replaces the previous hook; without one, reports
/// are printed to standard error.
pub fn set_bug_hook(hook: impl Fn(&BugReport) + Send + Sync + 'static) {
    *HOOK.write().unwrap_or_else(|e| e.into_inner()) = Some(Box::new(hook));
}

/// What `bug_condition!` calls.
#[doc(hidden)]
pub fn report_bug(
    message: String,
    fields: Vec<(&'static str, String)>,
    file: &'static str,
    line: u32,
) {
    let pending = Pending {
        message,
        fields,
        file,
        line,
        tag: None,
    };
    if DEPTH.get() > 0 {
        REPORTS.with_borrow_mut(|reports| reports.push(pending));
        PENDING.fetch_add(1, Ordering::Relaxed);
    } else {
        let report = pending.into_report(None);
        let hook = HOOK.read().unwrap_or_else(|e| e.into_inner());
        match &*hook {
            Some(hook) => hook(&report),
            None => eprintln!(
                "causet: bug condition at {}:{}: {}",
                report.file, report.line, report.message
            ),
        }
    }
}

impl Pending {
    pub(crate) fn into_report(self, place: Option<BugPlace>) -> BugReport {
        BugReport {
            message: self.message,
            fields: self
                .fields
                .into_iter()
                .map(|(name, value)| (Cow::Borrowed(name), value))
                .collect(),
            file: Cow::Borrowed(self.file),
            line: self.line,
            place,
        }
    }
}

/// Whether any report waits; the Store's fast path.
#[inline(always)]
pub(crate) fn pending() -> bool {
    PENDING.load(Ordering::Relaxed) != 0
}

/// Gives the reports waiting untagged on this thread a place: they were made
/// by code that has just returned (a guard, an inline child, a lowerer).
#[inline(always)]
pub(crate) fn tag(phase: Phase, mount: usize) {
    if pending() {
        tag_slow(phase, mount);
    }
}

#[inline(never)]
fn tag_slow(phase: Phase, mount: usize) {
    REPORTS.with_borrow_mut(|reports| {
        for report in reports.iter_mut().filter(|r| r.tag.is_none()) {
            report.tag = Some((phase, mount));
        }
    });
}

/// Tags, as it drops, the reports its code made: placed around a guard, an
/// inline child or a lowerer, it keeps their place even if that code panics.
pub(crate) struct Attribute(pub(crate) Phase, pub(crate) usize);

impl Drop for Attribute {
    #[inline(always)]
    fn drop(&mut self) {
        tag(self.0, self.1);
    }
}

/// Takes the reports waiting on this thread.
pub(crate) fn take() -> Vec<Pending> {
    let taken = REPORTS.take();
    PENDING.fetch_sub(taken.len(), Ordering::Relaxed);
    taken
}

/// Collects this thread's reports for the life of the guard: a Store's root
/// input, or a `Harness` reduction. Nested scopes (a Store run from inside
/// another's handler) keep their reports apart: the outer scope's waiting
/// reports are set aside, which is needed only when some wait at all.
pub(crate) struct Scope {
    outer: Vec<Pending>,
}

impl Scope {
    #[inline]
    pub(crate) fn enter() -> Self {
        DEPTH.set(DEPTH.get() + 1);
        let outer = if pending() {
            REPORTS.take()
        } else {
            Vec::new()
        };
        Self { outer }
    }
}

impl Drop for Scope {
    #[inline]
    fn drop(&mut self) {
        DEPTH.set(DEPTH.get() - 1);
        if pending() || !self.outer.is_empty() {
            // Every exit takes its reports first; anything left (a panic out
            // of a Harness reducer) is dropped, and no longer counts.
            let left = REPORTS.replace(std::mem::take(&mut self.outer));
            PENDING.fetch_sub(left.len(), Ordering::Relaxed);
        }
    }
}

/// Reports a state the code believes impossible, then returns: the caller
/// carries on with its fallback. The same in every build.
///
/// ```
/// use causet_core::bug_condition;
///
/// fn remove(list: &mut Vec<u32>, index: usize) -> bool {
///     if index >= list.len() {
///         bug_condition!(index = index, len = list.len(); "the index is past the list");
///         return false; // the fallback
///     }
///     list.remove(index);
///     true
/// }
///
/// assert!(!remove(&mut vec![1], 5)); // outside a Store: printed to standard error
/// ```
///
/// Inside a Store's run the report reaches the observers (`Observer::bug`)
/// with its place. Elsewhere it goes to
/// the hook set by `set_bug_hook`. Tests check the reports after a run
/// (`BugLog`), so they exercise the same fallback production takes.
#[macro_export]
macro_rules! bug_condition {
    ($($name:ident = $value:expr),+ ; $($message:tt)+) => {
        $crate::report_bug(
            ::std::format!($($message)+),
            ::std::vec![$((::std::stringify!($name), ::std::format!("{:?}", $value))),+],
            ::std::file!(),
            ::std::line!(),
        )
    };
    ($($message:tt)+) => {
        $crate::report_bug(::std::format!($($message)+), ::std::vec::Vec::new(), ::std::file!(), ::std::line!())
    };
}
