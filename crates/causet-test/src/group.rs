//! Invariants over several nodes: each node publishes a view of its State
//! into a named group, and the group's checks read the views, after every
//! root input of any member.

use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::generate;

/// Why a group's checks run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupStep<'a> {
    /// The check was just added, on the views as they are.
    Added,
    /// `node` joined the group with its first view.
    Published {
        /// The node's name.
        node: &'a str,
    },
    /// A root input of `node` settled, and its views were refreshed.
    Settled {
        /// The node's name.
        node: &'a str,
    },
    /// `node` restarted, and its view is the restored State's.
    Restarted {
        /// The node's name.
        node: &'a str,
    },
}

/// The views of a group's members, by node name.
pub struct Views<'a>(&'a BTreeMap<String, Box<dyn Any>>);

impl Views<'_> {
    /// The view `node` published, or `None` if it has not, or has gone.
    /// Panics if the view is not a `V`: a test's mistake.
    pub fn get<V: 'static>(&self, node: &str) -> Option<&V> {
        let view = self.0.get(node)?;
        Some(view.downcast_ref().unwrap_or_else(|| {
            panic!(
                "the view of {node:?} is not a {}",
                std::any::type_name::<V>()
            )
        }))
    }

    /// The members' names, in order.
    pub fn nodes(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }
}

type GroupCheck = Box<dyn FnMut(&Views<'_>, &GroupStep<'_>) -> Result<(), String>>;

struct Inner {
    name: String,
    views: BTreeMap<String, Box<dyn Any>>,
    checks: Vec<(&'static str, GroupCheck)>,
}

/// A named group of nodes whose views shared checks read (tests only).
/// Cloning gives another handle on the same group.
#[derive(Clone)]
pub struct Group(Rc<RefCell<Inner>>);

impl Group {
    /// An empty group; `name` is part of its failures' keys.
    pub fn new(name: &str) -> Self {
        Self(Rc::new(RefCell::new(Inner {
            name: name.to_owned(),
            views: BTreeMap::new(),
            checks: Vec::new(),
        })))
    }

    /// The group's name.
    pub fn name(&self) -> String {
        self.0.borrow().name.clone()
    }

    /// A shared invariant: what is wrong across the views, if anything. It
    /// runs now, then whenever a member publishes, settles a root input or
    /// restarts. A violation panics with the key
    /// `group <group>: invariant "<name>"`; a panic of the check itself is
    /// keyed `... panicked`. Its captures are its memory.
    pub fn check(
        &self,
        name: &'static str,
        check: impl FnMut(&Views<'_>, &GroupStep<'_>) -> Result<(), String> + 'static,
    ) {
        self.0.borrow_mut().checks.push((name, Box::new(check)));
        let last = self.0.borrow().checks.len() - 1;
        self.run_from(last, GroupStep::Added);
    }

    /// The same group as `other`.
    pub(crate) fn same(&self, other: &Group) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }

    /// `node` joins with its first view. Panics if a live node already
    /// published under that name.
    pub(crate) fn join(&self, node: &str, view: Box<dyn Any>) -> Membership {
        {
            let mut inner = self.0.borrow_mut();
            assert!(
                !inner.views.contains_key(node),
                "group {}: node {node:?} has already published",
                inner.name
            );
            inner.views.insert(node.to_owned(), view);
        }
        // Held before the checks run: if one fails, its unwinding drops the
        // membership, and the view goes with it.
        let membership = Membership {
            group: self.clone(),
            node: node.to_owned(),
        };
        self.run(GroupStep::Published { node });
        membership
    }

    /// Replaces `node`'s view, without running checks.
    pub(crate) fn update(&self, node: &str, view: Box<dyn Any>) {
        self.0.borrow_mut().views.insert(node.to_owned(), view);
    }

    /// Runs every check.
    pub(crate) fn run(&self, step: GroupStep<'_>) {
        self.run_from(0, step);
    }

    fn run_from(&self, first: usize, step: GroupStep<'_>) {
        let mut inner = self.0.borrow_mut();
        let Inner {
            name: group,
            views,
            checks,
        } = &mut *inner;
        let views = Views(views);
        for (name, check) in &mut checks[first..] {
            // Named before it runs, so a panic of the check is keyed too.
            generate::failing(format!("group {group}: invariant {name:?} panicked"));
            let result = check(&views, &step);
            generate::not_failing();
            if let Err(e) = result {
                generate::failing(format!("group {group}: invariant {name:?}"));
                panic!(
                    "group {group}, {}: invariant {name:?}: {e}",
                    describe(&step)
                );
            }
        }
    }
}

fn describe(step: &GroupStep<'_>) -> String {
    match step {
        GroupStep::Added => "when the check was added".to_owned(),
        GroupStep::Published { node } => format!("when {node} published"),
        GroupStep::Settled { node } => format!("after a root input of {node}"),
        GroupStep::Restarted { node } => format!("when {node} restarted"),
    }
}

/// A node's place in a group: dropping it removes the node's view, without
/// running checks.
pub(crate) struct Membership {
    pub(crate) group: Group,
    pub(crate) node: String,
}

impl Drop for Membership {
    fn drop(&mut self) {
        // A check panicking with the group borrowed drops nodes as it
        // unwinds: their views go with the group then.
        if let Ok(mut inner) = self.group.0.try_borrow_mut() {
            inner.views.remove(&self.node);
        }
    }
}
