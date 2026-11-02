//! Bounded, allocation-free context trail for observability reports.
//!
//! This is a small ring buffer meant to capture recent, structured context
//! so that reports can include the last N entries when helpful.
//! After initialization, pushing entries does not allocate.

/// A fixed-capacity ring buffer of context entries.
///
/// - No allocations after `new` (except for the contained entries themselves).
/// - Oldest entries are overwritten when capacity is exceeded.
#[derive(Clone, Debug)]
pub struct ContextTrail<T> {
    buf: Vec<Option<T>>,
    head: usize,
    len: usize,
}

impl<T> ContextTrail<T> {
    /// Create a new trail with the given capacity.
    pub fn new(capacity: usize) -> Self {
        let mut buf = Vec::with_capacity(capacity);
        buf.resize_with(capacity, || None);
        Self {
            buf,
            head: 0,
            len: 0,
        }
    }

    /// Return the configured capacity.
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Return the number of entries currently stored.
    pub fn len(&self) -> usize {
        self.len
    }

    /// True if no entries are stored.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Remove all entries.
    pub fn clear(&mut self) {
        for slot in self.buf.iter_mut() {
            *slot = None;
        }
        self.head = 0;
        self.len = 0;
    }

    /// Push a new entry, overwriting the oldest if full.
    pub fn push(&mut self, item: T) {
        let cap = self.buf.len();
        if cap == 0 {
            return;
        }
        if let Some(slot) = self.buf.get_mut(self.head) {
            *slot = Some(item);
        }
        let next = self.head.wrapping_add(1);
        self.head = if next >= cap {
            next.wrapping_sub(cap)
        } else {
            next
        };
        if self.len < cap {
            self.len = self.len.saturating_add(1);
        }
    }

    /// Push a new entry and return a mutable reference to it.
    ///
    /// This is useful for callers that want to initialize an entry and then
    /// update it in-place without an additional lookup.
    pub fn push_and_get_mut(&mut self, item: T) -> Option<&mut T> {
        let cap = self.buf.len();
        if cap == 0 {
            return None;
        }

        let idx = self.head;
        let slot = self.buf.get_mut(idx)?;
        *slot = Some(item);
        let next = idx.wrapping_add(1);
        self.head = if next >= cap {
            next.wrapping_sub(cap)
        } else {
            next
        };
        if self.len < cap {
            self.len = self.len.saturating_add(1);
        }
        self.buf.get_mut(idx).and_then(|slot| slot.as_mut())
    }

    /// Find the first matching entry and return a mutable reference to it.
    ///
    /// This scans the entire buffer (capacity-bounded) and does not allocate.
    pub fn find_mut<F>(&mut self, mut predicate: F) -> Option<&mut T>
    where
        F: FnMut(&T) -> bool,
    {
        for slot in self.buf.iter_mut() {
            let Some(item) = slot.as_mut() else {
                continue;
            };
            if predicate(item) {
                return Some(item);
            }
        }
        None
    }

    /// Find the first matching entry, or push a new one and return it.
    ///
    /// This is capacity-bounded and allocation-free after initialization.
    pub fn get_or_push_mut<P, F>(&mut self, mut predicate: P, make: F) -> Option<&mut T>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce() -> T,
    {
        if let Some(idx) = self.buf.iter().position(|slot| match slot.as_ref() {
            Some(item) => predicate(item),
            None => false,
        }) {
            return self.buf.get_mut(idx).and_then(|slot| slot.as_mut());
        }

        self.push_and_get_mut(make())
    }

    /// Iterate from oldest to newest.
    pub fn iter(&self) -> ContextTrailIter<'_, T> {
        let cap = self.buf.len();
        let start = if self.len == 0 || cap == 0 {
            0
        } else {
            let back = self.head.wrapping_add(cap).wrapping_sub(self.len);
            if back >= cap {
                back.wrapping_sub(cap)
            } else {
                back
            }
        };
        ContextTrailIter {
            trail: self,
            idx: start,
            remaining: self.len,
        }
    }

    /// Iterate over the most recent `max` entries, oldest to newest.
    pub fn iter_recent(&self, max: usize) -> ContextTrailIter<'_, T> {
        let cap = self.buf.len();
        let count = self.len.min(max);
        let start = if count == 0 || cap == 0 {
            0
        } else {
            let back = self.head.wrapping_add(cap).wrapping_sub(count);
            if back >= cap {
                back.wrapping_sub(cap)
            } else {
                back
            }
        };
        ContextTrailIter {
            trail: self,
            idx: start,
            remaining: count,
        }
    }
}

/// Iterator over a trail's entries, oldest to newest, returned by
/// `ContextTrail::iter` and `ContextTrail::iter_recent`.
pub struct ContextTrailIter<'a, T> {
    trail: &'a ContextTrail<T>,
    idx: usize,
    remaining: usize,
}

impl<'a, T> Iterator for ContextTrailIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let cap = self.trail.buf.len();
        let value = self.trail.buf.get(self.idx).and_then(|slot| slot.as_ref());
        let next = self.idx.wrapping_add(1);
        self.idx = if cap == 0 {
            0
        } else if next >= cap {
            next.wrapping_sub(cap)
        } else {
            next
        };
        self.remaining = self.remaining.saturating_sub(1);
        value
    }
}

#[cfg(test)]
mod tests {
    use super::ContextTrail;

    #[test]
    fn trail_overwrites_oldest() {
        let mut trail = ContextTrail::new(3);
        trail.push(1);
        trail.push(2);
        trail.push(3);
        let items: Vec<_> = trail.iter().copied().collect();
        assert_eq!(items, vec![1, 2, 3]);

        trail.push(4);
        let items: Vec<_> = trail.iter().copied().collect();
        assert_eq!(items, vec![2, 3, 4]);
    }

    #[test]
    fn trail_recent_iterates_oldest_to_newest() {
        let mut trail = ContextTrail::new(4);
        trail.push(10);
        trail.push(11);
        trail.push(12);
        trail.push(13);
        trail.push(14);
        let items: Vec<_> = trail.iter_recent(2).copied().collect();
        assert_eq!(items, vec![13, 14]);
    }

    #[test]
    fn trail_clear_resets() {
        let mut trail = ContextTrail::new(2);
        trail.push(1);
        trail.push(2);
        trail.clear();
        assert!(trail.is_empty());
        let items: Vec<_> = trail.iter().copied().collect();
        assert!(items.is_empty());
    }

    #[test]
    fn zero_capacity_is_safe() {
        let mut trail: ContextTrail<u32> = ContextTrail::new(0);
        trail.push(1);
        assert!(trail.is_empty());
        assert_eq!(trail.capacity(), 0);
        let items: Vec<_> = trail.iter().collect();
        assert!(items.is_empty());
    }
}
