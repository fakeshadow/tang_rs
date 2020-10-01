use core::cell::{Cell, UnsafeCell};
use core::ops::{Deref, DerefMut};

use std::collections::VecDeque;

/// `CellPool` is just a wrapper for `Cell/UnsafeCell` and would panic at runtime if concurrent access happen
pub struct CellPool<T> {
    state: Cell<bool>,
    inner: UnsafeCell<CellPoolInner<T>>,
}

struct CellPoolInner<T> {
    max_size: usize,
    active: usize,
    conn: VecDeque<T>,
}

impl<T> CellPool<T> {
    #[inline]
    pub fn new(max_size: usize) -> Self {
        Self {
            state: Cell::new(false),
            inner: UnsafeCell::new(CellPoolInner {
                max_size,
                active: 0,
                conn: VecDeque::with_capacity(max_size),
            }),
        }
    }
}

struct CellPoolGuard<'a, T> {
    pool: &'a CellPool<T>,
    inner: &'a mut CellPoolInner<T>,
}

impl<T> Deref for CellPoolGuard<'_, T> {
    type Target = CellPoolInner<T>;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<T> DerefMut for CellPoolGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

impl<T> Drop for CellPoolGuard<'_, T> {
    fn drop(&mut self) {
        self.pool.state.set(false);
    }
}

impl<T> CellPool<T> {
    fn guard(&self) -> CellPoolGuard<'_, T> {
        match self.state.replace(true) {
            true => panic!("Concurrent access to Cell pool is not allowed"),
            false => CellPoolGuard {
                pool: &self,
                // # Safety: Concurrent access to inner would cause panic as state is already true.
                inner: unsafe { &mut *self.inner.get() },
            },
        }
    }

    pub(crate) fn push_back(&self, value: T) {
        self.guard().inner.conn.push_back(value);
    }

    pub(crate) fn pop(&self) -> Option<T> {
        self.guard().inner.conn.pop_front()
    }

    // would return new active count
    pub(crate) fn dec_active(&self) -> usize {
        let mut guard = self.guard();

        guard.inner.active -= 1;

        guard.inner.active
    }

    pub(crate) fn try_inc_active(&self) -> bool {
        self.guard().try_inc_active()
    }

    /// Returns items in queue, active ount in tuple
    pub(crate) fn state(&self) -> (usize, usize) {
        let guard = self.guard();

        (guard.inner.conn.len(), guard.inner.active)
    }
}

impl<T> CellPoolGuard<'_, T> {
    fn try_inc_active(&mut self) -> bool {
        if self.inner.active < self.inner.max_size {
            self.inner.active += 1;
            true
        } else {
            false
        }
    }
}
