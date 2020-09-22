use core::cell::Cell;
use core::ops::{Deref, DerefMut};

use std::collections::VecDeque;

use crate::util::pool_error::PopError;
use std::cell::UnsafeCell;

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
    #[inline]
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

    #[inline]
    pub(crate) fn push_back(&self, value: T) {
        let guard = self.guard();

        guard.inner.conn.push_back(value);
    }

    #[inline]
    pub(crate) fn pop(&self) -> Result<T, PopError> {
        let mut guard = self.guard();

        match guard.inner.conn.pop_front() {
            Some(conn) => Ok(conn),
            None => {
                if guard.inner.active < guard.inner.max_size {
                    guard.inner.active += 1;
                    Err(PopError::SpawnNow)
                } else {
                    Err(PopError::Empty)
                }
            }
        }
    }

    // would return new active count
    pub(crate) fn dec_active(&self, count: usize) -> usize {
        let mut guard = self.guard();

        guard.inner.active -= count;

        guard.inner.active
    }

    pub(crate) fn inc_active(&self, count: usize) {
        let mut guard = self.guard();
        guard.inner.active += count;
    }

    /// Returns items in queue, active ount in tuple
    pub(crate) fn state(&self) -> (usize, usize) {
        let guard = self.guard();

        (guard.inner.conn.len(), guard.inner.active)
    }
}
