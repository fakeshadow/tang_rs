use core::cell::Cell;
use core::ops::{Deref, DerefMut};

use std::collections::VecDeque;

use crate::util::pool_error::PopError;

/// `CellPool` is just a wrapper for `Cell/UnsafeCell` and would panic at runtime if concurrent access happen
pub struct CellPool<T> {
    state: Cell<State<T>>,
}

enum State<T> {
    Free(StateInner<T>),
    Locked,
}

struct StateInner<T> {
    max_size: usize,
    active: usize,
    conn: VecDeque<T>,
}

impl<T> CellPool<T> {
    #[inline]
    pub fn new(max_size: usize) -> Self {
        Self {
            state: Cell::new(State::Free(StateInner {
                max_size,
                active: 0,
                conn: VecDeque::with_capacity(max_size),
            })),
        }
    }
}

struct CellPoolGuard<'a, T> {
    pool: &'a CellPool<T>,
    inner: Option<StateInner<T>>,
}

impl<T> Deref for CellPoolGuard<'_, T> {
    type Target = StateInner<T>;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl<T> DerefMut for CellPoolGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap()
    }
}

impl<T> Drop for CellPoolGuard<'_, T> {
    fn drop(&mut self) {
        self.pool
            .state
            .replace(State::Free(self.inner.take().unwrap()));
    }
}

impl<T> CellPool<T> {
    #[inline]
    fn guard(&self) -> CellPoolGuard<'_, T> {
        match self.state.replace(State::Locked) {
            State::Locked => panic!("Concurrent access to Cell pool is not allowed"),
            State::Free(inner) => CellPoolGuard {
                pool: &self,
                inner: Some(inner),
            },
        }
    }

    #[inline]
    pub(crate) fn push_back(&self, value: T) {
        let mut guard = self.guard();

        guard.conn.push_back(value);
    }

    #[inline]
    pub(crate) fn pop(&self) -> Result<T, PopError<T>> {
        let mut guard = self.guard();

        match guard.conn.pop_front() {
            Some(conn) => Ok(conn),
            None => {
                if guard.active < guard.max_size {
                    guard.active += 1;
                    Err(PopError::spawn_now(self))
                } else {
                    Err(PopError::Empty)
                }
            }
        }
    }

    // would return new active count
    pub(crate) fn dec_active(&self, count: usize) -> usize {
        let mut guard = self.guard();

        guard.active -= count;

        guard.active
    }

    /// Returns items in queue, active ount in tuple
    pub(crate) fn state(&self) -> (usize, usize) {
        let guard = self.guard();

        (guard.conn.len(), guard.active)
    }
}
