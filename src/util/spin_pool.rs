use core::cell::{Cell, UnsafeCell};
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{spin_loop_hint, AtomicUsize, Ordering};

use std::thread::yield_now;

/// Spin Pool is a spin lock for a pool of object.
///
/// It's useful if you have multiple associate data you want protect and mutated along with the pool of objects.
/// (Making them all atomic could be a big cost)
///
/// If you have only the objects to protect please consider using a general atomic MPMC queue.
pub struct SpinPool<T> {
    state: AtomicUsize,
    inner: UnsafeCell<T>,
}

const LOCKED: usize = 1 << 0;
const FREE: usize = 1 << 1;
const EMPTY: usize = 1 << 2 | FREE;

impl<T> SpinPool<T> {
    #[inline]
    pub const fn new(t: T) -> Self {
        Self {
            state: AtomicUsize::new(FREE),
            inner: UnsafeCell::new(t),
        }
    }
}

/// SpinPoolAPI is a helper trait for interacting with the SpinPool.
pub trait SpinPoolAPI {
    type Item;

    fn is_empty(&self) -> bool;
}

/// Data in pool can only be mutated when holding the guard.
pub struct SpinPoolGuard<'a, T: SpinPoolAPI> {
    state: &'a AtomicUsize,
    inner: &'a mut T,
}

impl<T: SpinPoolAPI> Deref for SpinPoolGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<T: SpinPoolAPI> DerefMut for SpinPoolGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

// On drop of guard we read the length of pool and set state to free or empty accordingly.
impl<T: SpinPoolAPI> Drop for SpinPoolGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        let state = if self.inner.is_empty() { EMPTY } else { FREE };

        self.state.store(state, Ordering::Release);
    }
}

impl<T: SpinPoolAPI> SpinPool<T> {
    #[inline]
    fn guard(&self) -> SpinPoolGuard<'_, T> {
        SpinPoolGuard {
            state: &self.state,
            inner: unsafe { &mut *self.inner.get() },
        }
    }

    /// try to lock the pool immediately.
    ///
    /// We return a `Result` instead of `Option` for the purpose of keep a consistency with std lib's locks
    #[inline]
    pub fn try_lock(&self) -> Result<SpinPoolGuard<'_, T>, ()> {
        if self.state.fetch_or(LOCKED, Ordering::Acquire) & LOCKED == 0 {
            return Ok(self.guard());
        }
        Err(())
    }

    /// try to lock the pool immediately if the pool is unlocked and not empty.
    #[inline]
    pub fn try_lock_2(&self) -> Result<SpinPoolGuard<'_, T>, ()> {
        if self.state.compare_and_swap(FREE, LOCKED, Ordering::Acquire) == FREE {
            return Ok(self.guard());
        }

        Err(())
    }

    /// spin wait for a lock and we want to back off after a certain amount of cycle.
    #[inline]
    pub fn lock(&self) -> SpinPoolGuard<'_, T> {
        let backoff = Backoff::new();
        while self.state.fetch_or(LOCKED, Ordering::Acquire) & LOCKED != 0 {
            backoff.snooze();
        }
        self.guard()
    }
}

/// SpinPool would be thread safe to share if the inner `T` is `Send`
unsafe impl<T: Send> Send for SpinPool<T> {}
unsafe impl<T: Send> Sync for SpinPool<T> {}

/// A simple backoff
struct Backoff {
    step: Cell<u32>,
}

const SPIN_LIMIT: u32 = 6;

impl Backoff {
    #[inline]
    pub fn new() -> Self {
        Backoff { step: Cell::new(0) }
    }

    #[inline]
    pub fn snooze(&self) {
        if self.step.get() <= SPIN_LIMIT {
            for _ in 0..1 << self.step.get() {
                spin_loop_hint();
            }
        } else {
            return yield_now();
        }

        self.step.set(self.step.get() + 1);
    }
}
