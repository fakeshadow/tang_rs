use core::cell::{Cell, UnsafeCell};
use core::hint::spin_loop;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicUsize, Ordering};
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

#[allow(clippy::identity_op)]
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

/// PoolAPI is a helper trait for interacting with the SpinPool.
pub trait PoolAPI {
    fn is_empty(&self) -> bool;
}

/// Data in pool can only be mutated when holding the guard.
pub struct SpinPoolGuard<'a, T: PoolAPI> {
    state: &'a AtomicUsize,
    inner: &'a mut T,
}

impl<T: PoolAPI> Deref for SpinPoolGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<T: PoolAPI> DerefMut for SpinPoolGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

// On drop of guard we read the length of pool and set state to free or empty accordingly.
impl<T: PoolAPI> Drop for SpinPoolGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        let state = if self.inner.is_empty() { EMPTY } else { FREE };

        self.state.store(state, Ordering::Release);
    }
}

impl<T: PoolAPI> SpinPool<T> {
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
        match self
            .state
            .compare_exchange(FREE, LOCKED, Ordering::Acquire, Ordering::Acquire)
        {
            Ok(free) if free == FREE => Ok(self.guard()),
            _ => Err(()),
        }
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

/// A simple backoff (`crossbeam-util::backoff::BackOff` with yield limit disabled.)
pub(super) struct Backoff {
    cycle: Cell<u8>,
}

const SPIN_LIMIT: u8 = 6;

impl Backoff {
    #[inline]
    pub(super) fn new() -> Self {
        Self {
            cycle: Cell::new(0),
        }
    }

    #[inline]
    pub(super) fn snooze(&self) {
        if self.cycle.get() <= SPIN_LIMIT {
            for _ in 0..1 << self.cycle.get() {
                spin_loop();
            }
        } else {
            return yield_now();
        }

        self.cycle.set(self.cycle.get() + 1);
    }
}
