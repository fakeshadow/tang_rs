use core::cell::{Cell, UnsafeCell};
use core::ops::{Deref, DerefMut};

/// `CellPool` is just a wrapper for `Cell/UnsafeCell` and would panic at runtime if concurrent access happen
pub struct CellPool<T> {
    // false means pool is free. true means pool is locked
    state: Cell<bool>,
    inner: UnsafeCell<T>,
}

impl<T> CellPool<T> {
    #[inline]
    pub const fn new(t: T) -> Self {
        Self {
            state: Cell::new(false),
            inner: UnsafeCell::new(t),
        }
    }
}

/// `T` can only be accessed through `CellPoolGuard`
pub struct CellPoolGuard<'a, T> {
    pool: &'a CellPool<T>,
    inner: &'a mut T,
}

impl<T> Drop for CellPoolGuard<'_, T> {
    fn drop(&mut self) {
        self.pool.state.set(false);
    }
}

impl<T> Deref for CellPoolGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<T> DerefMut for CellPoolGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

impl<T> CellPool<T> {
    #[inline]
    fn guard(&self) -> CellPoolGuard<'_, T> {
        CellPoolGuard {
            pool: self,
            inner: unsafe { &mut *self.inner.get() },
        }
    }

    /// try to lock the pool immediately.
    ///
    /// We return a `Result` instead of `Option` for the purpose of keep a consistency with std lib's locks
    #[inline]
    pub fn try_lock(&self) -> Result<CellPoolGuard<'_, T>, ()> {
        if self.state.replace(true) == false {
            return Ok(self.guard());
        }
        Err(())
    }

    /// lock the pool immediately. Will panic if the pool is already locked by others.
    #[inline]
    pub fn lock(&self) -> CellPoolGuard<'_, T> {
        self.try_lock()
            .expect("CellPool is already locked elsewhere")
    }
}
