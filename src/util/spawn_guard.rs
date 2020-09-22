use crate::pool_inner::PoolInner;
use crate::{Manager, SharedManagedPool};

// use a guard type to monitor the spawn result.
// this is necessary as spawn process is a future that can be canceled.
pub(crate) struct SpawnGuard<'a, M>
where
    M: Manager,
{
    pool_inner: &'a PoolInner<M>,
    fulfilled: bool,
}

impl<'a, M> SpawnGuard<'a, M>
where
    M: Manager,
{
    pub(crate) fn new(pool_inner: &'a PoolInner<M>) -> Self {
        Self {
            pool_inner,
            fulfilled: false,
        }
    }

    pub(crate) fn fulfilled(&mut self) {
        self.fulfilled = true;
    }
}

impl<M> Drop for SpawnGuard<'_, M>
where
    M: Manager,
{
    fn drop(&mut self) {
        if !self.fulfilled {
            self.pool_inner.dec_active();
        }
    }
}

// owned version of spawn guard which is used to send to another thread.
pub(crate) struct SpawnGuardOwned<M>
where
    M: Manager,
{
    shared_pool: SharedManagedPool<M>,
    fulfilled: bool,
}

impl<M> SpawnGuardOwned<M>
where
    M: Manager,
{
    pub(crate) fn new(shared_pool: SharedManagedPool<M>) -> Self {
        Self {
            shared_pool,
            fulfilled: false,
        }
    }

    pub(crate) fn shared_pool(&self) -> &SharedManagedPool<M> {
        &self.shared_pool
    }

    pub(crate) fn fulfilled(&mut self) {
        self.fulfilled = true;
    }
}

impl<M> Drop for SpawnGuardOwned<M>
where
    M: Manager,
{
    fn drop(&mut self) {
        if !self.fulfilled {
            self.shared_pool.dec_active();
        }
    }
}
