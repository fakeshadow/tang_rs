use crate::pool::PoolRefBehavior;
use crate::{Manager, SharedManagedPool};

// use a guard type to monitor the spawn result.
// this is necessary as spawn process is a future that can be canceled.
pub(crate) struct SpawnGuard<'a, M>
where
    M: Manager,
{
    shared_pool: &'a SharedManagedPool<M>,
    fulfilled: bool,
}

impl<'a, M> SpawnGuard<'a, M>
where
    M: Manager,
{
    pub(crate) fn new(shared_pool: &'a SharedManagedPool<M>) -> Self {
        Self {
            shared_pool,
            fulfilled: false,
        }
    }

    pub(crate) async fn add<R>(mut self) -> Result<R, M::Error>
    where
        R: PoolRefBehavior<'a, M>,
    {
        let p = self.shared_pool;

        p.add().await.map(|conn| {
            self.fulfilled = true;
            R::from_conn(conn, p)
        })
    }
}

impl<M> Drop for SpawnGuard<'_, M>
where
    M: Manager,
{
    fn drop(&mut self) {
        if !self.fulfilled {
            self.shared_pool.dec_active();
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
    pub(crate) fn new(shared_pool: &SharedManagedPool<M>) -> Self {
        Self {
            shared_pool: shared_pool.clone(),
            fulfilled: false,
        }
    }

    pub(crate) async fn add(mut self) {
        let p = &self.shared_pool;
        if let Ok(conn) = p.add().await {
            p.push_back(conn);
            self.fulfilled = true;
        };
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
