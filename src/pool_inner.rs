use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll};

#[cfg(feature = "no-send")]
use crate::util::cell_pool::{CellPool, CellPoolGuard};
use crate::util::linked_list::{
    linked_list_lock::{WakerListGuard, WakerListLock},
    WakerList,
};
use crate::util::lock_free_pool::{PoolInner, PopError};
use crate::{
    builder::Builder,
    manager::Manager,
    pool::{IdleConn, PoolRefBehavior},
    SharedManagedPool,
};

pub(crate) struct PoolLock<M: Manager> {
    inner: PoolInner<IdleConn<M>>,
    waiters: WakerListLock<WakerList>,
    config: Config,
}

impl<M: Manager> PoolLock<M> {
    pub(crate) fn from_builder(builder: &Builder) -> Self {
        Self {
            inner: PoolInner::new(builder.max_size),
            waiters: WakerListLock::new(WakerList::new()),
            config: Config::from_builder(builder),
        }
    }

    #[inline]
    pub(crate) fn lock_waiter(&self) -> WakerListGuard<'_, WakerList> {
        self.waiters.lock()
    }
}

pub(crate) struct Config {
    marker: AtomicUsize,
    min_idle: AtomicUsize,
    max_size: AtomicUsize,
}

impl Config {
    pub(crate) fn from_builder(builder: &Builder) -> Self {
        Self {
            marker: AtomicUsize::new(0),
            min_idle: AtomicUsize::new(builder.min_idle),
            max_size: AtomicUsize::new(builder.max_size),
        }
    }
}

impl<M: Manager> PoolLock<M> {
    #[inline]
    pub(crate) fn lock<'a, R>(
        &'a self,
        shared_pool: &'a SharedManagedPool<M>,
    ) -> PoolLockFuture<'a, M, R>
    where
        R: PoolRefBehavior<'a, M>,
    {
        PoolLockFuture {
            shared_pool,
            pool_lock: self,
            wait_key: None,
            _r: PhantomData,
        }
    }

    // add pending directly to pool inner if we try to spawn new connections.
    // and return the new pending count as option to notify the Pool to replenish connections
    pub(crate) fn dec_active(&self, marker: usize, should_spawn_new: bool) -> Option<usize> {
        let total = self.inner.dec_active(1) - 1;
        let self_marker = self.config.marker.load(Ordering::SeqCst);
        let self_min = self.config.min_idle.load(Ordering::SeqCst);

        if total < self_min && should_spawn_new && marker == self_marker {
            let pending_new = self_min - total;
            self.inner.inc_pending(pending_new);
            Some(pending_new)
        } else {
            None
        }
    }

    pub(crate) fn dec_pending(&self, count: usize) {
        self.inner.dec_pending(count);
    }

    pub(crate) fn inc_pending(&self, count: usize) {
        self.inner.inc_pending(count);
    }

    #[inline]
    pub(crate) fn put_back(&self, conn: IdleConn<M>) {
        let _ = self.inner.push_back(conn);
        self.lock_waiter().wake_one();
    }

    pub(crate) fn put_back_inc_spawned(&self, conn: IdleConn<M>) {
        let _ = self.inner.push_new(conn);
        self.lock_waiter().wake_one();
    }

    pub(crate) fn clear(&self) {}

    pub(crate) fn set_max_size(&self, size: usize) {
        self.config.max_size.store(size, Ordering::Relaxed);
    }

    fn max_size(&self) -> usize {
        self.config.max_size.load(Ordering::Relaxed)
    }

    pub(crate) fn set_min_idle(&self, size: usize) {
        self.config.min_idle.store(size, Ordering::Relaxed);
    }

    fn min_idle(&self) -> usize {
        self.config.min_idle.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn marker(&self) -> usize {
        self.config.marker.load(Ordering::Relaxed)
    }

    #[inline]
    fn spawn_idle_conn(&self, shared_pool: &SharedManagedPool<M>) {
        let shared_clone = shared_pool.clone();
        shared_pool.spawn(async move {
            let _ = shared_clone.add_idle_conn(0).await;
        });
    }

    pub(crate) fn state(&self) -> State {
        let (idle_connections, connections, pending_connections) = self.inner.state();

        State {
            connections,
            idle_connections,
            pending_connections,
        }
    }
}

pub(crate) struct PoolLockFuture<'a, M: Manager, R>
where
    M: Manager,
    R: PoolRefBehavior<'a, M>,
{
    shared_pool: &'a SharedManagedPool<M>,
    pool_lock: &'a PoolLock<M>,
    wait_key: Option<NonZeroUsize>,
    _r: PhantomData<R>,
}

impl<'a, M, R> Drop for PoolLockFuture<'a, M, R>
where
    M: Manager,
    R: PoolRefBehavior<'a, M>,
{
    fn drop(&mut self) {
        if let Some(wait_key) = self.wait_key {
            self.wake_cold(wait_key);
        }
    }
}

impl<'a, M: Manager, R> PoolLockFuture<'a, M, R>
where
    R: PoolRefBehavior<'a, M>,
{
    #[cold]
    fn wake_cold(&self, wait_key: NonZeroUsize) {
        let mut waiters = self.pool_lock.lock_waiter();

        let wait_key = unsafe { waiters.remove(wait_key) };

        if wait_key.is_none() {
            // We were awoken but didn't acquire the lock. Wake up another task.
            waiters.wake_one();
        }
    }
}

impl<'re, M, R> Future for PoolLockFuture<'re, M, R>
where
    M: Manager,
    R: PoolRefBehavior<'re, M> + Unpin,
{
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let shared_pool = self.shared_pool;

        // we return pending directly if the pool is in pausing state.
        if !shared_pool.is_running() {
            return Poll::Pending;
        }

        let pool_lock = self.pool_lock;

        match pool_lock.inner.pop() {
            Ok(conn) => {
                // remove self from waiter list
                if let Some(wait_key) = self.wait_key {
                    unsafe { pool_lock.lock_waiter().remove(wait_key) };
                    self.wait_key = None;
                }

                Poll::Ready(R::from_idle(conn, shared_pool))
            }
            Err(e) => {
                // spawn according to error.
                if PopError::SpawnNow == e {
                    pool_lock.spawn_idle_conn(shared_pool);
                }

                // insert or update wait key.
                match self.wait_key {
                    None => {
                        let wait_key = pool_lock.lock_waiter().insert(Some(cx.waker().clone()));

                        self.wait_key = Some(wait_key);
                    }
                    Some(wait_key) => {
                        let mut waiters = pool_lock.lock_waiter();
                        let opt = unsafe { waiters.get(wait_key) };
                        // We replace the waker if we are woken and have no waker in waiter list or have a new context which will not wake the old waker.
                        if opt
                            .as_ref()
                            .map(|waker| !waker.will_wake(cx.waker()))
                            .unwrap_or_else(|| true)
                        {
                            *opt = Some(cx.waker().clone());
                        }
                    }
                }

                Poll::Pending
            }
        }
    }
}

#[cfg(not(feature = "no-send"))]
unsafe impl<M: Manager + Send> Send for PoolLock<M> {}

#[cfg(not(feature = "no-send"))]
unsafe impl<M: Manager + Send> Sync for PoolLock<M> {}

pub struct State {
    pub connections: usize,
    pub idle_connections: usize,
    pub pending_connections: usize,
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("State")
            .field("connections", &self.connections)
            .field("idle_connections", &self.idle_connections)
            .field("pending_connections", &self.pending_connections)
            .finish()
    }
}
