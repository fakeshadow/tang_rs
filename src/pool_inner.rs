use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll};

#[cfg(feature = "no-send")]
use crate::util::cell_pool::CellPool;

#[cfg(not(feature = "no-send"))]
use crate::util::lock_free_pool::PoolInner;

use crate::util::{
    backoff::Backoff,
    linked_list::{
        linked_list_lock::{WakerListGuard, WakerListLock},
        WakerList,
    },
    pool_error::PopError,
};
use crate::{
    builder::Builder,
    manager::Manager,
    pool::{DropAndSpawn, IdleConn, PoolRefBehavior},
    SharedManagedPool,
};

macro_rules! pool_lock {
    ($pool_ty: ident) => {
        pub(crate) struct PoolLock<M: Manager> {
            inner: $pool_ty<IdleConn<M>>,
            waiters: WakerListLock<WakerList>,
            state: PoolLockState,
        }

        impl<M: Manager> PoolLock<M> {
            pub(crate) fn from_builder(builder: &Builder) -> Self {
                Self {
                    inner: $pool_ty::new(builder.max_size),
                    waiters: WakerListLock::new(WakerList::new()),
                    state: PoolLockState::from_builder(builder),
                }
            }

            #[inline]
            pub(crate) fn lock_waiter(&self) -> WakerListGuard<'_, WakerList> {
                self.waiters.lock()
            }
        }
    };
}

#[cfg(not(feature = "no-send"))]
pool_lock!(PoolInner);

#[cfg(feature = "no-send")]
pool_lock!(CellPool);

pub(crate) struct PoolLockState {
    min_idle: usize,
}

impl PoolLockState {
    fn from_builder(builder: &Builder) -> Self {
        Self {
            min_idle: builder.min_idle,
        }
    }
}

impl<M: Manager> PoolLock<M> {
    #[inline]
    pub(crate) async fn lock<'a, R>(&'a self, shared_pool: &'a SharedManagedPool<M>) -> R
    where
        R: PoolRefBehavior<'a, M> + Unpin,
    {
        let res = self
            .inner
            .pop()
            .map(|conn| R::from_idle(conn, shared_pool))
            .map_err(|e| {
                // spawn according to error.
                if e.is_spawn_now() {
                    shared_pool.spawn_idle();
                }
            });

        match res {
            Ok(r) => r,
            Err(_) => {
                PoolLockFuture {
                    shared_pool,
                    pool_lock: self,
                    wait_key: None,
                    _r: PhantomData,
                }
                .await
            }
        }
    }

    // ToDo: for now this call does not check the running state of pool inner.
    pub(crate) fn lock_sync<'a, R>(&'a self, shared_pool: &'a SharedManagedPool<M>) -> R
    where
        R: PoolRefBehavior<'a, M>,
    {
        let backoff = Backoff::new();

        loop {
            match self.inner.pop() {
                Ok(conn) => {
                    return R::from_idle(conn, shared_pool);
                }
                Err(e) => {
                    if PopError::SpawnNow == e {
                        shared_pool.spawn_idle();
                    }
                }
            }
            backoff.snooze();
        }
    }

    // add pending directly to pool inner if we try to spawn new connections.
    // and return the new pending count as option to notify the Pool to replenish connections
    pub(crate) fn dec_active(&self) -> Option<usize> {
        let total = self.inner.dec_active(1);
        let self_min = self.min_idle();

        if total < self_min {
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
    pub(crate) fn push_back(&self, conn: IdleConn<M>) {
        self.inner.push_back(conn);
        self.lock_waiter().wake_one();
    }

    pub(crate) fn push_new(&self, conn: IdleConn<M>) {
        self.inner.push_new(conn);
        self.lock_waiter().wake_one();
    }

    fn min_idle(&self) -> usize {
        self.state.min_idle
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
        let pool_lock = self.pool_lock;

        // insert or update wait key.
        match self.wait_key {
            None => {
                let wait_key = pool_lock.lock_waiter().insert(Some(cx.waker().clone()));
                self.wait_key = Some(wait_key);
            }
            Some(wait_key) => {
                match pool_lock.inner.pop() {
                    Ok(conn) => {
                        let r = R::from_idle(conn, shared_pool);

                        // remove self from waiter list
                        unsafe { pool_lock.lock_waiter().remove(wait_key) };
                        self.wait_key = None;

                        return Poll::Ready(r);
                    }
                    Err(e) => {
                        // spawn according to error.
                        if e.is_spawn_now() {
                            shared_pool.spawn_idle();
                        }

                        // insert or update wait key.
                        let mut waiters = pool_lock.lock_waiter();
                        let opt = unsafe { waiters.get(wait_key) };
                        // We replace the waker if we are woken and have no waker in waiter list or have a new context which will not wake the old waker.
                        if opt
                            .as_ref()
                            .map(|waker| !waker.will_wake(cx.waker()))
                            .unwrap_or(true)
                        {
                            *opt = Some(cx.waker().clone());
                        }
                    }
                }
            }
        }

        Poll::Pending
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
