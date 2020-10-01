use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll};

#[cfg(feature = "no-send")]
use crate::util::cell_pool::CellPool;

#[cfg(not(feature = "no-send"))]
use crate::util::atomic_pool::AtomicPool;

use crate::util::{
    backoff::Backoff,
    linked_list::{
        linked_list_lock::{WakerListGuard, WakerListLock},
        WakerList,
    },
    spawn_guard::SpawnGuard,
};
use crate::{
    builder::Builder,
    manager::Manager,
    pool::{Conn, DropAndSpawn, PoolRefBehavior},
    SharedManagedPool,
};

macro_rules! pool_inner {
    ($pool_ty: ident) => {
        pub(crate) struct PoolInner<M: Manager> {
            pool: $pool_ty<Conn<M>>,
            waiter: WakerListLock<WakerList>,
            state: PoolInnerState,
        }

        impl<M: Manager> PoolInner<M> {
            pub(crate) fn from_builder(builder: &Builder) -> Self {
                Self {
                    pool: $pool_ty::new(builder.max_size),
                    waiter: WakerListLock::new(WakerList::new()),
                    state: PoolInnerState::from_builder(builder),
                }
            }

            pub(crate) fn lock_waiter(&self) -> WakerListGuard<'_, WakerList> {
                self.waiter.lock()
            }
        }
    };
}

#[cfg(not(feature = "no-send"))]
pool_inner!(AtomicPool);

#[cfg(feature = "no-send")]
pool_inner!(CellPool);

pub(crate) struct PoolInnerState {
    min_idle: usize,
}

impl PoolInnerState {
    fn from_builder(builder: &Builder) -> Self {
        Self {
            min_idle: builder.min_idle,
        }
    }
}

impl<M> PoolInner<M>
where
    M: Manager,
{
    pub(crate) async fn get_inner<'a, R>(
        &'a self,
        shared_pool: &'a SharedManagedPool<M>,
    ) -> Result<R, M::Error>
    where
        R: PoolRefBehavior<'a, M> + Unpin,
    {
        // ToDo: async-std/smol would not compile with direct match.
        let res = self.pool.pop();

        match res {
            Some(conn) => Ok(R::from_conn(conn, shared_pool)),
            None => {
                if self.try_inc_active() {
                    SpawnGuard::new(shared_pool).add().await
                } else {
                    Ok(PoolInnerFuture {
                        shared_pool,
                        wait_key: None,
                        _r: PhantomData,
                    }
                    .await)
                }
            }
        }
    }

    pub(crate) fn get_inner_sync<'a, R>(&'a self, shared_pool: &'a SharedManagedPool<M>) -> R
    where
        R: PoolRefBehavior<'a, M>,
    {
        let backoff = Backoff::new();

        loop {
            match self.pool.pop() {
                Some(conn) => {
                    return R::from_conn(conn, shared_pool);
                }
                None => {
                    if self.try_inc_active() {
                        shared_pool.spawn_idle();
                    }
                }
            }
            backoff.snooze();
        }
    }

    // return true if we should spawn new.
    pub(crate) fn dec_active(&self) -> bool {
        self.pool.dec_active() < self.min_idle()
    }

    pub(crate) fn try_inc_active(&self) -> bool {
        self.pool.try_inc_active()
    }

    pub(crate) fn push_back(&self, conn: Conn<M>) {
        self.pool.push_back(conn);
        self.lock_waiter().wake_one();
    }

    fn min_idle(&self) -> usize {
        self.state.min_idle
    }

    pub(crate) fn state(&self) -> State {
        let (idle_connections, connections) = self.pool.state();

        State {
            connections,
            idle_connections,
        }
    }
}

pub(crate) struct PoolInnerFuture<'a, M, R>
where
    M: Manager,
    R: PoolRefBehavior<'a, M>,
{
    shared_pool: &'a SharedManagedPool<M>,
    wait_key: Option<NonZeroUsize>,
    _r: PhantomData<R>,
}

impl<'a, M, R> Drop for PoolInnerFuture<'a, M, R>
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

impl<'a, M: Manager, R> PoolInnerFuture<'a, M, R>
where
    R: PoolRefBehavior<'a, M>,
{
    #[cold]
    fn wake_cold(&self, wait_key: NonZeroUsize) {
        let mut waiters = self.shared_pool.inner().lock_waiter();
        // # Safety: The future is dropped after we register the waker to list, before it acquire a
        // connection. So we remove the wait_key here.
        let wait_key = unsafe { waiters.remove(wait_key) };

        if wait_key.is_none() {
            // We were awoken but didn't acquire the lock. Wake up another task.
            waiters.wake_one();
        }
    }
}

impl<'re, M, R> Future for PoolInnerFuture<'re, M, R>
where
    M: Manager,
    R: PoolRefBehavior<'re, M> + Unpin,
{
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let pool_inner = self.shared_pool.inner();

        // insert or update wait key.
        match self.wait_key {
            Some(wait_key) => {
                match pool_inner.pool.pop() {
                    Some(conn) => {
                        let r = R::from_conn(conn, self.shared_pool);

                        // # Safety:
                        // this unsafe would take a lock and release it when block is gone.
                        // we remove the wait_key by the NonZeroUsize we get when inserting.
                        unsafe { pool_inner.lock_waiter().remove(wait_key) };
                        self.wait_key = None;

                        return Poll::Ready(r);
                    }
                    None => {
                        if pool_inner.try_inc_active() {
                            self.shared_pool.spawn_idle();
                        }
                        // insert or update wait key.
                        // # Safety:
                        // the same as Ok variant of this match.
                        let mut waiters = pool_inner.lock_waiter();
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
            None => {
                let wait_key = pool_inner.lock_waiter().insert(Some(cx.waker().clone()));
                self.wait_key = Some(wait_key);
            }
        }

        Poll::Pending
    }
}

#[cfg(not(feature = "no-send"))]
unsafe impl<M: Manager + Send> Send for PoolInner<M> {}

#[cfg(not(feature = "no-send"))]
unsafe impl<M: Manager + Send> Sync for PoolInner<M> {}

pub struct State {
    pub connections: usize,
    pub idle_connections: usize,
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("State")
            .field("connections", &self.connections)
            .field("idle_connections", &self.idle_connections)
            .finish()
    }
}
