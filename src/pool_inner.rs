use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::collections::VecDeque;
use std::time::Instant;

#[cfg(feature = "no-send")]
use crate::util::cell_pool::{CellPool, CellPoolGuard};
use crate::util::linked_list::{
    linked_list_lock::{WakerListGuard, WakerListLock},
    WakerList,
};
#[cfg(not(feature = "no-send"))]
use crate::util::spin_pool::{PoolAPI, SpinPool, SpinPoolGuard};
use crate::{
    builder::Builder,
    manager::Manager,
    pool::{IdleConn, PoolRefBehavior},
    SharedManagedPool,
};

// PoolLock contains two locks(one for pooled objects and one for the waiters queue).
// By default they are spin locks and other locks can be used with macro impl.
macro_rules! pool_lock {
    (
        $pool_lock_type: ident,
        $pool_guard_type: ident,
        $pool_lock_method: ident,
        $pool_try_lock_method: ident,
        $waiter_lock_type: ident,
        $waiter_guard_type: ident,
        $waiter_lock_method: ident
        $(, $opt:ident)*
    ) => {
        pub(crate) struct PoolLock<M: Manager> {
            inner: $pool_lock_type<PoolInner<M>>,
            waiters: $waiter_lock_type<WakerList>,
            config: Config,
        }

        impl<M: Manager> PoolLock<M> {
            pub(crate) fn from_builder(builder: &Builder) -> Self {
                Self {
                    inner: $pool_lock_type::new(PoolInner::with_capacity(builder.max_size)),
                    waiters: $waiter_lock_type::new(WakerList::new()),
                    config: Config::from_builder(builder),
                }
            }

            #[inline]
            pub(crate) fn lock_inner(&self) -> $pool_guard_type<'_, PoolInner<M>> {
                self.inner.$pool_lock_method()
            }

            #[inline]
            pub(crate) fn try_lock_inner(&self) -> Option<$pool_guard_type<'_, PoolInner<M>>> {
                self.inner.$pool_try_lock_method().ok()
            }

            #[inline]
            pub(crate) fn lock_waiter(&self) -> $waiter_guard_type<'_, WakerList> {
                self.waiters.$waiter_lock_method()$(.$opt())*
            }
        }
    };
}

#[cfg(not(feature = "no-send"))]
pool_lock!(
    SpinPool,
    SpinPoolGuard,
    lock,
    try_lock,
    WakerListLock,
    WakerListGuard,
    lock
);

#[cfg(feature = "no-send")]
pool_lock!(
    CellPool,
    CellPoolGuard,
    lock,
    try_lock,
    WakerListLock,
    WakerListGuard,
    lock
);

#[cfg(not(feature = "no-send"))]
impl<M: Manager> PoolAPI for PoolInner<M> {
    fn is_empty(&self) -> bool {
        self.conn.is_empty()
    }
}

pub(crate) struct Config {
    min_idle: AtomicUsize,
    max_size: AtomicUsize,
}

impl Config {
    pub(crate) fn from_builder(builder: &Builder) -> Self {
        Self {
            min_idle: AtomicUsize::new(builder.min_idle),
            max_size: AtomicUsize::new(builder.max_size),
        }
    }
}

// pool inner holds all the objects and associate info
pub(crate) struct PoolInner<M: Manager> {
    spawned: usize,
    marker: usize,
    pending: VecDeque<Pending>,
    conn: VecDeque<IdleConn<M>>,
}

impl<M: Manager> PoolInner<M> {
    fn with_capacity(size: usize) -> Self {
        Self {
            spawned: 0,
            marker: 0,
            pending: VecDeque::with_capacity(size),
            conn: VecDeque::with_capacity(size),
        }
    }

    #[inline]
    fn marker(&self) -> usize {
        self.marker
    }

    fn incr_marker(&mut self) {
        self.marker += 1;
    }

    #[inline]
    fn total(&self) -> usize {
        self.spawned + self.pending.len()
    }

    #[inline]
    fn spawned(&self) -> usize {
        self.spawned
    }

    fn _incr_spawned(&mut self, count: usize) {
        self.spawned += count;
    }

    fn _dec_spawned(&mut self, count: usize) {
        self.spawned -= count;
    }

    fn pending_mut(&mut self) -> &mut VecDeque<Pending> {
        &mut self.pending
    }

    fn inc_pending(&mut self, count: usize) {
        for _i in 0..count {
            self.pending.push_back(Pending::new());
        }
    }

    fn _dec_pending(&mut self, count: usize) {
        for _i in 0..count {
            self.pending.pop_front();
        }
    }

    fn clear_pending(&mut self, pool_size: usize) {
        self.pending = VecDeque::with_capacity(pool_size);
    }

    fn conn_len(&self) -> usize {
        self.conn.len()
    }

    fn conn_mut(&mut self) -> &mut VecDeque<IdleConn<M>> {
        &mut self.conn
    }

    #[inline]
    fn pop_conn(&mut self) -> Option<IdleConn<M>> {
        self.conn.pop_front()
    }

    #[inline]
    fn push_conn(&mut self, conn: IdleConn<M>) {
        self.conn.push_back(conn);
    }

    fn clear_conn(&mut self, pool_size: usize) {
        self.conn = VecDeque::with_capacity(pool_size);
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
    pub(crate) fn dec_spawned(&self, marker: usize, should_spawn_new: bool) -> Option<usize> {
        let mut inner = self.lock_inner();

        inner._dec_spawned(1);

        let total = inner.total();
        let min_idle = self.min_idle();

        if total < min_idle && should_spawn_new && marker == inner.marker() {
            let pending_new = min_idle - total;
            inner.inc_pending(pending_new);
            Some(pending_new)
        } else {
            None
        }
    }

    pub(crate) fn dec_pending(&self, count: usize) {
        self.lock_inner()._dec_pending(count);
    }

    pub(crate) fn drop_pending<F>(&self, mut should_drop: F)
    where
        F: FnMut(&Pending) -> bool,
    {
        self.lock_inner()
            .pending_mut()
            .retain(|pending| !should_drop(pending));
    }

    // return new pending count and marker as Option<(usize, usize)>.
    pub(crate) fn try_drop_conn<F>(&self, mut should_drop: F) -> Option<(usize, usize)>
    where
        F: FnMut(&IdleConn<M>) -> bool,
    {
        self.try_lock_inner().and_then(|mut inner| {
            let len = inner.conn_len();

            inner.conn_mut().retain(|conn| !should_drop(conn));

            let diff = len - inner.conn_len();

            if diff > 0 {
                inner._dec_spawned(diff);
            }

            let total_now = inner.total();
            let min_idle = self.min_idle();

            if total_now < min_idle {
                let pending_new = min_idle - total_now;

                inner.inc_pending(pending_new);

                Some((pending_new, inner.marker()))
            } else {
                None
            }
        })
    }

    #[inline]
    pub(crate) fn put_back(&self, conn: IdleConn<M>) {
        let mut inner = self.lock_inner();

        let condition = inner.spawned() > self.max_size() || inner.marker() != conn.marker();

        if condition {
            inner._dec_spawned(1);
        } else {
            inner.push_conn(conn);
        }

        drop(inner);
        self.lock_waiter().wake_one_weak().wake();
    }

    pub(crate) fn put_back_inc_spawned(&self, conn: IdleConn<M>) {
        let mut inner = self.lock_inner();

        inner._dec_pending(1);

        let condition = inner.spawned() < self.max_size() && inner.marker() == conn.marker();

        if condition {
            inner.push_conn(conn);
            inner._incr_spawned(1);
        }

        drop(inner);
        self.lock_waiter().wake_one_weak().wake();
    }

    pub(crate) fn clear(&self) {
        let mut inner = self.lock_inner();
        let count = inner.conn_len();
        inner._dec_spawned(count);
        inner.incr_marker();

        let pool_size = self.max_size();
        inner.clear_pending(pool_size);
        inner.clear_conn(pool_size);
    }

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

    pub(crate) fn marker(&self) -> usize {
        self.lock_inner().marker()
    }

    pub(crate) fn state(&self) -> State {
        let mut inner = self.lock_inner();

        State {
            connections: inner.spawned(),
            idle_connections: inner.conn_len(),
            pending_connections: inner.pending_mut().iter().cloned().collect(),
        }
    }

    #[inline]
    fn spawn_idle_conn(&self, shared_pool: &SharedManagedPool<M>, inner: &mut PoolInner<M>) {
        if inner.total() < self.max_size() {
            let marker = inner.marker();
            let shared_clone = shared_pool.clone();
            shared_pool.spawn(async move {
                let _ = shared_clone.add_idle_conn(marker).await;
            });
            inner.inc_pending(1);
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
            let opt = waiters.wake_one_weak();
            drop(waiters);
            opt.wake();
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

        // Either insert our waker if we don't have a wait key yet or overwrite the old waker entry if we already have a wait key.
        match self.wait_key {
            Some(wait_key) => {
                // force lock if we are already in wait list
                let mut inner = pool_lock.lock_inner();

                if let Some(conn) = inner.pop_conn() {
                    drop(inner);

                    unsafe { pool_lock.lock_waiter().remove(wait_key) };
                    self.wait_key = None;

                    return Poll::Ready(R::from_idle(conn, shared_pool));
                }

                // We got pending so we spawn a new connection if we have not hit the max pool size.
                pool_lock.spawn_idle_conn(shared_pool, &mut inner);
                drop(inner);

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
            None => {
                #[cfg(not(feature = "no-send"))]
                {
                    // Safety: try_lock_2 only obtain lock when pool is not empty so the unwrap is safe.
                    if let Ok(mut guard) = pool_lock.inner.try_lock_2() {
                        return Poll::Ready(R::from_idle(guard.pop_conn().unwrap(), shared_pool));
                    }
                }
                #[cfg(feature = "no-send")]
                {
                    if let Some(mut guard) = pool_lock.try_lock_inner() {
                        if let Some(conn) = guard.pop_conn() {
                            return Poll::Ready(R::from_idle(conn, shared_pool));
                        }
                    }
                }

                let wait_key = pool_lock.lock_waiter().insert(Some(cx.waker().clone()));

                self.wait_key = Some(wait_key);
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
    pub pending_connections: Vec<Pending>,
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

trait WakerOpt {
    fn wake(self);
}

impl WakerOpt for Option<Waker> {
    fn wake(self) {
        if let Some(waker) = self {
            waker.wake();
        }
    }
}

#[derive(Debug, Clone)]
pub struct Pending {
    start_from: Instant,
}

impl Pending {
    fn new() -> Self {
        Pending {
            start_from: Instant::now(),
        }
    }

    pub(crate) fn should_remove(&self, connection_timeout: Duration) -> bool {
        Instant::now() > (self.start_from + connection_timeout * 6)
    }
}
