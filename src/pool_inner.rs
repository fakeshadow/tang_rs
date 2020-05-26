#[cfg(feature = "no-send")]
use std::cell::{RefCell, RefMut};
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
#[cfg(not(feature = "no-send"))]
use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::{
    manager::Manager,
    pool::{IdleConn, PoolRef},
    util::linked_list::WakerList,
    SharedManagedPool,
};

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

// PoolInner holds all the IdleConn and the waiters(a FIFO linked list) waiting for a connection.
pub(crate) struct PoolInner<M: Manager> {
    spawned: usize,
    marker: usize,
    pending: VecDeque<Pending>,
    conn: VecDeque<IdleConn<M>>,
    waiters: WakerList,
}

impl<M: Manager> PoolInner<M> {
    pub fn new(pool_size: usize) -> Self {
        Self {
            spawned: 0,
            marker: 0,
            pending: VecDeque::with_capacity(pool_size),
            conn: VecDeque::with_capacity(pool_size),
            waiters: WakerList::new(),
        }
    }

    #[inline]
    fn put_back(&mut self, conn: IdleConn<M>) {
        self.conn.push_back(conn);
    }

    fn incr_spawned(&mut self, count: usize) {
        self.spawned += count;
    }

    fn decr_spawned(&mut self, count: usize) {
        if self.spawned > count {
            self.spawned -= count
        } else {
            self.spawned = 0
        }
    }

    fn incr_pending(&mut self, count: usize) {
        for _i in 0..count {
            self.pending.push_back(Pending::new());
        }
    }

    fn decr_pending(&mut self, count: usize) {
        for _i in 0..count {
            self.pending.pop_front();
        }
    }

    #[inline]
    fn total(&mut self) -> usize {
        self.spawned + self.pending.len()
    }

    fn incr_marker(&mut self) {
        self.marker += 1;
    }

    fn clear_pending(&mut self, pool_size: usize) {
        self.pending = VecDeque::with_capacity(pool_size);
    }

    fn clear_conn(&mut self, pool_size: usize) {
        self.conn = VecDeque::with_capacity(pool_size);
    }
}

impl<M: Manager> PoolLock<M> {
    #[inline]
    pub(crate) fn lock<'a>(
        &'a self,
        shared_pool: &'a SharedManagedPool<M>,
    ) -> PoolLockFuture<'a, M> {
        PoolLockFuture {
            shared_pool,
            pool_lock: self,
            wait_key: None,
            acquired: false,
        }
    }

    // add pending directly to pool inner if we try to spawn new connections.
    // and return the new pending count as option to notify the Pool to replenish connections
    #[cfg(not(feature = "no-send"))]
    pub(crate) fn decr_spawned(
        &self,
        marker: usize,
        min_idle: usize,
        should_spawn_new: bool,
    ) -> Option<usize> {
        let mut inner = self._lock();

        inner.decr_spawned(1);
        let total = inner.total();

        if total < min_idle && should_spawn_new && marker == inner.marker {
            let pending_new = min_idle - total;
            inner.incr_pending(pending_new);
            Some(pending_new)
        } else {
            None
        }
    }

    // ToDo: currently we don't enable clear and pause function for single thread pool.
    #[cfg(feature = "no-send")]
    pub(crate) fn decr_spawned(&self, min_idle: usize) -> Option<usize> {
        let mut inner = self._lock();

        inner.decr_spawned(1);
        let total = inner.total();

        if total < min_idle {
            let pending_new = min_idle - total;
            inner.incr_pending(pending_new);
            Some(pending_new)
        } else {
            None
        }
    }

    pub(crate) fn decr_pending(&self, count: usize) {
        let mut inner = self._lock();
        inner.decr_pending(count);
    }

    pub(crate) fn drop_pendings<F>(&self, mut should_drop: F)
    where
        F: FnMut(&Pending) -> bool,
    {
        let mut inner = self._lock();

        let len = inner.pending.len();

        for index in 0..len {
            if let Some(pending) = inner.pending.get(index) {
                if should_drop(pending) {
                    inner.pending.remove(index);
                }
            }
        }
    }

    // return new pending count and marker as Option<(usize, usize)>.
    pub(crate) fn try_drop_conns<F>(
        &self,
        min_idle: usize,
        mut should_drop: F,
    ) -> Option<(usize, usize)>
    where
        F: FnMut(&IdleConn<M>) -> bool,
    {
        self._try_lock().and_then(|mut inner| {
            let len = inner.conn.len();
            for index in 0..len {
                if let Some(conn) = inner.conn.get(index) {
                    if should_drop(conn) {
                        inner.conn.remove(index);
                        inner.decr_spawned(1);
                    }
                }
            }

            let total_now = inner.total();
            if total_now < min_idle {
                let pending_new = min_idle - total_now;

                inner.incr_pending(pending_new);

                Some((pending_new, inner.marker))
            } else {
                None
            }
        })
    }

    #[inline]
    pub(crate) fn put_back(&self, conn: IdleConn<M>, max_size: usize) {
        let mut inner = self._lock();

        #[cfg(not(feature = "no-send"))]
        let condition = inner.spawned > max_size || inner.marker != conn.get_marker();

        // ToDo: currently we don't enable clear function for single thread pool.
        #[cfg(feature = "no-send")]
        let condition = inner.spawned > max_size;

        if condition {
            inner.decr_spawned(1);
        } else {
            inner.put_back(conn);
            let opt = inner.waiters.wake_one_weak();
            drop(inner);
            opt.wake();
        }
    }

    pub(crate) fn put_back_incr_spawned(&self, conn: IdleConn<M>, max_size: usize) {
        let mut inner = self._lock();

        inner.decr_pending(1);

        #[cfg(not(feature = "no-send"))]
        let condition = inner.spawned < max_size && inner.marker == conn.get_marker();

        // ToDo: currently we don't enable clear function for single thread pool.
        #[cfg(feature = "no-send")]
        let condition = inner.spawned < max_size;

        if condition {
            inner.put_back(conn);
            inner.incr_spawned(1);
            let opt = inner.waiters.wake_one_weak();
            drop(inner);
            opt.wake();
        }
    }

    pub(crate) fn clear(&self, pool_size: usize) {
        let mut inner = self._lock();
        let count = inner.conn.len();
        inner.decr_spawned(count);
        inner.incr_marker();
        inner.clear_pending(pool_size);
        inner.clear_conn(pool_size);
    }

    pub(crate) fn get_maker(&self) -> usize {
        let inner = self._lock();
        inner.marker
    }

    pub(crate) fn state(&self) -> State {
        let inner = self._lock();
        State {
            connections: inner.spawned,
            idle_connections: inner.conn.len(),
            pending_connections: inner.pending.iter().cloned().collect(),
        }
    }
}

// `PoolLockFuture` return a future of `PoolRef`. In the `Future` we pass it's `Waker` to `PoolLock`.
// Then when a `IdleConn` is returned to pool we lock the `PoolLock` and wake the `Wakers` inside it to notify other `PoolLockFuture` it's time to continue.
pub(crate) struct PoolLockFuture<'a, M: Manager> {
    shared_pool: &'a SharedManagedPool<M>,
    pool_lock: &'a PoolLock<M>,
    wait_key: Option<NonZeroUsize>,
    acquired: bool,
}

impl<M: Manager> Drop for PoolLockFuture<'_, M> {
    #[inline]
    fn drop(&mut self) {
        if let Some(wait_key) = self.wait_key {
            self.wake_cold(wait_key);
        }
    }
}

impl<'re, M: Manager> PoolLockFuture<'re, M> {
    #[cold]
    fn wake_cold(&self, wait_key: NonZeroUsize) {
        let mut inner = self.pool_lock._lock();
        let wait_key = unsafe { inner.waiters.remove(wait_key) };

        if wait_key.is_none() && !self.acquired {
            // We were awoken but didn't acquire the lock. Wake up another task.
            let opt = inner.waiters.wake_one_weak();
            drop(inner);
            opt.wake();
        }
    }
}

macro_rules! pool_lock {
    ($lock_type: ident, $guard_type: ident, $lock_method: ident, $try_lock_method: ident $(, $opt:ident)*) => {
        pub(crate) struct PoolLock<M: Manager> {
            inner: $lock_type<PoolInner<M>>,
        }

        impl<M: Manager> PoolLock<M> {
            pub(crate) fn new(pool_size: usize) -> Self {
                PoolLock {
                    inner: $lock_type::new(PoolInner::new(pool_size)),
                }
            }

            #[inline]
            pub(crate) fn _lock(&self) -> $guard_type<'_, PoolInner<M>> {
                self.inner.$lock_method()$(.$opt())*
            }

            #[inline]
            pub(crate) fn _try_lock(&self) -> Option<$guard_type<'_, PoolInner<M>>> {
                self.inner.$try_lock_method().ok()
            }
        }

        impl<'re, M: Manager> PoolLockFuture<'re, M> {
            #[inline]
            fn poll_pool_ref(
                &mut self,
                inner: &mut $guard_type<'re, PoolInner<M>>,
            ) -> Poll<PoolRef<'re, M>> {
                match inner.conn.pop_front() {
                    Some(conn) => {
                        self.acquired = true;
                        Poll::Ready(PoolRef::new(conn, self.shared_pool))
                    }
                    None => Poll::Pending,
                }
            }

            #[inline]
            fn spawn_idle_conn(&self, inner: &mut $guard_type<'_, PoolInner<M>>) {
                let shared = self.shared_pool;

                let builder = shared.get_builder();

                if inner.total() < builder.get_max_size() {
                    let marker = inner.marker;
                    let shared_clone = shared.clone();
                    shared.spawn(async move {
                        let _ = shared_clone.add_idle_conn(marker).await;
                    });
                    inner.incr_pending(1);
                }
            }
        }
    }
}

#[cfg(not(feature = "no-send"))]
pool_lock!(Mutex, MutexGuard, lock, try_lock, unwrap);

#[cfg(feature = "no-send")]
pool_lock!(RefCell, RefMut, borrow_mut, try_borrow_mut);

impl<'re, M: Manager> Future for PoolLockFuture<'re, M> {
    type Output = PoolRef<'re, M>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // we return pending status directly if the pool is in pausing state.
        // ToDo: currently we don't enable pause function for single thread pool.
        #[cfg(not(feature = "no-send"))]
        if !self.shared_pool.is_running() {
            return Poll::Pending;
        }

        let mut inner = self.pool_lock._lock();

        // poll a PoolRef and use the result to mutate PoolLockFuture state as well as PoolInner state.
        let poll = self.poll_pool_ref(&mut inner);

        // Either insert our waker if we don't have a wait key yet or overwrite the old waker entry if we already have a wait key.
        match self.wait_key {
            Some(wait_key) => {
                if poll.is_ready() {
                    // we got poll ready therefore remove self wait_key and entry in waiters
                    unsafe { inner.waiters.remove(wait_key) };
                    self.wait_key = None;
                } else {
                    // if we can't get a PoolRef then we spawn a new connection if we have not hit the max pool size.
                    self.spawn_idle_conn(&mut inner);

                    // if we are woken and have no key in waiters then we should not be in queue anymore.
                    let opt = unsafe { inner.waiters.get(wait_key) };
                    if opt.is_none() {
                        let waker = cx.waker().clone();
                        *opt = Some(waker);
                    }
                }
            }
            None => {
                if poll.is_pending() {
                    // if we can't get a PoolRef then we spawn a new connection if we have not hit the max pool size.
                    self.spawn_idle_conn(&mut inner);

                    let waker = cx.waker().clone();
                    let wait_key = inner.waiters.insert(Some(waker));
                    self.wait_key = Some(wait_key);
                }
            }
        }

        poll
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
