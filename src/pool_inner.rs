use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::{
    manager::Manager,
    pool::{IdleConn, ManagedPool, PoolRef},
    util::linked_list::WakerList,
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

pub(crate) struct PoolLock<M: Manager> {
    inner: Mutex<PoolInner<M>>,
}

impl<M: Manager> PoolLock<M> {
    pub(crate) fn new(pool_size: usize) -> Self {
        PoolLock {
            inner: Mutex::new(PoolInner {
                spawned: 0,
                marker: 0,
                pending: VecDeque::with_capacity(pool_size),
                conn: VecDeque::with_capacity(pool_size),
                waiters: WakerList::new(),
            }),
        }
    }

    #[inline]
    pub(crate) fn lock<'a>(
        &'a self,
        shared_pool: &'a Arc<ManagedPool<M>>,
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
    pub(crate) fn decr_spawned(
        &self,
        marker: Option<usize>,
        min_idle: usize,
        should_spawn_new: bool,
    ) -> Option<usize> {
        self.inner
            .lock()
            .map(|mut inner| {
                inner.decr_spawned(1);

                let total = inner.total();

                // we match the same marker as inner.marker to true and give a free pass if marker is None
                let match_marker = match marker {
                    Some(marker) => marker == inner.marker,
                    None => true,
                };

                if total < min_idle && should_spawn_new && match_marker {
                    let pending_new = min_idle - total;
                    inner.incr_pending(pending_new);
                    Some(pending_new)
                } else {
                    None
                }
            })
            .unwrap()
    }

    pub(crate) fn decr_pending(&self, count: usize) {
        self.inner.lock().unwrap().decr_pending(count);
    }

    pub(crate) fn drop_pendings<F>(&self, mut should_drop: F)
    where
        F: FnMut(&Pending) -> bool,
    {
        self.inner
            .lock()
            .map(|mut inner| {
                let len = inner.pending.len();

                for index in 0..len {
                    if let Some(pending) = inner.pending.get(index) {
                        if should_drop(pending) {
                            inner.pending.remove(index);
                        }
                    }
                }
            })
            .unwrap()
    }

    // return new pending count as Option<usize>.
    pub(crate) fn try_drop_conns<F>(&self, min_idle: usize, mut should_drop: F) -> Option<usize>
    where
        F: FnMut(&IdleConn<M>) -> bool,
    {
        self.inner.try_lock().ok().and_then(|mut inner| {
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

                Some(pending_new)
            } else {
                None
            }
        })
    }

    #[inline]
    pub(crate) fn put_back(&self, conn: IdleConn<M>, max_size: usize) {
        self.inner
            .lock()
            .map(|mut inner| {
                let marker = conn.get_marker().unwrap_or(inner.marker);
                if inner.spawned > max_size || inner.marker != marker {
                    inner.decr_spawned(1);
                    None
                } else {
                    inner.put_back(conn);
                    inner.waiters.wake_one_weak()
                }
            })
            .unwrap()
            .wake();
    }

    pub(crate) fn put_back_incr_spawned(&self, mut conn: IdleConn<M>, max_size: usize) {
        self.inner
            .lock()
            .map(|mut inner| {
                inner.decr_pending(1);
                if inner.spawned < max_size {
                    conn.set_marker(inner.marker);
                    inner.put_back(conn);
                    inner.incr_spawned(1);

                }
                inner.waiters.wake_one_weak()
            })
            .unwrap()
            .wake();
    }

    pub(crate) fn clear(&self, pool_size: usize) {
        self.inner
            .lock()
            .map(|mut inner| {
                let count = inner.conn.len();
                inner.decr_spawned(count);
                inner.incr_marker();
                inner.clear_pending(pool_size);
                inner.clear_conn(pool_size);
            })
            .unwrap()
    }

    pub(crate) fn state(&self) -> State {
        self.inner
            .lock()
            .map(|inner| State {
                connections: inner.spawned,
                idle_connections: inner.conn.len(),
                pending_connections: inner.pending.iter().cloned().collect(),
            })
            .unwrap()
    }
}

// `PoolLockFuture` return a future of `PoolRef`. In the `Future` we pass it's `Waker` to `PoolLock`.
// Then when a `IdleConn` is returned to pool we lock the `PoolLock` and wake the `Wakers` inside it to notify other `PoolLockFuture` it's time to continue.
pub(crate) struct PoolLockFuture<'a, M: Manager> {
    shared_pool: &'a Arc<ManagedPool<M>>,
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
    #[inline]
    fn poll_pool_ref(
        &mut self,
        inner: &mut MutexGuard<'re, PoolInner<M>>,
        is_running: bool,
    ) -> Poll<PoolRef<'re, M>> {
        // we return pending status directly if the pool is in pausing state.
        if !is_running {
            return Poll::Pending;
        }

        match inner.conn.pop_front() {
            Some(conn) => {
                self.acquired = true;
                Poll::Ready(PoolRef::new(conn, self.shared_pool))
            }
            None => Poll::Pending,
        }
    }

    #[inline]
    fn spawn_idle_conn(&self, inner: &mut MutexGuard<'_, PoolInner<M>>) {
        let shared = self.shared_pool;

        let builder = shared.get_builder();

        if inner.total() < builder.get_max_size() {
            let shared_clone = shared.clone();
            shared.spawn(async move {
                let _ = shared_clone.add_idle_conn().await;
            });
            inner.incr_pending(1);
        }
    }

    #[cold]
    fn wake_cold(&self, wait_key: NonZeroUsize) {
        let mut inner = self.pool_lock.inner.lock().unwrap();
        let wait_key = unsafe { inner.waiters.remove(wait_key) };

        if wait_key.is_none() && !self.acquired {
            // We were awoken but didn't acquire the lock. Wake up another task.
            let opt = inner.waiters.wake_one_weak();
            drop(inner);
            opt.wake();
        }
    }
}

impl<'re, M: Manager> Future for PoolLockFuture<'re, M> {
    type Output = PoolRef<'re, M>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.pool_lock.inner.lock().unwrap();

        let is_running = self.shared_pool.is_running();

        // poll a PoolRef and use the result to mutate PoolLockFuture state as well as PoolInner state.
        let poll = self.poll_pool_ref(&mut inner, is_running);

        // Either insert our waker if we don't have a wait key yet or overwrite the old waker entry if we already have a wait key.
        match self.wait_key {
            Some(wait_key) => {
                if poll.is_ready() {
                    // we got poll ready therefore remove self wait_key and entry in waiters
                    unsafe { inner.waiters.remove(wait_key) };
                    self.wait_key = None;
                } else {
                    // if we can't get a PoolRef then we spawn a new connection if we have not hit the max pool size.
                    if is_running {
                        self.spawn_idle_conn(&mut inner);
                    }

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
                    if is_running {
                        self.spawn_idle_conn(&mut inner);
                    }

                    let waker = cx.waker().clone();
                    let wait_key = inner.waiters.insert(Some(waker));
                    self.wait_key = Some(wait_key);
                }
            }
        }

        poll
    }
}

unsafe impl<M: Manager + Send> Send for PoolLock<M> {}

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
