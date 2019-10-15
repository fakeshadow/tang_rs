use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use slab::Slab;

use crate::{manager::Manager, IdleConn, SharedPool};

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

// temporary waiters helper queue to deal with Slab's unfair insert.
struct HelperQueue {
    queue: VecDeque<usize>,
}

impl Default for HelperQueue {
    fn default() -> Self {
        HelperQueue {
            queue: VecDeque::with_capacity(128 * 128),
        }
    }
}

impl HelperQueue {
    fn insert(&mut self, wait_key: usize) {
        self.queue.push_back(wait_key);
    }

    fn get(&mut self) -> Option<usize> {
        self.queue.pop_front()
    }
}

// PoolInner holds all the IdleConn and the waiters waiting for a connection.
/// PoolInner is basically a reimplementation of `async_std::sync::Mutex`.
pub(crate) struct PoolInner<M: Manager + Send> {
    spawned: u8,
    pending: VecDeque<Pending>,
    conn: VecDeque<IdleConn<M>>,
    waiters: Slab<Option<Waker>>,
    waiters_queue: HelperQueue,
}

impl<M: Manager + Send> PoolInner<M> {
    fn get_waker(&mut self) -> Option<Waker> {
        if let Some(waiter_key) = self.waiters_queue.get() {
            if let Some(waker) = self.waiters.get_mut(waiter_key) {
                return waker.take();
            }
        }

        if let Some((_i, waker)) = self.waiters.iter_mut().next() {
            return waker.take();
        }
        None
    }

    fn decr_spawned_inner(&mut self) {
        if self.spawned != 0 {
            self.spawned -= 1;
        }
    }

    fn decr_pending_inner(&mut self) {
        self.pending.pop_front();
    }

    fn total(&mut self) -> u8 {
        self.spawned + self.pending.len() as u8
    }

    fn incr_pending_inner(&mut self, count: u8) {
        for _i in 0..count {
            self.pending.push_back(Pending::new());
        }
    }
}

pub(crate) struct PoolLock<M: Manager + Send> {
    inner: Mutex<PoolInner<M>>,
}

impl<M: Manager + Send> PoolLock<M> {
    pub(crate) fn new(pool_size: usize) -> Self {
        PoolLock {
            inner: Mutex::new(PoolInner {
                spawned: 0,
                pending: VecDeque::with_capacity(pool_size),
                conn: VecDeque::with_capacity(pool_size),
                waiters: Slab::with_capacity(128 * 128),
                waiters_queue: Default::default(),
            }),
        }
    }

    #[inline]
    pub(crate) fn lock<'a>(&'a self, shared_pool: &'a Arc<SharedPool<M>>) -> PoolLockFuture<'a, M> {
        PoolLockFuture {
            shared_pool,
            pool_lock: self,
            wait_key: None,
            acquired: false,
        }
    }

    fn try_pop_conn(&self) -> Option<IdleConn<M>> {
        self.inner
            .try_lock()
            .ok()
            .and_then(|mut inner| inner.conn.pop_front())
    }

    // add pending directly to pool inner if we try to spawn new connections.
    // and return the new pending count as option to notify the Pool to replenish connections
    // we use closure here as it's not need to try spawn new connections every time we decr spawn count
    // (like decr spawn count when a connection doesn't return to pool successfully)
    pub(crate) fn decr_spawned<F>(&self, try_spawn: F) -> Option<u8>
    where
        F: FnOnce(u8) -> Option<u8>,
    {
        let mut inner = self.inner.lock().unwrap();
        inner.decr_spawned_inner();

        try_spawn(inner.total()).map(|pending_new| {
            inner.incr_pending_inner(pending_new);
            pending_new
        })
    }

    pub(crate) fn decr_pending<F>(&self, should_drop: F)
    where
        F: FnOnce(&Pending) -> bool,
    {
        let mut inner = self.inner.lock().unwrap();
        if let Some(pending) = inner.pending.front() {
            if should_drop(pending) {
                inner.decr_pending_inner();
            }
        }
    }

    // return new pending count as Some(u8).
    pub(crate) fn try_drop_conns<F>(&self, min_idle: u8, mut should_drop: F) -> Option<u8>
    where
        F: FnMut(&IdleConn<M>) -> bool,
    {
        self.inner.try_lock().ok().and_then(|mut inner| {
            let len = inner.conn.len();
            for index in 0..len {
                if let Some(conn) = inner.conn.get(index) {
                    if should_drop(conn) {
                        inner.conn.remove(index);
                        inner.decr_spawned_inner();
                    }
                }
            }

            let total_now = inner.total();
            if total_now < min_idle {
                let pending_new = min_idle - total_now;

                inner.incr_pending_inner(pending_new);

                Some(pending_new)
            } else {
                None
            }
        })
    }

    #[inline]
    pub(crate) fn put_back(&self, conn: IdleConn<M>) {
        if let Some(waker) = {
            let mut inner = self.inner.lock().unwrap();
            inner.conn.push_back(conn);
            inner.get_waker()
        } {
            waker.wake();
        }
    }

    pub(crate) fn put_back_incr_spawned(&self, conn: IdleConn<M>) {
        if let Some(waker) = {
            let mut inner = self.inner.lock().unwrap();
            inner.decr_pending_inner();
            if (inner.spawned as usize) < inner.conn.capacity() {
                inner.conn.push_back(conn);
                inner.spawned += 1;
            }
            inner.get_waker()
        } {
            waker.wake();
        }
    }

    pub(crate) fn state(&self) -> State {
        let inner = self.inner.lock().unwrap();
        State {
            connections: inner.spawned,
            idle_connections: inner.conn.len() as u8,
            pending_connections: inner.pending.iter().cloned().collect(),
        }
    }
}

// `PoolLockFuture` return a future of `IdleConn`. In the `Future` we pass it's `Waker` to `PoolLock`.
// Then when a `IdleConn` is returned to pool we lock the `PoolLock` and wake the `Wakers` inside it to notify other `PoolLockFuture` it's time to continue.
pub(crate) struct PoolLockFuture<'a, M: Manager + Send> {
    shared_pool: &'a Arc<SharedPool<M>>,
    pool_lock: &'a PoolLock<M>,
    wait_key: Option<usize>,
    acquired: bool,
}

impl<M: Manager + Send> Drop for PoolLockFuture<'_, M> {
    #[inline]
    fn drop(&mut self) {
        if let Some(wait_key) = self.wait_key {
            let mut inner = self.pool_lock.inner.lock().unwrap();
            let wait_key = inner.waiters.remove(wait_key);

            if wait_key.is_none() && !self.acquired {
                // We were awoken but didn't acquire the lock. Wake up another task.
                let waker = inner.get_waker();
                drop(inner);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
        }
    }
}

impl<M: Manager + Send> Future for PoolLockFuture<'_, M> {
    type Output = IdleConn<M>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let pool_lock = self.pool_lock;

        // if we get a connection we return directly
        match pool_lock.try_pop_conn() {
            Some(conn) => {
                self.acquired = true;
                Poll::Ready(conn)
            }
            None => {
                let mut inner = pool_lock.inner.lock().unwrap();

                // a connection could returned right before we force lock the pool.
                if let Some(conn) = inner.conn.pop_front() {
                    self.acquired = true;
                    return Poll::Ready(conn);
                }

                // if we can't get a connection then we spawn new ones if we have not hit the max pool size.
                let shared = self.shared_pool;
                #[cfg(not(feature = "actix-web"))]
                {
                    if inner.total() < shared.statics.max_size {
                        inner.incr_pending_inner(1);
                        let shared_clone = shared.clone();
                        let _ = shared
                            .spawn(async move { shared_clone.add_connection().await })
                            .map_err(|_| inner.decr_pending_inner());
                    }
                }

                #[cfg(feature = "actix-web")]
                let _clippy_ignore = shared;

                // Either insert our waker if we don't have a wait key yet or overwrite the old waker entry if we already have a wait key.
                match self.wait_key {
                    Some(wait_key) => {
                        // if we are woken and have no key in waitesr then we should not be in queue anymore.
                        if inner.waiters[wait_key].is_none() {
                            inner.waiters_queue.insert(wait_key);
                            inner.waiters[wait_key] = Some(cx.waker().clone());
                        }
                    }
                    None => {
                        let wait_key = inner.waiters.insert(Some(cx.waker().clone()));
                        self.wait_key = Some(wait_key);
                        inner.waiters_queue.insert(wait_key);
                    }
                }

                Poll::Pending
            }
        }
    }
}

unsafe impl<M: Manager + Send> Send for PoolLock<M> {}

unsafe impl<M: Manager + Send> Sync for PoolLock<M> {}

unsafe impl<M: Manager + Send> Send for PoolLockFuture<'_, M> {}

unsafe impl<M: Manager + Send> Sync for PoolLockFuture<'_, M> {}

pub struct State {
    pub connections: u8,
    pub idle_connections: u8,
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
