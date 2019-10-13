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
    #[inline]
    pub(crate) fn decr_spawned_count<F>(&self, try_spawn: F) -> Option<u8>
    where
        F: FnOnce(u8) -> Option<u8>,
    {
        let mut inner = self.inner.lock().unwrap();
        if inner.spawned != 0 {
            inner.spawned -= 1;
        }

        try_spawn(inner.spawned + inner.pending.len() as u8).map(|pending_new| {
            for _i in 0..pending_new {
                inner.pending.push_back(Pending::new());
            }
            pending_new
        })
    }

    // drop pending that last too long.
    #[inline]
    pub(crate) fn drop_pending<F>(&self, should_drop: F)
    where
        F: FnOnce(&Pending) -> bool,
    {
        let mut inner = self.inner.lock().unwrap();
        if let Some(pending) = inner.pending.front() {
            if should_drop(pending) {
                inner.pending.pop_front();
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
                        if inner.spawned != 0 {
                            inner.spawned -= 1;
                        }
                    }
                }
            }

            let total_now = inner.spawned + inner.pending.len() as u8;
            if total_now < min_idle {
                let pending_new = min_idle - total_now;
                for _i in 0..pending_new {
                    inner.pending.push_back(Pending::new())
                }

                Some(pending_new)
            } else {
                None
            }
        })
    }

    #[inline]
    pub(crate) fn put_back(&self, conn: IdleConn<M>) {
        let mut inner = self.inner.lock().unwrap();
        inner.conn.push_back(conn);
        let waker = inner.get_waker();
        drop(inner);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    #[inline]
    pub(crate) fn put_back_incr_spawn_count(&self, conn: IdleConn<M>) {
        let mut inner = self.inner.lock().unwrap();
        inner.pending.pop_front();
        if (inner.spawned as usize) < inner.conn.capacity() {
            inner.conn.push_back(conn);
            inner.spawned += 1;
        }
        let waker = inner.get_waker();
        drop(inner);
        if let Some(waker) = waker {
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
        if let Some(conn) = pool_lock.try_pop_conn() {
            self.acquired = true;
            return Poll::Ready(conn);
        }

        {
            let mut inner = pool_lock.inner.lock().unwrap();

            // if we can't get a connection then we spawn new ones if we have not hit the max pool size.
            #[cfg(not(feature = "actix-web"))]
            {
                if (inner.spawned + inner.pending.len() as u8) < self.shared_pool.statics.max_size {
                    inner.pending.push_back(Pending::new());
                    let shared_clone = self.shared_pool.clone();
                    let _ = self
                        .shared_pool
                        .spawn(async move { shared_clone.add_connection().await })
                        .map_err(|_| inner.pending.pop_front());
                }
            }

            #[cfg(feature = "actix-web")]
            let _clippy_ignore = self.shared_pool;

            // Either insert our waker if we don't have a wait key yet or register a new one if we already have a wait key.
            match self.wait_key {
                Some(wait_key) => {
                    // There is already an entry in the list of waiters.
                    // Just reset the waker if it was removed.
                    if inner.waiters[wait_key].is_none() {
                        let waker = cx.waker().clone();
                        inner.waiters[wait_key] = Some(waker);
                        inner.waiters_queue.insert(wait_key);
                    }
                }
                None => {
                    let waker = cx.waker().clone();
                    let wait_key = inner.waiters.insert(Some(waker));
                    self.wait_key = Some(wait_key);
                    inner.waiters_queue.insert(wait_key);
                }
            }
        }

        // double check to make sure if we can get a connection.
        // If we can then we just undone the previous steps and return with our connection.
        if let Some(conn) = pool_lock.try_pop_conn() {
            self.acquired = true;
            return Poll::Ready(conn);
        }

        Poll::Pending
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
