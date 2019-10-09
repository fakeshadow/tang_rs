use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use slab::Slab;
use tokio_executor::TypedExecutor;

use crate::{manager::Manager, IdleConn, SharedPool};

const WAIT_KEY_NONE: usize = std::usize::MAX;

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
        let now = Instant::now();
        if now > (self.start_from + connection_timeout * 6) {
            true
        } else {
            false
        }
    }
}

// temporary waiters helper queue to deal with Slab's unfair insert.
struct HelperQueue {
    queue: VecDeque<usize>,
    last_key: usize,
}

impl Default for HelperQueue {
    fn default() -> Self {
        HelperQueue {
            queue: VecDeque::new(),
            last_key: WAIT_KEY_NONE,
        }
    }
}

impl HelperQueue {
    fn insert(&mut self, wait_key: usize) {
        if self.last_key == wait_key {
            return;
        }
        if !self.queue.contains(&wait_key) {
            self.last_key = wait_key;
            self.queue.push_back(wait_key);
        }
    }

    fn insert_no_check(&mut self, wait_key: usize) {
        self.queue.push_back(wait_key);
        self.last_key = wait_key;
    }

    fn get(&mut self) -> Option<usize> {
        self.queue.pop_front()
    }
}

// PoolInner holds all the IdleConn and the waiters waiting for a connection.
// When we have a `Pending` for too long we know it's gone bad and it's safe to remove.
/// PoolInner is basically a reimplementation of `async_std::sync::Mutex`.
pub(crate) struct PoolInner<M: Manager + Send> {
    spawned: u8,
    pending: VecDeque<Pending>,
    conn: VecDeque<IdleConn<M>>,
    waiters: Slab<Option<Waker>>,
    waiters_queue: HelperQueue,
}

impl<M: Manager + Send> PoolInner<M> {
    fn wake_one(&mut self) {
        if let Some(waiter_key) = self.waiters_queue.get() {
            if let Some(waker) = self.waiters.get_mut(waiter_key) {
                if let Some(waker) = waker.take() {
                    waker.wake();
                    return;
                }
            }
        }

        if let Some((_i, waker)) = self.waiters.iter_mut().next() {
            if let Some(waker) = waker.take() {
                waker.wake();
            }
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
                waiters: Slab::new(),
                waiters_queue: Default::default(),
            }),
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

    pub(crate) fn lock<'a>(&'a self, shared_pool: &'a Arc<SharedPool<M>>) -> PoolLockFuture<'a, M> {
        PoolLockFuture {
            shared_pool,
            pool: self,
            wait_key: None,
            acquired: false,
        }
    }

    // return the new pending + spawned counter
    pub(crate) fn incr_pending_count(&self) -> u8 {
        let mut lock = self.inner.lock().unwrap();
        lock.pending.push_back(Pending::new());
        lock.pending.len() as u8 + lock.spawned
    }

    // return the new spawned value
    pub(crate) fn decr_spawn_count(&self) -> u8 {
        let mut lock = self.inner.lock().unwrap();
        if lock.spawned > 0 {
            lock.spawned -= 1;
        }

        lock.spawned
    }

    // remove pending that last too long.
    pub(crate) fn try_peek_pending<F>(&self, should_remove: F)
    where
        F: FnOnce(Option<&Pending>) -> bool,
    {
        if let Ok(mut inner) = self.inner.try_lock() {
            let pending = inner.pending.front();
            if should_remove(pending) {
                inner.pending.pop_front();
            }
        }
    }

    pub(crate) fn pop_pending(&self) -> Option<Pending> {
        self.inner.lock().unwrap().pending.pop_front()
    }

    pub(crate) fn try_pop_conn(&self) -> Option<IdleConn<M>> {
        if let Ok(mut inner) = self.inner.try_lock() {
            return inner.conn.pop_front();
        }
        None
    }

    pub(crate) fn put_back(&self, conn: IdleConn<M>) {
        let mut inner = self.inner.lock().unwrap();
        inner.conn.push_back(conn);
        inner.wake_one();
    }

    pub(crate) fn put_back_incr_spawn_count(&self, conn: IdleConn<M>) {
        let mut inner = self.inner.lock().unwrap();

        inner.pending.pop_front();

        if (inner.spawned as usize) < inner.conn.capacity() {
            inner.conn.push_back(conn);
            inner.spawned += 1;
            inner.wake_one();
        }
    }
}

// `PoolLockFuture` return a future of `IdleConn`. In the `Future` we pass it's `Waker` to `PoolLock`.
// Then when a `IdleConn` is returned to pool we lock the `PoolLock` and wake the `Wakers` inside it to notify other `PoolLockFuture` it's time to continue.
pub(crate) struct PoolLockFuture<'a, M: Manager + Send> {
    shared_pool: &'a Arc<SharedPool<M>>,
    pool: &'a PoolLock<M>,
    wait_key: Option<usize>,
    acquired: bool,
}

impl<M: Manager + Send> Drop for PoolLockFuture<'_, M> {
    #[inline]
    fn drop(&mut self) {
        if let Some(wait_key) = self.wait_key {
            let mut inner = self.pool.inner.lock().unwrap();
            let wait_key = inner.waiters.remove(wait_key);

            if wait_key.is_none() && !self.acquired {
                // We were awoken but didn't acquire the lock. Wake up another task.
                inner.wake_one();
            }
        }
    }
}

impl<M: Manager + Send> Future for PoolLockFuture<'_, M> {
    type Output = IdleConn<M>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let pool = self.pool;

        // if we get a connection we return directly
        if let Ok(mut inner) = pool.inner.try_lock() {
            if let Some(conn) = inner.conn.pop_front() {
                self.acquired = true;
                return Poll::Ready(conn);
            }
        }

        {
            let mut inner = pool.inner.lock().unwrap();

            // if we can't get a connection then we spawn new ones if we have not hit the max pool size.
            if (inner.spawned + inner.pending.len() as u8) < self.shared_pool.statics.max_size {
                inner.pending.push_back(Pending::new());

                let shared_pool = self.shared_pool.clone();
                let _ = tokio_executor::DefaultExecutor::current()
                    .spawn(Box::pin(async move {
                        let _ = shared_pool.add_connection().await;
                    }))
                    .map_err(|_| inner.pending.pop_front());
            }

            // Either insert our waker if we don't have a wait key yet or register a new one if we already have a wait key.
            match self.wait_key {
                Some(wait_key) => {
                    // There is already an entry in the list of waiters.
                    // Just reset the waker if it was removed.
                    if inner.waiters[wait_key].is_none() {
                        let waker = cx.waker().clone();
                        inner.waiters[wait_key] = Some(waker);
                        // then we insert our wait key to queue with some fairness check.
                        inner.waiters_queue.insert(wait_key);
                    }
                }
                None => {
                    let waker = cx.waker().clone();
                    let wait_key = inner.waiters.insert(Some(waker));
                    self.wait_key = Some(wait_key);
                    // then we insert our wait key to queue.
                    inner.waiters_queue.insert_no_check(wait_key);
                }
            }
        }

        // double check to make sure if we can get a connection.
        // If we can then we just undone the previous steps and return with our connection.
        if let Ok(mut inner) = pool.inner.try_lock() {
            if let Some(conn) = inner.conn.pop_front() {
                self.acquired = true;
                return Poll::Ready(conn);
            }
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
