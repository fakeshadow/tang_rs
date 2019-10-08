use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;
use std::{fmt, mem};

use slab::Slab;
use tokio_executor::TypedExecutor;

use crate::{manager::Manager, IdleConn, SharedPool};

const WAIT_KEY_NONE: usize = std::usize::MAX;

// an enum to determine the state of a `Waker`.
enum Waiter {
    Waiting(Waker),
    Woken,
}

impl Waiter {
    #[inline]
    fn register(&mut self, w: &Waker) {
        // if the new waker will wake the old one then we just ignore the register.
        // otherwise we overwrite the old waker.
        match self {
            Waiter::Waiting(waker) if w.will_wake(waker) => {}
            _ => *self = Waiter::Waiting(w.clone()),
        }
    }

    #[inline]
    fn wake(&mut self) {
        match mem::replace(self, Waiter::Woken) {
            Waiter::Waiting(waker) => waker.wake(),
            Waiter::Woken => {}
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
pub(crate) struct PoolInner<M: Manager + Send> {
    spawned: u8,
    // ToDo: garbage collector for pending VecDequeue. Consider remove pendings that last too long.
    pending: VecDeque<Pending>,
    conn: VecDeque<IdleConn<M>>,
    waiters: Slab<Waiter>,
    waiters_queue: HelperQueue,
}

impl<M: Manager + Send> PoolInner<M> {
    fn remove_waker_inner(&mut self, wait_key: usize, wake_another: bool) {
        match self.waiters.remove(wait_key) {
            Waiter::Waiting(_) => {}
            Waiter::Woken => {
                // We were awoken, but then dropped before we could
                // wake up to acquire the lock. Wake up another
                // waiter.
                if wake_another {
                    self.wake_one_inner();
                }
            }
        }
    }

    fn wake_one_inner(&mut self) {
        if let Some(waiter_key) = self.waiters_queue.get() {
            if let Some(waiter) = self.waiters.get_mut(waiter_key) {
                waiter.wake();
                return;
            }
        }

        // we can't find a waiter from waiters_queue so we just get the first one from waiters.
        if let Some((_i, waiter)) = self.waiters.iter_mut().next() {
            waiter.wake();
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
            lock: Some(self),
            wait_key: WAIT_KEY_NONE,
        }
    }

    fn remove_waker(&self, wait_key: usize, wake_another: bool) {
        if wait_key != WAIT_KEY_NONE {
            self.inner
                .lock()
                .unwrap()
                .remove_waker_inner(wait_key, wake_another);
        }
    }

    // return the new pending + spawned counter
    pub(crate) fn incr_pending_count(&self) -> u8 {
        let mut lock = self.inner.lock().unwrap();
        lock.pending.push_back(Pending::new());
        lock.pending.len() as u8 + lock.spawned
    }

    // return the original spawned value
    pub(crate) fn decr_spawn_count(&self) -> u8 {
        let mut lock = self.inner.lock().unwrap();
        if lock.spawned > 0 {
            lock.spawned -= 1;
        }

        lock.spawned
    }

    pub(crate) fn try_pop_pending(&self) -> Option<Pending> {
        if let Ok(mut inner) = self.inner.try_lock() {
            return inner.pending.pop_front();
        }
        None
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
        inner.wake_one_inner();
    }

    pub(crate) fn pub_back_incr_spawn_count(&self, conn: IdleConn<M>) {
        let mut inner = self.inner.lock().unwrap();

        if (inner.spawned as usize) < inner.conn.capacity() {
            inner.conn.push_back(conn);
            inner.spawned += 1;
        }

        inner.pending.pop_front();

        inner.wake_one_inner();
    }
}

// `PoolLockFuture` return a future of `IdleConn`. In the `Future` we pass it's `Waker` to `PoolLock`.
// Then when a `IdleConn` is returned to pool we lock the `PoolLock` and wake the `Wakers` inside it to notify other `PoolLockFuture` it's time to continue.
pub(crate) struct PoolLockFuture<'a, M: Manager + Send> {
    shared_pool: &'a Arc<SharedPool<M>>,
    lock: Option<&'a PoolLock<M>>,
    wait_key: usize,
}

impl<M: Manager + Send> Drop for PoolLockFuture<'_, M> {
    #[inline]
    fn drop(&mut self) {
        // PoolLockFuture is dropped before acquire the lock so we wake another one.
        if let Some(lock) = self.lock {
            lock.remove_waker(self.wait_key, true);
        }
    }
}

impl<M: Manager + Send> Future for PoolLockFuture<'_, M> {
    type Output = IdleConn<M>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let lock = self.lock.expect("polled after complete");

        // if we get a connection we remove our wait key and return.
        if let Ok(mut inner) = lock.inner.try_lock() {
            if let Some(conn) = inner.conn.pop_front() {
                if self.wait_key != WAIT_KEY_NONE {
                    inner.remove_waker_inner(self.wait_key, false);
                }

                self.lock = None;
                return Poll::Ready(conn);
            }
        }

        {
            let mut inner = lock.inner.lock().unwrap();

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
            if self.wait_key == WAIT_KEY_NONE {
                self.wait_key = inner.waiters.insert(Waiter::Waiting(cx.waker().clone()));
                inner.waiters_queue.insert_no_check(self.wait_key);
            } else {
                inner.waiters[self.wait_key].register(cx.waker());
                inner.waiters_queue.insert(self.wait_key);
            }
            // then we insert our wait key to queue.
        }

        // double check to make sure if we can get a connection.
        // If we can then we just undone the previous steps and return with our connection.
        if let Ok(mut inner) = lock.inner.try_lock() {
            if let Some(conn) = inner.conn.pop_front() {
                if self.wait_key != WAIT_KEY_NONE {
                    inner.remove_waker_inner(self.wait_key, false);
                }

                self.lock = None;
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
