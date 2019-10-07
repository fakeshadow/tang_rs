use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::{fmt, mem};

use slab::Slab;
use tokio_executor::TypedExecutor;

use crate::{manager::Manager, IdleConn, SharedPool};
use std::time::Instant;

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

// PoolInner holds all the IdleConn and the waiters waiting for a connection.
// When we have a `Pending` for too long we know it's gone bad and it's safe to remove.
pub(crate) struct PoolInner<M: Manager + Send> {
    spawned: u8,
    // ToDo: garbage collector for pending VecDequeue. Consider remove pendings that last too long.
    pending: VecDeque<Pending>,
    conn: VecDeque<IdleConn<M>>,
    waiters: Slab<Waiter>,
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
            }),
        }
    }

    pub(crate) fn state(&self) -> State {
        let inner = self.inner.lock().unwrap();
        State {
            connections: inner.spawned,
            idle_connections: inner.conn.len() as u8,
            pending_connections: inner.pending.iter().map(|p| p.clone()).collect(),
        }
    }

    pub(crate) fn lock<'a>(&'a self, shared_pool: &'a Arc<SharedPool<M>>) -> PoolLockFuture<'a, M> {
        PoolLockFuture {
            shared_pool,
            lock: Some(&self),
            wait_key: WAIT_KEY_NONE,
        }
    }

    fn remove_waker(&self, wait_key: usize, wake_another: bool) {
        if wait_key != WAIT_KEY_NONE {
            let mut inner = self.inner.lock().unwrap();
            match inner.waiters.remove(wait_key) {
                Waiter::Waiting(_) => {}
                Waiter::Woken => {
                    // We were awoken, but then dropped before we could
                    // wake up to acquire the lock. Wake up another
                    // waiter.
                    if wake_another {
                        if let Some((_i, waiter)) = inner.waiters.iter_mut().next() {
                            waiter.wake();
                        }
                    }
                }
            }
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
        self.inner.lock().unwrap().pending.pop_front()
    }

    pub(crate) fn try_pop_conn(&self) -> Option<IdleConn<M>> {
        self.inner.lock().unwrap().conn.pop_front()
    }

    pub(crate) fn put_back(&self, conn: IdleConn<M>) {
        let mut inner = self.inner.lock().unwrap();

        inner.conn.push_back(conn);

        if let Some((_i, waiter)) = inner.waiters.iter_mut().next() {
            waiter.wake();
        }
    }

    pub(crate) fn pub_back_incr_spawn_count(&self, conn: IdleConn<M>) {
        let mut inner = self.inner.lock().unwrap();

        if (inner.spawned as usize) < inner.conn.capacity() {
            inner.conn.push_back(conn);
            inner.spawned += 1;
        }

        inner.pending.pop_front();

        if let Some((_i, waiter)) = inner.waiters.iter_mut().next() {
            waiter.wake();
        }
    }
}

// `PoolLockFuture` return a future of `IdleConn`.
// When request for an connection we construct a `PoolLockFuture`. In the `Future` we pass it's `Waker` to `PoolLock`.
// Then when a connection is returned to pool we lock the `PoolLock` and wake the `Wakers` inside it to notify other `PoolLockFuture` it's time to continue.
pub(crate) struct PoolLockFuture<'a, M: Manager + Send> {
    shared_pool: &'a Arc<SharedPool<M>>,
    lock: Option<&'a PoolLock<M>>,
    wait_key: usize,
}

// PoolLockFuture is dropped before acquire the lock so we wake another one.
impl<M: Manager + Send> Drop for PoolLockFuture<'_, M> {
    #[inline]
    fn drop(&mut self) {
        if let Some(lock) = self.lock {
            lock.remove_waker(self.wait_key, true);
        }
    }
}

impl<M: Manager + Send> Future for PoolLockFuture<'_, M> {
    type Output = IdleConn<M>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let lock = self.lock.expect("polled after complete");

        {
            let mut inner = lock.inner.lock().unwrap();

            // if we get a connection then we remove the waker by wait key.
            if let Some(conn) = inner.conn.pop_front() {
                if self.wait_key != WAIT_KEY_NONE {
                    inner.waiters.remove(self.wait_key);
                }

                self.lock = None;
                return Poll::Ready(conn);
            }

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

            // otherwise we lock WaitersLock and either insert our waker if we don't have a wait key yet
            // or register a new one if we already have a wait key.
            if self.wait_key == WAIT_KEY_NONE {
                self.wait_key = inner.waiters.insert(Waiter::Waiting(cx.waker().clone()));
            } else {
                inner.waiters[self.wait_key].register(cx.waker());
            }
        }

        // double check to make sure if we can get a connection.
        // If we can then we just undone the previous steps and return with our connection.
        {
            let mut inner = lock.inner.lock().unwrap();
            if let Some(conn) = inner.conn.pop_front() {
                if self.wait_key != WAIT_KEY_NONE {
                    inner.waiters.remove(self.wait_key);
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
