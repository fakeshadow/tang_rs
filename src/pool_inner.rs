use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::{manager::Manager, util::linked_list::WakerList, IdleConn, SharedPool};

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

// PoolInner holds all the IdleConn and the waiters waiting for a connection.
/// PoolInner is basically a reimplementation of `async_std::sync::Mutex`.
pub(crate) struct PoolInner<M: Manager> {
    spawned: u8,
    pending: VecDeque<Pending>,
    conn: VecDeque<IdleConn<M>>,
    waiters: WakerList,
}

impl<M: Manager> PoolInner<M> {
    fn decr_spawned_inner(&mut self) {
        if self.spawned != 0 {
            self.spawned -= 1;
        }
    }

    fn decr_pending_inner(&mut self, count: u8) {
        for _i in 0..count {
            self.pending.pop_front();
        }
    }

    #[inline]
    fn total(&mut self) -> u8 {
        self.spawned + self.pending.len() as u8
    }

    fn incr_pending_inner(&mut self, count: u8) {
        for _i in 0..count {
            self.pending.push_back(Pending::new());
        }
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
                pending: VecDeque::with_capacity(pool_size),
                conn: VecDeque::with_capacity(pool_size),
                waiters: WakerList::new(),
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

    // add pending directly to pool inner if we try to spawn new connections.
    // and return the new pending count as option to notify the Pool to replenish connections
    // we use closure here as it's not need to try spawn new connections every time we decr spawn count
    // (like decr spawn count when a connection doesn't return to pool successfully)
    pub(crate) fn decr_spawned<F>(&self, try_spawn: F) -> Option<u8>
    where
        F: FnOnce(u8) -> Option<u8>,
    {
        self.inner
            .lock()
            .map(|mut inner| {
                inner.decr_spawned_inner();

                try_spawn(inner.total()).map(|pending_new| {
                    inner.incr_pending_inner(pending_new);
                    pending_new
                })
            })
            .expect("Lock poisoned when decrementing spawned count")
    }

    pub(crate) fn decr_pending(&self, count: u8) {
        self.inner.lock().unwrap().decr_pending_inner(count);
    }

    pub(crate) fn drop_pendings<F>(&self, mut should_drop: F)
    where
        F: FnMut(&Pending) -> bool,
    {
        let mut inner = self.inner.lock().unwrap();
        let len = inner.pending.len();

        for index in 0..len {
            if let Some(pending) = inner.pending.get(index) {
                if should_drop(pending) {
                    inner.pending.remove(index);
                }
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
        self.inner
            .lock()
            .map(|mut inner| {
                inner.conn.push_back(conn);
                inner.waiters.wake_one_weak()
            })
            .expect("Lock poisoned when putting back connection")
            .wake();
    }

    pub(crate) fn put_back_incr_spawned(&self, conn: IdleConn<M>) {
        self.inner
            .lock()
            .map(|mut inner| {
                inner.decr_pending_inner(1);
                if (inner.spawned as usize) < inner.conn.capacity() {
                    inner.conn.push_back(conn);
                    inner.spawned += 1;
                }
                inner.waiters.wake_one_weak()
            })
            .expect("Lock poisoned when putting back connection")
            .wake();
    }

    pub(crate) fn state(&self) -> State {
        self.inner
            .lock()
            .map(|inner| State {
                connections: inner.spawned,
                idle_connections: inner.conn.len() as u8,
                pending_connections: inner.pending.iter().cloned().collect(),
            })
            .unwrap()
    }
}

// `PoolLockFuture` return a future of `IdleConn`. In the `Future` we pass it's `Waker` to `PoolLock`.
// Then when a `IdleConn` is returned to pool we lock the `PoolLock` and wake the `Wakers` inside it to notify other `PoolLockFuture` it's time to continue.
pub(crate) struct PoolLockFuture<'a, M: Manager> {
    shared_pool: &'a Arc<SharedPool<M>>,
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

impl<M: Manager> PoolLockFuture<'_, M> {
    #[inline]
    fn poll_idle_conn(&mut self, inner: &mut MutexGuard<'_, PoolInner<M>>) -> Poll<IdleConn<M>> {
        match inner.conn.pop_front() {
            Some(conn) => {
                self.acquired = true;
                Poll::Ready(conn)
            }
            None => Poll::Pending,
        }
    }

    #[inline]
    fn spawn_idle_conn(&self, inner: &mut MutexGuard<'_, PoolInner<M>>) {
        let shared = self.shared_pool;
        if inner.total() < shared.statics.max_size {
            let shared_clone = shared.clone();
            shared.spawn(async move { shared_clone.add_idle_conn().await });
            inner.incr_pending_inner(1);
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

impl<M: Manager> Future for PoolLockFuture<'_, M> {
    type Output = IdleConn<M>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.pool_lock.inner.lock().unwrap();

        // poll a connection and use the result to mutate PoolLockFuture state as well as PoolInner state.
        let poll = self.poll_idle_conn(&mut inner);

        // Either insert our waker if we don't have a wait key yet or overwrite the old waker entry if we already have a wait key.
        match self.wait_key {
            Some(wait_key) => {
                if poll.is_ready() {
                    // we got poll ready therefore remove self wait_key
                    unsafe { inner.waiters.remove(wait_key) };
                    self.wait_key = None;
                } else {
                    // if we can't get a connection then we spawn new ones if we have not hit the max pool size.
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
                    // if we can't get a connection then we spawn new ones if we have not hit the max pool size.
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

unsafe impl<M: Manager + Send> Send for PoolLock<M> {}

unsafe impl<M: Manager + Send> Sync for PoolLock<M> {}

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
