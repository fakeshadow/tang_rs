use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;
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

// WaitersLock holds all the futures' wakers that are waiting for a connection.
pub(crate) struct WaitersLock(Mutex<Slab<Waiter>>);

impl WaitersLock {
    pub(crate) fn new() -> Self {
        WaitersLock(Mutex::new(Slab::new()))
    }

    fn remove_waker(&self, wait_key: usize, wake_another: bool) {
        if wait_key != WAIT_KEY_NONE {
            let mut waiters = self.0.lock();
            match waiters.remove(wait_key) {
                Waiter::Waiting(_) => {}
                Waiter::Woken => {
                    // We were awoken, but then dropped before we could
                    // wake up to acquire the lock. Wake up another
                    // waiter.
                    if wake_another {
                        if let Some((_i, waiter)) = waiters.iter_mut().next() {
                            waiter.wake();
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn wake_one(&self) {
        let mut waiters = self.0.lock();
        if let Some((_i, waiter)) = waiters.iter_mut().next() {
            waiter.wake();
        }
    }
}

// Waiting return a future of `IdleConn`.
// When pool is empty we construct a `Waiting`. In the `Future` we pass it's `Waker` to `WaitersLock`.
// Then when a connection is returned to pool we lock the `WaitersLock` and wake the `Wakers` inside it to notify `Waiting` it's time to continue.
pub(crate) struct Waiting<'a, M: Manager + Send> {
    pool: &'a Arc<SharedPool<M>>,
    waiters: Option<&'a WaitersLock>,
    wait_key: usize,
}

impl<'a, M: Manager + Send> Waiting<'a, M> {
    pub(crate) fn new(pool: &'a Arc<SharedPool<M>>, waiters: &'a WaitersLock) -> Self {
        Waiting {
            pool,
            waiters: Some(waiters),
            wait_key: WAIT_KEY_NONE,
        }
    }
}

impl<M: Manager + Send> Drop for Waiting<'_, M> {
    #[inline]
    fn drop(&mut self) {
        if let Some(waiters) = self.waiters {
            waiters.remove_waker(self.wait_key, true);
        }
    }
}

impl<M: Manager + Send> Future for Waiting<'_, M> {
    type Output = IdleConn<M>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waiters = self.waiters.expect("polled after complete");

        // if we get a connection then we remove the waker by wait key.
        if let Ok(conn) = self.pool.pool.pop() {
            waiters.remove_waker(self.wait_key, false);
            self.waiters = None;
            return Poll::Ready(conn);
        }

        // if we can't get a connection then we spawn new ones if we have not hit the max pool size.
        if self.pool.load_spawn() < self.pool.statics.max_size {
            let pool = self.pool.clone();
            self.pool.incr_spawn(1);
            let _ = tokio_executor::DefaultExecutor::current()
                .spawn(Box::pin(async move {
                    let _ = pool.add_connection().await;
                }))
                .map_err(|_| self.pool.decr_spawn(1));
        }

        // otherwise we lock WaitersLock and either insert our waker if we don't have a wait key yet
        // or register a new one if we already have a wait key.
        {
            let mut w = waiters.0.lock();
            if self.wait_key == WAIT_KEY_NONE {
                self.wait_key = w.insert(Waiter::Waiting(cx.waker().clone()));
            } else {
                w[self.wait_key].register(cx.waker());
            }
        }

        //        // double check to make sure if we can get a connection.
        //        // If we can then we just undone the previous steps and return with our connection.
        //        if let Ok(conn) = self.pool.pop() {
        //            waiters.remove_waker(self.wait_key, false);
        //            self.waiters = None;
        //            return Poll::Ready(conn);
        //        }

        Poll::Pending
    }
}

//unsafe impl Send for WaitersLock {}
//unsafe impl Sync for WaitersLock {}
//unsafe impl< M: Manager + Send> Send for Waiting<'_, M> {}
//unsafe impl< M: Manager + Send> Sync for Waiting<'_, M> {}
