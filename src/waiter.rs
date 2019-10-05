use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crossbeam_queue::ArrayQueue;
use parking_lot::Mutex;
use slab::Slab;

use crate::{manager::Manager, IdleConn};

pub(crate) const WAIT_KEY_NONE: usize = std::usize::MAX;

// an enum to determine the state of a `Waker`.
enum Waiter {
    Waiting(Waker),
    Woken,
}

impl Waiter {
    #[inline]
    fn register(&mut self, w: &Waker) {
        // if the newly waker will wake the old one then we just ignore the register.
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

/// WaitersLock holds all the futures' Wakers that are waiting for a connection.
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
pub(crate) struct Waiting<'a, M: Manager> {
    pool: &'a ArrayQueue<IdleConn<M>>,
    waiters: Option<&'a WaitersLock>,
    wait_key: usize,
}

impl<'a, M: Manager> Waiting<'a, M> {
    pub(crate) fn new(pool: &'a ArrayQueue<IdleConn<M>>, waiters: &'a WaitersLock) -> Self {
        Waiting {
            pool,
            waiters: Some(waiters),
            wait_key: WAIT_KEY_NONE,
        }
    }
}

impl<'a, M: Manager> Drop for Waiting<'a, M> {
    #[inline]
    fn drop(&mut self) {
        if let Some(waiters) = self.waiters {
            waiters.remove_waker(self.wait_key, true);
        }
    }
}

impl<'a, M: Manager> Future for Waiting<'a, M> {
    type Output = IdleConn<M>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waiters = self.waiters.expect("polled after complete");

        // if we get a connection then we remove the waker by key.
        if let Ok(conn) = self.pool.pop() {
            waiters.remove_waker(self.wait_key, false);
            self.waiters = None;
            return Poll::Ready(conn);
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

        // double check to make sure if we can get a connection.
        // If we can then we just undone the previous steps and return with our connection.
        if let Ok(conn) = self.pool.pop() {
            waiters.remove_waker(self.wait_key, false);
            self.waiters = None;
            return Poll::Ready(conn);
        }

        Poll::Pending
    }
}
