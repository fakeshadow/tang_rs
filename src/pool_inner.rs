// #[cfg(feature = "no-send")]
// use std::cell::{RefCell, RefMut};
// use std::collections::VecDeque;
// use std::fmt;
// use std::future::Future;
// use std::marker::PhantomData;
// use std::num::NonZeroUsize;
// use std::pin::Pin;
// #[cfg(not(feature = "no-send"))]
// use std::sync::{Mutex, MutexGuard};
// use std::task::{Context, Poll, Waker};
// use std::time::{Duration, Instant};
//
// use crate::{
//     manager::Manager,
//     pool::{IdleConn, PoolRefBehavior},
//     SharedManagedPool,
//     util::linked_list::WakerList,
// };
//
// // PoolInner holds all the IdleConn and the waiters(a FIFO linked list) waiting for a connection.
// pub(crate) struct PoolInner<M: Manager> {
//     spawned: usize,
//     marker: usize,
//     pending: VecDeque<Pending>,
//     conn: VecDeque<IdleConn<M>>,
//     waiters: WakerList,
// }
//
// impl<M: Manager> PoolInner<M> {
//     pub fn new(pool_size: usize) -> Self {
//         Self {
//             spawned: 0,
//             marker: 0,
//             pending: VecDeque::with_capacity(pool_size),
//             conn: VecDeque::with_capacity(pool_size),
//             waiters: WakerList::new(),
//         }
//     }
//
//     #[inline]
//     fn put_back(&mut self, conn: IdleConn<M>) {
//         self.conn.push_back(conn);
//     }
//
//     fn incr_spawned(&mut self, count: usize) {
//         self.spawned += count;
//     }
//
//     fn decr_spawned(&mut self, count: usize) {
//         if self.spawned > count {
//             self.spawned -= count
//         } else {
//             self.spawned = 0
//         }
//     }
//
//     fn incr_pending(&mut self, count: usize) {
//         for _i in 0..count {
//             self.pending.push_back(Pending::new());
//         }
//     }
//
//     fn decr_pending(&mut self, count: usize) {
//         for _i in 0..count {
//             self.pending.pop_front();
//         }
//     }
//
//     #[inline]
//     fn total(&mut self) -> usize {
//         self.spawned + self.pending.len()
//     }
//
//     fn incr_marker(&mut self) {
//         self.marker += 1;
//     }
//
//     fn clear_pending(&mut self, pool_size: usize) {
//         self.pending = VecDeque::with_capacity(pool_size);
//     }
//
//     fn clear_conn(&mut self, pool_size: usize) {
//         self.conn = VecDeque::with_capacity(pool_size);
//     }
// }
//
// macro_rules! pool_lock {
//     ($lock_type: ident, $guard_type: ident, $lock_method: ident, $try_lock_method: ident $(, $opt:ident)*) => {
//         pub(crate) struct PoolLock<M: Manager> {
//             inner: $lock_type<PoolInner<M>>
//         }
//
//         impl<M: Manager> PoolLock<M> {
//             pub(crate) fn new(pool_size: usize) -> Self {
//                 PoolLock {
//                     inner: $lock_type::new(PoolInner::new(pool_size))
//                 }
//             }
//
//             #[inline]
//             pub(crate) fn _lock(&self) -> $guard_type<'_, PoolInner<M>> {
//                 self.inner.$lock_method()$(.$opt())*
//             }
//
//             #[inline]
//             pub(crate) fn _try_lock(&self) -> Option<$guard_type<'_, PoolInner<M>>> {
//                 self.inner.$try_lock_method().ok()
//             }
//         }
//
//         impl<'re, M, R> PoolLockFuture<'re, M, R>
//         where M: Manager,
//               R: PoolRefBehavior<'re, M>
//         {
//             #[inline]
//             fn poll_pool_ref(
//                 &mut self,
//                 inner: &mut $guard_type<'re, PoolInner<M>>,
//             ) -> Poll<R> {
//                 match inner.conn.pop_front() {
//                     Some(conn) => {
//                         self.acquired = true;
//                         Poll::Ready(R::from_idle(conn, self.shared_pool))
//                     }
//                     None => Poll::Pending,
//                 }
//             }
//
//             #[inline]
//             fn spawn_idle_conn(&self, inner: &mut $guard_type<'_, PoolInner<M>>) {
//                 let shared = self.shared_pool;
//
//                 let builder = shared.get_builder();
//
//                 if inner.total() < builder.get_max_size() {
//                     let marker = inner.marker;
//                     let shared_clone = shared.clone();
//                     shared.spawn(async move {
//                         let _ = shared_clone.add_idle_conn(marker).await;
//                     });
//                     inner.incr_pending(1);
//                 }
//             }
//         }
//     }
// }
//
// #[cfg(not(feature = "no-send"))]
// pool_lock!(Mutex, MutexGuard, lock, try_lock, unwrap);
//
// #[cfg(feature = "no-send")]
// pool_lock!(RefCell, RefMut, borrow_mut, try_borrow_mut);
//
// impl<M: Manager> PoolLock<M> {
//     #[inline]
//     pub(crate) fn lock<'a, R>(
//         &'a self,
//         shared_pool: &'a SharedManagedPool<M>,
//     ) -> PoolLockFuture<'a, M, R> {
//         PoolLockFuture {
//             shared_pool,
//             pool_lock: self,
//             wait_key: None,
//             acquired: false,
//             _r: PhantomData,
//         }
//     }
//
//     // add pending directly to pool inner if we try to spawn new connections.
//     // and return the new pending count as option to notify the Pool to replenish connections
//     #[cfg(not(feature = "no-send"))]
//     pub(crate) fn decr_spawned(
//         &self,
//         marker: usize,
//         min_idle: usize,
//         should_spawn_new: bool,
//     ) -> Option<usize> {
//         let mut inner = self._lock();
//
//         inner.decr_spawned(1);
//         let total = inner.total();
//
//         if total < min_idle && should_spawn_new && marker == inner.marker {
//             let pending_new = min_idle - total;
//             inner.incr_pending(pending_new);
//             Some(pending_new)
//         } else {
//             None
//         }
//     }
//
//     // ToDo: currently we don't enable clear and pause function for single thread pool.
//     #[cfg(feature = "no-send")]
//     pub(crate) fn decr_spawned(&self, min_idle: usize) -> Option<usize> {
//         let mut inner = self._lock();
//
//         inner.decr_spawned(1);
//         let total = inner.total();
//
//         if total < min_idle {
//             let pending_new = min_idle - total;
//             inner.incr_pending(pending_new);
//             Some(pending_new)
//         } else {
//             None
//         }
//     }
//
//     pub(crate) fn decr_pending(&self, count: usize) {
//         let mut inner = self._lock();
//         inner.decr_pending(count);
//     }
//
//     pub(crate) fn drop_pendings<F>(&self, mut should_drop: F)
//         where
//             F: FnMut(&Pending) -> bool,
//     {
//         let mut inner = self._lock();
//
//         inner.pending.retain(|pending| !should_drop(pending));
//     }
//
//     // return new pending count and marker as Option<(usize, usize)>.
//     pub(crate) fn try_drop_conns<F>(
//         &self,
//         min_idle: usize,
//         mut should_drop: F,
//     ) -> Option<(usize, usize)>
//         where
//             F: FnMut(&IdleConn<M>) -> bool,
//     {
//         self._try_lock().and_then(|mut inner| {
//             let conn = &mut inner.conn;
//
//             let len = conn.len();
//
//             conn.retain(|conn| !should_drop(conn));
//
//             let diff = len - conn.len();
//
//             if diff > 0 {
//                 inner.decr_spawned(diff);
//             }
//
//             let total_now = inner.total();
//             if total_now < min_idle {
//                 let pending_new = min_idle - total_now;
//
//                 inner.incr_pending(pending_new);
//
//                 Some((pending_new, inner.marker))
//             } else {
//                 None
//             }
//         })
//     }
//
//     #[inline]
//     pub(crate) fn put_back(&self, conn: IdleConn<M>, max_size: usize) {
//         let mut inner = self._lock();
//
//         #[cfg(not(feature = "no-send"))]
//             let condition = inner.spawned > max_size || inner.marker != conn.get_marker();
//
//         // ToDo: currently we don't enable clear function for single thread pool.
//         #[cfg(feature = "no-send")]
//             let condition = inner.spawned > max_size;
//
//         if condition {
//             inner.decr_spawned(1);
//         } else {
//             inner.put_back(conn);
//             let opt = inner.waiters.wake_one_weak();
//             drop(inner);
//             opt.wake();
//         }
//     }
//
//     pub(crate) fn put_back_incr_spawned(&self, conn: IdleConn<M>, max_size: usize) {
//         let mut inner = self._lock();
//
//         inner.decr_pending(1);
//
//         #[cfg(not(feature = "no-send"))]
//             let condition = inner.spawned < max_size && inner.marker == conn.get_marker();
//
//         // ToDo: currently we don't enable clear function for single thread pool.
//         #[cfg(feature = "no-send")]
//             let condition = inner.spawned < max_size;
//
//         if condition {
//             inner.put_back(conn);
//             inner.incr_spawned(1);
//             let opt = inner.waiters.wake_one_weak();
//             drop(inner);
//             opt.wake();
//         }
//     }
//
//     pub(crate) fn clear(&self, pool_size: usize) {
//         let mut inner = self._lock();
//         let count = inner.conn.len();
//         inner.decr_spawned(count);
//         inner.incr_marker();
//         inner.clear_pending(pool_size);
//         inner.clear_conn(pool_size);
//     }
//
//     pub(crate) fn get_maker(&self) -> usize {
//         let inner = self._lock();
//         inner.marker
//     }
//
//     pub(crate) fn state(&self) -> State {
//         let inner = self._lock();
//         State {
//             connections: inner.spawned,
//             idle_connections: inner.conn.len(),
//             pending_connections: inner.pending.iter().cloned().collect(),
//         }
//     }
// }
//
// // `PoolLockFuture` return a future of `PoolRef`. In the `Future` we pass it's `Waker` to `PoolLock`.
// // Then when a `IdleConn` is returned to pool we lock the `PoolLock` and wake the `Wakers` inside it to notify other `PoolLockFuture` it's time to continue.
// pub(crate) struct PoolLockFuture<'a, M: Manager, R> {
//     shared_pool: &'a SharedManagedPool<M>,
//     pool_lock: &'a PoolLock<M>,
//     wait_key: Option<NonZeroUsize>,
//     acquired: bool,
//     _r: PhantomData<R>,
// }
//
// impl<M: Manager, R> Drop for PoolLockFuture<'_, M, R> {
//     #[inline]
//     fn drop(&mut self) {
//         if let Some(wait_key) = self.wait_key {
//             self.wake_cold(wait_key);
//         }
//     }
// }
//
// impl<M: Manager, R> PoolLockFuture<'_, M, R> {
//     #[cold]
//     fn wake_cold(&self, wait_key: NonZeroUsize) {
//         let mut inner = self.pool_lock._lock();
//         let wait_key = unsafe { inner.waiters.remove(wait_key) };
//
//         if wait_key.is_none() && !self.acquired {
//             // We were awoken but didn't acquire the lock. Wake up another task.
//             let opt = inner.waiters.wake_one_weak();
//             drop(inner);
//             opt.wake();
//         }
//     }
// }
//
// impl<'re, M, R> Future for PoolLockFuture<'re, M, R>
//     where
//         M: Manager,
//         R: PoolRefBehavior<'re, M> + Unpin,
// {
//     type Output = R;
//
//     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         // we return pending status directly if the pool is in pausing state.
//         // ToDo: currently we don't enable pause function for single thread pool.
//         #[cfg(not(feature = "no-send"))]
//         if !self.shared_pool.is_running() {
//             return Poll::Pending;
//         }
//
//         let mut inner = self.pool_lock._lock();
//
//         // poll a PoolRef and use the result to mutate PoolLockFuture state as well as PoolInner state.
//         let poll = self.poll_pool_ref(&mut inner);
//
//         // Either insert our waker if we don't have a wait key yet or overwrite the old waker entry if we already have a wait key.
//         match self.wait_key {
//             Some(wait_key) => {
//                 if poll.is_ready() {
//                     // we got poll ready therefore remove self wait_key and entry in waiters
//                     unsafe { inner.waiters.remove(wait_key) };
//                     self.wait_key = None;
//                 } else {
//                     // if we can't get a PoolRef then we spawn a new connection if we have not hit the max pool size.
//                     self.spawn_idle_conn(&mut inner);
//
//                     let opt = unsafe { inner.waiters.get(wait_key) };
//
//                     // We replace the waker if we are woken and have no key in waiters or have a new context which will not wake the old waker.
//                     if opt
//                         .as_ref()
//                         .map(|waker| !waker.will_wake(cx.waker()))
//                         .unwrap_or_else(|| true)
//                     {
//                         let waker = cx.waker().clone();
//                         *opt = Some(waker);
//                     }
//                 }
//             }
//             None => {
//                 if poll.is_pending() {
//                     // if we can't get a PoolRef then we spawn a new connection if we have not hit the max pool size.
//                     self.spawn_idle_conn(&mut inner);
//
//                     let waker = cx.waker().clone();
//                     let wait_key = inner.waiters.insert(Some(waker));
//                     self.wait_key = Some(wait_key);
//                 }
//             }
//         }
//
//         poll
//     }
// }

use std::cell::UnsafeCell;
#[cfg(feature = "no-send")]
use std::cell::{RefCell, RefMut};
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(feature = "no-send"))]
use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::{
    builder::Builder,
    manager::Manager,
    pool::{IdleConn, PoolRefBehavior},
    util::linked_list::WakerList,
    SharedManagedPool,
};

pub(crate) struct Config {
    min_idle: AtomicUsize,
    max_size: AtomicUsize,
}

impl Config {
    pub(crate) fn from_builder(builder: &Builder) -> Self {
        Self {
            min_idle: AtomicUsize::new(builder.min_idle),
            max_size: AtomicUsize::new(builder.max_size),
        }
    }
}

macro_rules! pool_lock {
    ($lock_type: ident, $guard_type: ident, $lock_method: ident, $try_lock_method: ident $(, $opt:ident)*) => {
        pub(crate) struct PoolLock<M: Manager> {
            inner: UnsafeCell<PoolInner<M>>,
            waiters: $lock_type<WakerList>,
            config: Config
        }

        impl<M: Manager> PoolLock<M> {
            pub(crate) fn from_builder(builder: &Builder) -> Self {
                Self {
                    inner: UnsafeCell::new(PoolInner {
                        spawned: 0,
                        marker: 0,
                        pending: VecDeque::with_capacity(builder.max_size),
                        conn: VecDeque::with_capacity(builder.max_size),
                    }),
                    waiters: $lock_type::new(WakerList::new()),
                    config: Config::from_builder(builder)
                }
            }

            #[inline]
            pub(crate) fn _lock(&self) -> $guard_type<'_, WakerList> {
                self.waiters.$lock_method()$(.$opt())*
            }

            #[inline]
            pub(crate) fn _try_lock(&self) -> Option<$guard_type<'_, WakerList>> {
                self.waiters.$try_lock_method().ok()
            }
        }
    };
}

#[cfg(not(feature = "no-send"))]
pool_lock!(Mutex, MutexGuard, lock, try_lock, unwrap);

#[cfg(feature = "no-send")]
pool_lock!(RefCell, RefMut, borrow_mut, try_borrow_mut);

struct PoolInner<M: Manager> {
    spawned: usize,
    marker: usize,
    pending: VecDeque<Pending>,
    conn: VecDeque<IdleConn<M>>,
}

// safety: These functions are only called when holding MutexGuard<'_, WaiterList> or RefMut<'_, WaiterList>
// (We never use Ref<'_, WaiterList> so there is no concurrent reader).
// So there could be only one reader/writer at the same time.
impl<M: Manager> PoolLock<M> {
    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn borrow_inner(&self) -> &mut PoolInner<M> {
        unsafe { &mut *self.inner.get() }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn borrow_conn(&self) -> &mut VecDeque<IdleConn<M>> {
        &mut self.borrow_inner().conn
    }

    #[allow(clippy::mut_from_ref)]
    fn borrow_pending(&self) -> &mut VecDeque<Pending> {
        &mut self.borrow_inner().pending
    }

    #[inline]
    fn marker(&self) -> usize {
        self.borrow_inner().marker
    }

    #[inline]
    fn spawned(&self) -> usize {
        self.borrow_inner().spawned
    }

    fn _incr_spawned(&self, count: usize) {
        self.borrow_inner().spawned += count;
    }

    fn _decr_spawned(&self, count: usize) {
        self.borrow_inner().spawned -= count;
    }

    fn incr_marker(&self) {
        self.borrow_inner().marker += 1;
    }

    #[inline]
    fn total(&self) -> usize {
        let inner = self.borrow_inner();
        inner.spawned + inner.pending.len()
    }

    fn pending_owned(&self) -> Vec<Pending> {
        self.borrow_pending().iter().cloned().collect()
    }

    fn conn_len(&self) -> usize {
        self.borrow_conn().len()
    }

    #[inline]
    fn pop_conn(&self) -> Option<IdleConn<M>> {
        self.borrow_conn().pop_front()
    }

    #[inline]
    fn push_conn(&self, conn: IdleConn<M>) {
        self.borrow_conn().push_back(conn);
    }

    fn incr_pending(&self, count: usize) {
        let pending = self.borrow_pending();
        for _i in 0..count {
            pending.push_back(Pending::new());
        }
    }

    fn _decr_pending(&self, count: usize) {
        let pending = self.borrow_pending();

        for _i in 0..count {
            pending.pop_front();
        }
    }

    fn clear_pending(&self) {
        let pool_size = self.max_size();
        *(self.borrow_pending()) = VecDeque::with_capacity(pool_size);
    }

    fn clear_conn(&self) {
        let pool_size = self.max_size();
        *(self.borrow_conn()) = VecDeque::with_capacity(pool_size);
    }
}

impl<M: Manager> PoolLock<M> {
    #[inline]
    pub(crate) fn lock<'a, R>(
        &'a self,
        shared_pool: &'a SharedManagedPool<M>,
    ) -> PoolLockFuture<'a, M, R> {
        PoolLockFuture {
            shared_pool,
            pool_lock: Some(self),
            wait_key: None,
            _r: PhantomData,
        }
    }

    // add pending directly to pool inner if we try to spawn new connections.
    // and return the new pending count as option to notify the Pool to replenish connections
    #[cfg(not(feature = "no-send"))]
    pub(crate) fn decr_spawned(&self, marker: usize, should_spawn_new: bool) -> Option<usize> {
        let _waiters = self._lock();

        self._decr_spawned(1);

        let total = self.total();
        let min_idle = self.min_idle();

        if total < min_idle && should_spawn_new && marker == self.marker() {
            let pending_new = min_idle - total;
            self.incr_pending(pending_new);
            Some(pending_new)
        } else {
            None
        }
    }

    #[cfg(feature = "no-send")]
    pub(crate) fn decr_spawned(&self) -> Option<usize> {
        let _waiters = self._lock();

        self._decr_spawned(1);

        let total = self.total();
        let min_idle = self.min_idle();

        if total < min_idle {
            let pending_new = min_idle - total;
            self.incr_pending(pending_new);
            Some(pending_new)
        } else {
            None
        }
    }

    pub(crate) fn decr_pending(&self, count: usize) {
        let _waiters = self._lock();
        self._decr_pending(count);
    }

    pub(crate) fn drop_pendings<F>(&self, mut should_drop: F)
    where
        F: FnMut(&Pending) -> bool,
    {
        let _waiters = self._lock();

        self.borrow_pending()
            .retain(|pending| !should_drop(pending));
    }

    // return new pending count and marker as Option<(usize, usize)>.
    pub(crate) fn try_drop_conns<F>(&self, mut should_drop: F) -> Option<(usize, usize)>
    where
        F: FnMut(&IdleConn<M>) -> bool,
    {
        self._try_lock().and_then(|_| {
            let len = self.conn_len();

            self.borrow_conn().retain(|conn| !should_drop(conn));

            let diff = len - self.conn_len();

            if diff > 0 {
                self._decr_spawned(diff);
            }

            let total_now = self.total();
            let min_idle = self.min_idle();

            if total_now < min_idle {
                let pending_new = min_idle - total_now;

                self.incr_pending(pending_new);

                Some((pending_new, self.marker()))
            } else {
                None
            }
        })
    }

    #[inline]
    pub(crate) fn put_back(&self, conn: IdleConn<M>) {
        let mut waiters = self._lock();

        #[cfg(not(feature = "no-send"))]
        let condition = self.spawned() > self.max_size() || self.marker() != conn.marker();

        // ToDo: currently we don't enable clear function for single thread pool.
        #[cfg(feature = "no-send")]
        let condition = self.spawned() > self.max_size();

        if condition {
            self._decr_spawned(1);
        } else {
            self.push_conn(conn);
            let opt = waiters.wake_one_weak();
            drop(waiters);
            opt.wake();
        }
    }

    pub(crate) fn put_back_incr_spawned(&self, conn: IdleConn<M>) {
        let mut waiters = self._lock();

        self._decr_pending(1);

        #[cfg(not(feature = "no-send"))]
        let condition = self.spawned() < self.max_size() && self.marker() == conn.marker();

        // ToDo: currently we don't enable clear function for single thread pool.
        #[cfg(feature = "no-send")]
        let condition = self.spawned() < self.max_size();

        if condition {
            self.push_conn(conn);
            self._incr_spawned(1);
            let opt = waiters.wake_one_weak();
            drop(waiters);
            opt.wake();
        }
    }

    pub(crate) fn clear(&self) {
        let _waiters = self._lock();
        let count = self.conn_len();
        self._decr_spawned(count);
        self.incr_marker();
        self.clear_pending();
        self.clear_conn();
    }

    pub(crate) fn set_max_size(&self, size: usize) {
        self.config.max_size.store(size, Ordering::Relaxed);
    }

    fn max_size(&self) -> usize {
        self.config.max_size.load(Ordering::Relaxed)
    }

    pub(crate) fn set_min_idle(&self, size: usize) {
        self.config.min_idle.store(size, Ordering::Relaxed);
    }

    fn min_idle(&self) -> usize {
        self.config.min_idle.load(Ordering::Relaxed)
    }

    pub(crate) fn get_maker(&self) -> usize {
        let _waiters = self._lock();
        self.marker()
    }

    pub(crate) fn state(&self) -> State {
        let _waiters = self._lock();

        State {
            connections: self.spawned(),
            idle_connections: self.conn_len(),
            pending_connections: self.pending_owned(),
        }
    }

    #[inline]
    fn _spawn_idle_conn(&self, shared_pool: &SharedManagedPool<M>) {
        if self.total() < self.max_size() {
            let marker = self.marker();
            let shared_clone = shared_pool.clone();
            shared_pool.spawn(async move {
                let _ = shared_clone.add_idle_conn(marker).await;
            });
            self.incr_pending(1);
        }
    }
}

pub(crate) struct PoolLockFuture<'a, M: Manager, R> {
    shared_pool: &'a SharedManagedPool<M>,
    pool_lock: Option<&'a PoolLock<M>>,
    wait_key: Option<NonZeroUsize>,
    _r: PhantomData<R>,
}

impl<M: Manager, R> Drop for PoolLockFuture<'_, M, R> {
    fn drop(&mut self) {
        if let Some(wait_key) = self.wait_key {
            self.wake_cold(wait_key);
        }
    }
}

impl<M: Manager, R> PoolLockFuture<'_, M, R> {
    #[cold]
    fn wake_cold(&self, wait_key: NonZeroUsize) {
        if let Some(pool_lock) = self.pool_lock {
            let mut waiters = pool_lock._lock();
            let wait_key = unsafe { waiters.remove(wait_key) };

            if wait_key.is_none() {
                // We were awoken but didn't acquire the lock. Wake up another task.
                let opt = waiters.wake_one_weak();
                drop(waiters);
                opt.wake();
            }
        }
    }
}

impl<'re, M, R> Future for PoolLockFuture<'re, M, R>
where
    M: Manager,
    R: PoolRefBehavior<'re, M> + Unpin,
{
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let shared_pool = self.shared_pool;

        // we return pending status directly if the pool is in pausing state.
        // ToDo: currently we don't enable pause function for single thread pool.
        #[cfg(not(feature = "no-send"))]
        {
            if !shared_pool.is_running() {
                return Poll::Pending;
            }
        }

        let pool_lock = self.pool_lock.expect("Future polled after finish");

        let mut waiters = pool_lock._lock();

        // Poll a type impl with PoolRefBehavior and use the result to mutate PoolLockFuture state as well as PoolLock state.
        let poll = match pool_lock.pop_conn() {
            Some(conn) => {
                self.pool_lock = None;
                Poll::Ready(R::from_idle(conn, self.shared_pool))
            }
            None => Poll::Pending,
        };

        // Either insert our waker if we don't have a wait key yet or overwrite the old waker entry if we already have a wait key.
        match self.wait_key {
            Some(wait_key) => {
                if poll.is_ready() {
                    // We got poll ready therefore remove self wait_key and entry in waiters
                    unsafe { waiters.remove(wait_key) };
                    self.wait_key = None;
                } else {
                    // We got pending so we spawn a new connection if we have not hit the max pool size.
                    pool_lock._spawn_idle_conn(shared_pool);

                    let opt = unsafe { waiters.get(wait_key) };

                    // We replace the waker if we are woken and have no key in waiters or have a new context which will not wake the old waker.
                    if opt
                        .as_ref()
                        .map(|waker| !waker.will_wake(cx.waker()))
                        .unwrap_or_else(|| true)
                    {
                        *opt = Some(cx.waker().clone());
                    }
                }
            }
            None => {
                if poll.is_pending() {
                    // We got pending so we spawn a new connection if we have not hit the max pool size.
                    pool_lock._spawn_idle_conn(shared_pool);

                    let wait_key = waiters.insert(Some(cx.waker().clone()));
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
