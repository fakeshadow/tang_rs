use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
#[cfg(feature = "no-send")]
use std::rc::Rc;
#[cfg(not(feature = "no-send"))]
use std::sync::Arc;
use std::time::Duration;

use crate::pool::SharedManagedPool;

#[cfg(not(feature = "no-send"))]
pub type ManagerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[cfg(feature = "no-send")]
pub type ManagerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[cfg(not(feature = "no-send"))]
pub trait Manager: Sized + Send + Sync + 'static {
    type Connection: Send + 'static;
    type Error: Send + Debug + From<Self::TimeoutError> + 'static;
    type TimeoutError: Send + Debug + 'static;

    /// generate a new connection and put it into pool.
    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>>;

    /// check if a connection is valid.
    ///
    /// *. Only called when `Builder.always_check == true`
    fn is_valid<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
    ) -> ManagerFuture<'a, Result<(), Self::Error>>;

    /// check if a connection is closed.
    ///
    /// This happens before a connection put back to pool.
    fn is_closed(&self, conn: &mut Self::Connection) -> bool;

    /// spawn futures on your executor
    ///
    /// The future have to be `Send + 'static` and the return type(e.g. JoinHandler) of your executor will be ignored.
    fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static;

    /// Used to cancel futures and return `Manager::TimeoutError`.
    ///
    /// The duration is determined by `Builder.wait_timeout` and `Builder.connection_timeout`
    ///
    /// By default we ignore the timeout and await on the future directly.
    ///
    /// Override this method if you actually want to handle the timeout.
    fn timeout<'fu, Fut>(
        &self,
        fut: Fut,
        _dur: Duration,
    ) -> ManagerFuture<'fu, Result<Fut::Output, Self::TimeoutError>>
    where
        Fut: Future + Send + 'fu,
    {
        Box::pin(async move {
            let res = fut.await;
            Ok(res)
        })
    }

    /// This method will be called when `Pool<Manager>::init()` executes.
    fn on_start(&self, _shared_pool: &SharedManagedPool<Self>) {}

    /// This method will be called when `Pool<Manager>` is dropping
    fn on_stop(&self) {}
}

#[cfg(feature = "no-send")]
pub trait Manager: Sized + 'static {
    type Connection: Send + 'static;
    type Error: Send + Debug + From<Self::TimeoutError> + 'static;
    type TimeoutError: Send + Debug + 'static;

    /// generate a new connection and put it into pool.
    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>>;

    /// check if a connection is valid.
    ///
    /// *. Only called when `Builder.always_check == true`
    fn is_valid<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
    ) -> ManagerFuture<'a, Result<(), Self::Error>>;

    /// check if a connection is closed.
    ///
    /// This happens before a connection put back to pool.
    fn is_closed(&self, conn: &mut Self::Connection) -> bool;

    /// spawn futures on your executor
    ///
    /// The future have to be `Send + 'static` and the return type(e.g. JoinHandler) of your executor will be ignored.
    fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + 'static;

    /// Used to cancel futures and return `Manager::TimeoutError`.
    ///
    /// The duration is determined by `Builder.wait_timeout` and `Builder.connection_timeout`
    ///
    /// By default we ignore the timeout and await on the future directly.
    ///
    /// Override this method if you actually want to handle the timeout.
    fn timeout<'fu, Fut>(
        &self,
        fut: Fut,
        _dur: Duration,
    ) -> ManagerFuture<'fu, Result<Fut::Output, Self::TimeoutError>>
    where
        Fut: Future + 'fu,
    {
        Box::pin(async move {
            let res = fut.await;
            Ok(res)
        })
    }

    /// This method will be called when `Pool<Manager>::init()` executes.
    fn on_start(&self, _shared_pool: &SharedManagedPool<Self>) {}

    /// This method will be called when `Pool<Manager>` is dropping
    fn on_stop(&self) {}
}

macro_rules! default_behaviors {
    ($wrapper: tt) => {
        /// helper trait to spawn default garbage collect process to `Pool<Manager>`.
        pub trait GarbageCollect: Manager + ManagerInterval {
            fn garbage_collect(&self, shared_pool: &SharedManagedPool<Self>) {
                let builder = shared_pool.get_builder();
                if builder.use_gc {
                    let rate = builder.get_reaper_rate();
                    let shared_pool = $wrapper::downgrade(shared_pool);

                    let mut interval = Self::interval(rate * 6);
                    self.spawn(async move {
                        loop {
                            let _i = Self::tick(&mut interval).await;
                            match shared_pool.upgrade() {
                                Some(shared_pool) => {
                                    if shared_pool.is_running() {
                                        shared_pool.garbage_collect();
                                    }
                                }
                                None => break,
                            }
                        }
                    });
                }
            }
        }

        /// helper trait to spawn default schedule reaping process to `Pool<Manager>`.
        pub trait ScheduleReaping: Manager + ManagerInterval {
            // schedule reaping runs in a spawned future.
            fn schedule_reaping(&self, shared_pool: &SharedManagedPool<Self>) {
                let builder = shared_pool.get_builder();
                if builder.max_lifetime.is_some() || builder.idle_timeout.is_some() {
                    let rate = builder.get_reaper_rate();

                    let shared_pool = $wrapper::downgrade(shared_pool);

                    let mut interval = Self::interval(rate);
                    self.spawn(async move {
                        loop {
                            let _i = Self::tick(&mut interval).await;
                            match shared_pool.upgrade() {
                                Some(shared_pool) => {
                                    if shared_pool.is_running() {
                                        let _ = shared_pool.reap_idle_conn().await;
                                    }
                                }
                                None => break,
                            }
                        }
                    });
                }
            }
        }
    };
}

#[cfg(not(feature = "no-send"))]
default_behaviors!(Arc);
#[cfg(feature = "no-send")]
default_behaviors!(Rc);

/// helper trait as we have different interval tick api in different runtime
pub trait ManagerInterval {
    type Interval: Send;
    type Tick: Send;

    fn interval(dur: Duration) -> Self::Interval;

    fn tick(tick: &mut Self::Interval) -> ManagerFuture<'_, Self::Tick>;
}
