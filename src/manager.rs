use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::pool::{SharedManagedPool, WeakSharedManagedPool};

pub type ManagerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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

    /// Override this method if you actually want to handle a schedule task.
    ///
    /// The schedule interval is determined by `Builder.reaper_rate`
    ///
    /// *. Only called when `Builder.max_lifetime.is_some() || Builder.idle_timeout.is_some()`
    fn schedule_inner(_shared_pool: WeakSharedManagedPool<Self>) -> ManagerFuture<'static, ()> {
        Box::pin(async {})
    }

    /// Override this method if you actually want to do some garbage collection.
    ///
    /// The garbage collect interval is determined by `Builder.reaper_rate * 6`
    ///
    /// *. Only called when `Builder.use_gc == true`
    fn garbage_collect_inner(
        _shared_pool: WeakSharedManagedPool<Self>,
    ) -> ManagerFuture<'static, ()> {
        Box::pin(async {})
    }

    /// Default jobs to do when the pool start.
    ///
    /// By overriding it you can take control what will be run when the pool starts.
    fn on_start(&self, shared_pool: &SharedManagedPool<Self>) {
        self.schedule_reaping(shared_pool);
        self.garbage_collect(shared_pool);
    }

    /// This method will be called when `Pool<Manager>` is dropping
    fn on_stop(&self) {}

    // schedule reaping runs in a spawned future.
    fn schedule_reaping(&self, shared_pool: &SharedManagedPool<Self>) {
        let builder = shared_pool.get_builder();
        if builder.max_lifetime.is_some() || builder.idle_timeout.is_some() {
            let fut = Self::schedule_inner(Arc::downgrade(shared_pool));
            self.spawn(fut);
        }
    }

    // schedule garbage collection runs in a spawned future.
    fn garbage_collect(&self, shared_pool: &SharedManagedPool<Self>) {
        let builder = shared_pool.get_builder();
        if builder.use_gc {
            let fut = Self::garbage_collect_inner(Arc::downgrade(shared_pool));
            self.spawn(fut);
        }
    }
}
