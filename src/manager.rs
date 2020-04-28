use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::SharedManagedPool;

pub type ManagerFuture<'a, T> = Pin<Box<dyn Future<Output=T> + Send + 'a>>;

pub trait Manager: Sized + Send + Sync + 'static {
    type Connection: Send + 'static;
    type Error: Send + Debug + From<Self::TimeoutError> + 'static;
    type TimeoutError: Send + Debug + 'static;

    /// generate a new connection
    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>>;

    /// check if a connection is valid
    fn is_valid<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
    ) -> ManagerFuture<'a, Result<(), Self::Error>>;

    ///  check if a connection is closed
    fn is_closed(&self, conn: &mut Self::Connection) -> bool;

    /// spawn futures on your executor
    /// The future have to be `Send` and the return type(e.g. JoinHandler) of your executor will be ignored.
    fn spawn<Fut>(&self, fut: Fut)
        where
            Fut: Future<Output=()> + Send + 'static;

    /// timeout method is used to cancel futures and return a TimeoutError.
    /// By default we just ignore the timeout error type and return the future directly.
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

    fn schedule_inner(shared_pool: SharedManagedPool<Self>) -> ManagerFuture<'static, ()>;

    fn garbage_collect_inner(shared_pool: SharedManagedPool<Self>) ->  ManagerFuture<'static, ()>;

    /// job to do when the pool start.
    fn on_start(&self, shared_pool: &SharedManagedPool<Self>) {
        self.schedule_reaping(shared_pool);
        self.garbage_collect(shared_pool);
    }

    // schedule reaping runs in a spawned future.
    fn schedule_reaping(&self, shared_pool: &SharedManagedPool<Self>) {
        let statics = &shared_pool.builder;
        if statics.max_lifetime.is_some() || statics.idle_timeout.is_some() {
            let fut = Self::schedule_inner(shared_pool.clone());
            self.spawn(fut);
        }
    }

    // schedule garbage collection runs in a spawned future.
    fn garbage_collect(&self, shared_pool: &SharedManagedPool<Self>) {
        let statics = &shared_pool.builder;
        if statics.use_gc {
            let fut = Self::garbage_collect_inner(shared_pool.clone());
            self.spawn(fut);
        }
    }
}