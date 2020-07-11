use core::fmt::Debug;
use core::future::Future;
use core::pin::Pin;
use core::time::Duration;
#[cfg(feature = "no-send")]
use std::rc::Rc as WrapPointer;
#[cfg(not(feature = "no-send"))]
use std::sync::Arc as WrapPointer;

use crate::pool::SharedManagedPool;
use crate::util::timeout::ManagerTimeout;

#[cfg(not(feature = "no-send"))]
pub type ManagerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[cfg(feature = "no-send")]
pub type ManagerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// # Types for different runtimes:
///
/// Trait type                  runtime                 Type                        constructor
/// <Manager::Timeout>          tokio                   tokio::time::Delay          tokio::time::delay_for
///                             async-std               smol::Timer                 smol::Timer::after
///
/// <Manager::TimeoutError>     tokio                   ()
///                             async-std               std::time::Instant
pub trait Manager: Sized + Send + Sync + 'static {
    type Connection: Unpin + 'static;
    type Error: Debug + From<Self::TimeoutError> + 'static;
    type Timeout: Future<Output = Self::TimeoutError> + Send;
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
    #[cfg(not(feature = "no-send"))]
    fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static;

    #[cfg(feature = "no-send")]
    fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + 'static;

    /// Used to cancel futures and return `Manager::TimeoutError`.
    ///
    /// The duration is determined by `Builder.wait_timeout` and `Builder.connection_timeout`
    fn timeout<Fut: Future>(&self, fut: Fut, _dur: Duration) -> ManagerTimeout<Fut, Self::Timeout>;

    /// This method will be called when `Pool<Manager>::init()` executes.
    fn on_start(&self, _shared_pool: &SharedManagedPool<Self>) {}

    /// This method will be called when `Pool<Manager>` is dropping
    fn on_stop(&self) {}
}
