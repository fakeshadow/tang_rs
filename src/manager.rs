use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

pub(crate) type ManagerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// trait come from bb8.
#[cfg(feature = "default")]
pub trait Manager: Send + Sync + 'static {
    type Connection: Send + 'static;
    type Error: Send
        + 'static
        + Debug
        + From<tokio_timer::timeout::Elapsed>
        + From<futures::channel::oneshot::Canceled>;

    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>>;

    fn is_valid<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
    ) -> ManagerFuture<'a, Result<(), Self::Error>>;

    fn is_closed(&self, conn: &mut Self::Connection) -> bool;
}

#[cfg(feature = "actix-web")]
pub trait Manager: Send + Sync + 'static {
    type Connection: Send + 'static;
    type Error: Send + 'static + Debug + From<tokio_timer01::timeout::Error<Self::Error>>;

    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>>;

    fn is_valid<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
    ) -> ManagerFuture<'a, Result<(), Self::Error>>;

    fn is_closed(&self, conn: &mut Self::Connection) -> bool;
}
