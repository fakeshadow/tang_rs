use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

pub(crate) type ManagerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// trait come from bb8.
pub trait Manager: Send + Sync + 'static {
    type Connection: Send + 'static;
    type Error: Send + 'static + Debug + From<tokio_timer::timeout::Elapsed>;

    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>>;

    fn is_valid<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
    ) -> ManagerFuture<'a, Result<(), Self::Error>>;

    fn is_closed(&self, conn: &mut Self::Connection) -> bool;
}
