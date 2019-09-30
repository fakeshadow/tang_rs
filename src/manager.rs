use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

/// trait come from bb8.
pub trait Manager: Send + Sync + 'static {
    type Connection: Send + 'static;
    type Error: Send + 'static + Debug;

    fn connect<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'a>>;

    fn is_valid<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>;

    fn is_closed(&self, conn: &mut Self::Connection) -> bool;
}
