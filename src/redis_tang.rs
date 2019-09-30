use std::future::Future;
use std::pin::Pin;

use futures::TryFutureExt;
use redis::{aio::SharedConnection, Client, IntoConnectionInfo, RedisError};

use crate::manager::Manager;
use std::fmt;

#[derive(Clone)]
pub struct RedisManager {
    client: Client,
}

impl RedisManager {
    /// Create a new `RedisManager`
    pub fn new(params: impl IntoConnectionInfo) -> Self {
        RedisManager {
            client: Client::open(params).expect("Failed to open redis client"),
        }
    }
}

impl Manager for RedisManager {
    type Connection = SharedConnection;
    type Error = RedisError;

    fn connect<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'a>> {
        Box::pin(self.client.get_shared_async_connection())
    }

    fn is_valid<'a>(
        &'a self,
        c: &'a mut Self::Connection,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(
            redis::cmd("PING")
                .query_async(c.clone())
                .map_ok(|(_, ())| ()),
        )
    }

    fn is_closed(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

impl std::fmt::Debug for RedisManager {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("RedisManager").finish()
    }
}

pub enum RedisPoolError {
    Inner(RedisError),
    TimeOut,
}

impl fmt::Debug for RedisPoolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RedisPoolError::Inner(e) => e.fmt(f),
            RedisPoolError::TimeOut => f
                .debug_struct("RedisError")
                .field("source", &"Connection Timeout")
                .finish(),
        }
    }
}

impl From<RedisError> for RedisPoolError {
    fn from(e: RedisError) -> Self {
        RedisPoolError::Inner(e)
    }
}

#[cfg(feature = "default")]
impl From<tokio_timer::timeout::Elapsed> for RedisPoolError {
    fn from(_e: tokio_timer::timeout::Elapsed) -> RedisPoolError {
        RedisPoolError::TimeOut
    }
}

#[cfg(feature = "actix-web")]
impl<T> From<tokio_timer01::timeout::Error<T>> for RedisPoolError {
    fn from(_e: tokio_timer01::timeout::Error<T>) -> RedisPoolError {
        RedisPoolError::TimeOut
    }
}
