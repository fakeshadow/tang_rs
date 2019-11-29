use std::future::Future;
use std::pin::Pin;

use futures_util::TryFutureExt;
use redis::{aio::MultiplexedConnection, Client, IntoConnectionInfo, RedisError};

use crate::manager::{Manager, ManagerFuture};
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
    type Connection = MultiplexedConnection;
    type Error = RedisPoolError;

    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>> {
        Box::pin(self.client.get_multiplexed_tokio_connection().err_into())
    }

    fn is_valid<'a>(
        &'a self,
        c: &'a mut Self::Connection,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(async move { redis::cmd("PING").query_async(c).err_into().await })
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

impl From<tokio::time::Elapsed> for RedisPoolError {
    fn from(_e: tokio::time::Elapsed) -> RedisPoolError {
        RedisPoolError::TimeOut
    }
}
