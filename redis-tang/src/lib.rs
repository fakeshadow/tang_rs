pub use tang_rs::{Builder, Pool, PoolRef};

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use redis::{aio::MultiplexedConnection, Client, IntoConnectionInfo, RedisError};
use tang_rs::{Manager, ManagerFuture, SharedManagedPool};
use tokio::time::{interval, timeout, Elapsed};

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
    type TimeoutError = Elapsed;

    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>> {
        Box::pin(async move {
            let conn = self.client.get_multiplexed_tokio_connection().await?;
            Ok(conn)
        })
    }

    fn is_valid<'a>(
        &'a self,
        c: &'a mut Self::Connection,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            let _ = redis::cmd("PING").query_async(c).await?;
            Ok(())
        })
    }

    fn is_closed(&self, _conn: &mut Self::Connection) -> bool {
        false
    }

    fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(fut);
    }

    fn timeout<'fu, Fut>(
        &self,
        fut: Fut,
        dur: Duration,
    ) -> ManagerFuture<'fu, Result<Fut::Output, Self::TimeoutError>>
    where
        Fut: Future + Send + 'fu,
    {
        Box::pin(timeout(dur, fut))
    }

    fn schedule_inner(shared_pool: SharedManagedPool<Self>) -> ManagerFuture<'static, ()> {
        let mut interval = interval(shared_pool.get_builder().get_reaper_rate());
        Box::pin(async move {
            loop {
                let _i = interval.tick().await;
                let _ = shared_pool.reap_idle_conn().await;
            }
        })
    }

    fn garbage_collect_inner(shared_pool: SharedManagedPool<Self>) -> ManagerFuture<'static, ()> {
        let mut interval = interval(shared_pool.get_builder().get_reaper_rate() * 6);
        Box::pin(async move {
            loop {
                let _i = interval.tick().await;
                shared_pool.garbage_collect();
            }
        })
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

impl From<Elapsed> for RedisPoolError {
    fn from(_e: Elapsed) -> RedisPoolError {
        RedisPoolError::TimeOut
    }
}
