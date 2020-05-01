pub use tang_rs::{Builder, Pool, PoolRef};

use std::fmt;
use std::future::Future;
use std::time::Duration;

#[cfg(feature = "with-async-std")]
use async_std::{
    future::{timeout, TimeoutError},
    prelude::StreamExt,
    stream::interval,
    task,
};
use redis::{aio::MultiplexedConnection, Client, IntoConnectionInfo, RedisError};
use tang_rs::{Manager, ManagerFuture, WeakSharedManagedPool};
#[cfg(feature = "with-tokio")]
use tokio::time::{interval, timeout, Elapsed as TimeoutError};

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

macro_rules! impl_manager {
    ($connection: ty, $get_connection: ident, $spawn: path, $timeout: ident, $interval_tick: ident) => {
        impl Manager for RedisManager {
            type Connection = $connection;
            type Error = RedisPoolError;
            type TimeoutError = TimeoutError;

            fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>> {
                Box::pin(async move {
                    let conn = self.client.$get_connection().await?;
                    Ok(conn)
                })
            }

            fn is_valid<'a>(
                &self,
                c: &'a mut Self::Connection,
            ) -> ManagerFuture<'a, Result<(), Self::Error>> {
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
                $spawn(fut);
            }

            fn timeout<'fu, Fut>(
                &self,
                fut: Fut,
                dur: Duration,
            ) -> ManagerFuture<'fu, Result<Fut::Output, Self::TimeoutError>>
            where
                Fut: Future + Send + 'fu,
            {
                Box::pin($timeout(dur, fut))
            }

            fn schedule_inner(
                shared_pool: WeakSharedManagedPool<Self>,
            ) -> ManagerFuture<'static, ()> {
                let rate = shared_pool
                    .upgrade()
                    .expect("Pool is gone before we start schedule work")
                    .get_builder()
                    .get_reaper_rate();
                let mut interval = interval(rate);
                Box::pin(async move {
                    loop {
                        let _i = interval.$interval_tick().await;
                        match shared_pool.upgrade() {
                            Some(shared_pool) => {
                                let _ = shared_pool.reap_idle_conn().await;
                            }
                            None => break,
                        }
                    }
                })
            }

            fn garbage_collect_inner(
                shared_pool: WeakSharedManagedPool<Self>,
            ) -> ManagerFuture<'static, ()> {
                let rate = shared_pool
                    .upgrade()
                    .expect("Pool is gone before we start garbage collection")
                    .get_builder()
                    .get_reaper_rate();
                let mut interval = interval(rate * 6);
                Box::pin(async move {
                    loop {
                        let _i = interval.$interval_tick().await;
                        match shared_pool.upgrade() {
                            Some(shared_pool) => shared_pool.garbage_collect(),
                            None => break,
                        }
                    }
                })
            }
        }
    };
}

#[cfg(feature = "with-tokio")]
impl_manager!(
    MultiplexedConnection,
    get_multiplexed_tokio_connection,
    tokio::spawn,
    timeout,
    tick
);

#[cfg(feature = "with-async-std")]
impl_manager!(
    MultiplexedConnection,
    get_multiplexed_async_std_connection,
    task::spawn,
    timeout,
    next
);

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

impl From<TimeoutError> for RedisPoolError {
    fn from(_e: TimeoutError) -> RedisPoolError {
        RedisPoolError::TimeOut
    }
}
