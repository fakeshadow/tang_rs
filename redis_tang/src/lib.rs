use std::fmt;
use std::future::Future;
use std::time::Duration;

#[cfg(feature = "with-async-std")]
use async_std::{
    future::{timeout, TimeoutError},
    prelude::StreamExt,
    stream::{interval, Interval},
    task,
};
use redis::{aio::MultiplexedConnection, Client, IntoConnectionInfo, RedisError};
#[cfg(not(feature = "with-async-std"))]
use tokio::time::{
    timeout, Elapsed as TimeoutError, {interval, Instant, Interval},
};

pub use tang_rs::{Builder, Pool, PoolRef, PoolRefOwned};
use tang_rs::{
    GarbageCollect, Manager, ManagerFuture, ManagerInterval, ScheduleReaping, SharedManagedPool,
};

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

macro_rules! impl_manager_interval {
    ($interval_type: ty, $tick_type: ty, $tick_method: ident) => {
        impl ManagerInterval for RedisManager {
            type Interval = $interval_type;
            type Tick = $tick_type;

            fn interval(dur: Duration) -> Self::Interval {
                interval(dur)
            }

            fn tick(tick: &mut Self::Interval) -> ManagerFuture<'_, Self::Tick> {
                Box::pin(tick.$tick_method())
            }
        }
    };
}

#[cfg(not(feature = "with-async-std"))]
impl_manager_interval!(Interval, Instant, tick);

#[cfg(feature = "with-async-std")]
impl_manager_interval!(Interval, Option<()>, next);

impl ScheduleReaping for RedisManager {}

impl GarbageCollect for RedisManager {}

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

            #[cfg(not(feature = "with-ntex"))]
            fn spawn<Fut>(&self, fut: Fut)
            where
                Fut: Future<Output = ()> + Send + 'static,
            {
                $spawn(fut);
            }

            #[cfg(feature = "with-ntex")]
            fn spawn<Fut>(&self, fut: Fut)
            where
                Fut: Future<Output = ()> + 'static,
            {
                $spawn(fut);
            }

            #[cfg(not(feature = "with-ntex"))]
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

            #[cfg(feature = "with-ntex")]
            fn timeout<'fu, Fut>(
                &self,
                fut: Fut,
                dur: Duration,
            ) -> ManagerFuture<'fu, Result<Fut::Output, Self::TimeoutError>>
            where
                Fut: Future + 'fu,
            {
                Box::pin($timeout(dur, fut))
            }

            fn on_start(&self, shared_pool: &SharedManagedPool<Self>) {
                self.schedule_reaping(shared_pool);
                self.garbage_collect(shared_pool);
            }
        }
    };
}

#[cfg(feature = "with-ntex")]
impl_manager!(
    MultiplexedConnection,
    get_multiplexed_tokio_connection,
    tokio::task::spawn_local,
    timeout,
    tick
);

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
