//! ## feature
//!
//! * `with-tokio` - default feature run on `tokio` runtime.
//! * `with-async-std` - run on `smol` runtime.
//! * `with-ntex` - run on `ntex` and `actix` runtime

#![forbid(unsafe_code)]
use core::fmt;
use core::future::Future;
use core::time::Duration;

use redis::{aio::MultiplexedConnection, Client, IntoConnectionInfo, RedisError};

pub use tang_rs::{Builder, Pool, PoolRef, PoolRefOwned};
use tang_rs::{Manager, ManagerFuture, ManagerTimeout};

#[derive(Clone)]
pub struct RedisManager {
    client: Client,
}

impl RedisManager {
    pub fn new(params: impl IntoConnectionInfo) -> Self {
        RedisManager {
            client: Client::open(params).expect("Failed to open redis client"),
        }
    }
}

macro_rules! manager {
    ($connection: ty, $get_connection: ident, $spawn: path, $timeout: path, $timeout_err: ty, $delay_fn: path) => {
        impl Manager for RedisManager {
            type Connection = $connection;
            type Error = RedisPoolError;
            type Timeout = $timeout;
            type TimeoutError = $timeout_err;

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

            fn timeout<Fut: Future>(
                &self,
                fut: Fut,
                dur: Duration,
            ) -> ManagerTimeout<Fut, Self::Timeout> {
                ManagerTimeout::new(fut, $delay_fn(dur))
            }
        }
    };
}

#[cfg(feature = "with-ntex")]
manager!(
    MultiplexedConnection,
    get_multiplexed_tokio_connection,
    tokio::task::spawn_local,
    tokio::time::Delay,
    (),
    tokio::time::delay_for
);

#[cfg(feature = "with-tokio")]
manager!(
    MultiplexedConnection,
    get_multiplexed_tokio_connection,
    tokio::spawn,
    tokio::time::Delay,
    (),
    tokio::time::delay_for
);

#[cfg(feature = "with-async-std")]
manager!(
    MultiplexedConnection,
    get_multiplexed_async_std_connection,
    async_std::task::spawn,
    smol::Timer,
    std::time::Instant,
    smol::Timer::new
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

#[cfg(not(feature = "with-async-std"))]
impl From<()> for RedisPoolError {
    fn from(_: ()) -> RedisPoolError {
        RedisPoolError::TimeOut
    }
}

#[cfg(feature = "with-async-std")]
impl From<std::time::Instant> for RedisPoolError {
    fn from(_: std::time::Instant) -> RedisPoolError {
        RedisPoolError::TimeOut
    }
}
