//! # A tokio-postgres connection pool.
//! use `tokio-0.2` runtime.
//! smoe code come from
//! [bb8](https://docs.rs/bb8/0.3.1/bb8/)
//! [L3-37](https://github.com/OneSignal/L3-37/)
//!
//! # Known Limitation:
//! No tests.
//! no `tokio-0.1` support.
//! can't be used in nested runtimes.
//!
//! # Example:
//!``` rust
//!use std::time::Duration;
//!
//!use futures::TryStreamExt;
//!use tang_rs::{Builder, PostgresPoolError, PostgresManager};
//!
//!#[tokio::main]
//!async fn main() -> std::io::Result<()> {
//! let db_url = "postgres://postgres:123@localhost/test";
//!    // make prepared statements. pass Vec<tokio_postgres::types::Type> if you want typed statement. pass vec![] for no typed statement.
//!    // pass vec![] if you don't need any prepared statements.
//!    let statements = vec![
//!            ("SELECT * FROM topics WHERE id=ANY($1)", vec![tokio_postgres::types::Type::OID_ARRAY]),
//!            ("SELECT * FROM posts WHERE id=ANY($1)", vec![])
//!        ];
//!
//!    // setup manager
//!    let mgr =
//!        PostgresManager::new_from_stringlike(
//!            db_url,
//!            statements,
//!            tokio_postgres::NoTls,
//!        ).unwrap_or_else(|_| panic!("can't make postgres manager"));
//!
//!    // make pool
//!    let pool = Builder::new()
//!        .always_check(false) // if set true every connection will be checked before checkout.
//!        .idle_timeout(None) // set idle_timeout and max_lifetime both to None to ignore idle connection drop.
//!        .max_lifetime(Some(Duration::from_secs(30 * 60)))
//!        .connection_timeout(Duration::from_secs(5)) // set the timeout when connection to database(used when establish new connection and doing always_check).
//!        .wait_timeout(Duration::from_secs(5)) // set the timeout when waiting for a connection.
//!        .min_idle(1)
//!        .max_size(12)
//!        .build(mgr)
//!        .await
//!        .unwrap_or_else(|_| panic!("can't make pool"));
//!    // wait a bit as the pool spawn connections asynchronously
//!    tokio::timer::delay(std::time::Instant::now() + std::time::Duration::from_secs(1)).await;
//!
//!    // get a pool ref
//!    let pool_ref = pool.get().await.expect("can't get pool ref");
//!
//!    // deref or derefmut to get connection.
//!    let (client, statements) = &*pool_ref;
//!
//!    // statement index is the same as the input vector when building the pool.
//!    let statement = statements.get(0).unwrap();
//!    let rows = client.query(statement, &[]).await?;
//!
//!    // drop the pool ref to return connection to pool
//!    drop(pool_ref);
//!
//!    // run the pool and use closure to query the pool.
//!    let _rows = pool
//!        .run(|mut conn| Box::pin(  // pin the async function to make sure the &mut Conn outlives our closure.
//!            async move {
//!                let (client, statements) = &mut conn;
//!                let statement = statements.get(0).unwrap();
//!
//!                // it's possible to overwrite the source statements with new prepared ones.
//!                // but be ware when a new connection spawn the associated statements will be the ones you passed in builder.
//!
//!                let rows = client.query(statement, &[]).await?;
//!
//!                // default error type.
//!                // you can infer your own error type as long as it impl From trait for tang_rs::PostgresPoolError and tang_rs::RedisPoolError
//!                Ok::<_, PostgresPoolError>(rows)
//!             }
//!        ))
//!        .await
//!        .map_err(|e| {
//!            match e {
//!                PostgresPoolError::Inner(e) => println!("{:?}", e),
//!                PostgresPoolError::TimeOut => ()
//!                };
//!            std::io::Error::new(std::io::ErrorKind::Other, "place holder error")
//!        })?;
//!   Ok(())
//!}
//!```

use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

#[cfg(feature = "actix-web")]
use futures::{compat::Future01CompatExt, FutureExt, TryFutureExt};
#[cfg(not(feature = "actix-web"))]
use tokio_executor::Executor;
use tokio_timer::Interval;
#[cfg(not(feature = "actix-web"))]
use tokio_timer::Timeout;
#[cfg(feature = "actix-web")]
use tokio_timer01::Timeout;

pub use builder::Builder;
pub use manager::Manager;
#[cfg(feature = "mongodb")]
pub use mongo_tang::{MongoManager, MongoPoolError};
#[cfg(feature = "tokio-postgres")]
pub use postgres_tang::{PostgresManager, PostgresPoolError};
#[cfg(feature = "redis")]
pub use redis_tang::{RedisManager, RedisPoolError};

use crate::manager::ManagerFuture;
use crate::pool_inner::{PoolLock, State};

mod builder;
mod manager;
#[cfg(feature = "mongodb")]
mod mongo_tang;
mod pool_inner;
#[cfg(feature = "tokio-postgres")]
mod postgres_tang;
#[cfg(feature = "redis")]
mod redis_tang;

pub struct Conn<M: Manager> {
    conn: M::Connection,
    birth: Instant,
}

pub struct IdleConn<M: Manager> {
    conn: Conn<M>,
    idle_start: Instant,
}

impl<M: Manager> IdleConn<M> {
    fn new(conn: M::Connection) -> Self {
        let now = Instant::now();
        IdleConn {
            conn: Conn { conn, birth: now },
            idle_start: now,
        }
    }
}

impl<M: Manager> From<Conn<M>> for IdleConn<M> {
    fn from(conn: Conn<M>) -> IdleConn<M> {
        let now = Instant::now();
        IdleConn {
            conn,
            idle_start: now,
        }
    }
}

impl<M: Manager> From<IdleConn<M>> for Conn<M> {
    fn from(conn: IdleConn<M>) -> Conn<M> {
        Conn {
            conn: conn.conn.conn,
            birth: conn.conn.birth,
        }
    }
}

struct SharedPool<M: Manager> {
    statics: Builder,
    manager: M,
    pool_lock: PoolLock<M>,
}

impl<M: Manager> SharedPool<M> {
    async fn drop_connection(&self, _conn: Conn<M>) -> Result<Result<(), M::Error>, M::Error> {
        //  We might need to spin up more connections to maintain the idle limit, e.g.
        //  if we hit connection lifetime limits
        if self.pool_lock.decr_spawn_count() < self.statics.min_idle {
            #[cfg(not(feature = "actix-web"))]
            {
                self.replenish_idle_connections().await
            }

            #[cfg(feature = "actix-web")]
            {
                Ok(Ok(()))
            }
        } else {
            Ok(Ok(()))
        }
    }

    // use `Builder`'s connection_timeout setting to cancel the `connect` method and return error.
    async fn add_connection(&self) -> Result<Result<(), M::Error>, M::Error> {
        #[cfg(not(feature = "actix-web"))]
        let result = self
            .manager
            .connect()
            .timeout(self.statics.connection_timeout)
            .await
            .map_err(|e| {
                self.pool_lock.pop_pending();
                e
            })?;

        #[cfg(not(feature = "actix-web"))]
        let conn = match result {
            Ok(conn) => conn,
            Err(e) => {
                self.pool_lock.pop_pending();
                return Ok(Err(e));
            }
        };

        #[cfg(feature = "actix-web")]
        let conn = Timeout::new(
            self.manager.connect().boxed().compat(),
            self.statics.connection_timeout,
        )
        .compat()
        .await
        .map_err(|e| {
            self.pool_lock.pop_pending();
            e
        })?;

        let conn = IdleConn::new(conn);

        self.pool_lock.put_back_incr_spawn_count(conn);

        Ok(Ok(()))
    }

    // we map the check result's error to drop connection then return the result.
    #[cfg(not(feature = "actix-web"))]
    async fn check_connection(
        &self,
        mut conn: Conn<M>,
    ) -> Result<Result<Conn<M>, M::Error>, M::Error> {
        match self
            .manager
            .is_valid(&mut conn.conn)
            .timeout(self.statics.connection_timeout)
            .await
        {
            Ok(result) => match result {
                Err(e) => {
                    let _ = self.drop_connection(conn).await?;
                    Ok(Err(e))
                }
                Ok(()) => Ok(Ok(conn)),
            },

            Err(e) => {
                // we did't get any response. drop the connection and return a timeout error.
                // ToDo: we should spawn this in executor to ignore the timeout error from drop_connection.
                let _ = self.drop_connection(conn).await?;
                Err(e.into())
            }
        }
    }

    #[cfg(not(feature = "actix-web"))]
    async fn replenish_idle_connections(&self) -> Result<Result<(), M::Error>, M::Error> {
        // incr pending count will return the sum of spawned and pending count.
        while self.pool_lock.incr_pending_count() < self.statics.min_idle {
            if let Err(e) = self.add_connection().await? {
                return Ok(Err(e));
            }
        }
        if let Err(e) = self.add_connection().await? {
            return Ok(Err(e));
        };

        Ok(Ok(()))
    }

    // only used to spawn connections when starting the pool
    #[cfg(feature = "actix-web")]
    async fn replenish_idle_connections_temp(&self) -> Result<(), M::Error> {
        while self.pool_lock.incr_pending_count() <= self.statics.min_idle {
            let conn = self
                .manager
                .connect()
                .await
                .map_err(|e| {
                    self.pool_lock.pop_pending();
                    e
                })
                .unwrap();

            let conn = IdleConn::new(conn);

            self.pool_lock.put_back_incr_spawn_count(conn.into());
        }
        Ok(())
    }

    async fn reap_connections(&self) -> Result<(), M::Error> {
        let now = Instant::now();

        if let Some(conn) = self.pool_lock.try_pop_conn() {
            let mut should_drop = false;
            if let Some(timeout) = self.statics.idle_timeout {
                should_drop |= now - conn.idle_start >= timeout;
            }
            if let Some(lifetime) = self.statics.max_lifetime {
                should_drop |= now - conn.conn.birth >= lifetime;
            }

            if should_drop {
                self.drop_connection(conn.into()).await??;
            } else {
                self.pool_lock.put_back(conn);
            }
        }

        Ok(())
    }

    fn garbage_collect(&self) {
        self.pool_lock.try_peek_pending(|pending| {
            let pending = match pending {
                Some(pending) => pending,
                None => return false,
            };
            pending.should_remove(self.statics.connection_timeout)
        });
    }
}

pub struct Pool<M: Manager>(Arc<SharedPool<M>>);

impl<M: Manager> Clone for Pool<M> {
    fn clone(&self) -> Self {
        Pool(self.0.clone())
    }
}

impl<M: Manager> fmt::Debug for Pool<M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("Pool({:p})", self.0))
    }
}

impl<M: Manager> Pool<M> {
    fn new(builder: Builder, manager: M) -> Self {
        let size = builder.max_size as usize;

        let shared_pool = Arc::new(SharedPool {
            statics: builder,
            manager,
            pool_lock: PoolLock::new(size),
        });

        // spawn a loop interval future to handle the lifetime and time out of connections.
        schedule_one_reaping(&shared_pool);

        garbage_collect(&shared_pool);

        Pool(shared_pool)
    }

    /// Return a reference of `Arc<SharedPool<Manager>>` and a `Option<Manager::Connection>`.
    /// The `PoolRef` should be drop asap when you finish the use of it.
    pub async fn get(&self) -> Result<PoolRef<'_, M>, M::Error> {
        let conn = self.get_idle_connection(0).await?;

        Ok(PoolRef {
            conn: Some(conn),
            pool: &self.0,
        })
    }

    /// Run the pool with a closure.
    /// Usually slightly faster than `Pool.get()` as we only do conditional broken check according to the closure result.
    pub async fn run<T, E, EE, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut M::Connection) -> Pin<Box<dyn Future<Output = Result<T, EE>> + Send + '_>>,
        EE: From<M::Error>,
        E: From<EE> + From<M::Error>,
        T: Send + 'static,
    {
        let mut conn = self.get_idle_connection(0).await?;

        let result = f(&mut conn.conn).await;

        // only check if the connection is broken when we get an error from result
        let broken = if result.is_err() {
            self.0.manager.is_closed(&mut conn.conn)
        } else {
            false
        };

        if broken {
            let _r = self.0.drop_connection(conn).await;
        } else {
            self.0.pool_lock.put_back(conn.into())
        }
        Ok(result?)
    }

    // recursive when the connection is broken. we exit with at most 3 retries.
    fn get_idle_connection(&self, mut retry: u8) -> ManagerFuture<Result<Conn<M>, M::Error>> {
        Box::pin(async move {
            let pool = &self.0;

            #[cfg(not(feature = "actix-web"))]
            let conn = pool
                .pool_lock
                .lock(pool)
                .timeout(pool.statics.wait_timeout)
                .await?
                .into();

            #[cfg(feature = "actix-web")]
            let conn = match Timeout::new(
                pool.pool_lock.lock(pool).boxed().unit_error().compat(),
                pool.statics.wait_timeout,
            )
            .compat()
            .await
            {
                Ok(conn) => conn.into(),
                Err(_e) => {
                    retry += 1;
                    return self.get_idle_connection(retry).await;
                }
            };

            if pool.statics.always_check {
                #[cfg(not(feature = "actix-web"))]
                match self.0.check_connection(conn).await? {
                    Ok(conn) => Ok(conn),
                    Err(e) => {
                        if retry > 3 {
                            Err(e)
                        } else {
                            retry += 1;
                            self.get_idle_connection(retry).await
                        }
                    }
                }

                #[cfg(feature = "actix-web")]
                unreachable!("we should not get here with actix-web features")
            } else {
                Ok(conn)
            }
        })
    }

    pub fn state(&self) -> State {
        self.0.pool_lock.state()
    }
}

pub struct PoolRef<'a, M: Manager + Send> {
    conn: Option<Conn<M>>,
    pool: &'a Arc<SharedPool<M>>,
}

impl<M: Manager + Send> Deref for PoolRef<'_, M> {
    type Target = M::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn.as_ref().unwrap().conn
    }
}

impl<M: Manager + Send> DerefMut for PoolRef<'_, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn.as_mut().unwrap().conn
    }
}

impl<M: Manager + Send> PoolRef<'_, M> {
    /// get a mut reference of connection.
    pub fn get_conn(&mut self) -> &mut M::Connection {
        &mut *self
    }

    /// take the the ownership of connection from pool and it won't be pushed back to pool anymore.
    pub fn take_conn(&mut self) -> Option<M::Connection> {
        self.conn.take().map(|c| c.conn)
    }

    /// manually push a connection to pool. We treat this connection as a new born one.
    /// operation will fail if the pool is already in full capacity(no error will return)
    pub fn push_conn(&mut self, conn: M::Connection) {
        self.conn = Some(Conn {
            conn,
            birth: Instant::now(),
        });
    }
}

impl<M: Manager + Send> Drop for PoolRef<'_, M> {
    #[inline]
    fn drop(&mut self) {
        let mut conn = match self.conn.take() {
            Some(conn) => conn,
            None => {
                self.pool.pool_lock.decr_spawn_count();
                return;
            }
        };

        let broken = self.pool.manager.is_closed(&mut conn.conn);
        if broken {
            #[cfg(not(feature = "actix-web"))]
            {
                let pool = self.pool.clone();

                let _ = tokio_executor::DefaultExecutor::current()
                    .spawn(Box::pin(async move {
                        let _ = pool.drop_connection(conn).await;
                    }))
                    .map_err(|_| {
                        self.pool.pool_lock.decr_spawn_count();
                    });
            }

        /*  we don't drop connection when running in actix runtime  */
        } else {
            self.pool.pool_lock.put_back(conn.into());
        };
    }
}

// schedule reaping runs in a spawned future.
fn schedule_one_reaping<M: Manager + Send>(shared: &Arc<SharedPool<M>>) {
    if shared.statics.max_lifetime.is_some() || shared.statics.idle_timeout.is_some() {
        let mut interval = Interval::new_interval(shared.statics.reaper_rate);
        let shared = shared.clone();

        tokio_executor::spawn(async move {
            loop {
                let _i = interval.next().await;
                let _ = shared.reap_connections().await;
            }
        });
    }
}

// schedule garbage collection runs in a spawned future.
fn garbage_collect<M: Manager + Send>(shared: &Arc<SharedPool<M>>) {
    if shared.statics.use_gc {
        let mut interval = Interval::new_interval(shared.statics.reaper_rate * 6);
        let shared = shared.clone();

        tokio_executor::spawn(async move {
            loop {
                let _i = interval.next().await;
                shared.garbage_collect();
            }
        });
    }
}

// a shortcut for tokio timeout
trait CrateTimeOut: Sized {
    fn timeout(self, dur: std::time::Duration) -> Timeout<Self> {
        Timeout::new(self, dur)
    }
}

impl<F: Future + Send> CrateTimeOut for F {}
