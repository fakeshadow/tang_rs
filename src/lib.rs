//! # A connection pool running one tokio runtime.
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
//!```ignore
//!use std::time::Duration;
//!
//!use futures_util::TryStreamExt;
//!use tang_rs::{Builder, PostgresPoolError, PostgresManager};
//!
//!#[tokio::main]
//!async fn main() -> std::io::Result<()> {
//!    let db_url = "postgres://postgres:123@localhost/test";
//!
//!    // setup manager
//!    let mgr =
//!        PostgresManager::new_from_stringlike(
//!            db_url,
//!            tokio_postgres::NoTls,
//!        ).unwrap_or_else(|_| panic!("can't make postgres manager"));
//!
//!    //make prepared statements to speed up frequent used queries. It just stores your statement info in a hash map and
//!    //you can skip this step if you don't need any prepared statement.
//!    let mgr = mgr
//!        // alias is used to call according statement later.
//!        // pass &[tokio_postgres::types::Type] if you want typed statement. pass &[] for no typed statement.
//!        .prepare_statement("get_topics", "SELECT * FROM topics WHERE id=ANY($1)", &[tokio_postgres::types::Type::OID_ARRAY])
//!        .prepare_statement("get_users", "SELECT * FROM posts WHERE id=ANY($1)", &[]);
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
//!
//!    // wait a bit as the pool spawn connections asynchronously
//!    tokio::timer::delay(std::time::Instant::now() + std::time::Duration::from_secs(1)).await;
//!
//!    // get a pool ref
//!    let pool_ref = pool.get().await.expect("can't get pool ref");
//!
//!    // deref or derefmut to get connection.
//!    let (client, statements) = &*pool_ref;
//!
//!    /*
//!        It's possible to insert new statement into statements from pool_ref.
//!        But be ware the statement will only work on this specific connection and not other connections in the pool.
//!        The additional statement will be dropped when the connection is dropped from pool.
//!        A newly spawned connection will not include this additional statement.
//!
//!        * This newly inserted statement most likely can't take advantage of the pipeline query features
//!        as we didn't join futures when prepare this statement.
//!
//!        * It's suggested that if you want pipelined statements you should join the futures of prepare before calling await on them.
//!        There is tang_rs::CacheStatement trait for PoolRef<PostgresManager<T>> to help you streamline this operation.
//!    */
//!
//!    // use the alias input when building manager to get specific statement.
//!    let statement = statements.get("get_topics").unwrap();
//!    let rows = client.query(statement, &[]).await.expect("Query failed");
//!
//!    // drop the pool ref to return connection to pool
//!    drop(pool_ref);
//!
//!    // run the pool and use closure to query the pool.
//!    let _rows = pool
//!        .run(|mut conn| Box::pin(  // pin the async function to make sure the &mut Conn outlives our closure.
//!            async move {
//!                let (client, statements) = &conn;
//!                let statement = statements.get("get_topics").unwrap();
//!                let rows = client.query(statement, &[]).await?;
//!
//!                // default error type.
//!                // you can infer your own error type as long as it impl From trait for tang_rs::PostgresPoolError
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
//!
//!   Ok(())
//!}
//!```

use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures_util::FutureExt;
use tokio_executor::{DefaultExecutor, Executor};
use tokio_timer::{Interval, Timeout};

pub use builder::Builder;
pub use manager::Manager;
#[cfg(feature = "mongodb")]
pub use mongo_tang::{MongoManager, MongoPoolError};
#[cfg(feature = "tokio-postgres")]
pub use postgres_tang::{CacheStatement, PostgresManager, PostgresPoolError, PrepareStatement};
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
mod util;

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

struct SharedPool<M: Manager + Send> {
    statics: Builder,
    manager: M,
    pool_lock: PoolLock<M>,
}

impl<M: Manager + Send> SharedPool<M> {
    async fn drop_conn(&self) -> Result<(), M::Error> {
        //  We might need to spin up more connections to maintain the idle limit, e.g.
        //  if we hit connection lifetime limits
        let pending_count = self.pool_lock.decr_spawned(|total_now| {
            if total_now < self.statics.min_idle {
                Some(self.statics.min_idle - total_now)
            } else {
                None
            }
        });

        match pending_count {
            Some(count) => self.replenish_idle_conn(count).await,
            None => Ok(()),
        }
    }

    // use `Builder`'s connection_timeout setting to cancel the `connect` method and return error.
    async fn add_idle_conn(&self) -> Result<(), M::Error> {
        let conn = self
            .manager
            .connect()
            .timeout(self.statics.connection_timeout)
            .await
            .map_err(|e| {
                self.pool_lock.decr_pending(1);
                e
            })?
            .map_err(|e| {
                self.pool_lock.decr_pending(1);
                e
            })?;

        self.pool_lock.put_back_incr_spawned(IdleConn::new(conn));

        Ok(())
    }

    async fn check_conn(&self, conn: &mut Conn<M>) -> Result<Result<(), M::Error>, M::Error> {
        self.manager
            .is_valid(&mut conn.conn)
            .timeout(self.statics.connection_timeout)
            .await?
            .map(Ok)
    }

    async fn replenish_idle_conn(&self, pending_count: u8) -> Result<(), M::Error> {
        for i in 0..pending_count {
            self.add_idle_conn().await.map_err(|e| {
                // we return when an error occur so we should drop all the pending after the i.
                // (the pending of i is already dropped in add_connection method)
                self.pool_lock.decr_pending(pending_count - i - 1);
                e
            })?;
        }
        Ok(())
    }

    async fn reap_idle_conn(&self) -> Result<(), M::Error> {
        let now = Instant::now();

        let pending_new = self
            .pool_lock
            .try_drop_conns(self.statics.min_idle, |conn| {
                let mut should_drop = false;
                if let Some(timeout) = self.statics.idle_timeout {
                    should_drop |= now >= conn.idle_start + timeout;
                }
                if let Some(lifetime) = self.statics.max_lifetime {
                    should_drop |= now >= conn.idle_start + lifetime;
                }
                should_drop
            });

        match pending_new {
            Some(pending_new) => self.replenish_idle_conn(pending_new).await,
            None => Ok(()),
        }
    }

    fn garbage_collect(&self) {
        self.pool_lock
            .drop_pendings(|pending| pending.should_remove(self.statics.connection_timeout));
    }

    // ToDo: handle errors here.
    fn spawn<F>(&self, f: F) -> Result<(), tokio_executor::SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        DefaultExecutor::current().spawn(Box::pin(f.map(|_| ())))
    }
}

pub struct Pool<M: Manager + Send>(Arc<SharedPool<M>>);

impl<M: Manager + Send> Clone for Pool<M> {
    fn clone(&self) -> Self {
        Pool(self.0.clone())
    }
}

impl<M: Manager + Send> fmt::Debug for Pool<M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("Pool({:p})", self.0))
    }
}

impl<M: Manager + Send> Pool<M> {
    fn new(builder: Builder, manager: M) -> Self {
        let size = builder.max_size as usize;

        Pool(Arc::new(SharedPool {
            statics: builder,
            manager,
            pool_lock: PoolLock::new(size),
        }))
    }

    /// manually initialize pool. this is usually called when the `Pool` is built with `build_uninitialized`
    /// This is useful when you want to make a empty `Pool` and init it later.
    /// # example:
    /// ```ignore
    /// #[macro_use]
    /// extern crate lazy_static;
    ///
    /// use tang_rs::{Pool, PostgresManager, Builder};
    /// use tokio_postgres::NoTls;
    ///
    /// lazy_static! {
    ///    static ref POOL: Pool<PostgresManager<NoTls>> = Builder::new()
    ///         .always_check(false)
    ///         .idle_timeout(None)
    ///         .max_lifetime(None)
    ///         .min_idle(24)
    ///         .max_size(24)
    ///         .build_uninitialized(
    ///             PostgresManager::new_from_stringlike("postgres://postgres:123@localhost/test", NoTls)
    ///                 .expect("can't make postgres manager")
    ///         )
    ///         .unwrap_or_else(|e| panic!("{:?}", e));
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> std::io::Result<()> {
    ///     POOL.init().await.expect("Failed to initialize postgres pool");
    ///     Ok(())
    /// }
    /// ```
    pub async fn init(&self) -> Result<(), M::Error> {
        let shared_pool = &self.0;

        schedule_reaping(shared_pool);
        garbage_collect(shared_pool);

        shared_pool
            .replenish_idle_conn(shared_pool.statics.min_idle)
            .await?;

        Ok(())
    }

    /// Return a reference of `Arc<SharedPool<Manager>>` and a `Option<Manager::Connection>`.
    /// The `PoolRef` should be drop asap when you finish the use of it.
    pub async fn get(&self) -> Result<PoolRef<'_, M>, M::Error> {
        let conn = self.get_conn(0).await?;

        Ok(PoolRef {
            conn: Some(conn),
            pool: &self.0,
        })
    }

    /// Run the pool with a closure.
    /// Usually slightly faster than `Pool.get()` as we only do conditional broken check according to the closure result.
    pub async fn run<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut M::Connection) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + '_>>,
        E: From<M::Error>,
        T: Send + 'static,
    {
        let mut conn = self.get_conn(0).await?;

        let result = f(&mut conn.conn).await;

        // only check if the connection is broken when we get an error from result
        let broken = if result.is_err() {
            self.0.manager.is_closed(&mut conn.conn)
        } else {
            false
        };

        if broken {
            spawn_drop(&self.0);
        } else {
            self.0.pool_lock.put_back(conn.into())
        }
        result
    }

    // Recursive when the connection is broken(When enabling the always_check). We exit with at most 3 retries and return an error.
    fn get_conn(&self, mut retry: u8) -> ManagerFuture<Result<Conn<M>, M::Error>> {
        Box::pin(async move {
            let shared_pool = &self.0;

            let mut conn = shared_pool
                .pool_lock
                .lock(shared_pool)
                .timeout(shared_pool.statics.wait_timeout)
                .await?
                .into();

            if shared_pool.statics.always_check {
                let result = shared_pool.check_conn(&mut conn).await.map_err(|e| {
                    spawn_drop(shared_pool);
                    e
                })?;

                if let Err(e) = result {
                    spawn_drop(shared_pool);
                    if retry == 3 {
                        return Err(e);
                    } else {
                        retry += 1;
                        return self.get_conn(retry).await;
                    }
                }
            };

            Ok(conn)
        })
    }

    /// Return a state of the pool inner. This call will block the thread and wait for lock.
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
                self.pool.pool_lock.decr_spawned(|_| None);
                return;
            }
        };

        let broken = self.pool.manager.is_closed(&mut conn.conn);
        if broken {
            spawn_drop(self.pool);
        } else {
            self.pool.pool_lock.put_back(conn.into());
        };
    }
}

// helper function to spawn drop connection and spawn new ones if needed.
// Conn<M> should be dropped in place where spawn_drop() is used.
fn spawn_drop<M: Manager + Send>(shared: &Arc<SharedPool<M>>) {
    let shared_clone = shared.clone();
    shared
        .spawn(async move { shared_clone.drop_conn().await })
        .unwrap_or_else(|_| {
            shared.pool_lock.decr_spawned(|_| None);
        });
}

// schedule reaping runs in a spawned future.
fn schedule_reaping<M: Manager + Send>(shared_pool: &Arc<SharedPool<M>>) {
    let statics = &shared_pool.statics;
    if statics.max_lifetime.is_some() || statics.idle_timeout.is_some() {
        let shared_clone = shared_pool.clone();
        let mut interval = Interval::new_interval(statics.reaper_rate);
        let fut = async move {
            loop {
                let _i = interval.next().await;
                let _ = shared_clone.reap_idle_conn().await;
            }
        };
        shared_pool.spawn(fut).unwrap_or_else(|e| panic!("{}", e));
    }
}

// schedule garbage collection runs in a spawned future.
fn garbage_collect<M: Manager + Send>(shared_pool: &Arc<SharedPool<M>>) {
    let statics = &shared_pool.statics;
    if statics.use_gc {
        let shared_clone = shared_pool.clone();
        let mut interval = Interval::new_interval(statics.reaper_rate * 6);
        let fut = async move {
            loop {
                let _i = interval.next().await;
                let _ = shared_clone.garbage_collect();
            }
        };
        shared_pool.spawn(fut).unwrap_or_else(|e| panic!("{}", e));
    }
}

// a shortcut for tokio timeout
trait CrateTimeOut: Sized {
    fn timeout(self, dur: std::time::Duration) -> Timeout<Self> {
        Timeout::new(self, dur)
    }
}

impl<F: Future + Send> CrateTimeOut for F {}
