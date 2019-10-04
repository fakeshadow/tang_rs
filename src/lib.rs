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
//!    // only support NoTls for now
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
//!        .min_idle(1)
//!        .max_size(12)
//!        .build(mgr)
//!        .await
//!        .unwrap_or_else(|_| panic!("can't make pool"));
//!    // wait a bit as the pool spawn connections asynchronously
//!    tokio::timer::delay(std::time::Instant::now() + std::time::Duration::from_secs(1)).await;
//!
//!    // run the pool and use closure to query the pool.
//!    let _row = pool
//!        .run(|mut conn| Box::pin(  // pin the async function to make sure the &mut Conn outlives our closure.
//!            async move {
//!                let (client, statements) = &mut conn;
//!
//!                // statement index is the same as the input vector when building the pool.
//!                let statement = statements.get(0).unwrap();
//!
//!                // it's possible to overwrite the source statements with new prepared ones.
//!                // but be ware when a new connection spawn the associated statements will be the ones you passed in builder.
//!
//!                let ids = vec![1u32, 2, 3, 4, 5];
//!                let row = client.query(statement, &[&ids]).try_collect::<Vec<tokio_postgres::Row>>().await?;
//!
//!                // default error type.
//!                // you can infer your own error type as long as it impl From trait for tang_rs::PostgresPoolError and tang_rs::RedisPoolError
//!                Ok::<_, PostgresPoolError>(row)
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

use std::cmp::min;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_queue::{ArrayQueue, SegQueue};
use futures::channel::oneshot::{channel, Sender};
#[cfg(feature = "actix-web")]
use futures::{compat::Future01CompatExt, FutureExt, TryFutureExt};
#[cfg(feature = "default")]
use tokio_executor::Executor;
use tokio_timer::Interval;
#[cfg(feature = "default")]
use tokio_timer::Timeout;
#[cfg(feature = "actix-web")]
use tokio_timer01::Timeout;

pub use builder::Builder;
pub use manager::Manager;
#[cfg(feature = "tokio-postgres")]
pub use postgres_tang::{PostgresManager, PostgresPoolError};
#[cfg(feature = "redis")]
pub use redis_tang::{RedisManager, RedisPoolError};

mod builder;
mod manager;
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

/// `crossbeam_queue` is used to manage the pool and waiting queue.
/// `AtomicUsize` is used to track the total amount of `Conn + IdleConn`
struct SharedPool<M: Manager> {
    statics: Builder,
    manager: M,
    pool: ArrayQueue<IdleConn<M>>,
    queue: SegQueue<Sender<Conn<M>>>,
    spawned: AtomicUsize,
}

impl<M: Manager> SharedPool<M> {
    fn decr_spawn(&self, count: usize) -> usize {
        self.spawned.fetch_sub(count, Ordering::Relaxed)
    }

    fn incr_spawn(&self, count: usize) -> usize {
        self.spawned.fetch_add(count, Ordering::Relaxed)
    }

    fn load_spawn(&self) -> usize {
        self.spawned.load(Ordering::Relaxed)
    }

    fn put_back_conn(&self, conn: Conn<M>) {
        let mut conn = conn;
        while let Ok(tx) = self.queue.pop() {
            match tx.send(conn) {
                Ok(()) => return,
                Err(c) => conn = c,
            }
        }

        if self.pool.push(conn.into()).is_err() {
            self.decr_spawn(1);
        }
    }

    async fn drop_connection(&self, _conn: Conn<M>) -> Result<(), M::Error> {
        //  We might need to spin up more connections to maintain the idle limit, e.g.
        //  if we hit connection lifetime limits
        if self.decr_spawn(1) <= self.statics.min_idle {
            self.add_connection().await
        } else {
            Ok(())
        }
    }

    // use `Builder`'s connection_timeout setting to cancel the `connect` method and return error.
    async fn add_connection(&self) -> Result<(), M::Error> {
        self.incr_spawn(1);

        #[cfg(feature = "default")]
        let conn = self
            .manager
            .connect()
            .timeout(self.statics.connection_timeout)
            .await
            .map(|r| {
                r.map_err(|e| {
                    self.decr_spawn(1);
                    e
                })
            })
            .map_err(|e| {
                self.decr_spawn(1);
                e
            })??;

        #[cfg(feature = "actix-web")]
        let conn = Timeout::new(
            self.manager.connect().boxed().compat(),
            self.statics.connection_timeout,
        )
        .compat()
        .await
        .map_err(|e| {
            self.decr_spawn(1);
            e
        })?;

        let conn = IdleConn::new(conn);

        self.put_back_conn(conn.into());

        Ok(())
    }

    async fn replenish_idle_connections(&self) -> Result<(), M::Error> {
        while self.load_spawn() < min(self.statics.min_idle, self.statics.max_size) {
            self.add_connection().await?;
        }
        Ok(())
    }

    // only used to spawn connections when starting the pool
    #[cfg(feature = "actix-web")]
    async fn replenish_idle_connections_temp(&self) -> Result<(), M::Error> {
        while self.load_spawn() < min(self.statics.min_idle, self.statics.max_size) {
            self.incr_spawn(1);

            let conn = self
                .manager
                .connect()
                .await
                .map_err(|e| {
                    self.decr_spawn(1);
                    e
                })
                .unwrap();

            let conn = IdleConn::new(conn);

            self.put_back_conn(conn.into());
        }
        Ok(())
    }

    async fn reap_connections(&self) -> Result<(), M::Error> {
        let now = Instant::now();

        if let Ok(conn) = self.pool.pop() {
            let mut should_drop = false;
            if let Some(timeout) = self.statics.idle_timeout {
                should_drop |= now - conn.idle_start >= timeout;
            }
            if let Some(lifetime) = self.statics.max_lifetime {
                should_drop |= now - conn.conn.birth >= lifetime;
            }

            if should_drop {
                let _ = self.drop_connection(conn.into()).await;
            } else {
                self.put_back_conn(conn.into());
            }
        }

        Ok(())
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
            pool: ArrayQueue::new(size),
            queue: SegQueue::new(),
            spawned: AtomicUsize::new(0),
        });

        // spawn a loop interval future to handle the lifetime and time out of connections.
        schedule_one_reaping(&shared_pool);

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
        let mut broken = false;
        if result.is_err() {
            broken = self.0.manager.is_closed(&mut conn.conn);
        }

        if broken {
            let _r = self.0.drop_connection(conn).await;
        } else {
            self.0.put_back_conn(conn);
        }
        Ok(result?)
    }

    /// recursive when the connection is broken or we get an error from oneshot channel.
    /// we exit with at most 3 retries.
    fn get_idle_connection<'a>(
        &'a self,
        mut retry: u8,
    ) -> Pin<Box<dyn Future<Output = Result<Conn<M>, M::Error>> + Send + 'a>> {
        Box::pin(async move {
            let pool = &self.0;
            let mut conn = match pool.pool.pop() {
                Ok(conn) => conn.into(),
                Err(_) => {
                    if pool.load_spawn() < pool.statics.max_size {
                        // we return with an error if for any reason we get an error when spawning new connection
                        pool.add_connection().await?;
                    }

                    let (tx, rx) = channel();
                    pool.queue.push(tx);

                    // we return error if the timeout reached when waiting for the queue.
                    #[cfg(feature = "default")]
                    match rx.timeout(pool.statics.queue_timeout).await? {
                        Ok(conn) => conn,
                        Err(e) => {
                            if retry > 3 {
                                return Err(e.into());
                            } else {
                                retry += 1;
                                return self.get_idle_connection(retry).await;
                            }
                        }
                    }
                    #[cfg(feature = "actix-web")]
                    match Timeout::new(rx.boxed().compat(), pool.statics.queue_timeout)
                        .compat()
                        .await
                    {
                        Ok(conn) => conn,
                        Err(_) => {
                            retry += 1;
                            return self.get_idle_connection(retry).await;
                        }
                    }
                }
            };

            // Spin up a new connection if necessary to retain our minimum idle count
            if pool.load_spawn() < pool.statics.min_idle {
                // We put back conn and return error if we failed to spawn new connections.
                if let Err(e) = self.0.replenish_idle_connections().await {
                    self.0.put_back_conn(conn);
                    return Err(e);
                }
            }

            if pool.statics.always_check {
                // drop the connection if the connection is not valid anymore
                if let Err(e) = pool.manager.is_valid(&mut conn.conn).await {
                    let _ = pool.drop_connection(conn).await;

                    if retry > 3 {
                        return Err(e);
                    } else {
                        retry += 1;
                        return self.get_idle_connection(retry).await;
                    }
                }
            }
            Ok(conn)
        })
    }

    pub fn state(&self) -> State {
        State {
            connections: self.0.load_spawn() as u8,
            idle_connections: self.0.pool.len() as u8,
        }
    }
}

pub struct State {
    pub connections: u8,
    pub idle_connections: u8,
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("State")
            .field("connections", &self.connections)
            .field("idle_connections", &self.idle_connections)
            .finish()
    }
}

pub struct PoolRef<'a, M: Manager> {
    conn: Option<Conn<M>>,
    pool: &'a Arc<SharedPool<M>>,
}

impl<'a, M: Manager> PoolRef<'a, M> {
    /// get a mut reference of connection.
    pub fn get_conn(&mut self) -> &mut M::Connection {
        &mut self.conn.as_mut().unwrap().conn
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

impl<'a, M: Manager> Drop for PoolRef<'a, M> {
    fn drop(&mut self) {
        let mut conn = match self.conn.take() {
            Some(conn) => conn,
            None => {
                self.pool.decr_spawn(1);
                return;
            }
        };

        let shared_pool = self.pool.clone();

        // ToDo: actix_rt doesn't have spawn error so we can't adjust spawned count accordingly.
        #[cfg(feature = "actix-web")]
        actix_rt::spawn(
            async move {
                let broken = shared_pool.manager.is_closed(&mut conn.conn);
                if broken {
                    let _r = shared_pool.drop_connection(conn).await;
                } else {
                    shared_pool.put_back_conn(conn);
                };
                Ok(())
            }
                .boxed_local()
                .compat(),
        );

        #[cfg(feature = "default")]
        let _ = tokio_executor::DefaultExecutor::current()
            .spawn(Box::pin(async move {
                let broken = shared_pool.manager.is_closed(&mut conn.conn);
                if broken {
                    let _r = shared_pool.drop_connection(conn).await;
                } else {
                    shared_pool.put_back_conn(conn);
                }
            }))
            .map_err(|_| {
                self.pool.decr_spawn(1);
            });
    }
}

// schedule reaping runs in a spawned future.
fn schedule_one_reaping<M: Manager>(shared: &Arc<SharedPool<M>>) {
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

// a shortcut for tokio timeout
trait CrateTimeOut: Sized {
    fn timeout(self, dur: std::time::Duration) -> Timeout<Self> {
        Timeout::new(self, dur)
    }
}

impl<F: Future> CrateTimeOut for F {}
