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
//!use futures::TryStreamExt;
//!use tang_rs::{Builder, PoolError, PostgresManager};
//!
//!#[tokio::main]
//!async fn main() -> std::io::Result<()> {
//!    let db_url = "postgres://postgres:123@localhost/test";
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
//!    // make pool
//!    let pool = Builder::new()
//!        .always_check(false) // if set true every connection will be checked before checkout.
//!        .idle_timeout(None) // set idle_timeout and max_lifetime both to None to ignore idle connection drop.
//!        .max_lifetime(None)
//!        .min_idle(1)
//!        .max_size(12)
//!        .build(mgr)
//!        .await
//!        .unwrap_or_else(|_| panic!("can't make pool"));
//!    // wait a bit as the pool spawn connections asynchronously
//!    tokio::timer::delay(std::time::Instant::now() + std::time::Duration::from_secs(1)).await;
//!    // run the pool and use closure to query the database.
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
//!
//!                let row = client.query(statement, &[&ids]).try_collect::<Vec<tokio_postgres::Row>>().await?;
//!
//!                // default error type.
//!                // you can return your own error type as long as it impl From trait for tokio_postgres::Error and tokio_timer::timeout::Elapsed
//!                Ok::<_, PoolError>(row)
//!             }
//!        ))
//!        .await
//!        .map_err(|e| {
//!                // return error will be wrapped in Inner.
//!            match e {
//!                PoolError::Inner(e) => println!("{:?}", e),
//!                PoolError::TimeOut => ()
//!                };
//!            std::io::Error::new(std::io::ErrorKind::Other, "place holder error")
//!        })?;
//!   Ok(())
//!}
//!```

use std::cmp::{max, min};
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
    pub conn: M::Connection,
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

/// use future aware mutex for inner Pool lock.
/// So the waiter queue is handled by the mutex lock and not the pool.
struct SharedPool<M: Manager> {
    statics: Builder,
    manager: M,
    pool: ArrayQueue<IdleConn<M>>,
    queue: SegQueue<Sender<Conn<M>>>,
    spawned: AtomicUsize,
    pending: AtomicUsize,
}

impl<M: Manager> SharedPool<M> {
    fn decr_spawn(&self, count: usize) {
        self.spawned.fetch_sub(count, Ordering::SeqCst);
    }

    fn incr_spawn(&self, count: usize) {
        self.spawned.fetch_add(count, Ordering::SeqCst);
    }

    fn load_spawn(&self) -> usize {
        self.spawned.load(Ordering::SeqCst)
    }

    fn decr_pending(&self, count: usize) {
        self.pending.fetch_sub(count, Ordering::SeqCst);
    }

    fn incr_pending(&self, count: usize) {
        self.pending.fetch_add(count, Ordering::SeqCst);
    }

    fn load_pending(&self) -> usize {
        self.pending.load(Ordering::SeqCst)
    }

    fn put_back_conn(&self, conn: Conn<M>) {
        let mut conn = conn;
        while let Ok(tx) = self.queue.pop() {
            match tx.send(conn) {
                Ok(()) => {
                    return;
                }
                Err(c) => conn = c,
            }
        }

        if self.pool.push(conn.into()).is_err() {
            self.decr_spawn(1);
        }
    }

    // ToDo: spawn this method in thread pool
    async fn drop_connections(&self, to_drop: Vec<Conn<M>>) -> Result<(), M::Error> {
        self.decr_spawn(to_drop.len());
        // We might need to spin up more connections to maintain the idle limit, e.g.
        // if we hit connection lifetime limits
        let min = self.statics.min_idle as usize;
        if self.load_spawn() < min {
            self.replenish_idle_connections().await
        } else {
            Ok(())
        }
    }

    // ToDo: spawn this method in thread pool
    async fn add_connection(&self) -> Result<(), M::Error> {
        self.incr_pending(1);
        let conn = self.manager.connect().await.map_err(|e| {
            self.decr_pending(1);
            e
        })?;

        self.decr_pending(1);
        self.incr_spawn(1);

        let conn = IdleConn::new(conn);

        self.put_back_conn(conn.into());

        Ok(())
    }

    async fn replenish_idle_connections(&self) -> Result<(), M::Error> {
        let pending = self.load_pending() as u8;
        let spawned = self.load_spawn() as u8;

        let slots_available = self.statics.max_size - spawned - pending;

        let idle = self.pool.len() as u8;
        let desired = self.statics.min_idle;

        for _i in idle..max(idle + 1, min(desired, slots_available)) {
            let _ = self.add_connection().await;
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
                let _ = self.drop_connections(vec![conn.into()]).await;
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
    fn new_inner(builder: Builder, manager: M) -> Self {
        let size = builder.max_size as usize;

        let shared_pool = Arc::new(SharedPool {
            statics: builder,
            manager,
            pool: ArrayQueue::new(size),
            queue: SegQueue::new(),
            spawned: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
        });

        // spawn a loop interval future to handle the lifetime and time out of connections.
        schedule_one_reaping(&shared_pool);

        Pool(shared_pool)
    }

    /// `pool.get()` return a reference of pool and a Conn. So that you can use the Conn outside a closure.
    /// The limitation is we can't figure out if your get a error when use the Conn and make conditional broken check before push the Conn back to pool
    #[cfg(feature = "default")]
    pub async fn get<'a, E>(&'a self) -> Result<PoolRef<'a, M>, E>
    where
        E: From<M::Error> + From<tokio_timer::timeout::Elapsed>,
    {
        let conn = Timeout::new(
            self.get_idle_connection(),
            self.0.statics.connection_timeout,
        )
        .await??;

        Ok(PoolRef {
            conn: Some(conn),
            pool: &self.0,
        })
    }

    #[cfg(feature = "actix-web")]
    pub async fn get<'a, E>(&'a self) -> Result<PoolRef<'a, M>, E>
    where
        E: From<M::Error> + From<tokio_timer01::timeout::Error<M::Error>>,
    {
        let conn = Timeout::new(
            self.get_idle_connection().boxed_local().compat(),
            self.0.statics.connection_timeout,
        )
        .compat()
        .await?;

        Ok(PoolRef {
            conn: Some(conn),
            pool: &self.0,
        })
    }

    #[cfg(feature = "default")]
    pub async fn run<T, E, EE, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut M::Connection) -> Pin<Box<dyn Future<Output = Result<T, EE>> + Send + '_>>,
        EE: From<M::Error>,
        E: From<EE> + From<tokio_timer::timeout::Elapsed> + From<M::Error>,
        T: Send + 'static,
    {
        let mut conn = Timeout::new(
            self.get_idle_connection(),
            self.0.statics.connection_timeout,
        )
        .await??;

        let result = f(&mut conn.conn).await;

        let shared = self.0.clone();

        // only check if the connection is broken when we get an error from result
        let mut broken = false;
        if result.is_err() {
            broken = shared.manager.is_closed(&mut conn.conn);
        }

        if broken {
            let _r = self.0.drop_connections(vec![conn]).await;
        } else {
            self.0.put_back_conn(conn);
        }
        Ok(result?)
    }

    #[cfg(feature = "actix-web")]
    pub async fn run<T, E, EE, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut M::Connection) -> Pin<Box<dyn Future<Output = Result<T, EE>> + Send + '_>>,
        EE: From<M::Error>,
        E: From<EE> + From<M::Error> + From<tokio_timer01::timeout::Error<M::Error>>,
        T: Send + 'static,
    {
        let mut conn = Timeout::new(
            self.get_idle_connection().boxed_local().compat(),
            self.0.statics.connection_timeout,
        )
        .compat()
        .await?;

        let result = f(&mut conn.conn).await;

        // only check if the connection is broken when we get an error from result
        let mut broken = false;
        if result.is_err() {
            broken = self.0.manager.is_closed(&mut conn.conn);
        }

        if broken {
            let _r = self.0.drop_connections(vec![conn]).await;
        } else {
            self.0.put_back_conn(conn);
        }
        Ok(result?)
    }

    /// recursive when failed to get a connection or the connection is broken. This method wrapped in a timeout future for canceling.
    fn get_idle_connection<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Conn<M>, M::Error>> + Send + 'a>> {
        Box::pin(async move {
            let mut conn = match self.0.pool.pop() {
                Ok(conn) => conn.into(),
                Err(_) => {
                    if (self.0.load_pending() + self.0.load_spawn())
                        < self.0.statics.max_size as usize
                    {
                        let _r = self.0.add_connection().await;
                    }

                    let (tx, rx) = channel();
                    self.0.queue.push(tx);

                    match rx.await {
                        Ok(conn) => conn,
                        Err(_) => return self.get_idle_connection().await,
                    }
                }
            };

            // Spin up a new connection if necessary to retain our minimum idle count
            if (self.0.load_pending() + self.0.load_spawn()) < self.0.statics.min_idle as usize {
                let _r = self.0.replenish_idle_connections().await;
            }

            if self.0.statics.always_check {
                // drop the connection if the connection is not valid anymore
                if self.0.manager.is_valid(&mut conn.conn).await.is_err() {
                    let _ = self.0.drop_connections(vec![conn]).await;
                    return self.get_idle_connection().await;
                }
            }
            Ok(conn)
        })
    }

    pub async fn state(&self) -> State {
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
    pub fn get_conn(&mut self) -> &mut M::Connection {
        &mut self.conn.as_mut().unwrap().conn
    }
}

impl<'a, M: Manager> Drop for PoolRef<'a, M> {
    fn drop(&mut self) {
        let mut conn = self.conn.take().unwrap();
        let shared_pool = self.pool.clone();
        #[cfg(feature = "actix-web")]
        actix_rt::spawn(
            async move {
                let broken = shared_pool.manager.is_closed(&mut conn.conn);
                if broken {
                    let _r = shared_pool.drop_connections(vec![conn]).await;
                } else {
                    shared_pool.put_back_conn(conn);
                };
                Ok(())
            }
                .boxed_local()
                .compat(),
        );

        #[cfg(feature = "default")]
        tokio_executor::spawn(async move {
            let broken = shared_pool.manager.is_closed(&mut conn.conn);
            if broken {
                let _r = shared_pool.drop_connections(vec![conn]).await;
            } else {
                shared_pool.put_back_conn(conn);
            }
        });
    }
}

/// schedule reaping runs in a spawned future.
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
