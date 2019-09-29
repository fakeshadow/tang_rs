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
//!use tang_rs::{Builder, PoolError, PostgresConnectionManager};
//!
//!#[tokio::main]
//!async fn main() -> std::io::Result<()> {
//!    let db_url = "postgres://postgres:123@localhost/test";
//!    // setup manager
//!    // only support NoTls for now
//!    let mgr =
//!        PostgresConnectionManager::new_from_stringlike(
//!            db_url,
//!            tokio_postgres::NoTls,
//!        ).unwrap_or_else(|_| panic!("can't make postgres manager"));
//!    // make prepared statements. pass Vec<tokio_postgres::types::Type> if you want typed statement. pass vec![] for no typed statement.
//!    // ignore if you don't need any prepared statements.
//!    let statements = vec![
//!            ("SELECT * FROM topics WHERE id=ANY($1)", vec![tokio_postgres::types::Type::OID_ARRAY]),
//!            ("SELECT * FROM posts WHERE id=ANY($1)", vec![])
//!        ];
//!    // make pool
//!    let pool = Builder::new()
//!        .always_check(false) // if set true every connection will be checked before checkout.
//!        .idle_timeout(None) // set idle_timeout and max_lifetime both to None to ignore idle connection drop.
//!        .max_lifetime(None)
//!        .min_idle(1)   // when working with heavy load the min_idle count should be set a higher count close to max_size.
//!        .max_size(12)
//!        .prepare_statements(statements)
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
use std::sync::{Arc, Weak};
use std::time::Instant;

use crossbeam_queue::{ArrayQueue, SegQueue};
use futures::channel::oneshot::{channel, Sender};
#[cfg(feature = "actix-web")]
use futures::{compat::Future01CompatExt, FutureExt, TryFutureExt};
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Client, Error, Socket, Statement,
};
use tokio_timer::Interval;
#[cfg(feature = "default")]
use tokio_timer::Timeout;
#[cfg(feature = "actix-web")]
use tokio_timer01::Timeout;

pub use builder::Builder;
pub use error::PoolError;
pub use postgres::PostgresConnectionManager;

mod builder;
mod error;
mod postgres;

static SPAWNED_COUNTER: AtomicUsize = AtomicUsize::new(0);
static PENDING_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Conn type contains connection itself and prepared statements for this connection.
pub struct Conn {
    pub conn: (Client, Vec<Statement>),
    birth: Instant,
}

pub struct IdleConn {
    conn: Conn,
    idle_start: Instant,
}

impl IdleConn {
    fn new(conn: (Client, Vec<Statement>)) -> Self {
        let now = Instant::now();
        IdleConn {
            conn: Conn { conn, birth: now },
            idle_start: now,
        }
    }
}

impl From<Conn> for IdleConn {
    fn from(conn: Conn) -> IdleConn {
        let now = Instant::now();
        IdleConn {
            conn,
            idle_start: now,
        }
    }
}

impl From<IdleConn> for Conn {
    fn from(conn: IdleConn) -> Conn {
        Conn {
            conn: conn.conn.conn,
            birth: conn.conn.birth,
        }
    }
}

/// use future aware mutex for inner Pool lock.
/// So the waiter queue is handled by the mutex lock and not the pool.
struct SharedPool<Tls: MakeTlsConnect<Socket>> {
    statics: Builder,
    manager: PostgresConnectionManager<Tls>,
    pool: ArrayQueue<IdleConn>,
    queue: SegQueue<Sender<Conn>>,
}

impl<Tls> SharedPool<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn put_back_conn(&self, conn: Conn) {
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
            SPAWNED_COUNTER.fetch_sub(1, Ordering::SeqCst);
        }
    }

    // ToDo: spawn this method in thread pool
    async fn drop_connections(&self, to_drop: Vec<Conn>) -> Result<(), Error> {
        SPAWNED_COUNTER.fetch_sub(to_drop.len(), Ordering::SeqCst);
        // We might need to spin up more connections to maintain the idle limit, e.g.
        // if we hit connection lifetime limits
        let min = self.statics.min_idle as usize;
        if SPAWNED_COUNTER.load(Ordering::SeqCst) < min {
            self.replenish_idle_connections().await
        } else {
            Ok(())
        }
    }

    // ToDo: spawn this method in thread pool
    async fn add_connection(&self) -> Result<(), Error> {
        PENDING_COUNTER.fetch_add(1, Ordering::SeqCst);
        let conn = self
            .manager
            .connect(&self.statics.statements)
            .await
            .map_err(|e| {
                PENDING_COUNTER.fetch_sub(1, Ordering::SeqCst);
                e
            })?;

        PENDING_COUNTER.fetch_sub(1, Ordering::SeqCst);
        SPAWNED_COUNTER.fetch_add(1, Ordering::SeqCst);

        let conn = IdleConn::new(conn);

        self.put_back_conn(conn.into());

        Ok(())
    }

    async fn replenish_idle_connections(&self) -> Result<(), Error> {
        let pending = PENDING_COUNTER.load(Ordering::SeqCst) as u8;
        let spawned = SPAWNED_COUNTER.load(Ordering::SeqCst) as u8;

        let slots_available = self.statics.max_size - spawned - pending;

        let idle = self.pool.len() as u8;
        let desired = self.statics.min_idle;

        for _i in idle..max(idle + 1, min(desired, slots_available)) {
            let _ = self.add_connection().await;
        }

        Ok(())
    }

    async fn reap_connections(&self) -> Result<(), Error> {
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

pub struct Pool<Tls: MakeTlsConnect<Socket>>(Arc<SharedPool<Tls>>);

impl<Tls: MakeTlsConnect<Socket>> Clone for Pool<Tls> {
    fn clone(&self) -> Self {
        Pool(self.0.clone())
    }
}

impl<Tls: MakeTlsConnect<Socket>> fmt::Debug for Pool<Tls> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("Pool({:p})", self.0))
    }
}

impl<Tls> Pool<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn new_inner(builder: Builder, manager: PostgresConnectionManager<Tls>) -> Self {
        let size = builder.max_size as usize;

        let shared_pool = Arc::new(SharedPool {
            statics: builder,
            manager,
            pool: ArrayQueue::new(size),
            queue: SegQueue::new(),
        });

        // spawn a loop interval future to handle the lifetime and time out of connections.
        schedule_one_reaping(&shared_pool);

        Pool(shared_pool)
    }

    /// `pool.get()` return a reference of pool and a Conn. So that you can use the Conn outside a closure.
    /// The limitation is we can't figure out if your get a error when use the Conn and make conditional broken check before push the Conn back to pool
    #[cfg(feature = "default")]
    pub async fn get<E>(&self) -> Result<PoolRef<Tls>, E>
    where
        E: From<Error> + From<tokio_timer::timeout::Elapsed>,
    {
        let conn = Timeout::new(
            self.get_idle_connection(),
            self.0.statics.connection_timeout,
        )
        .await??;

        Ok(PoolRef {
            conn: Some(conn),
            pool: Arc::downgrade(&self.0),
        })
    }

    #[cfg(feature = "actix-web")]
    pub async fn get<E>(&self) -> Result<PoolRef<Tls>, E>
    where
        E: From<Error> + From<tokio_timer01::timeout::Error<Error>>,
    {
        let conn = Timeout::new(
            self.get_idle_connection().boxed_local().compat(),
            self.0.statics.connection_timeout,
        )
        .compat()
        .await?;

        Ok(PoolRef {
            conn: Some(conn),
            pool: Arc::downgrade(&self.0),
        })
    }

    #[cfg(feature = "default")]
    pub async fn run<T, E, EE, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(
            &mut (Client, Vec<Statement>),
        ) -> Pin<Box<dyn Future<Output = Result<T, EE>> + Send + '_>>,
        EE: From<Error>,
        E: From<EE> + From<tokio_timer::timeout::Elapsed> + From<Error>,
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
            broken = shared.manager.is_closed(&mut conn.conn.0);
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
        F: FnOnce(
            &mut (Client, Vec<Statement>),
        ) -> Pin<Box<dyn Future<Output = Result<T, EE>> + Send + '_>>,
        EE: From<Error>,
        E: From<EE> + From<Error> + From<tokio_timer01::timeout::Error<Error>>,
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
            broken = self.0.manager.is_closed(&mut conn.conn.0);
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
    ) -> Pin<Box<dyn Future<Output = Result<Conn, Error>> + Send + 'a>> {
        Box::pin(async move {
            let mut conn = match self.0.pool.pop() {
                Ok(conn) => conn.into(),
                Err(_) => {
                    if (PENDING_COUNTER.load(Ordering::SeqCst)
                        + SPAWNED_COUNTER.load(Ordering::SeqCst))
                        < self.0.statics.max_size as usize
                    {
                        // ToDo: spawn a future to add connection
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
            if (PENDING_COUNTER.load(Ordering::SeqCst) + SPAWNED_COUNTER.load(Ordering::SeqCst))
                < self.0.statics.min_idle as usize
            {
                let _r = self.0.replenish_idle_connections().await;
            }

            if self.0.statics.always_check {
                // drop the connection if the connection is not valid anymore
                if self.0.manager.is_valid(&mut conn.conn.0).await.is_err() {
                    let _ = self.0.drop_connections(vec![conn]).await;
                    return self.get_idle_connection().await;
                }
            }
            Ok(conn)
        })
    }

    pub async fn state(&self) -> State {
        State {
            connections: SPAWNED_COUNTER.load(Ordering::Relaxed) as u8,
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

pub struct PoolRef<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn: Option<Conn>,
    pool: Weak<SharedPool<Tls>>,
}

impl<Tls> PoolRef<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn get_conn(&mut self) -> &mut (Client, Vec<Statement>) {
        &mut self.conn.as_mut().unwrap().conn
    }
}

impl<Tls> Drop for PoolRef<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn drop(&mut self) {
        if let Some(shared_pool) = self.pool.upgrade() {
            let mut conn = self.conn.take().unwrap();

            #[cfg(feature = "actix-web")]
            actix_rt::spawn(
                async move {
                    let broken = shared_pool.manager.is_closed(&mut conn.conn.0);
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
                let broken = shared_pool.manager.is_closed(&mut conn.conn.0);
                if broken {
                    let _r = shared_pool.drop_connections(vec![conn]).await;
                } else {
                    shared_pool.put_back_conn(conn);
                }
            });
        }
    }
}

/// schedule reaping runs in a spawned future.
fn schedule_one_reaping<Tls>(shared: &Arc<SharedPool<Tls>>)
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    if shared.statics.max_lifetime.is_some() || shared.statics.idle_timeout.is_some() {
        let mut interval = Interval::new_interval(shared.statics.reaper_rate);
        let weak_shared = Arc::downgrade(&shared);

        tokio_executor::spawn(async move {
            loop {
                let _i = interval.next().await;
                if let Some(shared) = weak_shared.upgrade() {
                    let _ = shared.reap_connections().await;
                };
            }
        });
    }
}
