//! # A tokio-postgres connection pool.
//! use `tokio-0.2` runtime. most code come from [bb8](https://docs.rs/bb8/0.3.1/bb8/)
//!
//! # Known Limitation:
//! No tests.
//! no `tokio-0.1` support.
//! can't be used in nested runtimes.
//! low setting of idle_connect count could result in large amount of timeout error when the pool is under pressure.(before newly spawned connections catch up)
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
//!                Ok(row)
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

#![feature(no_more_cas)]

use std::cmp::{max, min};
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

#[cfg(feature = "default")]
use futures::lock::Mutex;
#[cfg(feature = "actix-web")]
use futures::{compat::Future01CompatExt, compat::Stream01CompatExt, TryFutureExt};
use futures::{FutureExt, Stream, StreamExt};
#[cfg(feature = "actix-web")]
use parking_lot::Mutex;
use tokio_executor::current_thread::spawn as spawn_current;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Client, Error, Socket, Statement,
};
#[cfg(feature = "default")]
use tokio_timer::{delay, Interval, Timeout};
#[cfg(feature = "actix-web")]
use tokio_timer01::{Delay, Interval, Timeout};

pub use builder::Builder;
pub use error::PoolError;
pub use postgres::PostgresConnectionManager;

mod builder;
mod error;
mod postgres;

const LOOP_DELAY: Duration = Duration::from_millis(20);

static IDLE_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Conn type contains connection itself and prepared statements for this connection.
pub struct Conn {
    pub conn: (Client, Vec<Statement>),
    birth: Instant,
}

pub struct IdleConn {
    conn: Conn,
    idle_start: Instant,
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

struct PoolInner {
    conns: VecDeque<IdleConn>,
    num_conns: u32,
    pending_conns: u32,
}

impl PoolInner {
    fn put_idle_conn(&mut self, conn: IdleConn) {
        self.conns.push_back(conn);
    }
}

/// use future aware mutex for inner Pool lock.
/// So the waiter queue is handled by the mutex lock and not the pool.
struct SharedPool<Tls: MakeTlsConnect<Socket>> {
    statics: Builder,
    manager: PostgresConnectionManager<Tls>,
    pool: Mutex<PoolInner>,
}

pub struct Pool<Tls: MakeTlsConnect<Socket>> {
    inner: Arc<SharedPool<Tls>>,
}

impl<Tls: MakeTlsConnect<Socket>> Clone for Pool<Tls> {
    fn clone(&self) -> Self {
        Pool {
            inner: self.inner.clone(),
        }
    }
}

impl<Tls: MakeTlsConnect<Socket>> fmt::Debug for Pool<Tls> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("Pool({:p})", self.inner))
    }
}

impl<Tls> Pool<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn new_inner(builder: Builder, manager: PostgresConnectionManager<Tls>) -> Pool<Tls> {
        let inner = PoolInner {
            conns: VecDeque::new(),
            num_conns: 0,
            pending_conns: 0,
        };

        let shared = Arc::new(SharedPool {
            statics: builder,
            manager,
            pool: Mutex::new(inner),
        });

        // spawn a loop interval future to handle the lifetime and time out of connections.
        if (shared.statics.max_lifetime.is_some() || shared.statics.idle_timeout.is_some())
            && shared.statics.min_idle < shared.statics.max_size
        {
            let interval = Interval::new_interval(shared.statics.reaper_rate);
            let weak_shared = Arc::downgrade(&shared);

            schedule_one_reaping(interval, weak_shared);
        }

        Pool { inner: shared }
    }

    async fn replenish_idle_connections_locked(
        pool: &Arc<SharedPool<Tls>>,
        inner: &mut PoolInner,
    ) -> Result<(), Error> {
        let slots_available = pool.statics.max_size - inner.num_conns - inner.pending_conns;

        let idle = inner.conns.len() as u32;
        let desired = pool.statics.min_idle;

        for _i in idle..max(idle + 1, min(desired, slots_available)) {
            let _ = add_connection(pool, inner).await;
        }

        Ok(())
    }

    async fn replenish_idle_connections(&self) -> Result<(), Error> {
        #[cfg(feature = "actix-web")]
        let mut locked = self.inner.pool.lock();
        #[cfg(feature = "default")]
        let mut locked = self.inner.pool.lock().await;

        Pool::replenish_idle_connections_locked(&self.inner, &mut locked).await
    }

    pub fn builder() -> Builder {
        Builder::new()
    }

    pub async fn run<T, E, F>(&self, f: F) -> Result<T, PoolError<E>>
    where
        F: FnOnce(
            &mut (Client, Vec<Statement>),
        ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + '_>>,
        PoolError<E>: From<Error>,
        T: Send + 'static,
    {
        let inner = self.inner.clone();

        #[cfg(feature = "actix-web")]
        let mut conn = Timeout::new(
            get_idle_connection(inner).boxed_local().compat(),
            self.inner.statics.connection_timeout,
        )
        .compat()
        .await?;

        #[cfg(feature = "default")]
        let mut conn = Timeout::new(
            get_idle_connection(inner),
            self.inner.statics.connection_timeout,
        )
        .await??;

        let result = f(&mut conn.conn).await.map_err(PoolError::Inner);

        let shared = self.inner.clone();

        let broken = shared.manager.is_closed(&mut conn.conn.0);
        #[cfg(feature = "actix-web")]
        let mut inner = shared.pool.lock();
        #[cfg(feature = "default")]
        let mut inner = shared.pool.lock().await;

        if broken {
            let _r = drop_connections(&shared, &mut inner, vec![conn]).await;
        } else {
            inner.conns.push_back(conn.into());
            IDLE_COUNTER.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub async fn state(&self) -> State {
        #[cfg(feature = "actix-web")]
        let inner = self.inner.pool.lock();
        #[cfg(feature = "default")]
        let inner = self.inner.pool.lock().await;

        State {
            connections: inner.num_conns,
            idle_connections: inner.conns.len() as u32,
        }
    }
}

pub struct State {
    pub connections: u32,
    pub idle_connections: u32,
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("State")
            .field("connections", &self.connections)
            .field("idle_connections", &self.idle_connections)
            .finish()
    }
}

async fn add_connection<Tls>(
    pool: &Arc<SharedPool<Tls>>,
    inner: &mut PoolInner,
) -> Result<(), Error>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    inner.pending_conns += 1;

    let conn = pool
        .manager
        .connect(&pool.statics.statements)
        .await
        .map_err(|e| {
            inner.pending_conns -= 1;
            e
        })?;

    let now = Instant::now();
    let conn = IdleConn {
        conn: Conn { conn, birth: now },
        idle_start: now,
    };

    inner.num_conns += 1;
    inner.pending_conns -= 1;

    inner.put_idle_conn(conn);
    IDLE_COUNTER.fetch_add(1, Ordering::Relaxed);

    Ok(())
}

async fn get_idle_connection<Tls>(shared: Arc<SharedPool<Tls>>) -> Result<Conn, Error>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    // async idle will return a stream of (Conn, u32). u32 is the current IdleConn count in pool plus the pending Conn count.
    // we may need to poll this stream multiple times as we would check if the conn is valid until we get a valid connection.
    let mut stream = AsyncIdle::get_idle(shared.clone());

    loop {
        if let Some((mut conn, current_count)) = stream.next().await {
            // Spin up a new connection if necessary to retain our minimum idle count
            if current_count < shared.statics.max_size {
                // ToDo: lock twice. could reduce lock time to one.

                #[cfg(feature = "actix-web")]
                let mut inner = shared.pool.lock();
                #[cfg(feature = "default")]
                let mut inner = shared.pool.lock().await;

                /*
                    we have to double check as we lost the lock when we poll the stream.
                    so there could possible a racing condition.
                */
                if inner.num_conns + inner.pending_conns < shared.statics.max_size {
                    let _r = Pool::replenish_idle_connections_locked(&shared, &mut *inner).await;
                }
                drop(inner);
            }

            if shared.statics.always_check {
                // drop the connection if the connection is not valid anymore
                let r = shared.manager.is_valid(&mut conn.conn.conn.0).await;
                match r {
                    Ok(_) => break Ok(conn.into()),
                    Err(_) => {
                        let shared_1 = shared.clone();

                        #[cfg(feature = "actix-web")]
                        let mut inner = shared_1.pool.lock();
                        #[cfg(feature = "default")]
                        let mut inner = shared_1.pool.lock().await;

                        let _ = drop_connections(&shared, &mut inner, vec![conn.into()]).await;
                    }
                }
            } else {
                break Ok(conn.into());
            }
        }
        #[cfg(feature = "actix-web")]
        let _ = Delay::new(Instant::now() + LOOP_DELAY).compat().await;
        #[cfg(feature = "default")]
        delay(Instant::now() + LOOP_DELAY).await;
    }
}

// Drop connections
// NB: This is called with the pool lock held.
async fn drop_connections<Tls>(
    shared: &Arc<SharedPool<Tls>>,
    inner: &mut PoolInner,
    to_drop: Vec<Conn>,
) -> Result<(), Error>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    inner.num_conns -= to_drop.len() as u32;

    IDLE_COUNTER.fetch_sub(to_drop.len(), Ordering::Relaxed);

    // We might need to spin up more connections to maintain the idle limit, e.g.
    // if we hit connection lifetime limits
    let f = if inner.num_conns + inner.pending_conns < shared.statics.min_idle {
        Pool::replenish_idle_connections_locked(shared, &mut *inner).await
    } else {
        Ok(())
    };

    // And drop the connections
    // TODO: connection_customizer::on_release! That would require figuring out the
    // locking situation though
    f
}

// Reap connections if necessary.
// NB: This is called with the pool lock held.
async fn reap_connections<Tls>(
    shared: &Arc<SharedPool<Tls>>,
    inner: &mut PoolInner,
) -> Result<(), Error>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let now = Instant::now();

    let (to_drop, preserve) = inner.conns.drain(..).partition(|conn: &IdleConn| {
        let mut reap = false;
        if let Some(timeout) = shared.statics.idle_timeout {
            reap |= now - conn.idle_start >= timeout;
        }
        if let Some(lifetime) = shared.statics.max_lifetime {
            reap |= now - conn.conn.birth >= lifetime;
        }
        reap
    });
    inner.conns = preserve;

    let to_drop = to_drop.into_iter().map(|c| c.conn).collect();

    drop_connections(shared, inner, to_drop).await
}

fn schedule_one_reaping<Tls>(interval: Interval, weak_shared: Weak<SharedPool<Tls>>)
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    spawn_current(async move {
        #[cfg(feature = "actix-web")]
        let mut interval = interval.compat();
        #[cfg(feature = "default")]
        let mut interval = interval;
        loop {
            let _i = interval.next().await;
            if let Some(shared) = weak_shared.upgrade() {
                #[cfg(feature = "actix-web")]
                let mut inner = shared.pool.lock();
                #[cfg(feature = "default")]
                let mut inner = shared.pool.lock().await;

                let _ = reap_connections(&shared, &mut inner).await;
            };
        }
    });
}

/// Get IdleConn from pool asynchronously
pub struct AsyncIdle<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    shared: Arc<SharedPool<Tls>>,
}

impl<Tls> AsyncIdle<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn get_idle(shared: Arc<SharedPool<Tls>>) -> Self {
        AsyncIdle { shared }
    }
}

impl<Tls> Stream for AsyncIdle<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Item = (IdleConn, u32);

    /// lock and require a `PoolInner`.
    /// we wake the context if the shared pool have a counter bigger than 0.
    /// After that we try to poll the lock and if there is a connection in locked pool we poll the stream with Some.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let waker = cx.waker();

        if IDLE_COUNTER.load(Ordering::SeqCst) > 0 {
            waker.wake_by_ref()
        }

        let this = self.get_mut();

        #[cfg(feature = "actix-web")]
        let mut inner = this.shared.pool.lock();
        #[cfg(feature = "default")]
        let mut inner = futures::ready!(this.shared.pool.lock().poll_unpin(cx));

        if let Some(conn) = inner.conns.pop_front() {
            IDLE_COUNTER.fetch_sub(1, Ordering::Relaxed);
            let count = inner.num_conns + inner.pending_conns;

            Poll::Ready(Some((conn, count)))
        } else {
            Poll::Ready(None)
        }
    }
}
