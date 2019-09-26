use std::cmp::{max, min};
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::lock::{MutexGuard, MutexLockFuture};
use futures::{lock::Mutex, FutureExt, Stream, StreamExt};
use tokio_executor::spawn;
use tokio_postgres::tls::TlsConnect;
use tokio_postgres::{tls::MakeTlsConnect, types::Type, Client, Error, Socket, Statement};
use tokio_timer::Interval;

pub use error::PoolError;
pub use postgres::PostgresConnectionManager;
use postgres::PreparedStatement;

mod error;
mod postgres;

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

pub struct Builder {
    max_size: u32,
    min_idle: u32,
    /// Whether or not to test the connection on checkout.
    always_check: bool,
    max_lifetime: Option<Duration>,
    idle_timeout: Option<Duration>,
    connection_timeout: Duration,
    /// The time interval used to wake up and reap connections.
    reaper_rate: Duration,
    statements: Vec<PreparedStatement>,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            max_size: 10,
            min_idle: 0,
            always_check: true,
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            connection_timeout: Duration::from_secs(30),
            reaper_rate: Duration::from_secs(30),
            statements: vec![],
        }
    }
}

impl Builder {
    pub fn new() -> Builder {
        Default::default()
    }

    pub fn max_size(mut self, max_size: u32) -> Builder {
        if max_size > 0 {
            self.max_size = max_size;
        }
        self
    }

    pub fn min_idle(mut self, min_idle: u32) -> Builder {
        self.min_idle = min_idle;
        self
    }

    /// If true, the health of a connection will be verified through a call to
    /// `ManageConnection::is_valid` before it is provided to a pool user.
    ///
    /// Defaults to true.
    pub fn always_check(mut self, always_check: bool) -> Builder {
        self.always_check = always_check;
        self
    }

    /// Sets the maximum lifetime of connections in the pool.
    ///
    /// If set, connections will be closed at the next reaping after surviving
    /// past this duration.
    ///
    /// If a connection reachs its maximum lifetime while checked out it will be
    /// closed when it is returned to the pool.
    ///
    /// Defaults to 30 minutes.
    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> Builder {
        self.max_lifetime = max_lifetime;
        self
    }

    /// Sets the idle timeout used by the pool.
    ///
    /// If set, idle connections in excess of `min_idle` will be closed at the
    /// next reaping after remaining idle past this duration.
    ///
    /// Defaults to 10 minutes.
    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Builder {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Sets the connection timeout used by the pool.
    ///
    /// Futures returned by `Pool::get` will wait this long before giving up and
    /// resolving with an error.
    ///
    /// Defaults to 30 seconds.
    pub fn connection_timeout(mut self, connection_timeout: impl Into<Duration>) -> Builder {
        self.connection_timeout = connection_timeout.into();
        self
    }

    /// prepared statements can be passed when connecting to speed up frequent used queries.
    /// example:
    /// ```rust
    /// use tokio_postgres::types::Type;
    ///
    /// let statements = vec![
    ///     ("SELECT * from table WHERE id = $1".to_owned(), vec![Type::OID]),
    ///     ("SELECT * from table2 WHERE id = $1", vec![]) // pass empty vec if you don't want a type specific prepare.
    /// ];
    /// let builder = crate::Builder::new().prepare_statements(statements);
    /// ```
    pub fn prepare_statements(mut self, statements: Vec<(&str, Vec<Type>)>) -> Builder {
        let statements = statements
            .into_iter()
            .map(|p| p.into())
            .collect::<Vec<PreparedStatement>>();

        self.statements = statements;
        self
    }

    /// Consumes the builder, returning a new, initialized `Pool`.
    pub async fn build<Tls>(
        self,
        manager: PostgresConnectionManager<Tls>,
    ) -> Result<Pool<Tls>, PoolError<Error>>
    where
        Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
        <Tls as MakeTlsConnect<Socket>>::Stream: Send,
        <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
        <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        assert!(
            self.max_size >= self.min_idle,
            "min_idle must be no larger than max_size"
        );

        let pool = Pool::new_inner(self, manager);

        pool.replenish_idle_connections().await?;
        Ok(pool)
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
        if shared.statics.max_lifetime.is_some() || shared.statics.idle_timeout.is_some() {
            let interval = Interval::new_interval(shared.statics.reaper_rate);
            let weak_shared = Arc::downgrade(&shared);

            // ToDo: in case spawn error then change to lazy.
            schedule_one_reaping(interval, weak_shared);
        }

        Pool { inner: shared }
    }

    // ToDo: replenish connection is blocking for now.
    async fn replenish_idle_connections_locked(
        pool: &Arc<SharedPool<Tls>>,
        inner: &mut PoolInner,
    ) -> Result<(), Error> {
        let slots_available = pool.statics.max_size - inner.num_conns - inner.pending_conns;
        let idle = inner.conns.len() as u32;
        let desired = pool.statics.min_idle;

        for _i in idle..max(idle, min(desired, idle + slots_available)) {
            add_connection(pool, inner).await?;
        }
        Ok(())
    }

    async fn replenish_idle_connections(&self) -> Result<(), Error> {
        let mut locked = self.inner.pool.lock().await;
        Pool::replenish_idle_connections_locked(&self.inner, &mut locked).await
    }

    pub fn builder() -> Builder {
        Builder::new()
    }

    pub async fn run<T, E, F, Fut>(&self, f: F) -> Result<T, PoolError<E>>
    where
        // ToDo: possible take in Statements as Option
        // we take advantage of async/await and pass mut reference to closure.
        // this could be a limitation to the future's lifetime in closure.
        F: FnOnce(Conn) -> Fut,
        Fut: Future<Output = Result<(T, Conn), E>> + Send + 'static,
        PoolError<E>: From<Error>,
        T: Send + 'static,
    {
        let inner = self.inner.clone();
        // ToDo: we should add timeout here.
        let conn = get_idle_connection(inner).await?;

        let (r, mut conn) = f(conn).await.map_err(|e| PoolError::Inner(e))?;

        let shared = self.inner.clone();

        // ToDo: check if closed here?
        let broken = shared.manager.is_closed(&mut conn.conn.0);
        let mut inner: MutexGuard<PoolInner> = shared.pool.lock().await;

        if broken {
            let _r = drop_connections(&shared, inner, vec![conn]).await;
        } else {
            inner.conns.push_back(conn.into());
        }

        Ok(r)
    }
}

// Outside of Pool to avoid borrow splitting issues on self
// NB: This is called with the pool lock held.
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
    assert!(inner.num_conns + inner.pending_conns < pool.statics.max_size);
    inner.pending_conns += 1;

    // ToDo: in case the add connection is slow try oneshot channel.
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
    inner.pending_conns -= 1;
    inner.num_conns += 1;
    inner.put_idle_conn(conn);

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
    // ToDo: lock once
    let mut stream = AsyncIdle::get_idle(shared.clone());

    // ToDo: is there a chance this loop will leak?
    loop {
        if let Some((mut conn, current_count)) = stream.next().await {
            // Spin up a new connection if necessary to retain our minimum idle count
            if current_count < shared.statics.max_size {
                // ToDo: lock twice. could reduce lock time to one.
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
                        let inner: MutexGuard<PoolInner> = shared_1.pool.lock().await;
                        let _ = drop_connections(&shared, inner, vec![conn.into()]).await;
                    }
                }
            } else {
                break Ok(conn.into());
            }
        }
    }
}

// Drop connections
// NB: This is called with the pool lock held.
async fn drop_connections<Tls>(
    shared: &Arc<SharedPool<Tls>>,
    mut inner: MutexGuard<'_, PoolInner>,
    to_drop: Vec<Conn>,
) -> Result<(), Error>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    inner.num_conns -= to_drop.len() as u32;

    // We might need to spin up more connections to maintain the idle limit, e.g.
    // if we hit connection lifetime limits
    let f = if inner.num_conns + inner.pending_conns < shared.statics.max_size {
        Pool::replenish_idle_connections_locked(shared, &mut *inner).await
    } else {
        Ok(())
    };

    // Maybe unlock. If we're passed a MutexGuard, this will unlock. If we're passed a
    // &mut MutexGuard it won't.
    drop(inner);

    // And drop the connections
    // TODO: connection_customizer::on_release! That would require figuring out the
    // locking situation though
    f
}

// Reap connections if necessary.
// NB: This is called with the pool lock held.
async fn reap_connections<Tls>(
    shared: &Arc<SharedPool<Tls>>,
    mut inner: MutexGuard<'_, PoolInner>,
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

fn schedule_one_reaping<Tls>(mut interval: Interval, weak_shared: Weak<SharedPool<Tls>>)
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    spawn(async move {
        loop {
            let _i = interval.next().await;
            if let Some(shared) = weak_shared.upgrade() {
                let inner = shared.pool.lock().await;
                let _ = reap_connections(&shared, inner).await;
            };
        }
    })
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
    /// if there is no connection from the pool we return Pending until we get a connection.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut future: MutexLockFuture<PoolInner> = self.shared.pool.lock();

            let mut inner: MutexGuard<PoolInner> = match future.poll_unpin(cx) {
                Poll::Ready(inner) => inner,
                Poll::Pending => {
                    drop(future);
                    return Poll::Pending;
                }
            };

            if let Some(conn) = inner.conns.pop_front() {
                let count = inner.num_conns + inner.pending_conns;
                break Poll::Ready(Some((conn, count)));
            } else {
                drop(future);
                return Poll::Pending;
            }
        }
    }
}
