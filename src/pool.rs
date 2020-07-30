use core::fmt;
use core::future::Future;
use core::ops::{Deref, DerefMut};
#[cfg(not(feature = "no-send"))]
use std::sync::{Arc as WrapPointer, Weak};
use std::time::Instant;
#[cfg(feature = "no-send")]
use std::{
    rc::{Rc as WrapPointer, Weak},
    thread::{self, ThreadId},
};

use crate::builder::Builder;
use crate::manager::Manager;
use crate::pool_inner::{PoolLock, State};

pub struct Pool<M: Manager> {
    pool: SharedManagedPool<M>,
    #[cfg(feature = "no-send")]
    id: ThreadId,
}

impl<M: Manager> Pool<M> {
    /// Return a `PoolRef` contains reference of `SharedManagedPool<Manager>` and an `Option<Manager::Connection>`.
    ///
    /// The `PoolRef` should be dropped asap when you finish the use of it. Hold it in scope would prevent the connection from pushed back to pool.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[inline]
    pub async fn get(&self) -> Result<PoolRef<'_, M>, M::Error> {
        let shared_pool = &self.pool;

        shared_pool.get_conn(shared_pool).await
    }

    /// Return a `PoolRefOwned` contains a weak smart pointer of `SharedManagedPool<Manager>` and an `Option<Manager::Connection>`.
    ///
    /// You can move `PoolRefOwned` to async blocks and across await point. But the performance is considerably worse than `Pool::get`.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[inline]
    pub async fn get_owned(&self) -> Result<PoolRefOwned<M>, M::Error> {
        let shared_pool = &self.pool;

        shared_pool.get_conn(shared_pool).await
    }

    /// Sync version of `Pool::get`.
    ///
    /// This call would <b>BLOCK</b> the current thread and spin wait for a connection.
    #[inline]
    pub fn get_sync(&self) -> PoolRef<'_, M> {
        let shared_pool = &self.pool;
        shared_pool.pool_lock.lock_sync(shared_pool)
    }

    /// Sync version of `Pool::get_owned`.
    ///
    /// This call would <b>BLOCK</b> the current thread and spin wait for a connection
    #[inline]
    pub fn get_sync_owned(&self) -> PoolRefOwned<M> {
        let shared_pool = &self.pool;
        shared_pool.pool_lock.lock_sync(shared_pool)
    }

    /// Run the pool with an async closure.
    ///
    /// # example:
    /// ```ignore
    /// pool.run(|mut pool_ref| async {
    ///     let connection = &mut *pool_ref;
    ///     Ok(())
    /// })
    /// ```
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[inline]
    pub async fn run<'a, T, E, F, FF>(&'a self, f: F) -> Result<T, E>
    where
        F: FnOnce(PoolRef<'a, M>) -> FF,
        FF: Future<Output = Result<T, E>> + Send + 'a,
        E: From<M::Error>,
        T: Send + 'static,
    {
        let pool_ref = self.get().await?;
        f(pool_ref).await
    }

    /// manually initialize pool. this is usually called when the `Pool` is built with `build_uninitialized`
    /// This is useful when you want to make an empty `Pool` and initialize it later.
    /// # example:
    /// ```ignore
    /// #[macro_use]
    /// extern crate lazy_static;
    ///
    /// use tokio_postgres_tang::{Pool, PostgresManager, Builder};
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
    ///         );
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> std::io::Result<()> {
    ///     POOL.init().await.expect("Failed to initialize postgres pool");
    ///     Ok(())
    /// }
    /// ```
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub async fn init(&self) -> Result<(), M::Error> {
        let shared_pool = &self.pool;

        let count = shared_pool.builder.min_idle;

        shared_pool.pool_lock.inc_pending(count);

        shared_pool.replenish_idle_conn(count).await?;

        shared_pool.manager.on_start(shared_pool);

        Ok(())
    }

    /// Return a state of the pool inner. This call will block the thread and wait for lock.
    pub fn state(&self) -> State {
        self.pool.pool_lock.state()
    }

    /// Return the thread id the pool is running on.(This is only useful when running the pool on single threads)
    #[cfg(feature = "no-send")]
    pub fn thread_id(&self) -> ThreadId {
        self.id
    }

    pub(crate) fn new(builder: Builder, manager: M) -> Self {
        let pool = ManagedPool::new(builder, manager);
        Pool {
            pool: WrapPointer::new(pool),
            #[cfg(feature = "no-send")]
            id: thread::current().id(),
        }
    }
}

impl<M: Manager> Clone for Pool<M> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            #[cfg(feature = "no-send")]
            id: self.id,
        }
    }
}

impl<M: Manager> fmt::Debug for Pool<M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("Pool({:p})", self.pool))
    }
}

impl<M: Manager> Drop for Pool<M> {
    fn drop(&mut self) {
        self.pool.manager.on_stop();
    }
}

pub struct PoolRef<'a, M: Manager> {
    conn: Option<Conn<M>>,
    shared_pool: &'a SharedManagedPool<M>,
}

pub struct PoolRefOwned<M: Manager> {
    conn: Option<Conn<M>>,
    shared_pool: WeakSharedManagedPool<M>,
}

impl<M: Manager> PoolRef<'_, M> {
    /// get a mut reference of connection.
    pub fn get_conn(&mut self) -> &mut M::Connection {
        &mut *self
    }

    /// take the the ownership of connection from pool and it won't be pushed back to pool anymore.
    pub fn take_conn(&mut self) -> Option<M::Connection> {
        self.conn.take().map(|conn| conn.conn)
    }

    /// manually push a connection to pool. We treat this connection as a new born one.
    ///
    /// operation will fail if the pool is already in full capacity(no error will return and this connection will be dropped silently)
    pub fn push_conn(&mut self, conn: M::Connection) {
        self.conn = Some(Conn {
            conn,
            birth: Instant::now(),
        });
    }

    pub fn get_manager(&self) -> &M {
        &self.shared_pool.manager
    }
}

impl<M: Manager> PoolRefOwned<M> {
    /// get a mut reference of connection.
    pub fn get_conn(&mut self) -> &mut M::Connection {
        &mut *self
    }

    /// take the the ownership of connection from pool and it won't be pushed back to pool anymore.
    pub fn take_conn(&mut self) -> Option<M::Connection> {
        self.conn.take().map(|conn| conn.conn)
    }

    /// manually push a connection to pool. We treat this connection as a new born one.
    ///
    /// operation will fail if the pool is already in full capacity(no error will return and this connection will be dropped silently)
    pub fn push_conn(&mut self, conn: M::Connection) {
        self.conn = Some(Conn {
            conn,
            birth: Instant::now(),
        });
    }
}

pub(crate) trait PoolRefBehavior<'a, M: Manager>
where
    Self: DerefMut<Target = M::Connection> + Sized,
{
    fn from_idle(conn: IdleConn<M>, shared_pool: &'a SharedManagedPool<M>) -> Self;

    fn take_drop(self) {}
}

impl<'re, M: Manager> PoolRefBehavior<'re, M> for PoolRef<'re, M> {
    fn from_idle(conn: IdleConn<M>, shared_pool: &'re SharedManagedPool<M>) -> Self {
        Self {
            conn: Some(conn.into()),
            shared_pool,
        }
    }

    fn take_drop(mut self) {
        let _ = self.take_conn();
    }
}

impl<M: Manager> PoolRefBehavior<'_, M> for PoolRefOwned<M> {
    fn from_idle(conn: IdleConn<M>, shared_pool: &SharedManagedPool<M>) -> Self {
        Self {
            conn: Some(conn.into()),
            shared_pool: WrapPointer::downgrade(shared_pool),
        }
    }

    fn take_drop(mut self) {
        let _ = self.take_conn();
    }
}

impl<M: Manager> Deref for PoolRef<'_, M> {
    type Target = M::Connection;

    fn deref(&self) -> &Self::Target {
        &self
            .conn
            .as_ref()
            .expect("Connection has already been taken")
            .conn
    }
}

impl<M: Manager> DerefMut for PoolRef<'_, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self
            .conn
            .as_mut()
            .expect("Connection has already been taken")
            .conn
    }
}

impl<M: Manager> Deref for PoolRefOwned<M> {
    type Target = M::Connection;

    fn deref(&self) -> &Self::Target {
        &self
            .conn
            .as_ref()
            .expect("Connection has already been taken")
            .conn
    }
}

impl<M: Manager> DerefMut for PoolRefOwned<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self
            .conn
            .as_mut()
            .expect("Connection has already been taken")
            .conn
    }
}

impl<M: Manager> Drop for PoolRef<'_, M> {
    #[inline]
    fn drop(&mut self) {
        self.shared_pool.drop_pool_ref(&mut self.conn);
    }
}

impl<M: Manager> Drop for PoolRefOwned<M> {
    #[inline]
    fn drop(&mut self) {
        if let Some(shared_pool) = self.shared_pool.upgrade() {
            shared_pool.drop_pool_ref(&mut self.conn);
        }
    }
}

pub(crate) trait DropAndSpawn<M: Manager> {
    fn drop_pool_ref(&self, conn: &mut Option<Conn<M>>);

    fn spawn_drop(&self);

    fn spawn_idle(&self);
}

impl<M: Manager> DropAndSpawn<M> for SharedManagedPool<M> {
    #[inline]
    fn drop_pool_ref(&self, conn: &mut Option<Conn<M>>) {
        let mut conn = match conn.take() {
            Some(conn) => conn,
            None => return self.spawn_drop(),
        };

        let is_closed = self.manager.is_closed(&mut conn.conn);
        let lifetime_check = self.lifetime_check(&conn);

        if is_closed || lifetime_check {
            self.spawn_drop();
        } else {
            self.pool_lock.push_back(conn.into());
        };
    }

    // helper function to spawn drop connection and spawn new ones if needed.
    // Conn<M> should be dropped in place where spawn_drop() is used.
    // ToDo: Will get unsolvable pending if the spawn is panic;
    #[cold]
    fn spawn_drop(&self) {
        let opt = self.drop_conn();

        if let Some(pending) = opt {
            let shared_clone = self.clone();
            self.spawn(async move {
                let _ = shared_clone.replenish_idle_conn(pending).await;
            });
        }
    }

    #[inline]
    fn spawn_idle(&self) {
        let shared_clone = self.clone();
        self.spawn(async move {
            let _ = shared_clone.add_idle_conn().await;
        });
    }
}

pub struct ManagedPool<M: Manager> {
    builder: Builder,
    manager: M,
    pool_lock: PoolLock<M>,
}

impl<M: Manager> ManagedPool<M> {
    fn new(builder: Builder, manager: M) -> Self {
        let pool_lock = PoolLock::from_builder(&builder);

        Self {
            builder,
            manager,
            pool_lock,
        }
    }

    #[inline]
    async fn get_conn<'a, R>(&'a self, shared_pool: &'a SharedManagedPool<M>) -> Result<R, M::Error>
    where
        R: PoolRefBehavior<'a, M> + Unpin,
    {
        let fut = self.pool_lock.lock::<R>(shared_pool);
        let timeout = self.builder.wait_timeout;

        let mut pool_ref = self.manager.timeout(fut, timeout).await?;

        if self.builder.always_check {
            let mut retry = 0u8;
            loop {
                let result = self.check_conn(&mut pool_ref).await;
                match result {
                    Ok(Ok(_)) => break,
                    Ok(Err(e)) => {
                        pool_ref.take_drop();
                        if retry == 3 {
                            return Err(e);
                        } else {
                            retry += 1;
                        };
                    }
                    Err(timeout_err) => {
                        pool_ref.take_drop();
                        return Err(timeout_err);
                    }
                }

                let fut = self.pool_lock.lock::<R>(shared_pool);

                pool_ref = self.manager.timeout(fut, timeout).await?;
            }
        };

        Ok(pool_ref)
    }

    fn drop_conn(&self) -> Option<usize> {
        self.pool_lock.dec_active()
    }

    #[inline]
    fn lifetime_check(&self, conn: &Conn<M>) -> bool {
        match self.builder.max_lifetime {
            Some(dur) => conn.birth.elapsed() > dur,
            None => false,
        }
    }

    pub(crate) async fn add_idle_conn(&self) -> Result<(), M::Error> {
        let fut = self.manager.connect();
        let timeout = self.builder.connection_timeout;

        let conn = self
            .manager
            .timeout(fut, timeout)
            .await
            .map_err(|e| {
                self.pool_lock.dec_pending(1);
                e
            })?
            .map_err(|e| {
                self.pool_lock.dec_pending(1);
                e
            })?;

        self.pool_lock.push_new(IdleConn::new(conn));

        Ok(())
    }

    async fn check_conn(&self, conn: &mut M::Connection) -> Result<Result<(), M::Error>, M::Error> {
        let fut = self.manager.is_valid(conn);

        let timeout = self.builder.connection_timeout;

        let res = self.manager.timeout(fut, timeout).await?;

        Ok(res)
    }

    async fn replenish_idle_conn(&self, pending_count: usize) -> Result<(), M::Error> {
        for i in 0..pending_count {
            self.add_idle_conn().await.map_err(|e| {
                // we return when an error occur so we should drop all the pending after the i.
                // (the pending of i is already dropped in add_connection method)
                let count = pending_count - i - 1;
                if count > 0 {
                    self.pool_lock.dec_pending(count);
                };
                e
            })?;
        }

        Ok(())
    }

    #[cfg(not(feature = "no-send"))]
    pub(crate) fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.manager.spawn(fut);
    }

    #[cfg(feature = "no-send")]
    pub(crate) fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + 'static,
    {
        self.manager.spawn(fut);
    }

    /// expose `Builder` to public
    pub fn get_builder(&self) -> &Builder {
        &self.builder
    }
}

pub type SharedManagedPool<M> = WrapPointer<ManagedPool<M>>;

pub type WeakSharedManagedPool<M> = Weak<ManagedPool<M>>;

pub struct Conn<M: Manager> {
    conn: M::Connection,
    birth: Instant,
}

pub struct IdleConn<M: Manager> {
    conn: Conn<M>,
    // idle_start: Instant,
}

impl<M: Manager> IdleConn<M> {
    fn new(conn: M::Connection) -> Self {
        let now = Instant::now();
        IdleConn {
            conn: Conn { conn, birth: now },
            // idle_start: now,
        }
    }
}

impl<M: Manager> From<Conn<M>> for IdleConn<M> {
    fn from(conn: Conn<M>) -> IdleConn<M> {
        // let now = Instant::now();
        IdleConn {
            conn,
            // idle_start: now,
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
