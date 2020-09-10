use core::fmt;
use core::future::Future;
use core::ops::{Deref, DerefMut};
use core::time::Duration;
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
use crate::pool_inner::{PoolInner, State};
use crate::util::spawn_guard::SpawnGuardOwned;

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

        shared_pool._get(shared_pool).await
    }

    /// Return a `PoolRefOwned` contains a weak smart pointer of `SharedManagedPool<Manager>` and an `Option<Manager::Connection>`.
    ///
    /// You can move `PoolRefOwned` to async blocks and across await point. But the performance is considerably worse than `Pool::get`.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[inline]
    pub async fn get_owned(&self) -> Result<PoolRefOwned<M>, M::Error> {
        let shared_pool = &self.pool;

        shared_pool._get(shared_pool).await
    }

    /// Sync version of `Pool::get`.
    ///
    /// This call would <b>BLOCK</b> the current thread and spin wait for a connection.
    #[inline]
    pub fn get_sync(&self) -> PoolRef<'_, M> {
        let shared_pool = &self.pool;
        shared_pool.inner.get_inner_sync(shared_pool)
    }

    /// Sync version of `Pool::get_owned`.
    ///
    /// This call would <b>BLOCK</b> the current thread and spin wait for a connection
    #[inline]
    pub fn get_sync_owned(&self) -> PoolRefOwned<M> {
        let shared_pool = &self.pool;
        shared_pool.inner.get_inner_sync(shared_pool)
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

        shared_pool.add_multi(count).await?;

        shared_pool.manager.on_start(shared_pool);

        Ok(())
    }

    /// Return a state of the pool inner. This call will block the thread and wait for lock.
    pub fn state(&self) -> State {
        self.pool.inner.state()
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
    fn from_conn(conn: Conn<M>, shared_pool: &'a SharedManagedPool<M>) -> Self;

    fn take_drop(self) {}
}

impl<'re, M: Manager> PoolRefBehavior<'re, M> for PoolRef<'re, M> {
    fn from_conn(conn: Conn<M>, shared_pool: &'re SharedManagedPool<M>) -> Self {
        Self {
            conn: Some(conn),
            shared_pool,
        }
    }

    fn take_drop(mut self) {
        let _ = self.take_conn();
    }
}

impl<M: Manager> PoolRefBehavior<'_, M> for PoolRefOwned<M> {
    fn from_conn(conn: Conn<M>, shared_pool: &SharedManagedPool<M>) -> Self {
        Self {
            conn: Some(conn),
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
            self.push_back(conn);
        };
    }

    // helper function to spawn drop connection and spawn new ones if needed.
    // Conn<M> should be dropped in place where spawn_drop() is used.
    #[cold]
    fn spawn_drop(&self) {
        let should_spawn_new = self.dec_active();

        if should_spawn_new {
            self.inc_active();
            self.spawn_idle();
        }
    }

    fn spawn_idle(&self) {
        let mut spawn_guard = SpawnGuardOwned::new(self.clone());
        self.manager.spawn(async move {
            let shared_pool = spawn_guard.shared_pool();
            if let Ok(conn) = shared_pool.add().await {
                shared_pool.push_back(conn);
                spawn_guard.fulfilled();
            };
        });
    }
}

pub struct ManagedPool<M: Manager> {
    builder: Builder,
    manager: M,
    inner: PoolInner<M>,
}

impl<M: Manager> ManagedPool<M> {
    fn new(builder: Builder, manager: M) -> Self {
        let inner = PoolInner::from_builder(&builder);

        Self {
            builder,
            manager,
            inner,
        }
    }

    #[inline]
    async fn _get<'a, R>(&'a self, shared_pool: &'a SharedManagedPool<M>) -> Result<R, M::Error>
    where
        R: PoolRefBehavior<'a, M> + Unpin,
    {
        let fut = self.inner.get_inner::<R>(shared_pool);
        let timeout = self.wait_timeout();

        let mut pool_ref = self.manager.timeout(fut, timeout).await??;

        if self.builder.always_check {
            let mut retry = 0u8;
            loop {
                let result = self.valid_check(&mut pool_ref).await;
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

                let fut = self.inner.get_inner::<R>(shared_pool);

                pool_ref = self.manager.timeout(fut, timeout).await??;
            }
        };

        Ok(pool_ref)
    }

    #[inline]
    fn push_back(&self, conn: Conn<M>) {
        self.inner.push_back(conn);
    }

    #[inline]
    fn lifetime_check(&self, conn: &Conn<M>) -> bool {
        match self.builder.max_lifetime {
            Some(dur) => conn.birth.elapsed() > dur,
            None => false,
        }
    }

    async fn valid_check(
        &self,
        conn: &mut M::Connection,
    ) -> Result<Result<(), M::Error>, M::Error> {
        let fut = self.manager.is_valid(conn);
        let timeout = self.connection_timeout();

        let res = self.manager.timeout(fut, timeout).await?;

        Ok(res)
    }

    pub(crate) async fn add(&self) -> Result<Conn<M>, M::Error> {
        let fut = self.manager.connect();
        let timeout = self.connection_timeout();

        self.manager.timeout(fut, timeout).await?.map(Conn::new)
    }

    async fn add_multi(&self, count: usize) -> Result<(), M::Error> {
        for _ in 0..count {
            let conn = self.add().await?;
            self.inner.push_back(conn);
            self.inner.inc_active();
        }

        Ok(())
    }

    // a hack to return Manager::TimeoutError immediately.
    // pub(crate) async fn timeout_now(&self) -> Result<(), M::Error> {
    //     let fut = async {};
    //     let timeout = Duration::from_nanos(0);
    //     let _ = self.manager.timeout(fut, timeout).await?;
    //     Ok(())
    // }

    /// expose `Builder` to public
    pub fn get_builder(&self) -> &Builder {
        &self.builder
    }

    pub(crate) fn dec_active(&self) -> bool {
        self.inner.dec_active()
    }

    pub(crate) fn inc_active(&self) {
        self.inner.inc_active();
    }

    fn connection_timeout(&self) -> Duration {
        self.builder.connection_timeout
    }

    fn wait_timeout(&self) -> Duration {
        self.builder.wait_timeout
    }
}

pub type SharedManagedPool<M> = WrapPointer<ManagedPool<M>>;

pub type WeakSharedManagedPool<M> = Weak<ManagedPool<M>>;

pub struct Conn<M: Manager> {
    conn: M::Connection,
    birth: Instant,
}

impl<M: Manager> Conn<M> {
    fn new(conn: M::Connection) -> Self {
        let now = Instant::now();
        Self { conn, birth: now }
    }
}
