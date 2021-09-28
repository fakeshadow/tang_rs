use core::fmt;
use core::future::Future;
use core::ops::{Deref, DerefMut};
use std::time::Instant;
#[cfg(feature = "no-send")]
use {
    core::cell::Cell,
    std::{
        rc::{Rc as WrapPointer, Weak},
        thread::{self, ThreadId},
    },
};
#[cfg(not(feature = "no-send"))]
use {
    core::sync::atomic::{AtomicBool, Ordering},
    std::sync::{Arc as WrapPointer, Weak},
};

use crate::builder::Builder;
use crate::connection::{Conn, IdleConn};
use crate::manager::Manager;
use crate::pool_inner::{PoolLock, State};

pub struct ManagedPool<M: Manager> {
    builder: Builder,
    manager: M,
    #[cfg(not(feature = "no-send"))]
    running: AtomicBool,
    #[cfg(feature = "no-send")]
    running: Cell<bool>,
    pool_lock: PoolLock<M>,
}

impl<M: Manager> ManagedPool<M> {
    fn new(builder: Builder, manager: M) -> Self {
        let pool_lock = PoolLock::from_builder(&builder);

        Self {
            builder,
            manager,
            #[cfg(not(feature = "no-send"))]
            running: AtomicBool::new(true),
            #[cfg(feature = "no-send")]
            running: Cell::new(true),
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

    fn drop_conn(&self, marker: usize, should_spawn_new: bool) -> Option<usize> {
        //  We might need to spin up more connections to maintain the idle limit.
        //  e.g. if we hit connection lifetime limits
        self.pool_lock.dec_spawned(marker, should_spawn_new)
    }

    pub(crate) async fn add_idle_conn(&self, marker: usize) -> Result<(), M::Error> {
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

        self.pool_lock.put_new(IdleConn::new(conn, marker));

        Ok(())
    }

    async fn check_conn(&self, conn: &mut M::Connection) -> Result<Result<(), M::Error>, M::Error> {
        let fut = self.manager.is_valid(conn);

        let timeout = self.builder.connection_timeout;

        let res = self.manager.timeout(fut, timeout).await?;

        Ok(res)
    }

    async fn replenish_idle_conn(
        &self,
        pending_count: usize,
        marker: usize,
    ) -> Result<(), M::Error> {
        for i in 0..pending_count {
            self.add_idle_conn(marker).await.map_err(|e| {
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

    fn if_running(&self, running: bool) {
        #[cfg(not(feature = "no-send"))]
        self.running.store(running, Ordering::Release);
        #[cfg(feature = "no-send")]
        self.running.set(running);
    }

    pub(crate) fn is_running(&self) -> bool {
        #[cfg(not(feature = "no-send"))]
        {
            self.running.load(Ordering::Acquire)
        }
        #[cfg(feature = "no-send")]
        {
            self.running.get()
        }
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

    pub async fn reap_idle_conn(&self) -> Result<(), M::Error> {
        let now = Instant::now();

        let pending_new = self.pool_lock.try_drop_conn(|idle_conn| {
            let mut should_drop = false;
            if let Some(timeout) = self.builder.idle_timeout {
                should_drop |= now >= idle_conn.idle_start() + timeout;
            }
            if let Some(lifetime) = self.builder.max_lifetime {
                should_drop |= now >= idle_conn.birth() + lifetime;
            }
            should_drop
        });

        match pending_new {
            Some((pending_new, marker)) => self.replenish_idle_conn(pending_new, marker).await,
            None => Ok(()),
        }
    }

    pub fn garbage_collect(&self) {
        self.pool_lock
            .drop_pending(|pending| pending.should_remove(self.builder.connection_timeout));
    }

    /// expose `Builder` to public
    pub fn get_builder(&self) -> &Builder {
        &self.builder
    }
}

pub type SharedManagedPool<M> = WrapPointer<ManagedPool<M>>;

pub type WeakSharedManagedPool<M> = Weak<ManagedPool<M>>;

pub struct Pool<M: Manager> {
    pool: SharedManagedPool<M>,
    #[cfg(feature = "no-send")]
    id: ThreadId,
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

impl<M: Manager> Pool<M> {
    /// Return a `PoolRef` contains reference of `SharedManagedPool<Manager>` and an `Option<Manager::Connection>`.
    ///
    /// The `PoolRef` should be dropped asap when you finish the use of it. Hold it in scope would prevent the connection from pushed back to pool.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub async fn get(&self) -> Result<PoolRef<'_, M>, M::Error> {
        let shared_pool = &self.pool;

        shared_pool.get_conn(shared_pool).await
    }

    /// Return a `PoolRefOwned` contains a weak smart pointer of `SharedManagedPool<Manager>` and an `Option<Manager::Connection>`.
    ///
    /// You can move `PoolRefOwned` to async blocks and across await point. But the performance is considerably worse than `Pool::get`.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub async fn get_owned(&self) -> Result<PoolRefOwned<M>, M::Error> {
        let shared_pool = &self.pool;

        shared_pool.get_conn(shared_pool).await
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

    /// Pause the pool
    ///
    /// these functionalities will stop:
    /// - get connection. `Pool<Manager>::get()` would eventually be timed out
    ///     (If `Manager::timeout` is manually implemented with proper timeout function. *. Otherwise it will stuck forever in executor unless you cancel the future).
    /// - spawn of new connection.
    /// - default scheduled works (They would skip at least one iteration if the schedule time come across with the time period the pool is paused).
    /// - put back connection. (connection will be dropped instead.)
    pub fn pause(&self) {
        self.pool.if_running(false);
    }

    /// restart the pool.
    // ToDo: for now pool would lose accuracy for min_idle after restart. It would recover after certain amount of requests to pool.
    pub fn resume(&self) {
        self.pool.if_running(true);
    }

    /// check if the pool is running.
    pub fn running(&self) -> bool {
        self.pool.is_running()
    }

    /// Clear the pool.
    ///
    /// All pending connections will also be destroyed.
    ///
    /// Spawned count is not reset to 0 so new connections can't fill the pool until all outgoing `PoolRef` are dropped.
    ///
    /// All `PoolRef' and 'PoolRefOwned` outside of pool before the clear happen would be destroyed when trying to return it's connection to pool.
    pub fn clear(&self) {
        self.pool.pool_lock.clear()
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

        let marker = shared_pool.pool_lock.marker();

        shared_pool
            .replenish_idle_conn(shared_pool.builder.min_idle, marker)
            .await?;

        shared_pool.manager.on_start(shared_pool);

        Ok(())
    }

    /// Change the max size of pool. This operation could result in some reallocation of `PoolInner` and impact the performance.
    /// (`Pool<Manager>::clear()` will recalibrate the pool with a capacity of current max/min pool size)
    ///
    /// No actual check is used for new `max_size`. Be ware not to pass a size smaller than `min_idle`.
    pub fn set_max_size(&self, size: usize) {
        self.pool.pool_lock.set_max_size(size);
    }

    /// Change the min idle size of pool.
    /// (`Pool<Manager>::clear()` will recalibrate the pool with a capacity of current max/min pool size)
    ///
    /// No actual check is used for new `min_idle`. Be ware not to pass a size bigger than `max_size`.
    pub fn set_min_idle(&self, size: usize) {
        self.pool.pool_lock.set_min_idle(size);
    }

    /// expose `Manager` to public
    pub fn get_manager(&self) -> &M {
        &self.pool.manager
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

pub struct PoolRef<'a, M: Manager> {
    conn: Option<Conn<M::Connection>>,
    shared_pool: &'a SharedManagedPool<M>,
    // marker is only used to store the marker of Conn<M> if it's been taken from pool
    marker: Option<usize>,
}

pub struct PoolRefOwned<M: Manager> {
    conn: Option<Conn<M::Connection>>,
    shared_pool: WeakSharedManagedPool<M>,
    marker: Option<usize>,
}

impl<M: Manager> PoolRef<'_, M> {
    /// get a mut reference of connection.
    pub fn get_conn(&mut self) -> &mut M::Connection {
        &mut *self
    }

    /// take the the ownership of connection from pool and it won't be pushed back to pool anymore.
    pub fn take_conn(&mut self) -> Option<M::Connection> {
        self.conn.take().map(|c| {
            self.marker = Some(c.marker());
            c.into_conn()
        })
    }

    /// manually push a connection to pool. We treat this connection as a new born one.
    ///
    /// operation will fail if the pool is already in full capacity(no error will return and this connection will be dropped silently)
    pub fn push_conn(&mut self, conn: M::Connection) {
        // if the PoolRef have a marker then the conn must have been taken.
        // otherwise we give the marker of self.conn to the newly generated one.
        let marker = match self.marker {
            Some(marker) => marker,
            None => self.conn.as_ref().map(|c| c.marker()).unwrap(),
        };

        self.conn = Some(Conn::new(conn, marker));
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
        self.conn.take().map(|c| {
            self.marker = Some(c.marker());
            c.into_conn()
        })
    }

    /// manually push a connection to pool. We treat this connection as a new born one.
    ///
    /// operation will fail if the pool is already in full capacity(no error will return and this connection will be dropped silently)
    pub fn push_conn(&mut self, conn: M::Connection) {
        // if the PoolRef have a marker then the conn must have been taken.
        // otherwise we give the marker of self.conn to the newly generated one.
        let marker = match self.marker {
            Some(marker) => marker,
            None => self.conn.as_ref().map(|c| c.marker()).unwrap(),
        };

        self.conn = Some(Conn::new(conn, marker));
    }
}

pub(crate) trait PoolRefBehavior<'a, M: Manager>
where
    Self: DerefMut<Target = M::Connection> + Sized,
{
    fn from_idle(conn: IdleConn<M::Connection>, shared_pool: &'a SharedManagedPool<M>) -> Self;

    fn take_drop(self) {}
}

impl<'re, M: Manager> PoolRefBehavior<'re, M> for PoolRef<'re, M> {
    fn from_idle(conn: IdleConn<M::Connection>, shared_pool: &'re SharedManagedPool<M>) -> Self {
        Self {
            conn: Some(conn.into()),
            shared_pool,
            marker: None,
        }
    }

    fn take_drop(mut self) {
        let _ = self.take_conn();
    }
}

impl<M: Manager> PoolRefBehavior<'_, M> for PoolRefOwned<M> {
    fn from_idle(conn: IdleConn<M::Connection>, shared_pool: &SharedManagedPool<M>) -> Self {
        Self {
            conn: Some(conn.into()),
            shared_pool: WrapPointer::downgrade(shared_pool),
            marker: None,
        }
    }

    fn take_drop(mut self) {
        let _ = self.take_conn();
    }
}

impl<M: Manager> Deref for PoolRef<'_, M> {
    type Target = M::Connection;

    fn deref(&self) -> &Self::Target {
        self.conn
            .as_ref()
            .expect("Connection has already been taken")
            .conn_ref()
    }
}

impl<M: Manager> DerefMut for PoolRef<'_, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
            .as_mut()
            .expect("Connection has already been taken")
            .conn_ref_mut()
    }
}

impl<M: Manager> Deref for PoolRefOwned<M> {
    type Target = M::Connection;

    fn deref(&self) -> &Self::Target {
        self.conn
            .as_ref()
            .expect("Connection has already been taken")
            .conn_ref()
    }
}

impl<M: Manager> DerefMut for PoolRefOwned<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
            .as_mut()
            .expect("Connection has already been taken")
            .conn_ref_mut()
    }
}

impl<M: Manager> Drop for PoolRef<'_, M> {
    #[inline]
    fn drop(&mut self) {
        self.shared_pool.drop_pool_ref(&mut self.conn, self.marker);
    }
}

impl<M: Manager> Drop for PoolRefOwned<M> {
    #[inline]
    fn drop(&mut self) {
        if let Some(shared_pool) = self.shared_pool.upgrade() {
            shared_pool.drop_pool_ref(&mut self.conn, self.marker);
        }
    }
}

trait DropAndSpawn<M: Manager> {
    fn drop_pool_ref(&self, conn: &mut Option<Conn<M::Connection>>, marker: Option<usize>);

    fn spawn_drop(&self, marker: usize);
}

impl<M: Manager> DropAndSpawn<M> for SharedManagedPool<M> {
    #[inline]
    fn drop_pool_ref(&self, conn: &mut Option<Conn<M::Connection>>, marker: Option<usize>) {
        if !self.is_running() {
            // marker here doesn't matter as should_spawn_new would reject new connection generation
            self.drop_conn(0, false);
            return;
        }

        let mut conn = match conn.take() {
            Some(conn) => conn,
            None => {
                // if the connection is taken then self.marker must be Some(usize)
                self.spawn_drop(marker.unwrap());
                return;
            }
        };

        let is_closed = self.manager.is_closed(conn.conn_ref_mut());
        if is_closed {
            self.spawn_drop(conn.marker());
        } else {
            self.pool_lock.put_back(conn.into());
        };
    }

    // helper function to spawn drop connection and spawn new ones if needed.
    // Conn<M::Connection> should be dropped in place where spawn_drop() is used.
    // ToDo: Will get unsolvable pending if the spawn is panic;
    #[cold]
    fn spawn_drop(&self, marker: usize) {
        let opt = self.drop_conn(marker, true);

        if let Some(pending) = opt {
            let shared_clone = self.clone();
            self.spawn(async move {
                let _ = shared_clone.replenish_idle_conn(pending, marker).await;
            });
        }
    }
}
