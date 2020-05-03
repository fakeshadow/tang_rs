use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Instant;

use crate::builder::Builder;
use crate::manager::{Manager, ManagerFuture};
use crate::pool_inner::{PoolLock, State};

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

pub struct ManagedPool<M: Manager + Send> {
    builder: Builder,
    manager: M,
    pool_lock: PoolLock<M>,
}

impl<M: Manager + Send> ManagedPool<M> {
    // Recursive if the connection is broken when `Builder.always_check == true`. We exit with at most 3 retries.
    fn get_conn<'a>(
        &'a self,
        shared_pool: &'a SharedManagedPool<M>,
        mut retry: u8,
    ) -> ManagerFuture<Result<PoolRef<'a, M>, M::Error>> {
        Box::pin(async move {
            let fut = self.pool_lock.lock(shared_pool);
            let timeout = self.builder.wait_timeout;

            let mut pool_ref = self.manager.timeout(fut, timeout).await?;

            if self.builder.always_check {
                let result = self.check_conn(&mut *pool_ref).await;

                match result {
                    Ok(result) => {
                        if let Err(e) = result {
                            pool_ref.take_drop();
                            return if retry == 3 {
                                Err(e)
                            } else {
                                retry += 1;
                                self.get_conn(shared_pool, retry).await
                            };
                        }
                    }
                    Err(timeout) => {
                        pool_ref.take_drop();
                        return Err(timeout);
                    }
                }
            };
            Ok(pool_ref)
        })
    }

    fn drop_conn(&self) -> Option<u8> {
        //  We might need to spin up more connections to maintain the idle limit.
        //  e.g. if we hit connection lifetime limits
        self.pool_lock.decr_spawned(self.builder.min_idle)
    }

    pub(crate) async fn add_idle_conn(&self) -> Result<(), M::Error> {
        let fut = self.manager.connect();
        let timeout = self.builder.connection_timeout;

        let conn = self
            .manager
            .timeout(fut, timeout)
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

    async fn check_conn(&self, conn: &mut M::Connection) -> Result<Result<(), M::Error>, M::Error> {
        let fut = self.manager.is_valid(conn);

        let timeout = self.builder.connection_timeout;

        let res = self.manager.timeout(fut, timeout).await?;

        Ok(res)
    }

    async fn replenish_idle_conn(&self, pending_count: u8) -> Result<(), M::Error> {
        for i in 0..pending_count {
            self.add_idle_conn().await.map_err(|e| {
                // we return when an error occur so we should drop all the pending after the i.
                // (the pending of i is already dropped in add_connection method)
                let count = pending_count - i - 1;
                if count > 0 {
                    self.pool_lock.decr_pending(count);
                };
                e
            })?;
        }

        Ok(())
    }

    // ToDo: we should figure a way to handle failed spawn.
    pub(crate) fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.manager.spawn(fut);
    }

    pub async fn reap_idle_conn(&self) -> Result<(), M::Error> {
        let now = Instant::now();

        let pending_new = self
            .pool_lock
            .try_drop_conns(self.builder.min_idle, |conn| {
                let mut should_drop = false;
                if let Some(timeout) = self.builder.idle_timeout {
                    should_drop |= now >= conn.idle_start + timeout;
                }
                if let Some(lifetime) = self.builder.max_lifetime {
                    should_drop |= now >= conn.idle_start + lifetime;
                }
                should_drop
            });

        match pending_new {
            Some(pending_new) => self.replenish_idle_conn(pending_new).await,
            None => Ok(()),
        }
    }

    pub fn garbage_collect(&self) {
        self.pool_lock
            .drop_pendings(|pending| pending.should_remove(self.builder.connection_timeout));
    }

    /// expose `Builder` to public
    pub fn get_builder(&self) -> &Builder {
        &self.builder
    }
}

pub type SharedManagedPool<M> = Arc<ManagedPool<M>>;
pub type WeakSharedManagedPool<M> = Weak<ManagedPool<M>>;

pub struct Pool<M: Manager + Send>(SharedManagedPool<M>);

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
    pub(crate) fn new(builder: Builder, manager: M) -> Self {
        let size = builder.max_size as usize;

        Pool(Arc::new(ManagedPool {
            builder,
            manager,
            pool_lock: PoolLock::new(size),
        }))
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

        shared_pool
            .replenish_idle_conn(shared_pool.builder.min_idle)
            .await?;

        shared_pool.manager.on_start(shared_pool);

        Ok(())
    }

    /// Return a `PoolRef` contains reference of `SharedManagedPool<Manager>` and an `Option<Manager::Connection>`.
    ///
    /// The `PoolRef` should be dropped asap when you finish the use of it. Hold it in scope would prevent the connection from pushed back to pool.
    pub async fn get(&self) -> Result<PoolRef<'_, M>, M::Error> {
        let shared_pool = &self.0;
        shared_pool.get_conn(shared_pool, 0).await
    }

    /// Run the pool with a closure.
    pub async fn run<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut M::Connection) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + '_>>,
        E: From<M::Error>,
        T: Send + 'static,
    {
        let shared_pool = &self.0;
        let mut pool_ref = shared_pool.get_conn(shared_pool, 0).await?;
        f(&mut *pool_ref).await
    }

    /// expose `Manager` to public
    pub fn get_manager(&self) -> &M {
        &self.0.manager
    }

    /// Return a state of the pool inner. This call will block the thread and wait for lock.
    pub fn state(&self) -> State {
        self.0.pool_lock.state()
    }
}

pub struct PoolRef<'a, M: Manager + Send> {
    conn: Option<Conn<M>>,
    shared_pool: &'a Arc<ManagedPool<M>>,
}

impl<'a, M: Manager + Send> PoolRef<'a, M> {
    pub(crate) fn new(conn: IdleConn<M>, shared_pool: &'a Arc<ManagedPool<M>>) -> Self {
        PoolRef {
            conn: Some(conn.into()),
            shared_pool,
        }
    }
}

impl<M: Manager + Send> Deref for PoolRef<'_, M> {
    type Target = M::Connection;

    fn deref(&self) -> &Self::Target {
        &self
            .conn
            .as_ref()
            .expect("Connection has already been taken")
            .conn
    }
}

impl<M: Manager + Send> DerefMut for PoolRef<'_, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self
            .conn
            .as_mut()
            .expect("Connection has already been taken")
            .conn
    }
}

impl<M: Manager + Send> PoolRef<'_, M> {
    pub fn get_manager(&self) -> &M {
        &self.shared_pool.manager
    }

    /// get a mut reference of connection.
    pub fn get_conn(&mut self) -> &mut M::Connection {
        &mut *self
    }

    /// take the the ownership of connection from pool and it won't be pushed back to pool anymore.
    pub fn take_conn(&mut self) -> Option<M::Connection> {
        self.conn.take().map(|c| c.conn)
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

    fn take_drop(mut self) {
        self.conn.take();
    }
}

impl<M: Manager + Send> Drop for PoolRef<'_, M> {
    #[inline]
    fn drop(&mut self) {
        let mut conn = match self.conn.take() {
            Some(conn) => conn,
            None => {
                spawn_drop(self.shared_pool);
                return;
            }
        };

        let is_closed = self.shared_pool.manager.is_closed(&mut conn.conn);
        if is_closed {
            spawn_drop(self.shared_pool);
        } else {
            self.shared_pool.pool_lock.put_back(conn.into());
        };
    }
}

// helper function to spawn drop connection and spawn new ones if needed.
// Conn<M> should be dropped in place where spawn_drop() is used.
// ToDo: Will get unsolvable pending if the spawn is panic;
fn spawn_drop<M: Manager + Send>(shared_pool: &Arc<ManagedPool<M>>) {
    if let Some(pending) = shared_pool.drop_conn() {
        let shared_clone = shared_pool.clone();
        shared_pool.spawn(async move {
            let _ = shared_clone.replenish_idle_conn(pending).await;
        });
    }
}
