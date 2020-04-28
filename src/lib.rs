//! # A connection pool running one tokio runtime.
//! smoe code come from
//! [bb8](https://docs.rs/bb8/0.3.1/bb8/)
//! [L3-37](https://github.com/OneSignal/L3-37/)
//!
//! # Known Limitation:
//! No tests.
//! can't be used in nested runtimes.

pub use builder::Builder;
pub use manager::{Manager, ManagerFuture};

use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use crate::pool_inner::{PoolLock, State};

mod builder;
mod manager;
mod pool_inner;
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

pub struct ManagedPool<M: Manager + Send> {
    builder: Builder,
    manager: M,
    pool_lock: PoolLock<M>,
}

impl<M: Manager + Send> ManagedPool<M> {
    fn drop_conn(&self) -> Option<u8> {
        //  We might need to spin up more connections to maintain the idle limit, e.g.
        //  if we hit connection lifetime limits
        self.pool_lock.decr_spawned(|total_now| {
            if total_now < self.builder.min_idle {
                Some(self.builder.min_idle - total_now)
            } else {
                None
            }
        })
    }

    // use `Builder`'s connection_timeout setting to cancel the `connect` method and return error.
    async fn add_idle_conn(&self) -> Result<(), M::Error> {
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

    async fn check_conn(&self, conn: &mut Conn<M>) -> Result<Result<(), M::Error>, M::Error> {
        let fut = self.manager.is_valid(&mut conn.conn);

        let timeout = self.builder.connection_timeout;

        self.manager.timeout(fut, timeout).await?.map(Ok)
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

    pub async fn reap_idle_conn(&self) -> Result<(), M::Error> {
        let now = Instant::now();

        println!(
            "reaping connection. pool state now is: {:#?}",
            self.pool_lock.state()
        );

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

    // ToDo: we should figure a way to handle failed spawn.
    fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.manager.spawn(fut);
    }

    pub fn get_builder(&self) -> &Builder
    {
        &self.builder
    }
}

pub type SharedManagedPool<M> = Arc<ManagedPool<M>>;

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
    fn new(builder: Builder, manager: M) -> Self {
        let size = builder.max_size as usize;

        Pool(Arc::new(ManagedPool {
            builder,
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

        shared_pool
            .replenish_idle_conn(shared_pool.builder.min_idle)
            .await?;

        shared_pool.manager.on_start(shared_pool);

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

            let fut = shared_pool.pool_lock.lock(shared_pool);
            let timeout = shared_pool.builder.wait_timeout;

            let mut conn = shared_pool.manager.timeout(fut, timeout).await?.into();

            if shared_pool.builder.always_check {
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
    pool: &'a Arc<ManagedPool<M>>,
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
    pub fn get_manager(&self) -> &M {
        &self.pool.manager
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
// ToDo: Will get unsolvable pendings if the spawn is panic;
fn spawn_drop<M: Manager + Send>(shared_pool: &Arc<ManagedPool<M>>) {
    if let Some(pending) = shared_pool.drop_conn() {
        let shared_clone = shared_pool.clone();
        shared_pool.spawn(async move {
            let _ = shared_clone.replenish_idle_conn(pending).await;
        });
    }
}
