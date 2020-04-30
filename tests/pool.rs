use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_std::{prelude::*, stream::interval, task};
use tang_rs::{Builder, Manager, ManagerFuture, WeakSharedManagedPool};

struct TestPoolManager(AtomicUsize);

struct TestPoolError;

impl Debug for TestPoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("TestPoolError")
            .field("source", &"Unknown")
            .finish()
    }
}

impl Manager for TestPoolManager {
    type Connection = usize;
    type Error = TestPoolError;
    type TimeoutError = TestPoolError;

    fn connect(&self) -> ManagerFuture<'_, Result<Self::Connection, Self::Error>> {
        Box::pin(async move { Ok(self.0.fetch_add(1, Ordering::SeqCst)) })
    }

    fn is_valid<'a>(
        &self,
        conn: &'a mut Self::Connection,
    ) -> ManagerFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            if *conn % 2 == 0 {
                Ok(())
            } else {
                Err(TestPoolError)
            }
        })
    }

    fn is_closed(&self, conn: &mut Self::Connection) -> bool {
        if *conn % 4 == 0 {
            true
        } else {
            false
        }
    }

    fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        task::spawn(fut);
    }

    fn schedule_inner(shared_pool: WeakSharedManagedPool<Self>) -> ManagerFuture<'static, ()> {
        let rate = shared_pool
            .upgrade()
            .expect("Pool is gone before we start schedule work")
            .get_builder()
            .get_reaper_rate();
        let mut interval = interval(rate);
        Box::pin(async move {
            loop {
                let _i = interval.next().await;
                match shared_pool.upgrade() {
                    Some(shared_pool) => {
                        let _ = shared_pool.reap_idle_conn().await;
                    }
                    None => break,
                }
            }
        })
    }

    fn garbage_collect_inner(
        shared_pool: WeakSharedManagedPool<Self>,
    ) -> ManagerFuture<'static, ()> {
        let rate = shared_pool
            .upgrade()
            .expect("Pool is gone before we start garbage collection")
            .get_builder()
            .get_reaper_rate();
        let mut interval = interval(rate * 6);
        Box::pin(async move {
            loop {
                let _i = interval.next().await;
                match shared_pool.upgrade() {
                    Some(shared_pool) => shared_pool.garbage_collect(),
                    None => break,
                }
            }
        })
    }
}

#[async_std::test]
async fn test_init_limit() {
    let mgr = TestPoolManager(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(false)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(10)
        .max_size(24)
        .build(mgr)
        .await
        .expect("fail to build pool");

    let state = pool.state();

    assert_eq!(0, state.pending_connections.len());
    assert_eq!(10, state.connections);
    assert_eq!(10, state.idle_connections);
}

#[async_std::test]
async fn test_valid_closed() {
    let mgr = TestPoolManager(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(true)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(4)
        .max_size(4)
        .build(mgr)
        .await
        .expect("fail to build pool");

    let mut interval = interval(Duration::from_secs(1));

    let conn0 = pool.get().await;
    interval.next().await;
    assert_eq!(true, conn0.is_ok());

    let conn1 = pool.get().await;
    interval.next().await;
    assert_eq!(true, conn1.is_ok());

    let conn2 = pool.get().await;
    interval.next().await;
    assert_eq!(true, conn2.is_ok());

    assert_eq!(true, *(conn0.unwrap()) == 0);
    assert_eq!(true, *(conn1.unwrap()) == 2);
    assert_eq!(true, *(conn2.unwrap()) == 4);

    for _i in 0..4 {
        let conn = pool.get().await;
        let num = *(conn.unwrap());
        assert_eq!(true, num == 0 || num == 2 || num == 6 || num == 8);
    }

    interval.next().await;

    let state = pool.state();
    assert_eq!(4, state.idle_connections);
    assert_eq!(4, state.connections);
    assert_eq!(0, state.pending_connections.len());
}

#[async_std::test]
async fn retry_limit() {
    struct TestPoolManagerRetry(AtomicUsize);

    impl Manager for TestPoolManagerRetry {
        type Connection = usize;
        type Error = TestPoolError;
        type TimeoutError = TestPoolError;

        fn connect(&self) -> ManagerFuture<'_, Result<Self::Connection, Self::Error>> {
            Box::pin(async move { Ok(self.0.fetch_add(1, Ordering::SeqCst)) })
        }

        fn is_valid<'a>(
            &self,
            conn: &'a mut Self::Connection,
        ) -> ManagerFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                if *conn % 5 == 0 {
                    Ok(())
                } else {
                    Err(TestPoolError)
                }
            })
        }

        fn is_closed(&self, _conn: &mut Self::Connection) -> bool {
            false
        }

        fn spawn<Fut>(&self, fut: Fut)
        where
            Fut: Future<Output = ()> + Send + 'static,
        {
            task::spawn(fut);
        }
    }

    let mgr = TestPoolManagerRetry(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(true)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(4)
        .max_size(8)
        .build(mgr)
        .await
        .expect("fail to build pool");

    let mut interval = interval(Duration::from_secs(1));

    let conn0 = pool.get().await;
    interval.next().await;
    assert_eq!(true, conn0.is_ok());

    let conn1 = pool.get().await;
    interval.next().await;
    assert_eq!(true, conn1.is_err());

    let conn2 = pool.get().await;
    interval.next().await;
    assert_eq!(true, conn2.is_ok());

    assert_eq!(true, *(conn0.unwrap()) == 0);
    assert_eq!(true, *(conn2.unwrap()) == 5);

    interval.next().await;

    let state = pool.state();
    assert_eq!(4, state.idle_connections);
    assert_eq!(4, state.connections);
    assert_eq!(0, state.pending_connections.len());
}

#[async_std::test]
async fn idle_test() {
    let mgr = TestPoolManager(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(true)
        .idle_timeout(Some(Duration::from_secs(3)))
        .reaper_rate(Duration::from_secs(3))
        .min_idle(2)
        .max_size(8)
        .build(mgr)
        .await
        .expect("fail to build pool");

    let mut conns = Vec::new();
    for _i in 0..8 {
        let conn = pool.get().await;
        conns.push(conn);
    }

    assert_eq!(8, conns.len());
    drop(conns);

    let state = pool.state();
    assert_eq!(4, state.idle_connections);
    assert_eq!(4, state.connections);
    assert_eq!(0, state.pending_connections.len());

    let mut interval = interval(Duration::from_secs(6));

    interval.next().await;

    let state = pool.state();
    assert_eq!(2, state.idle_connections);
    assert_eq!(2, state.connections);
    assert_eq!(0, state.pending_connections.len());
}

#[async_std::test]
async fn max_life_test() {
    let mgr = TestPoolManager(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(true)
        .max_lifetime(Some(Duration::from_secs(3)))
        .reaper_rate(Duration::from_secs(3))
        .min_idle(2)
        .max_size(8)
        .build(mgr)
        .await
        .expect("fail to build pool");

    let mut conns = Vec::new();
    for _i in 0..8 {
        let conn = pool.get().await;
        conns.push(conn);
    }

    assert_eq!(8, conns.len());
    drop(conns);

    let state = pool.state();
    assert_eq!(4, state.idle_connections);
    assert_eq!(4, state.connections);
    assert_eq!(0, state.pending_connections.len());

    let mut interval = async_std::stream::interval(Duration::from_secs(6));

    interval.next().await;

    let state = pool.state();
    assert_eq!(2, state.idle_connections);
    assert_eq!(2, state.connections);
    assert_eq!(0, state.pending_connections.len());
}
