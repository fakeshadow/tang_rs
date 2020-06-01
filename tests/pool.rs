use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_std::{
    prelude::*,
    stream::{interval, Interval},
    task::{self, sleep},
};
use smol::Timer;
use tang_rs::{
    Builder, GarbageCollect, Manager, ManagerFuture, ManagerInterval, ManagerTimeout, Pool,
    PoolRef, ScheduleReaping, SharedManagedPool,
};

macro_rules! test_pool {
    ($valid_condition: expr, $broken_condition: expr) => {
        struct TestPoolManager(AtomicUsize);

        struct TestPoolError;

        impl From<Instant> for TestPoolError {
            fn from(_: Instant) -> Self {
                TestPoolError
            }
        }

        impl Debug for TestPoolError {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                f.debug_struct("TestPoolError")
                    .field("source", &"Unknown")
                    .finish()
            }
        }

        impl ManagerInterval for TestPoolManager {
            type Interval = Interval;
            type Tick = Option<()>;

            fn interval(dur: Duration) -> Self::Interval {
                interval(dur)
            }

            fn tick(tick: &mut Self::Interval) -> ManagerFuture<'_, Self::Tick> {
                Box::pin(tick.next())
            }
        }

        impl GarbageCollect for TestPoolManager {}

        impl ScheduleReaping for TestPoolManager {}

        impl Manager for TestPoolManager {
            type Connection = usize;
            type Error = TestPoolError;
            type Timeout = Timer;
            type TimeoutError = Instant;

            fn connect(&self) -> ManagerFuture<'_, Result<Self::Connection, Self::Error>> {
                Box::pin(async move { Ok(self.0.fetch_add(1, Ordering::SeqCst)) })
            }

            fn is_valid<'a>(
                &self,
                conn: &'a mut Self::Connection,
            ) -> ManagerFuture<'a, Result<(), Self::Error>> {
                Box::pin(async move {
                    if *conn % $valid_condition == 0 {
                        Ok(())
                    } else {
                        Err(TestPoolError)
                    }
                })
            }

            fn is_closed(&self, conn: &mut Self::Connection) -> bool {
                if *conn % $broken_condition == 0 {
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

            fn timeout<Fut: Future>(
                &self,
                fut: Fut,
                dur: Duration,
            ) -> ManagerTimeout<Fut, Self::Timeout> {
                ManagerTimeout::new(fut, Timer::after(dur))
            }

            fn on_start(&self, shared_pool: &SharedManagedPool<Self>) {
                self.schedule_reaping(shared_pool);
                self.garbage_collect(shared_pool);
            }
        }
    };
}

#[async_std::test]
async fn limit() {
    test_pool!(2, 4);

    let mgr = TestPoolManager(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(true)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(10)
        .max_size(24)
        .build(mgr)
        .await
        .expect("fail to build pool");

    pool_state(&pool, 10, 10, 0);

    let mut conns = Vec::new();

    for _i in 0..24 {
        let conn = pool.get().await.unwrap();
        conns.push(conn);
    }

    let state = pool.state();

    assert_eq!(24, conns.len());

    assert_eq!(0, state.pending_connections.len());
    assert_eq!(24, state.connections);
    assert_eq!(0, state.idle_connections);

    drop(conns);

    pool_state(&pool, 12, 12, 0);
}

#[async_std::test]
async fn valid_closed() {
    test_pool!(2, 4);

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

    pool_state(&pool, 4, 4, 0);
}

#[async_std::test]
async fn retry_limit() {
    test_pool!(5, 1);

    let mgr = TestPoolManager(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(true)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(4)
        .max_size(8)
        .build(mgr)
        .await
        .expect("fail to build pool");

    let mut errs = 0;

    let f = |errs: &mut i32, result: Result<PoolRef<'_, TestPoolManager>, TestPoolError>| {
        if result.is_ok() {
            let conn = *(result.unwrap());
            assert_eq!(true, conn == 0 || conn == 5);
        } else {
            *errs = *errs + 1;
        }
    };

    let mut interval = interval(Duration::from_secs(1));

    let conn0 = pool.get().await;
    interval.next().await;
    let conn1 = pool.get().await;
    interval.next().await;
    let conn2 = pool.get().await;
    interval.next().await;

    f(&mut errs, conn0);
    f(&mut errs, conn1);
    f(&mut errs, conn2);

    assert_eq!(true, errs == 1);

    interval.next().await;

    pool_state(&pool, 4, 4, 0);
}

#[async_std::test]
async fn idle_timeout() {
    test_pool!(2, 4);

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

    pool_state(&pool, 4, 4, 0);

    sleep(Duration::from_secs(6)).await;

    pool_state(&pool, 2, 2, 0);
}

#[async_std::test]
async fn max_lifetime() {
    test_pool!(2, 4);

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

    pool_state(&pool, 4, 4, 0);

    sleep(Duration::from_secs(6)).await;

    pool_state(&pool, 2, 2, 0);
}

#[async_std::test]
async fn pause() {
    test_pool!(1, 100);

    let mgr = TestPoolManager(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(false)
        .max_lifetime(Some(Duration::from_secs(1)))
        .reaper_rate(Duration::from_secs(1))
        .wait_timeout(Duration::from_secs(2))
        .min_idle(8)
        .max_size(16)
        .build(mgr)
        .await
        .expect("fail to build pool");

    pool.pause();

    let now = Instant::now();
    let res = pool.get().await;

    assert_eq!(true, res.is_err());
    assert_eq!(
        true,
        Instant::now().duration_since(now) > Duration::from_secs(2)
    );

    pool.resume();

    let mut conns = Vec::new();
    for _i in 0..8 {
        let conn = pool.get().await.unwrap();
        conns.push(conn);
    }

    pool.pause();

    drop(conns);

    pool_state(&pool, 0, 0, 0);

    pool.resume();

    let mut conns = Vec::new();
    for _i in 0..16 {
        let conn = pool.get().await.unwrap();
        conns.push(conn);
    }

    drop(conns);

    pool.pause();

    pool_state(&pool, 16, 16, 0);

    pool.resume();

    task::sleep(Duration::from_secs(3)).await;

    pool_state(&pool, 8, 8, 0);
}

#[async_std::test]
async fn set_max() {
    test_pool!(1, 100);

    let mgr = TestPoolManager(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(false)
        .min_idle(4)
        .max_size(16)
        .build(mgr)
        .await
        .expect("fail to build pool");

    let mut conns = Vec::new();
    for _i in 0..16 {
        let conn = pool.get().await.unwrap();
        conns.push(conn);
    }

    pool.set_max_size(7);

    drop(conns);

    pool_state(&pool, 7, 7, 0);
}

#[async_std::test]
async fn set_min() {
    test_pool!(1, 100);

    let mgr = TestPoolManager(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(false)
        .min_idle(4)
        .max_size(16)
        .reaper_rate(Duration::from_secs(1))
        .build(mgr)
        .await
        .expect("fail to build pool");

    pool.set_min_idle(7);

    sleep(Duration::from_secs(3)).await;

    pool_state(&pool, 7, 7, 0);
}

#[async_std::test]
async fn clear() {
    test_pool!(1, 100);

    let mgr = TestPoolManager(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(false)
        .min_idle(16)
        .max_size(16)
        .build(mgr)
        .await
        .expect("fail to build pool");

    let mut conns = Vec::new();
    for _i in 0..8 {
        let conn = pool.get().await.unwrap();
        conns.push(conn);
    }

    pool_state(&pool, 16, 8, 0);

    pool.clear();

    pool_state(&pool, 8, 0, 0);

    let mut pool_ref = conns.pop().unwrap();
    let _ = pool_ref.take_conn();

    drop(pool_ref);

    let mut pool_ref = conns.pop().unwrap();
    let _ = pool_ref.push_conn(80usize);

    drop(pool_ref);

    drop(conns);

    pool_state(&pool, 0, 0, 0);
}

fn pool_state<M: Manager>(pool: &Pool<M>, conn: usize, idle: usize, pending: usize) {
    let state = pool.state();

    assert_eq!(conn, state.connections);
    assert_eq!(idle, state.idle_connections);
    assert_eq!(pending, state.pending_connections.len());
}
