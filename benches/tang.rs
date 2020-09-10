#![feature(test)]

extern crate test;

use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use test::Bencher;

use async_std::task;
use smol::Timer;

use tang_rs::{Builder, Manager, ManagerFuture, ManagerTimeout};

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
        _conn: &'a mut Self::Connection,
    ) -> ManagerFuture<'a, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
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

    fn timeout<Fut: Future>(&self, fut: Fut, dur: Duration) -> ManagerTimeout<Fut, Self::Timeout> {
        ManagerTimeout::new(fut, Timer::after(dur))
    }
}

#[bench]
fn async_std(b: &mut Bencher) {
    b.iter(|| task::block_on(run(12, 1000)));
}

async fn run(task: usize, iter: usize) {
    let mgr = TestPoolManager(AtomicUsize::new(0));

    let pool = Builder::new()
        .always_check(false)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(24)
        .max_size(24)
        .build(mgr)
        .await
        .expect("fail to build pool");

    let mut tasks = Vec::new();

    for _ in 0..task {
        let p = pool.clone();
        tasks.push(task::spawn(async move {
            for _ in 0..iter {
                let _conn = p.get().await.unwrap();
            }
        }));
    }

    for t in tasks {
        t.await;
    }
}
