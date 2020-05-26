// This example shows how to implement the pool on async_std runtime.
// Most of the xxx-tang are implemented with tokio runtime so they can be seen as examples on that matter.

use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_std::{
    future::{timeout, TimeoutError},
    prelude::StreamExt,
    stream::{interval, Interval},
    task,
};
use tang_rs::{
    Builder, GarbageCollect, Manager, ManagerFuture, ManagerInterval, ScheduleReaping,
    SharedManagedPool,
};

// our test pool would just generate usize from 0 as connections.
struct TestPoolManager(AtomicUsize);

impl TestPoolManager {
    fn new() -> Self {
        TestPoolManager(AtomicUsize::new(0))
    }
}

// dummy error type
struct TestPoolError;

impl Debug for TestPoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("TestPoolError")
            .field("source", &"Unknown")
            .finish()
    }
}

// you can use the same Error type for Manager::Error and Manager::TimeoutError. and ignore the error convert.
impl From<TimeoutError> for TestPoolError {
    fn from(_e: TimeoutError) -> Self {
        TestPoolError
    }
}

// We can impl some default pool behavior as trait to our TestPool like garbage collect and schedule recycle connections
impl GarbageCollect for TestPoolManager {}

impl ScheduleReaping for TestPoolManager {}

// If we impl certain behaviors we have to impl this boilerplate too.
// because different runtime use different interval :(
impl ManagerInterval for TestPoolManager {
    // the interval type runtime returns when constructed
    type Interval = Interval;
    // the interval tick return type
    type Tick = Option<()>;

    fn interval(dur: Duration) -> Self::Interval {
        interval(dur)
    }

    fn tick(tick: &mut Self::Interval) -> ManagerFuture<'_, Self::Tick> {
        Box::pin(tick.next())
    }
}

impl Manager for TestPoolManager {
    type Connection = usize;
    type Error = TestPoolError;
    type TimeoutError = TimeoutError;

    fn connect(&self) -> ManagerFuture<'_, Result<Self::Connection, Self::Error>> {
        // how we generate new connections and put them into pool.
        Box::pin(async move { Ok(self.0.fetch_add(1, Ordering::SeqCst)) })
    }

    fn is_valid<'a>(
        &'a self,
        _conn: &'a mut Self::Connection,
    ) -> ManagerFuture<'a, Result<(), Self::Error>> {
        Box::pin(async {
            // when the connection is pulled from the pool we can check if it's valid.
            Ok(())
        })
    }

    fn is_closed(&self, _conn: &mut Self::Connection) -> bool {
        // return true if you check the connection and want it to be dropped from the pool because it's closed.
        false
    }

    fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let _handler = task::spawn(fut);
    }

    // override timeout method if you want to handle the timeout error.
    // The duration is determined by `Builder::wait_timeout` or `Builder::connection_timeout`
    fn timeout<'fu, Fut>(
        &self,
        fut: Fut,
        dur: Duration,
    ) -> ManagerFuture<'fu, Result<Fut::Output, Self::TimeoutError>>
    where
        Fut: Future + Send + 'fu,
    {
        Box::pin(timeout(dur, fut))
    }

    // We have to attach the behaviors(GarbageCollect and ScheduleReaping traits) to this hook so that they can start with pool.
    fn on_start(&self, shared_pool: &SharedManagedPool<Self>) {
        self.schedule_reaping(shared_pool);
        self.garbage_collect(shared_pool);
    }
}

#[async_std::main]
async fn main() {
    let mgr = TestPoolManager::new();

    let pool = Builder::new()
        .always_check(false)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(24)
        .max_size(24)
        .build(mgr)
        .await
        .expect("fail to build pool");

    let count = 1_000_000;
    let (tx, rx) = async_std::sync::channel(count);

    // spawn 1_000_000 futures and pull connections from pool at the same time.
    let now = std::time::Instant::now();
    for _i in 0..count {
        let pool = pool.clone();
        let tx = tx.clone();
        task::spawn(async move {
            let mut pool_ref = pool.get().await.expect("fail to get PoolRef");

            // we can get &Mananger::Connection from pool_ref
            let _conn_ref = &*pool_ref;

            // we can also get a mut reference from pool_ref
            let conn_ref = &mut *pool_ref;

            let _ = tx.send(*conn_ref);

            // when pool_ref goes out of scope the connection is put back to pool.
        });
    }
    drop(tx);

    while let Ok(_conn) = rx.recv().await {
        // We just wait until all connections are pulled out once
    }
    let duration = std::time::Instant::now().duration_since(now);
    println!("Total time is : {:#?}", duration);
}
