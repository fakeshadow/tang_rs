//! # An asynchronous connection pool.
//! some code come from
//! [bb8](https://docs.rs/bb8/0.3.1/bb8/)
//! [L3-37](https://github.com/OneSignal/L3-37/)
//!
//! ## feature
//!
//! * `default` - multi thread pool where `Send` bound is needed for all futures.
//! * `no-send` - single thread pool where `!Send` futures are accepted.
//!
//! # Known Limitation:
//! can't be used in nested runtimes.
//!
//! # Example:
//! ```
//! // This example shows how to implement the pool on async_std runtime.
//! // Most of the xxx-tang crates are implemented with tokio runtime so they can be seen as examples on that matter.
//!
//! use std::fmt::{Debug, Formatter, Result as FmtResult};
//! use std::future::Future;
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! use std::time::{Duration, Instant};
//!
//! use async_std::task;
//! use smol::Timer;
//! use tang_rs::{Builder, Manager, ManagerFuture, ManagerTimeout};
//!
//! // our test pool would just generate usize from 0 as connections.
//! struct TestPoolManager(AtomicUsize);
//!
//! impl TestPoolManager {
//!     fn new() -> Self {
//!         TestPoolManager(AtomicUsize::new(0))
//!     }
//! }
//!
//! // dummy error type
//! struct TestPoolError;
//!
//! impl Debug for TestPoolError {
//!     fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
//!         f.debug_struct("TestPoolError")
//!             .field("source", &"Unknown")
//!             .finish()
//!     }
//! }
//!
//! // convert instant as timeout error to our pool error.
//! impl From<Instant> for TestPoolError {
//!     fn from(_: Instant) -> Self {
//!         TestPoolError
//!     }
//! }
//!
//! impl Manager for TestPoolManager {
//!     type Connection = usize;
//!     type Error = TestPoolError;
//!     type Timeout = Timer;
//!     type TimeoutError = Instant;
//!
//!     fn connect(&self) -> ManagerFuture<'_, Result<Self::Connection, Self::Error>> {
//!         // how we generate new connections and put them into pool.
//!         Box::pin(async move { Ok(self.0.fetch_add(1, Ordering::SeqCst)) })
//!     }
//!
//!     fn is_valid<'a>(
//!         &self,
//!         _conn: &'a mut Self::Connection,
//!     ) -> ManagerFuture<'a, Result<(), Self::Error>> {
//!         Box::pin(async {
//!             // when the connection is pulled from the pool we can check if it's valid.
//!             Ok(())
//!         })
//!     }
//!
//!     fn is_closed(&self, _conn: &mut Self::Connection) -> bool {
//!         // return true if you check the connection and want it to be dropped from the pool because it's closed.
//!         false
//!     }
//!
//!     fn spawn<Fut>(&self, fut: Fut)
//!     where
//!         Fut: Future<Output = ()> + Send + 'static,
//!     {
//!         // some pool inner functions would want to spawn on your executor.
//!         // you can use the handler to further manage them if you want.
//!         // normally we just spawn the task and forget about it.
//!         let _handler = task::spawn(fut);
//!     }
//!
//!     // Boilerplate implement for runtime specific timeout future.
//!     fn timeout<Fut: Future>(&self,fut: Fut, dur: Duration) -> ManagerTimeout<Fut, Self::Timeout> {
//!         ManagerTimeout::new(fut, Timer::new(dur))
//!     }
//! }
//!
//! #[async_std::main]
//! async fn main() {
//!     let mgr = TestPoolManager::new();
//!
//!     let builder = Builder::new()
//!         .always_check(false)
//!         .idle_timeout(None)
//!         .max_lifetime(None)
//!         .min_idle(24)
//!         .max_size(24)
//!         .build(mgr);
//!
//!     let pool = builder.await.expect("fail to build pool");
//!
//!     // spawn 24 futures and pull connections from pool at the same time.
//!     let (tx, rx) = async_std::sync::channel(100);
//!     for _i in 0..24 {
//!         let pool = pool.clone();
//!         let tx = tx.clone();
//!         task::spawn(async move {
//!             let mut pool_ref = pool.get().await.expect("fail to get PoolRef");
//!             let conn_ref = &*pool_ref;
//!             println!("we have the reference of a connection : {:?}", conn_ref);
//!
//!             // we can also get a mut reference from pool_ref
//!             let conn_ref = &mut *pool_ref;
//!
//!             let _ = tx.send(*conn_ref);
//!         }).await;
//!     }
//! }
//!```

pub use builder::Builder;
pub use manager::{Manager, ManagerFuture};
pub use pool::{Pool, PoolRef, PoolRefOwned, SharedManagedPool};
pub use util::timeout::ManagerTimeout;

mod builder;
mod manager;
mod pool;
mod pool_inner;
mod util;

#[cfg(all(feature = "default", feature = "no-send"))]
compile_error!("only one of 'default' or 'no-send' features can be enabled");
