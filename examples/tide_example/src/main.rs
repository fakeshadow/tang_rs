// an async_std only stack with tide and async-postgres
// try to isolate this example from the workplace if it for some reason fail to compile

#[macro_use]
extern crate serde_derive;

use std::future::Future;
use std::time::Duration;

use async_std::{
    future::{timeout, TimeoutError},
    task,
};
use futures_util::TryStreamExt;
use tang_rs::{Builder, Manager, ManagerFuture, Pool};
use tide::{Error, Request, Response, StatusCode};
use tokio_postgres::{
    types::{ToSql, Type},
    Client, Row,
};

struct AsyncPostgresManager;

#[derive(Debug)]
struct AsyncPostgresError;

impl Manager for AsyncPostgresManager {
    type Connection = Client;
    type Error = AsyncPostgresError;
    type TimeoutError = TimeoutError;

    fn connect(&self) -> ManagerFuture<'_, Result<Self::Connection, Self::Error>> {
        Box::pin(async move {
            let url = "postgres://postgres:123@localhost/test";
            let (c, conn) = async_postgres::connect(url.parse()?).await?;

            task::spawn(conn);

            Ok(c)
        })
    }

    fn is_valid<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
    ) -> ManagerFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            let _ = conn.simple_query("").await?;
            Ok(())
        })
    }

    fn is_closed(&self, conn: &mut Self::Connection) -> bool {
        conn.is_closed()
    }

    fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let _ = task::spawn(fut);
    }

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
}

impl From<tokio_postgres::Error> for AsyncPostgresError {
    fn from(_: tokio_postgres::Error) -> Self {
        AsyncPostgresError
    }
}

impl From<std::io::Error> for AsyncPostgresError {
    fn from(_: std::io::Error) -> Self {
        AsyncPostgresError
    }
}

impl From<TimeoutError> for AsyncPostgresError {
    fn from(_: TimeoutError) -> Self {
        AsyncPostgresError
    }
}

impl From<AsyncPostgresError> for tide::Error {
    fn from(_: AsyncPostgresError) -> Self {
        Error::from_str(StatusCode::InternalServerError, "lol")
    }
}

#[async_std::main]
async fn main() -> async_std::io::Result<()> {
    let pool = Builder::new()
        .always_check(false)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(1)
        .max_size(24)
        .build(AsyncPostgresManager)
        .await
        .expect("can't make postgres pool");

    let mut app = tide::with_state(pool);
    app.at("/").get(index);

    app.listen("localhost:8080").await
}

// dummy data
const IDS: &[u32] = &[
    1, 11, 9, 20, 3, 5, 2, 6, 19, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 4,
];

async fn index(req: Request<Pool<AsyncPostgresManager>>) -> Result<Response, Error> {
    let pool_ref = req.state().get().await?;

    let client = &*pool_ref;

    let st = client
        .prepare_typed("SELECT * FROM topics WHERE id=ANY($1)", &[Type::OID_ARRAY])
        .await?;

    let (t, _u) = client
        .query_raw(&st, [&IDS as &(dyn ToSql + Sync)].iter().map(|s| *s as _))
        .await?
        .try_fold(
            (Vec::with_capacity(20), Vec::with_capacity(20)),
            |(mut t, mut u), r: Row| {
                let uid: u32 = r.get(1);
                if !u.contains(&uid) {
                    u.push(uid);
                }
                t.push(Topic {
                    id: r.get(0),
                    user_id: r.get(1),
                    category_id: r.get(2),
                    title: r.get(3),
                    body: r.get(4),
                    thumbnail: r.get(5),
                    is_locked: r.get(8),
                    is_visible: r.get(9),
                });

                futures_util::future::ok((t, u))
            },
        )
        .await?;

    drop(pool_ref);

    Ok(Response::new(StatusCode::Ok).body_json(&t)?)
}

#[derive(Serialize, Deserialize, Debug)]
struct Topic {
    pub id: u32,
    pub user_id: u32,
    pub category_id: u32,
    pub title: String,
    pub body: String,
    pub thumbnail: String,
    pub is_locked: bool,
    pub is_visible: bool,
}
