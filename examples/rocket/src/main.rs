#![feature(proc_macro_hygiene)]

#[macro_use]
extern crate rocket;
#[macro_use]
extern crate serde_derive;

use std::convert::From;

use futures::TryStreamExt;
use rocket::{
    config::{Config, Environment},
    response::{content, Debug},
    State,
};
use tokio::runtime::Runtime;
use tokio_postgres::{types::Type, Row};

use tang_rs::{Builder, Pool, PostgresManager, PostgresPoolError, RedisManager, RedisPoolError};

// dummy data
const IDS: &[u32] = &[
    1, 11, 9, 20, 3, 5, 2, 6, 19, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 4,
];

// don't use tokio macro and async main as this will result in nested runtime.
fn main() {
    let runtime = Runtime::new().expect("Failed to create tokio runtime");

    let db_url = "postgres://postgres:123@localhost/test";

    // make prepared statements. pass Vec<tokio_postgres::types::Type> if you want typed statement. pass vec![] for no typed statement.
    // pass vec![] if you don't want any prepared statements.
    let statements = vec![
        (
            "SELECT * FROM topics WHERE id=ANY($1)",
            vec![Type::OID_ARRAY],
        ),
        ("SELECT * FROM users WHERE id=ANY($1)", vec![]),
    ];

    // setup manager
    let mgr = PostgresManager::new_from_stringlike(db_url, statements, tokio_postgres::NoTls)
        .unwrap_or_else(|_| panic!("can't make postgres manager"));

    // build postgres pool
    let pool = runtime
        .block_on(
            Builder::new()
                .always_check(false)
                .idle_timeout(Some(std::time::Duration::from_secs(10 * 60)))
                .max_lifetime(Some(std::time::Duration::from_secs(30 * 60)))
                .reaper_rate(std::time::Duration::from_secs(15))
                .min_idle(1)
                .max_size(24)
                .build(mgr),
        )
        .unwrap_or_else(|_| panic!("can't make pool"));

    // setup manager
    let mgr = RedisManager::new("redis://127.0.0.1");

    let pool_redis = runtime
        .block_on(
            Builder::new()
                .always_check(false)
                .idle_timeout(Some(std::time::Duration::from_secs(10 * 60)))
                .max_lifetime(Some(std::time::Duration::from_secs(30 * 60)))
                .reaper_rate(std::time::Duration::from_secs(15))
                .min_idle(1)
                .max_size(24)
                .build(mgr),
        )
        .unwrap_or_else(|_| panic!("can't make redis pool"));

    let cfg = Config::build(Environment::Production)
        .address("localhost")
        .port(8000)
        .workers(36)
        .keep_alive(10)
        .expect("Failed to build Rocket Config");

    // build server
    let server = rocket::custom(cfg)
        .mount("/test", routes![index, index2])
        .manage(pool)
        .manage(pool_redis)
        .spawn_on(&runtime);

    runtime.block_on(async move {
        let _ = server.await;
    });
}

#[get("/")]
async fn index(
    pool: State<'_, Pool<PostgresManager<tokio_postgres::NoTls>>>,
) -> Result<content::Json<String>, Debug<std::io::Error>> {
    // pool.get return the Conn and a reference of pool so that we can use the connection outside a closure.
    let mut pool_ref = pool.get().await.map_err(MyError::from)?;

    let (client, statements) = pool_ref.get_conn();

    let (t, u): (Vec<Topic>, Vec<u32>) = client
        .query(statements.get(0).unwrap(), &[&IDS])
        .try_fold(
            (Vec::with_capacity(20), Vec::with_capacity(20)),
            |(mut t, mut u), r| {
                u.push(r.get(1));
                t.push(r.into());
                futures::future::ok((t, u))
            },
        )
        .await
        .expect("Failed to query postgres");

    let _u = client
        .query(statements.get(1).unwrap(), &[&u])
        .try_collect::<Vec<Row>>()
        .await
        .expect("Failed to query postgres");

    drop(pool_ref); // drop the pool_ref when you finish use the pool. so that the connection can be put back to pool asap.

    Ok(content::Json(serde_json::to_string(&t).unwrap()))
}

#[get("/redis")]
async fn index2(pool: State<'_, Pool<RedisManager>>) -> Result<String, Debug<std::io::Error>> {
    let mut pool_ref = pool.get().await.map_err(MyError::from)?;
    let client = pool_ref.get_conn();

    // let's shadow name client var here. The connection will be pushed back to pool when pool_ref dropped.
    // the client var we shadowed is from redis query return and last till the function end.
    let (client, ()) = redis::cmd("PING")
        .query_async(client.clone())
        .await
        .expect("Failed to query redis");

    drop(pool_ref);

    Ok("done".into())
}

struct MyError;

impl From<PostgresPoolError> for MyError {
    fn from(e: PostgresPoolError) -> MyError {
        match e {
            PostgresPoolError::Inner(_e) => println!("inner is tokio_postgres::Error"),
            PostgresPoolError::TimeOut => (),
        };
        MyError
    }
}

impl From<RedisPoolError> for MyError {
    fn from(e: RedisPoolError) -> MyError {
        match e {
            RedisPoolError::Inner(_e) => println!("inner is redis::RedisError"),
            RedisPoolError::TimeOut => (),
        };
        MyError
    }
}

impl From<MyError> for Debug<std::io::Error> {
    fn from(_m: MyError) -> Debug<std::io::Error> {
        Debug(std::io::Error::new(std::io::ErrorKind::TimedOut, "oh no!"))
    }
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

impl From<Row> for Topic {
    fn from(r: Row) -> Topic {
        Topic {
            id: r.get(0),
            user_id: r.get(1),
            category_id: r.get(2),
            title: r.get(3),
            body: r.get(4),
            thumbnail: r.get(5),
            is_locked: r.get(8),
            is_visible: r.get(9),
        }
    }
}
