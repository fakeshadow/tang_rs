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
use tokio_postgres::{
    Row,
    types::Type,
};

use tang_rs::{Builder, Pool, PostgresConnectionManager};

// don't use tokio macro and async main as this will result in nested runtime.
fn main() {
    let runtime = Runtime::new().expect("Failed to create tokio runtime");

    // make manager.
    let db_url = "postgres://postgres:123@localhost/test";
    let mgr = PostgresConnectionManager::new_from_stringlike(db_url, tokio_postgres::NoTls)
        .unwrap_or_else(|_| panic!("can't make postgres manager"));

    // prepare statements
    let statements = vec![
        (
            "SELECT * FROM topics WHERE id=ANY($1)",
            vec![Type::OID_ARRAY],
        ),
        ("SELECT * FROM users WHERE id=ANY($1)", vec![]),
    ];

    // build pool
    let pool = runtime
        .block_on(
            Builder::new()
                .always_check(false)
                .idle_timeout(None)
                .max_lifetime(None)
                .min_idle(12)
                .max_size(24)
                .prepare_statements(statements)
                .build(mgr),
        )
        .unwrap_or_else(|_| panic!("can't make pool"));

    let cfg = Config::build(Environment::Production)
        .address("localhost")
        .port(8000)
        .workers(36)
        .keep_alive(10)
        .expect("Failed to build Rocket Config");

    // build server
    let server =
        rocket::custom(cfg)
            .mount("/test", routes![index])
            .manage(pool)
            .spawn_on(&runtime);

    runtime.block_on(async move {
        let _ = server.await;
    });
}

#[get("/")]
async fn index(pool: State<'_, Pool<tokio_postgres::NoTls>>) -> Result<content::Json<String>, Debug<std::io::Error>> {

    // run pool in closure
    let _t: Result<_, MyError> = pool
        .run(|mut conn|
            // pin the async function to make sure the &mut Conn outlives our closure.
            Box::pin(
                async move {
                    let (client, statements) = &mut conn;
                    let ids = vec![
                        1u32, 11, 9, 20, 3, 5, 2, 6, 19, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 4,
                    ];

                    // statement index is the same as the input vector when building the pool.
                    let statement = statements.get(0).unwrap();

                    let (t, u): (Vec<Topic>, Vec<u32>) = client
                        .query(statement, &[&ids])
                        .try_fold(
                            (Vec::with_capacity(20), Vec::with_capacity(20)),
                            |(mut t, mut u), r| {
                                u.push(r.get(1));
                                t.push(r.into());
                                futures::future::ok((t, u))
                            },
                        )
                        .await?;

                    let _u = client
                        .query(statements.get(1).unwrap(), &[&u])
                        .try_collect::<Vec<Row>>()
                        .await?;

                    // return custom Error as long as your error type impl From<tokio_postgres::Error> and From<tokio_timer::timeout::Elapsed>
                    Ok::<_, MyError>(t)
                    // or you could use default PollError<PoolError<tokio_postgres::Error>> here
//                Ok::<_, PoolError<tokio_postgres::Error>>
                }
            )
        )
        .await;

    let ids = vec![
        1u32, 11, 9, 20, 3, 5, 2, 6, 19, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 4,
    ];

    // pool.get return the Conn and a weak reference of pool so that we can use the connection outside a closure.

    let mut pool_ref = pool
        .get::<MyError>()
        .await?;

    let (client, statements) = pool_ref.get_conn();

    let (t, u): (Vec<Topic>, Vec<u32>) = client
        .query(statements.get(0).unwrap(), &[&ids])
        .try_fold(
            (Vec::with_capacity(20), Vec::with_capacity(20)),
            |(mut t, mut u), r| {
                u.push(r.get(1));
                t.push(r.into());
                futures::future::ok((t, u))
            },
        )
        .await
        .map_err(MyError::from)?;

    let _u = client
        .query(statements.get(1).unwrap(), &[&u])
        .try_collect::<Vec<Row>>()
        .await
        .map_err(MyError::from)?;

    drop(pool_ref); // drop the pool_ref when you finish use the pool. so that the connection can be put back to pool asap.

    Ok(content::Json(serde_json::to_string(&t).unwrap()))
}

struct MyError;

impl From<tokio_postgres::Error> for MyError {
    fn from(_e: tokio_postgres::Error) -> MyError {
        MyError
    }
}

impl From<tokio::timer::timeout::Elapsed> for MyError {
    fn from(_e: tokio::timer::timeout::Elapsed) -> MyError {
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