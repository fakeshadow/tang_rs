#[macro_use]
extern crate serde_derive;

use actix::prelude::Future as Future01;
use actix_web::{
    error::ErrorInternalServerError,
    web::{self, Data},
    App, Error, HttpResponse, HttpServer,
};
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use tokio_postgres::{types::Type, NoTls, Row};

use tang_rs::{Builder, Pool, PostgresManager, PostgresPoolError, RedisManager, RedisPoolError};

#[tokio::main]
async fn main() -> std::io::Result<()> {
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
    // only support NoTls for now
    let mgr = PostgresManager::new_from_stringlike(db_url, statements, tokio_postgres::NoTls)
        .unwrap_or_else(|_| panic!("can't make postgres manager"));

    /*
        limitation:

        actix-web still runs on tokio 0.1 under the hood. so spawning new connection won't work.
        so it best to set idle_timeout and max_lifetime both to None.
        min_idle equals to max_size.
        and the server has to be restart if all connections are gone broken
    */

    let pool = Builder::new()
        .always_check(false)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(24)
        .max_size(24)
        .build(mgr)
        .await
        .unwrap_or_else(|_| panic!("can't make pool"));

    let mgr = RedisManager::new("redis://127.0.0.1");

    let pool_redis = Builder::new()
        .always_check(false)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(12) // too many redis connection is not a good thing and could have negative impact on performance.
        .max_size(12) // so one connection for one worker is more than enough.
        .build(mgr)
        .await
        .unwrap_or_else(|_| panic!("can't make redis pool"));

    HttpServer::new(move || {
        App::new()
            .data(pool.clone())
            .data(pool_redis.clone())
            .service(web::resource("/test").route(web::get().to_async(test)))
            .service(web::resource("/test/redis").route(web::get().to_async(test_redis)))
    })
    .bind("localhost:8000")
    .unwrap()
    .run()
}

fn test(
    pool: Data<Pool<PostgresManager<NoTls>>>,
) -> impl Future01<Item = HttpResponse, Error = Error> {
    test_async(pool).boxed_local().compat()
}

async fn test_async(pool: Data<Pool<PostgresManager<NoTls>>>) -> Result<HttpResponse, Error> {
    let ids = vec![
        1u32, 11, 9, 20, 3, 5, 2, 6, 19, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 4,
    ];

    let mut pool_ref = pool
        .get::<PostgresPoolError>()
        .await
        .map_err(|_| ErrorInternalServerError("lol"))?;

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
        .map_err(|_| ErrorInternalServerError("lol"))?;

    let _u = client
        .query(statements.get(1).unwrap(), &[&u])
        .try_collect::<Vec<Row>>()
        .await
        .map_err(|_| ErrorInternalServerError("lol"))?;

    drop(pool_ref);

    Ok(HttpResponse::Ok().json(&t))
}

fn test_redis(pool: Data<Pool<RedisManager>>) -> impl Future01<Item = HttpResponse, Error = Error> {
    test_redisasync(pool).boxed_local().compat()
}

async fn test_redisasync(pool: Data<Pool<RedisManager>>) -> Result<HttpResponse, Error> {
    // you can also run code in closure like postgres pool. we skip that here.

    // Your Error type have to impl From<redis::RedisError> or you can use default error type tang_rs::RedisPoolError
    let mut pool_ref = pool
        .get::<RedisPoolError>()
        .await
        .map_err(|_| ErrorInternalServerError("lol"))?;

    let client = pool_ref.get_conn();

    // let's shadow name client var here. The connection will be pushed back to pool when pool_ref dropped.
    // the client var we shadowed is from redis query return and last till the function end.
    let (client, ()) = redis::cmd("PING")
        .query_async(client.clone())
        .await
        .map_err(|_| ErrorInternalServerError("lol"))?;

    drop(pool_ref);

    Ok(HttpResponse::Ok().finish())
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
    fn from(r: Row) -> Self {
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
