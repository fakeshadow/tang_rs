#[macro_use]
extern crate serde_derive;

use actix::prelude::Future as Future01;
use actix_web::{
    error::ErrorInternalServerError,
    web::{self, Data},
    App, Error, HttpResponse, HttpServer,
};
use futures_util::{FutureExt, TryFutureExt, TryStreamExt};
use tokio_postgres::{
    types::{ToSql, Type},
    NoTls, Row,
};

use tang_rs::{Builder, Pool, PostgresManager, RedisManager};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let db_url = "postgres://postgres:123@localhost/test";

    let mgr = PostgresManager::new_from_stringlike(db_url, NoTls)
        .unwrap_or_else(|_| panic!("can't make postgres manager"))
        .prepare_statement(
            "get_topics",
            "SELECT * FROM topics WHERE id=ANY($1)",
            &[Type::OID_ARRAY],
        )
        .prepare_statement("get_users", "SELECT * FROM posts WHERE id=ANY($1)", &[]);

    /*
        Limitation:

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
        .unwrap_or_else(|e| panic!("{:?}", e));

    let mgr = RedisManager::new("redis://127.0.0.1");

    let pool_redis = Builder::new()
        .always_check(false)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(24)
        .max_size(24)
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

type MyPool = Data<Pool<PostgresManager<NoTls>>>;

fn test(pool: MyPool) -> impl Future01<Item = HttpResponse, Error = Error> {
    test_async(pool).boxed_local().compat()
}

async fn test_async(pool: MyPool) -> Result<HttpResponse, Error> {
    let ids = vec![
        1u32, 11, 9, 20, 3, 5, 2, 6, 19, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 4,
    ];

    let pool_ref = pool
        .get()
        .await
        .map_err(|_| ErrorInternalServerError("lol"))?;

    let (client, _statements) = &*pool_ref;

    let st = client
        .prepare_typed("SELECT * FROM topics WHERE id=ANY($1)", &[Type::OID_ARRAY])
        .await
        .expect("Failed to prepare");

    let (t, _u) = client
        .query_raw(
            &st,
            [&ids as &(dyn ToSql + Sync)]
                .iter()
                .map(|s| *s as &dyn ToSql),
        )
        .await
        .map_err(|_| ErrorInternalServerError("lol"))?
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
        .await
        .map_err(|_| ErrorInternalServerError("lol"))?;

    drop(pool_ref);

    Ok(HttpResponse::Ok().json(&t))
}

type MyRedisPool = Data<Pool<RedisManager>>;

fn test_redis(pool: MyRedisPool) -> impl Future01<Item = HttpResponse, Error = Error> {
    test_redisasync(pool).boxed_local().compat()
}

async fn test_redisasync(pool: MyRedisPool) -> Result<HttpResponse, Error> {
    let mut client = pool
        .get()
        .await
        .map_err(|_| ErrorInternalServerError("lol"))?
        .get_conn()
        .clone();

    redis::cmd("PING")
        .query_async::<_, ()>(&mut client)
        .await
        .map_err(|_| ErrorInternalServerError("lol"))?;

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
