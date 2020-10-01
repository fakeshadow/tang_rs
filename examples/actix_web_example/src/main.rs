#[macro_use]
extern crate serde_derive;

use std::fmt::Debug;

use actix_web::{error::ErrorInternalServerError, web, App, Error, HttpResponse, HttpServer};
use futures_util::TryStreamExt;
use once_cell::sync::Lazy;
use redis_tang::RedisManager;
use tokio_postgres::{
    types::{ToSql, Type},
    NoTls, Row,
};
use tokio_postgres_tang::{Builder, Pool, PostgresManager};

// use once cell for a static tokio-postgres pool. so we don't have to pass the pool to App::data
static POOL: Lazy<Pool<PostgresManager<NoTls>>> = Lazy::new(|| {
    let db_url = "postgres://postgres:123@localhost/test2";

    let mgr = PostgresManager::new_from_stringlike(db_url, NoTls)
        .expect("can't make postgres manager")
        .prepare_statement(
            "get_topics",
            "SELECT * FROM topics WHERE id=ANY($1)",
            &[Type::OID_ARRAY],
        )
        .prepare_statement("get_users", "SELECT * FROM posts WHERE id=ANY($1)", &[]);

    Builder::new()
        .always_check(false)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(0)
        .max_size(24)
        .build_uninitialized(mgr)
});

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // initialize tokio-postgres pool.
    POOL.init()
        .await
        .expect("Failed to initialize tokio-postgres pool");

    let mgr = RedisManager::new("redis://127.0.0.1");
    let pool_redis = Builder::new()
        .always_check(false)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(0)
        .max_size(24)
        .build(mgr)
        .await
        .expect("Failed to initialize redis pool");

    HttpServer::new(move || {
        App::new()
            .data(pool_redis.clone())
            .service(web::resource("/test").route(web::get().to(test)))
            .service(web::resource("/test/redis").route(web::get().to(test_redis)))
    })
    .bind("0.0.0.0:8000")?
    .run()
    .await
}

async fn test() -> Result<HttpResponse, Error> {
    let ids = vec![
        1u32, 11, 9, 20, 3, 5, 2, 6, 19, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 4,
    ];

    let pool_ref = POOL
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

async fn test_redis(pool: web::Data<Pool<RedisManager>>) -> Result<HttpResponse, Error> {
    let mut client = pool
        .get()
        .await
        .map_err(|_| ErrorInternalServerError("lol"))?
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
