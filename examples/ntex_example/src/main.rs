// Use a single thread based connection pool.
// Every thread have one pool. Every pool has it's own logic and background tasks.

#[macro_use]
extern crate serde_derive;

use futures_util::TryStreamExt;
use ntex::web::{
    self, error::ErrorInternalServerError, types::Data, App, Error, HttpResponse, HttpServer,
};
use tokio_postgres::{
    types::{ToSql, Type},
    NoTls, Row,
};

use redis_tang::RedisManager;
use tokio_postgres_tang::{Builder, Pool, PostgresManager};

#[ntex::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
            // use data_factory to build pool for every thread
            .data_factory(|| {
                let db_url = "postgres://postgres:123@localhost/test";
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
                    // we build pool for every thread so the total connections would be threads * 3.
                    // (Make too many connections would result in very little performance gain and a relative heavy background tasks for pool scheduler tasks)
                    .min_idle(2)
                    .max_size(2)
                    .build(mgr)
            })
            .data_factory(|| {
                let mgr = RedisManager::new("redis://127.0.0.1");
                Builder::new()
                    .always_check(false)
                    .idle_timeout(Some(std::time::Duration::from_secs(10 * 60)))
                    .max_lifetime(Some(std::time::Duration::from_secs(30 * 60)))
                    .reaper_rate(std::time::Duration::from_secs(5))
                    .min_idle(2)
                    .max_size(2)
                    .build(mgr)
            })
            .service(web::resource("/test").route(web::get().to(test)))
            .service(web::resource("/test/redis").route(web::get().to(test_redis)))
    })
    .bind("0.0.0.0:8000")?
    .run()
    .await
}

async fn test(pool: Data<Pool<PostgresManager<NoTls>>>) -> Result<HttpResponse, Error> {
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

async fn test_redis(pool: Data<Pool<RedisManager>>) -> Result<HttpResponse, Error> {
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
