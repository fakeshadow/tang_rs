// an async_std only stack with tide

use redis_tang::{Builder, Pool, RedisManager};
use tide::{Error, Request, Response, StatusCode};
use tokio_postgres::NoTls;
use tokio_postgres_tang::PostgresManager;

struct State {
    postgres_pool: Pool<PostgresManager<NoTls>>,
    redis_pool: Pool<RedisManager>,
}

#[async_std::main]
async fn main() -> async_std::io::Result<()> {
    let mgr = PostgresManager::new_from_stringlike("postgres://postgres:123@localhost/test", NoTls)
        .unwrap();
    let postgres_pool = Builder::new()
        .always_check(false)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(1)
        .max_size(24)
        .build(mgr)
        .await
        .expect("can't make postgres pool");

    let mgr = RedisManager::new("redis://127.0.0.1");
    let redis_pool = Builder::new()
        .always_check(false)
        .idle_timeout(None)
        .max_lifetime(None)
        .min_idle(1)
        .max_size(24)
        .build(mgr)
        .await
        .expect("can't make redis pool");

    let mut app = tide::with_state(State {
        postgres_pool,
        redis_pool,
    });

    app.at("/test").get(index);

    app.listen("0.0.0.0:8000").await
}

async fn index(req: Request<State>) -> Result<Response, Error> {
    let state = req.state();
    let pool_ref = state.postgres_pool.get().await.unwrap();
    let (client, _statement_map) = &*pool_ref;

    let _ = client.simple_query("").await.unwrap();

    let mut pool_ref = state.redis_pool.get().await.unwrap();
    let client = &mut *pool_ref;

    redis::cmd("PING")
        .query_async::<_, ()>(client)
        .await
        .unwrap();

    Ok(Response::new(StatusCode::Ok).body_json(&"ping")?)
}
