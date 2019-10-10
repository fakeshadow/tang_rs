use tang_rs::{Builder, PostgresManager, PostgresPoolError, RedisManager};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let db_url = "postgres://postgres:123@localhost/test";

    // make prepared statements. pass Vec<tokio_postgres::types::Type> if you want typed statement. pass vec![] for no typed statement.
    // pass vec![] if you don't want any prepared statements.
    let statements = vec![
        (
            "SELECT * FROM topics WHERE id=ANY($1)",
            vec![tokio_postgres::types::Type::OID_ARRAY],
        ),
        ("SELECT * FROM posts WHERE id=ANY($1)", vec![]),
    ];

    // setup manager
    let mgr = PostgresManager::new_from_stringlike(db_url, statements, tokio_postgres::NoTls)
        .unwrap_or_else(|_| panic!("can't make postgres manager"));

    // make pool
    let pool = Builder::new()
        .always_check(false) // if set true every connection will be checked before checkout.
        .idle_timeout(None) // set to None to ignore idle connection drop.
        .max_lifetime(Some(std::time::Duration::from_secs(30 * 60)))
        .reaper_rate(std::time::Duration::from_secs(15)) // interval of idle and lifetime check.
        .min_idle(1)
        .max_size(12)
        .build(mgr)
        .await
        .unwrap_or_else(|_| panic!("can't make pool"));

    // wait a bit as the pool spawn connections asynchronously
    tokio::timer::delay(std::time::Instant::now() + std::time::Duration::from_secs(1)).await;

    // run pool in closure. it's slightly faster than pool.get().
    let _rows = pool
        .run(|conn| {
            Box::pin(
                // pin the async function to make sure the &mut Conn outlives our closure.
                async move {
                    let (client, statements) = &conn;

                    // statement index is the same as the input vector when building the pool.
                    let statement = statements.get(0).unwrap();

                    let ids = vec![1u32, 2, 3, 4, 5];

                    let rows = client.query(statement, &[&ids]).await?;

                    Ok::<_, PostgresPoolError>(rows)
                    // infer type here so that u can use your custom error in the closure. you error type have to impl From<PostgresPoolError> or From<RedisPoolError>.
                },
            )
        })
        .await
        .map_err(|e| {
            match e {
                PostgresPoolError::Inner(e) => {
                    println!("inner error is from tokio-postgres::Error. {:?}", e)
                }
                PostgresPoolError::TimeOut => (),
            };
            std::io::Error::new(std::io::ErrorKind::Other, "place holder error")
        })?;

    // get pool reference and run it outside of a closure
    let mut pool_ref = pool
        .get()
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "place holder error"))?;

    // use deref or deref mut to get our client from pool_ref
    let (client, statements) = &*pool_ref;

    let statement = statements.get(0).unwrap();

    let ids = vec![1u32, 2, 3, 4, 5];

    let _rows = client
        .query(statement, &[&ids])
        .await
        .expect("Failed to get row");

    // we can take the connection out of pool ref if you prefer.
    let conn = pool_ref.take_conn();

    assert_eq!(true, conn.is_some());

    // the connection won't be return to pool unless we push it back manually.
    pool_ref.push_conn(conn.unwrap());

    // it's a good thing to drop the pool_ref right after you finished as the connection will be put back to pool when the pool_ref is dropped.
    drop(pool_ref);

    // build redis pool just like postgres pool
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

    let mut pool_ref = pool_redis.get().await.expect("Failed to get redis pool");

    let client = &mut *pool_ref;

    redis::cmd("PING")
        .query_async::<_, ()>(client)
        .await
        .expect("Failed to ping redis pool");

    drop(pool_ref);

    Ok(())
}
