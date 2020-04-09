use tokio_postgres::types::Type;

use tang_rs::{Builder, PostgresManager, PostgresPoolError, RedisManager};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let db_url = "postgres://admin:admin@localhost/test";

    // setup manager
    let mgr = PostgresManager::new_from_stringlike(db_url, tokio_postgres::NoTls)
        .unwrap_or_else(|_| panic!("can't make postgres manager"));

    // make prepared statements to speed up frequent used queries. It just stores your statement info in a hash map and
    // you can skip this step if you don't need any prepared statement.
    let mgr = mgr
        // alias is used to call according statement later.
        // pass &[tokio_postgres::types::Type] if you want typed statement. pass &[] for no typed statement.
        .prepare_statement(
            "topics",
            "SELECT * FROM topics WHERE id=ANY($1)",
            &[Type::OID_ARRAY],
        )
        .prepare_statement("users", "SELECT * FROM posts WHERE id=ANY($1)", &[]);

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
        .expect("can't make pool");

    // wait a bit as the pool spawn connections asynchronously
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;

    // run pool in closure. it's slightly faster than pool.get().
    let _rows = pool
        .run(|conn| {
            Box::pin(
                // pin the async function to make sure the &mut Conn outlives our closure.
                async move {
                    let (client, statements) = &conn;

                    // use statement alias to call specific prepared statement.
                    let statement = statements.get("topics").expect(
                        "handle the option if you are not sure if a prepared statement exists",
                    );

                    let ids = vec![1u32, 2, 3, 4, 5];

                    let rows = client.query(statement, &[&ids]).await?;

                    Ok::<_, PostgresPoolError>(rows)
                    // infer type here so that u can use your custom error in the closure. you error type have to impl From<PostgresPoolError>.
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
    let mut pool_ref = pool.get().await.expect("Failed to get pool ref");

    // use deref or deref mut to get our client from pool_ref
    let (client, statements) = &mut *pool_ref;

    /*
        It's possible to insert new statement into statements from pool_ref.
        But be ware the statement will only work on this specific connection and not other connections in the pool.
        The additional statement will be dropped when the connection is dropped from pool.
        A newly spawned connection will not include this additional statement.

        * This newly inserted statement most likely can't take advantage of the pipeline query features
        as we didn't join futures when prepare this statement.

        * It's suggested that if you want pipelined statements you should join the futures of prepare before calling await on them.
        There is tang_rs::CacheStatement trait for PoolRef<PostgresManager<T>> to help you streamline this operation.
    */

    let statement = match statements.get("statement_new") {
        Some(statement_new) => statement_new,
        None => {
            let statement_new = client
                .prepare_typed("SELECT * FROM topics WHERE id=ANY($1)", &[])
                .await
                .expect("Failed to prepare statement");
            statements.insert("statement_new".into(), statement_new);
            statements.get("statement_new").unwrap()
        }
    };

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
