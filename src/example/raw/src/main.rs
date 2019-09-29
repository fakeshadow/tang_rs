use futures::TryStreamExt;
use tang_rs::{Builder, PoolError, PostgresConnectionManager};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let db_url = "postgres://postgres:123@localhost/test";

    // setup manager
    // only support NoTls for now
    let mgr =
        PostgresConnectionManager::new_from_stringlike(
            db_url,
            tokio_postgres::NoTls,
        ).unwrap_or_else(|_| panic!("can't make postgres manager"));

    // make prepared statements. pass Vec<tokio_postgres::types::Type> if you want typed statement. pass vec![] for no typed statement.
    // pass vec![] if you don't want any prepared statements.
    let statements = vec![
        ("SELECT * FROM topics WHERE id=ANY($1)", vec![tokio_postgres::types::Type::OID_ARRAY]),
        ("SELECT * FROM posts WHERE id=ANY($1)", vec![])
    ];

    // make pool
    let pool = Builder::new()
        .always_check(false) // if set true every connection will be checked before checkout.
        .idle_timeout(None) // set to None to ignore idle connection drop.
        .max_lifetime(None)
        .min_idle(1)
        .max_size(12)
        .prepare_statements(statements)
        .build(mgr)
        .await
        .unwrap_or_else(|_| panic!("can't make pool"));

    // wait a bit as the pool spawn connections asynchronously
    tokio::timer::delay(std::time::Instant::now() + std::time::Duration::from_secs(1)).await;

    let _row = pool
        .run(|mut conn| Box::pin(  // pin the async function to make sure the &mut Conn outlives our closure.
                                   async move {
                                       let (client, statements) = &mut conn;

                                       // statement index is the same as the input vector when building the pool.
                                       let statement = statements.get(0).unwrap();

                                       let ids = vec![1u32, 2, 3, 4, 5];

                                       let row = client
                                           .query(statement, &[&ids])
                                           .try_collect::<Vec<tokio_postgres::Row>>().await?;

                                       Ok::<_, PoolError>(row)
                                   }
        ))
        .await
        .map_err(|e| {
            // return error will be wrapped in Inner.
            match e {
                PoolError::Inner(e) => println!("{:?}", e),
                PoolError::TimeOut => ()
            };
            std::io::Error::new(std::io::ErrorKind::Other, "place holder error")
        })?;

    // get pool reference and run it outside of a closure
    let mut pool_ref = pool
        .get::<PoolError>()
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "place holder error"))?;

    let (client, statements) = pool_ref.get_conn();

    let statement = statements.get(0).unwrap();

    let ids = vec![1u32, 2, 3, 4, 5];

    let _row = client
        .query(statement, &[&ids])
        .try_collect::<Vec<tokio_postgres::Row>>()
        .await
        .expect("Failed to get row");

    // it's a good thing to drop the pool_ref right after you finished as the connection will be put back to pool when the poo_ref is dropped.
    drop(pool_ref);

    Ok(())
}