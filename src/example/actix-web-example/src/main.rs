#[macro_use]
extern crate serde_derive;

use actix::prelude::Future as Future01;
use actix_web::{App, Error, HttpResponse, HttpServer, web};
use actix_web::web::Data;
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use tang_rs::{Builder, Pool, PoolError, PostgresConnectionManager};
use tokio_postgres::Row;
use tokio_postgres::types::Type;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let db_url = "postgres://postgres:123@localhost/test";
    let mgr = PostgresConnectionManager::new_from_stringlike(db_url, tokio_postgres::NoTls)
        .unwrap_or_else(|_| panic!("can't make postgres manager"));

    let statements = vec![
        (
            "SELECT * FROM topics WHERE id=ANY($1)",
            vec![Type::OID_ARRAY],
        ),
        ("SELECT * FROM users WHERE id=ANY($1)", vec![]),
    ];

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
        .prepare_statements(statements)
        .build(mgr)
        .await
        .unwrap_or_else(|_| panic!("can't make pool"));

    HttpServer::new(move || {
        App::new()
            .data(pool.clone())
            .service(web::resource("/test").route(web::get().to_async(test)))
    })
    .bind("localhost:8000")
    .unwrap()
    .run()
}

fn test(
    pool: Data<Pool<tokio_postgres::NoTls>>,
) -> impl Future01<Item = HttpResponse, Error = Error> {
    test_async(pool).boxed_local().compat()
}

async fn test_async(pool: Data<Pool<tokio_postgres::NoTls>>) -> Result<HttpResponse, Error> {
    let t = pool
        .run(|mut c| {
            Box::pin(async move {
                let (client, statements) = &mut c;

                let ids = vec![
                    1u32, 11, 9, 20, 3, 5, 2, 6, 19, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 4,
                ];

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
                    .await?;

                let _u = client
                    .query(statements.get(1).unwrap(), &[&u])
                    .try_collect::<Vec<Row>>()
                    .await?;

                Ok::<_, PoolError>(t)
            })
        })
        .map_err(|e: PoolError| {
            match e {
                PoolError::Inner(e) => println!("{:?}", e),
                PoolError::TimeOut => (),
            };
            actix_web::error::ErrorInternalServerError("lol")
        })
        .await?;

    Ok(HttpResponse::Ok().json(&t))
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