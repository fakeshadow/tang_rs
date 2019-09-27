#![feature(proc_macro_hygiene)]

#[macro_use]
extern crate rocket;
#[macro_use]
extern crate serde_derive;

use futures::TryStreamExt;
use rocket::response::content;
use rocket::State;
use tokio::runtime::Runtime;
use tokio_postgres::types::Type;
use tokio_postgres::Row;

use tang_rs::{Builder, Pool, PoolError, PostgresConnectionManager};

fn main() {
    let runtime = Runtime::new().expect("Failed to create tokio runtime");

    let db_url = "postgres://postgres:123@localhost/test";
    let mgr = PostgresConnectionManager::new_from_stringlike(db_url, tokio_postgres::NoTls)
        .unwrap_or_else(|_| panic!("can't make postgres manager"));

    let statements = vec![
        (
            "SELECT * FROM topics WHERE id=ANY($1)",
            vec![Type::OID_ARRAY],
        ),
        ("SELECT * FROM posts WHERE id=ANY($1)", vec![]),
    ];

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

    let server = rocket::ignite()
        .mount("/test", routes![index])
        .manage(pool)
        .spawn_on(&runtime);

    runtime.block_on(async move {
        let _ = server.await;
    });
}

#[get("/")]
async fn index(pool: State<'_, Pool<tokio_postgres::NoTls>>) -> content::Json<String> {
    let t = pool
        .run(|mut c| {
            async move {
                let (client, statements) = &mut c.conn;

                let ids = vec![
                    1u32, 11, 9, 20, 3, 5, 2, 6, 19, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 4,
                ];

                let row = client
                    .query(statements.get(0).unwrap(), &[&ids])
                    .try_collect::<Vec<Row>>()
                    .await?;

                let mut t = Vec::with_capacity(21);

                for r in row.iter() {
                    t.push(Topic {
                        id: r.get(0),
                        user_id: r.get(1),
                        category_id: r.get(2),
                        title: r.get(3),
                        body: r.get(4),
                        thumbnail: r.get(5),
                        is_locked: r.get(8),
                        is_visible: r.get(9),
                    })
                }

                Ok((t, c))
            }
        })
        .await
        .unwrap_or_else(|e| {
            match e {
                PoolError::Inner(e) => println!("{:?}", e),
                PoolError::TimeOut => (),
            };
            panic!();
        });

    content::Json(serde_json::to_string(&t).unwrap())
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
