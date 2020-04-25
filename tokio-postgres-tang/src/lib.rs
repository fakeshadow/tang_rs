pub use tang_rs::{Builder, Pool, PoolRef};

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::RwLock;
use std::{fmt, str::FromStr};

use futures_util::{future::join_all, FutureExt, TryFutureExt};
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    types::Type,
    Client, Config, Error, Socket, Statement,
};

use tang_rs::{tokio_spawn, Manager, ManagerFuture, TokioTimeElapsed};

pub struct PostgresManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    config: Config,
    tls: Tls,
    prepares: RwLock<PreparedHashMap>,
}

impl<Tls> PostgresManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    /// Create a new `PostgresManager` with the specified `config`.
    /// prepared statements can be passed when connecting to speed up frequent used queries.
    pub fn new(config: Config, tls: Tls) -> PostgresManager<Tls> {
        PostgresManager {
            config,
            tls,
            prepares: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new `PostgresManager`, parsing the config from `params`.
    pub fn new_from_stringlike<T>(params: T, tls: Tls) -> Result<PostgresManager<Tls>, Error>
    where
        T: ToString,
    {
        let stringified_params = params.to_string();
        let config = Config::from_str(&stringified_params)?;
        Ok(Self::new(config, tls))
    }

    /// example:
    /// ```no_run
    /// use tokio_postgres::types::Type;
    /// use tokio_postgres::NoTls;
    /// use tokio_postgres_tang::PostgresManager;
    ///
    /// let db_url = "postgres://postgres:123@localhost/test";
    /// let mgr = PostgresManager::new_from_stringlike(db_url, NoTls)
    ///     .expect("Can't make manager")
    ///     .prepare_statement("get_table", "SELECT * from table", &[])
    ///     .prepare_statement("get_table_by_id", "SELECT * from table where id=$1, key=$2", &[Type::OID, Type::VARCHAR]);
    /// ```
    /// alias is used to call specific statement when using the connection.
    pub fn prepare_statement(self, alias: &str, query: &str, types: &[Type]) -> Self {
        self.prepares
            .write()
            .expect("Failed to lock/write prepared statements")
            .insert(alias.into(), (query, types).into());
        self
    }
}

impl<Tls> Manager for PostgresManager<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + Clone + 'static,
    Tls::Stream: Send,
    Tls::TlsConnect: Send,
    <Tls::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Connection = (Client, HashMap<String, Statement>);
    type Error = PostgresPoolError;

    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>> {
        Box::pin(async move {
            let (c, conn) = self.config.connect(self.tls.clone()).await?;
            tokio_spawn(conn.map(|_| ()));

            let prepares = self
                .prepares
                .read()
                .expect("Failed to lock/read prepared statements")
                .clone();

            let mut sts = HashMap::with_capacity(prepares.len());
            let mut futures = Vec::with_capacity(prepares.len());

            // make prepared statements if there is any and set manager prepares for later use.
            for p in prepares.iter() {
                let (alias, PreparedStatement(query, types)) = p;
                let alias = alias.to_string();
                let future = c.prepare_typed(query, &types).map_ok(|st| (alias, st));
                futures.push(future);
            }

            for result in join_all(futures).await.into_iter() {
                let (alias, st) = result?;
                sts.insert(alias, st);
            }

            Ok((c, sts))
        })
    }

    fn is_valid<'a>(
        &'a self,
        c: &'a mut Self::Connection,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(c.0.simple_query("").map_ok(|_| ()).err_into())
    }

    fn is_closed(&self, conn: &mut Self::Connection) -> bool {
        conn.0.is_closed()
    }
}

impl<Tls> fmt::Debug for PostgresManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PostgresConnectionManager")
            .field("config", &self.config)
            .finish()
    }
}

type StatementFuture<'a, SELF> =
    Pin<Box<dyn Future<Output = Result<&'a mut SELF, PostgresPoolError>> + Send + 'a>>;

/// helper trait for cached statement for this connection.
/// Statements only work on the connection prepare them and not other connections in the pool.
pub trait CacheStatement<'a> {
    /// Only statements with new alias as key in HashMap will be inserted.
    ///
    /// The statements with an already existed alias will be ignored.
    ///
    /// The format of statement is (<alias str> , <query str>, <tokio_postgres::types::Type>)
    fn insert_statements(
        &'a mut self,
        statements: &'a [(&'a str, &'a str, &'a [Type])],
    ) -> StatementFuture<'a, Self>;

    /// Clear the statements of this connection.
    fn clear_statements(&mut self) -> &mut Self;
}

impl<'a, Tls> CacheStatement<'a> for PoolRef<'_, PostgresManager<Tls>>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + Clone + 'static,
    Tls::Stream: Send,
    Tls::TlsConnect: Send,
    <Tls::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn insert_statements(
        &'a mut self,
        statements: &'a [(&'a str, &'a str, &'a [Type])],
    ) -> StatementFuture<'a, Self> {
        Box::pin(async move {
            let (cli, sts) = &mut **self;

            let mut futures = Vec::with_capacity(statements.len());
            for (alias, query, types) in statements
                .iter()
                .map(|(alias, query, types)| (*alias, *query, *types))
            {
                if !sts.contains_key(alias) {
                    let alias = alias.to_owned();
                    let f = cli.prepare_typed(query, types).map_ok(|st| (alias, st));
                    futures.push(f);
                }
            }

            for result in join_all(futures).await.into_iter() {
                let (alias, st) = result?;
                sts.insert(alias, st);
            }

            Ok(self)
        })
    }

    fn clear_statements(&mut self) -> &mut Self {
        let (_cli, sts) = &mut **self;
        sts.clear();
        self
    }
}

/// helper trait for add/remove prepared statements of PostgresManager.
pub trait PrepareStatement {
    /// The prepared statements will be constructed when new connections spawns into the pool.
    ///
    /// This can be achieved by calling `PoolRef.take_conn()` until all connections in pool are dropped.
    ///
    /// The format of statement is (<alias str> , <query str>, <tokio_postgres::types::Type>)
    fn prepare_statements(&mut self, statements: &[(&str, &str, &[Type])]) -> &mut Self;

    /// Clear the statements of PostgresManager.
    fn clear_prepared_statements(&mut self) -> &mut Self;
}

impl<Tls> PrepareStatement for PoolRef<'_, PostgresManager<Tls>>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + Clone + 'static,
    Tls::Stream: Send,
    Tls::TlsConnect: Send,
    <Tls::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn prepare_statements(&mut self, statements: &[(&str, &str, &[Type])]) -> &mut Self {
        // ToDo: check this {}.
        {
            let mut prepares = self
                .get_manager()
                .prepares
                .write()
                .expect("Failed to lock/write prepared statements");

            for (alias, query, types) in statements.iter() {
                prepares.insert((*alias).into(), (*query, *types).into());
            }
        }

        self
    }

    fn clear_prepared_statements(&mut self) -> &mut Self {
        self.get_manager()
            .prepares
            .write()
            .expect("Failed to lock/write prepared statements")
            .clear();
        self
    }
}

// type for prepared statement's hash map. key is used as statement's alias
type PreparedHashMap = HashMap<String, PreparedStatement>;

// wrapper type for prepared statement
#[derive(Clone)]
pub struct PreparedStatement(String, Vec<Type>);

impl From<(&str, &[Type])> for PreparedStatement {
    fn from((query, types): (&str, &[Type])) -> Self {
        PreparedStatement(query.into(), types.into())
    }
}

pub enum PostgresPoolError {
    Inner(Error),
    TimeOut,
}

impl fmt::Debug for PostgresPoolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PostgresPoolError::Inner(e) => e.fmt(f),
            PostgresPoolError::TimeOut => f
                .debug_struct("PostgresError")
                .field("source", &"Connection Timeout")
                .finish(),
        }
    }
}

impl From<Error> for PostgresPoolError {
    fn from(e: Error) -> Self {
        PostgresPoolError::Inner(e)
    }
}

impl From<TokioTimeElapsed> for PostgresPoolError {
    fn from(_e: TokioTimeElapsed) -> PostgresPoolError {
        PostgresPoolError::TimeOut
    }
}

#[cfg(test)]
mod tests {
    use super::PostgresManager;
    use tang_rs::Builder;
    use tokio_postgres::NoTls;

    #[tokio::test]
    async fn test_connection_limit() {
        let db_url = "postgres://postgres:prisma@localhost/";
        let mgr = PostgresManager::new_from_stringlike(db_url, NoTls).unwrap();

        let pool = Builder::new()
            .min_idle(10)
            .max_size(10)
            .build(mgr)
            .await
            .unwrap();

        assert_eq!(10, pool.state().connections)
    }
}
