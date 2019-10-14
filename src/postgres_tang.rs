use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::{fmt, str::FromStr};

use futures_util::{FutureExt, TryFutureExt};
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    types::Type,
    Client, Config, Error, Socket, Statement,
};

use crate::manager::{Manager, ManagerFuture};
use crate::PoolRef;

#[derive(Clone)]
pub struct PostgresManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    config: Config,
    tls: Tls,
    prepares: PreparedHashMap,
}

impl<Tls> PostgresManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    /// Create a new `PostgresConnectionManager` with the specified `config`.
    /// prepared statements can be passed when connecting to speed up frequent used queries.
    pub fn new(config: Config, tls: Tls) -> PostgresManager<Tls> {
        PostgresManager {
            config,
            tls,
            prepares: HashMap::new(),
        }
    }

    /// Create a new `PostgresConnectionManager`, parsing the config from `params`.
    pub fn new_from_stringlike<T>(params: T, tls: Tls) -> Result<PostgresManager<Tls>, Error>
    where
        T: ToString,
    {
        let stringified_params = params.to_string();
        let config = Config::from_str(&stringified_params)?;
        Ok(Self::new(config, tls))
    }

    /// example:
    /// ```rust
    /// use tokio_postgres::types::Type;
    /// use tokio_postgres::NoTls;
    /// use tang_rs::PostgresManager;
    ///
    /// let db_url = "postgres://postgres:123@localhost/test";
    /// let mgr = PostgresManager::new_from_stringlike(db_url, NoTls)
    ///     .expect("Can't make manager")
    ///     .prepare_statement("get_table", "SELECT * from table", &[])
    ///     .prepare_statement("get_table_by_id", "SELECT * from table where id=$1, key=$2", &[Type::OID, Type::VARCHAR]);
    /// ```
    /// alias is used to call specific statement when using the connection.
    pub fn prepare_statement(mut self, alias: &str, statement: &str, types: &[Type]) -> Self {
        self.prepares
            .insert(alias.into(), (statement, types).into());
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
            tokio_executor::spawn(conn.map(|_| ()));

            let prepares = &self.prepares;
            let mut statements = HashMap::with_capacity(prepares.len());
            let mut futures = Vec::with_capacity(prepares.len());

            // make prepared statements if there is any and set manager prepares for later use.
            for p in prepares.iter() {
                let (alias, PreparedStatement(query, types)) = p;
                let alias = alias.to_string();
                let future = c
                    .prepare_typed(query, &types)
                    .map_ok(|statement| (alias, statement));
                futures.push(future);
            }

            for result in futures_util::future::join_all(futures).await.into_iter() {
                let (alias, statement) = result?;
                statements.insert(alias, statement);
            }

            Ok((c, statements))
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

// type for prepared statement's hash map. key is used as statement's alias
type PreparedHashMap = HashMap<String, PreparedStatement>;

type StatementFuture<'a, SELF> =
    Pin<Box<dyn Future<Output = Result<&'a mut SELF, PostgresPoolError>> + Send + 'a>>;

/// helper trait for cached statement for this connection.
/// Statements only work on the connection prepare them and not other connections in the pool.
pub trait CacheStatement<'a> {
    /// Only statements with new alias as key in HashMap will be inserted.
    ///
    /// The statements with an already existed alias will be ignored.
    ///
    /// The format of statement is (<alias string> , <Sql string>, <tokio_postgres::types::Type>)
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

            for result in futures_util::future::join_all(futures).await.into_iter() {
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

// wrapper type for prepared statement
// ToDo: add runtime refresh of prepared statement
#[derive(Clone)]
pub struct PreparedStatement(String, Vec<Type>);

impl From<(&str, &[Type])> for PreparedStatement {
    fn from((st, typs): (&str, &[Type])) -> Self {
        PreparedStatement(st.into(), typs.into())
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

#[cfg(not(feature = "actix-web"))]
impl From<tokio_timer::timeout::Elapsed> for PostgresPoolError {
    fn from(_e: tokio_timer::timeout::Elapsed) -> PostgresPoolError {
        PostgresPoolError::TimeOut
    }
}

#[cfg(feature = "actix-web")]
impl<T> From<tokio_timer01::timeout::Error<T>> for PostgresPoolError {
    fn from(_e: tokio_timer01::timeout::Error<T>) -> PostgresPoolError {
        PostgresPoolError::TimeOut
    }
}
