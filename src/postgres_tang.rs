use std::future::Future;
use std::pin::Pin;
use std::{borrow::Cow, fmt, str::FromStr};

use futures::{FutureExt, TryFutureExt, TryStreamExt};
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    types::Type,
    Client, Config, Error, Socket, Statement,
};

use crate::manager::{Manager, ManagerFuture};

#[derive(Clone)]
pub struct PostgresManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    config: Config,
    tls: Tls,
    prepares: Vec<PreparedStatement>,
}

impl<Tls> PostgresManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    /// Create a new `PostgresConnectionManager` with the specified `config`.
    /// prepared statements can be passed when connecting to speed up frequent used queries.
    /// example:
    /// ```rust
    /// use tokio_postgres::types::Type;
    /// use tang_rs::PostgresManager;
    ///
    /// let db_url = "postgres://postgres:123@localhost/test";
    ///
    /// let statements = vec![
    ///     ("SELECT * from table WHERE id = $1", vec![Type::OID]),
    ///     ("SELECT * from table2 WHERE id = $1", vec![]) // pass empty vec if you don't want a type specific prepare.
    /// ];
    ///
    /// let mgr = PostgresManager::new_from_stringlike(db_url, statements, tokio_postgres::NoTls);
    /// ```
    pub fn new(
        config: Config,
        statements: Vec<(&str, Vec<Type>)>,
        tls: Tls,
    ) -> PostgresManager<Tls> {
        PostgresManager {
            config,
            tls,
            prepares: statements.into_iter().map(|p| p.into()).collect(),
        }
    }

    /// Create a new `PostgresConnectionManager`, parsing the config from `params`.
    pub fn new_from_stringlike<T>(
        params: T,
        statements: Vec<(&str, Vec<Type>)>,
        tls: Tls,
    ) -> Result<PostgresManager<Tls>, Error>
    where
        T: ToString,
    {
        let stringified_params = params.to_string();
        let config = Config::from_str(&stringified_params)?;
        Ok(Self::new(config, statements, tls))
    }
}

impl<Tls> Manager for PostgresManager<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + Clone + 'static,
    Tls::Stream: Send,
    Tls::TlsConnect: Send,
    <Tls::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Connection = (Client, Vec<Statement>);
    type Error = PostgresPoolError;

    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>> {
        Box::pin(async move {
            let (c, conn) = self.config.connect(self.tls.clone()).await?;
            tokio_executor::spawn(conn.map(|_| ()));

            let prepares = &self.prepares;

            // make prepared statements if there is any and set manager prepares for later use.
            let sts = if !prepares.is_empty() {
                let mut vec = Vec::with_capacity(prepares.len());

                for p in prepares.iter() {
                    let PreparedStatement(query, types) = p;
                    vec.push(c.prepare_typed(query, &types));
                    //                    if types.len() > 0 {
                    //                        vec.push(c.prepare_typed(query, &types));
                    //                    } else {
                    //                        vec.push(c.prepare_typed(query, &[]));
                    //                    }
                }

                let vec = futures::future::join_all(vec).await;

                let mut sts = Vec::with_capacity(vec.len());
                for v in vec.into_iter() {
                    sts.push(v?);
                }
                sts
            } else {
                vec![]
            };

            Ok((c, sts))
        })
    }

    /// pin and box simple_query so that we can only try once from the stream.
    fn is_valid<'a>(
        &'a self,
        c: &'a mut Self::Connection,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        // ugly double box.
        Box::pin(async move {
            Box::pin(c.0.simple_query(""))
                .try_next()
                .map_ok(|_| ())
                .err_into()
                .await
        })
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

/// wrapper type for prepared statement
// ToDo: add runtime refresh of prepared statement
#[derive(Clone)]
pub struct PreparedStatement(Cow<'static, str>, Cow<'static, [Type]>);

impl From<(&str, Vec<Type>)> for PreparedStatement {
    fn from((st, typs): (&str, Vec<Type>)) -> Self {
        PreparedStatement(Cow::Owned(st.into()), Cow::Owned(typs))
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
