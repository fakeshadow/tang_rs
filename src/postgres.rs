use std::{borrow::Cow, fmt, str::FromStr};

use futures::{future::Either, FutureExt, TryFutureExt, TryStreamExt};
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    types::Type,
    Client, Config, Error, Socket, Statement,
};

#[derive(Clone)]
pub struct PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    config: Config,
    tls: Tls,
    prepares: Vec<PreparedStatement>,
}

impl<Tls> PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    /// Create a new `PostgresConnectionManager` with the specified `config`.
    pub fn new(config: Config, tls: Tls) -> PostgresConnectionManager<Tls> {
        PostgresConnectionManager {
            config,
            tls,
            prepares: vec![],
        }
    }

    /// Create a new `PostgresConnectionManager`, parsing the config from `params`.
    pub fn new_from_stringlike<T>(
        params: T,
        tls: Tls,
    ) -> Result<PostgresConnectionManager<Tls>, Error>
    where
        T: ToString,
    {
        let stringified_params = params.to_string();
        let config = Config::from_str(&stringified_params)?;
        Ok(Self::new(config, tls))
    }

    pub(crate) async fn connect(
        &self,
        prepares: &[PreparedStatement],
    ) -> Result<(Client, Vec<Statement>), Error> {
        // ToDo: figure a way to pass self.tls to connect method.
        let (mut c, conn) = self.config.connect(tokio_postgres::NoTls).await?;

        tokio_executor::spawn(conn.map(|_| ()));

        // make prepared statements if there is any and set manager prepares for later use.
        let sts = if !prepares.is_empty() {
            let mut vec = Vec::with_capacity(prepares.len());

            for p in prepares.iter() {
                let PreparedStatement(query, types) = p;
                if types.len() > 0 {
                    vec.push(Either::Right(c.prepare_typed(query, types)));
                } else {
                    vec.push(Either::Left(c.prepare(query)));
                }
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
    }

    /// pin and box simple_query so that we can only try once from the stream.
    pub(crate) async fn is_valid(&self, c: &mut Client) -> Result<(), Error> {
        Box::pin(c.simple_query("")).try_next().map_ok(|_| ()).await
    }

    pub(crate) fn is_closed(&self, conn: &mut Client) -> bool {
        conn.is_closed()
    }
}

impl<Tls> fmt::Debug for PostgresConnectionManager<Tls>
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
pub(crate) struct PreparedStatement(Cow<'static, str>, Cow<'static, [Type]>);

impl From<(&str, Vec<Type>)> for PreparedStatement {
    fn from((st, typs): (&str, Vec<Type>)) -> Self {
        PreparedStatement(Cow::Owned(st.into()), Cow::Owned(typs))
    }
}
