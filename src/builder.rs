use std::time::Duration;

use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    types::Type,
    Error, Socket,
};

use crate::{postgres::PreparedStatement, Pool, PostgresConnectionManager};

pub struct Builder {
    pub(crate) max_size: u8,
    pub(crate) min_idle: u8,
    /// Whether or not to test the connection on checkout.
    pub(crate) always_check: bool,
    pub(crate) max_lifetime: Option<Duration>,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) connection_timeout: Duration,
    /// The time interval used to wake up and reap connections.
    pub(crate) reaper_rate: Duration,
    pub(crate) statements: Vec<PreparedStatement>,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            max_size: 10,
            min_idle: 1,
            always_check: true,
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            connection_timeout: Duration::from_secs(10),
            reaper_rate: Duration::from_secs(15),
            statements: vec![],
        }
    }
}

impl Builder {
    pub fn new() -> Builder {
        Default::default()
    }

    pub fn max_size(mut self, max_size: u8) -> Builder {
        if max_size > 0 {
            self.max_size = max_size;
        }
        self
    }

    pub fn min_idle(mut self, min_idle: u8) -> Builder {
        self.min_idle = min_idle;
        self
    }

    /// If true, the health of a connection will be verified when checkout.
    ///
    /// Defaults to true.
    pub fn always_check(mut self, always_check: bool) -> Builder {
        self.always_check = always_check;
        self
    }

    /// Sets the maximum lifetime of connections in the pool.
    ///
    /// If set, connections will be closed at the next reaping after surviving
    /// past this duration.
    ///
    /// If a connection reachs its maximum lifetime while checked out it will be
    /// closed when it is returned to the pool.
    ///
    /// Defaults to 30 minutes.
    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> Builder {
        self.max_lifetime = max_lifetime;
        self
    }

    /// Sets the idle timeout used by the pool.
    ///
    /// If set, idle connections in excess of `min_idle` will be closed at the
    /// next reaping after remaining idle past this duration.
    ///
    /// Defaults to 10 minutes.
    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Builder {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Sets the connection reaper rate.
    ///
    /// The connection that are idle and live beyond the time gate will be dropped..
    ///
    /// Default 15 seconds and only one connection will be checked in each interval.
    pub fn reaper_rate(mut self, reaper_rate: Duration) -> Builder {
        self.reaper_rate = reaper_rate.into();
        self
    }

    /// Sets the connection timeout used by the pool.
    ///
    /// The closure of pool.run() and the pool.get() method will cancel and return a timeout error.
    ///
    /// Default 10 seconds.
    pub fn connection_timeout(mut self, connection_timeout: Duration) -> Builder {
        self.connection_timeout = connection_timeout;
        self
    }

    /// prepared statements can be passed when connecting to speed up frequent used queries.
    /// example:
    /// ```rust
    /// use tokio_postgres::types::Type;
    ///
    /// let statements = vec![
    ///     ("SELECT * from table WHERE id = $1".to_owned(), vec![Type::OID]),
    ///     ("SELECT * from table2 WHERE id = $1", vec![]) // pass empty vec if you don't want a type specific prepare.
    /// ];
    /// let builder = crate::Builder::new().prepare_statements(statements);
    /// ```
    pub fn prepare_statements(mut self, statements: Vec<(&str, Vec<Type>)>) -> Self {
        let statements = statements
            .into_iter()
            .map(|p| p.into())
            .collect::<Vec<PreparedStatement>>();

        self.statements = statements;
        self
    }

    /// Consumes the builder, returning a new, initialized `Pool`.
    pub async fn build<Tls>(
        self,
        manager: PostgresConnectionManager<Tls>,
    ) -> Result<Pool<Tls>, Error>
    where
        Tls: MakeTlsConnect<Socket> + Send + Sync + 'static,
        <Tls as MakeTlsConnect<Socket>>::Stream: Send,
        <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
        <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        assert!(
            self.max_size >= self.min_idle,
            "min_idle must be no larger than max_size"
        );

        let pool = Pool::new_inner(self, manager);
        pool.0.replenish_idle_connections().await?;

        Ok(pool)
    }
}
