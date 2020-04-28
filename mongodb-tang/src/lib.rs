use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use bson::doc;
use mongodb::{
    error::Error,
    options::{ClientOptions, StreamAddress},
    Client,
};
use tang_rs::{Manager, ManagerFuture};
use tokio::time::{timeout, Elapsed};

pub struct MongoManager {
    host: String,
    port: u16,
    ping_db_name: String,
}

impl MongoManager {
    pub fn new(host: &str, port: u16, ping_db_name: &str) -> MongoManager {
        MongoManager {
            host: host.into(),
            port,
            ping_db_name: ping_db_name.into(),
        }
    }
}

impl Manager for MongoManager {
    type Connection = Client;
    type Error = MongoPoolError;
    type TimeoutError = Elapsed;

    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>> {
        Box::pin(async move {
            let option = ClientOptions::builder()
                .hosts(vec![StreamAddress {
                    hostname: (&self.host).into(),
                    port: Some(self.port),
                }])
                .build();
            let conn = Client::with_options(option)?;
            Ok(conn)
        })
    }

    fn is_valid<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            let db = conn.database(&self.ping_db_name);
            let _ = db.run_command(doc! { "ping" => 1 }, None)?;
            Ok(())
        })
    }

    fn is_closed(&self, _conn: &mut Self::Connection) -> bool {
        false
    }

    fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(fut);
    }

    fn timeout<'fu, Fut>(
        &self,
        fut: Fut,
        dur: Duration,
    ) -> ManagerFuture<'fu, Result<Fut::Output, Self::TimeoutError>>
    where
        Fut: Future + Send + 'fu,
    {
        Box::pin(timeout(dur, fut))
    }
}

impl fmt::Debug for MongoManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MongoManager")
            .field("host", &self.host)
            .field("port", &self.port)
            .field(
                "options",
                &"Can't show as no Debug impl for ClientOptions from mongodb crate",
            )
            .finish()
    }
}

pub enum MongoPoolError {
    Inner(Error),
    TimeOut,
}

impl fmt::Debug for MongoPoolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MongoPoolError::Inner(e) => e.fmt(f),
            MongoPoolError::TimeOut => f
                .debug_struct("MongoPoolError")
                .field("source", &"Connection Timeout")
                .finish(),
        }
    }
}

impl From<Error> for MongoPoolError {
    fn from(e: Error) -> Self {
        MongoPoolError::Inner(e)
    }
}

impl From<Elapsed> for MongoPoolError {
    fn from(_e: Elapsed) -> MongoPoolError {
        MongoPoolError::TimeOut
    }
}
