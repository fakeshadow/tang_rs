use std::fmt;
use std::future::Future;
use std::pin::Pin;

use mongodb::{db::ThreadedDatabase, Client, Error, ThreadedClient};

use crate::manager::{Manager, ManagerFuture};

pub struct MongoManager {
    host: String,
    port: u16,
}

impl MongoManager {
    pub fn new(host: &str, port: u16) -> MongoManager {
        MongoManager {
            host: host.to_string(),
            port,
        }
    }
}

impl Manager for MongoManager {
    type Connection = Client;
    type Error = MongoPoolError;

    fn connect(&self) -> ManagerFuture<Result<Self::Connection, Self::Error>> {
        Box::pin(async move {
            let conn = Client::connect(&self.host, self.port)?;
            Ok(conn)
        })
    }

    fn is_valid<'a>(
        &'a self,
        c: &'a mut Self::Connection,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            // ToDo: how to ping??
            let _v = c.is_master()?;
            Ok(())
        })
    }

    fn is_closed(&self, conn: &mut Self::Connection) -> bool {
        false
    }
}

impl fmt::Debug for MongoManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PostgresConnectionManager")
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

impl From<tokio_timer::timeout::Elapsed> for MongoPoolError {
    fn from(_e: tokio_timer::timeout::Elapsed) -> MongoPoolError {
        MongoPoolError::TimeOut
    }
}
