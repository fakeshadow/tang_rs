use std::fmt;
use std::future::Future;
use std::time::Duration;

use bson::doc;
use mongodb::{
    error::Error,
    options::{ClientOptions, StreamAddress},
    Client,
};

use tang_rs::{Manager, ManagerFuture, ManagerTimeout};

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
    type Timeout = tokio::tome::Delay;
    type TimeoutError = ();

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
    ) -> ManagerFuture<'a, Result<(), Self::Error>> {
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

    fn timeout<Fut: Future>(&self, fut: Fut, _dur: Duration) -> ManagerTimeout<Fut, Self::Timeout> {
        ManagerTimeout::new(fut, tokio::time::delay_for(dur))
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

impl From<()> for MongoPoolError {
    fn from(_e: ()) -> MongoPoolError {
        MongoPoolError::TimeOut
    }
}
