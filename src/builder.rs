use std::time::Duration;

use crate::manager::Manager;
use crate::Pool;

pub struct Builder {
    pub(crate) max_size: usize,
    pub(crate) min_idle: usize,
    pub(crate) always_check: bool,
    pub(crate) max_lifetime: Option<Duration>,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) connection_timeout: Duration,
    pub(crate) queue_timeout: Duration,
    pub(crate) reaper_rate: Duration,
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
            queue_timeout: Duration::from_secs(20),
            reaper_rate: Duration::from_secs(15),
        }
    }
}

impl Builder {
    pub fn new() -> Builder {
        Default::default()
    }

    pub fn max_size(mut self, max_size: u8) -> Builder {
        self.max_size = max_size as usize;
        self
    }

    pub fn min_idle(mut self, min_idle: u8) -> Builder {
        self.min_idle = min_idle as usize;
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
        self.reaper_rate = reaper_rate;
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

    /// Sets the wait timeout used by the queue.
    ///
    /// Similar to connection_timeout. A time out error will return.
    ///
    /// Default 20 seconds
    pub fn wait_timeout(mut self, queue_timeout: Duration) -> Builder {
        self.queue_timeout = queue_timeout;
        self
    }

    /// Consumes the builder, returning a new, initialized `Pool`.
    pub async fn build<M: Manager>(self, manager: M) -> Result<Pool<M>, M::Error> {
        assert!(
            self.max_size >= self.min_idle,
            "min_idle must be no larger than max_size"
        );

        let pool = Pool::new(self, manager);
        #[cfg(feature = "default")]
        pool.0.replenish_idle_connections().await?;
        #[cfg(feature = "actix-web")]
        pool.0.replenish_idle_connections_temp().await?;

        Ok(pool)
    }
}
