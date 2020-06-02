use std::time::Duration;

use crate::manager::Manager;
use crate::pool::Pool;

pub struct Builder {
    pub(crate) max_size: usize,
    pub(crate) min_idle: usize,
    pub(crate) always_check: bool,
    pub(crate) use_gc: bool,
    pub(crate) max_lifetime: Option<Duration>,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) connection_timeout: Duration,
    pub(crate) wait_timeout: Duration,
    pub(crate) reaper_rate: Duration,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            max_size: 10,
            min_idle: 1,
            always_check: true,
            use_gc: false,
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            connection_timeout: Duration::from_secs(10),
            wait_timeout: Duration::from_secs(20),
            reaper_rate: Duration::from_secs(15),
        }
    }
}

impl Builder {
    pub fn new() -> Builder {
        Default::default()
    }

    pub fn max_size(mut self, max_size: usize) -> Builder {
        self.max_size = max_size;
        self
    }

    pub fn min_idle(mut self, min_idle: usize) -> Builder {
        self.min_idle = min_idle;
        self
    }

    /// If true, the health of a connection will be verified when checkout.
    ///
    /// This check uses `Builder`'s `connection_timeout` setting to cancel the check and return a timeout error.
    ///
    /// Defaults to true.
    pub fn always_check(mut self, always_check: bool) -> Builder {
        self.always_check = always_check;
        self
    }

    /// If true, the pending connections that last for too long will be removed.( 6 times the `connection_timeout` duration)
    ///
    /// This is a placeholder feature. it works fine but in most cases it's not necessary or useful.
    ///
    /// Defaults to false.
    pub fn use_gc(mut self, use_gc: bool) -> Builder {
        self.use_gc = use_gc;
        self
    }

    /// Sets the maximum lifetime of connections in the pool.
    ///
    /// If set, connections will be closed at the next reaping after surviving
    /// past this duration.
    ///
    /// If a connection reaches its maximum lifetime while checked out it will be
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
    /// The connection that are idle and live beyond the time gate will be dropped.
    ///
    /// Default 15 seconds.(no guarantee as we don't force lock the pool)
    pub fn reaper_rate(mut self, reaper_rate: Duration) -> Builder {
        self.reaper_rate = reaper_rate;
        self
    }

    /// Sets the connection timeout used by the pool.
    ///
    /// Attempt to establish new connection to database will be canceled and return a timeout error if this Duration passed.
    ///
    /// It's recommended to set this duration the same or a bit longer than your database connection timeout setting.
    ///
    /// Default 10 seconds.
    pub fn connection_timeout(mut self, connection_timeout: Duration) -> Builder {
        self.connection_timeout = connection_timeout;
        self
    }

    /// Sets the wait timeout used by the queue.
    ///
    /// Similar to `connection_timeout`. A timeout error will return if we wait too long for a connection from pool.
    ///
    /// Default 20 seconds
    pub fn wait_timeout(mut self, wait_timeout: Duration) -> Builder {
        self.wait_timeout = wait_timeout;
        self
    }

    /// Consumes the `Builder`, returning a new, initialized `Pool`.
    pub async fn build<M: Manager>(self, manager: M) -> Result<Pool<M>, M::Error> {
        let pool = self.build_uninitialized(manager);
        pool.init().await?;
        Ok(pool)
    }

    /// Consumes the `Builder`, returning a new uninitialized `Pool`.
    ///
    /// (`Pool` have no connection and scheduled tasks like connection reaper and garbage collect)
    pub fn build_uninitialized<M: Manager>(self, manager: M) -> Pool<M> {
        assert!(
            self.max_size >= self.min_idle,
            "min_idle must be no larger than max_size"
        );

        Pool::new(self, manager)
    }

    /// expose `reaper_rate` to public.
    pub fn get_reaper_rate(&self) -> Duration {
        self.reaper_rate
    }
}
