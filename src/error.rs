use tokio_postgres::Error;

pub enum PoolError {
    Inner(Error),
    TimeOut,
}

impl From<Error> for PoolError {
    fn from(e: tokio_postgres::Error) -> PoolError {
        PoolError::Inner(e)
    }
}

#[cfg(feature = "default")]
impl From<tokio_timer::timeout::Elapsed> for PoolError {
    fn from(_e: tokio_timer::timeout::Elapsed) -> PoolError {
        PoolError::TimeOut
    }
}

#[cfg(feature = "actix-web")]
impl<T> From<tokio_timer01::timeout::Error<T>> for PoolError {
    fn from(_e: tokio_timer01::timeout::Error<T>) -> PoolError {
        PoolError::TimeOut
    }
}
