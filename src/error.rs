use tokio_postgres::Error;

pub enum PoolError<E> {
    Inner(E),
    TimeOut,
}

impl From<Error> for PoolError<Error> {
    fn from(e: tokio_postgres::Error) -> PoolError<Error> {
        PoolError::Inner(e)
    }
}

impl<E> From<tokio_timer::timeout::Elapsed> for PoolError<E> {
    fn from(_e: tokio_timer::timeout::Elapsed) -> PoolError<E> {
        PoolError::TimeOut
    }
}

impl<E> From<futures::channel::oneshot::Canceled> for PoolError<E> {
    fn from(_e: futures::channel::oneshot::Canceled) -> PoolError<E> {
        PoolError::TimeOut
    }
}
