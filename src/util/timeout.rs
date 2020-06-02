use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct ManagerTimeout<T, D> {
    future: T,
    delay: D,
}

impl<T, D> ManagerTimeout<T, D> {
    pub fn new(future: T, delay: D) -> Self {
        Self { future, delay }
    }
}

impl<T, D, E> Future for ManagerTimeout<T, D>
where
    T: Future,
    D: Future<Output = E>,
{
    type Output = Result<T::Output, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe {
            let p = self.as_mut().map_unchecked_mut(|this| &mut this.future);
            if let Poll::Ready(fut) = p.poll(cx) {
                return Poll::Ready(Ok(fut));
            }
        }

        unsafe {
            match self.map_unchecked_mut(|this| &mut this.delay).poll(cx) {
                Poll::Ready(e) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}
