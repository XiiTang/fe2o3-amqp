//! Dispositions for native answers whose operation stopped waiting.
//!
//! Install a typed sink with [`CompletionSink::scope`] before polling an operation.
//! Channels created by that operation capture the sink. A sender finding its
//! receiver closed, or a receiver dropped with an unread answer, invokes it once.
//! Sinks run synchronously, outside channel locks; queue asynchronous recovery to
//! its owner. Scopes are inherited by nested scopes, but not by spawned tasks.
//!
//! Native answer types are `Result<u16, connection::AllocSessionError>` for session
//! allocation and `Result<u32, session::error::AllocLinkError>` for link allocation.
//! Settlement (including controller Declare/Discharge) uses
//! `Result<Option<types::messaging::DeliveryState>, link::LinkStateError>`.
//! Engine answers use `Result<(), connection::Error>` or `Result<(), session::Error>`.
//! An allocation's local slot is released by its engine before reuse; its sink
//! observes the allocation result and need not release that local slot itself.
use parking_lot::Mutex;
use std::{
    any::Any,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::{oneshot, Notify};

tokio::task_local! {
    static SINKS: Vec<Arc<dyn Any + Send + Sync>>;
}

/// A per-operation disposition for an untaken native answer of type `T`.
pub struct CompletionSink<T>(Arc<dyn Fn(T) + Send + Sync>);
impl<T> Clone for CompletionSink<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> fmt::Debug for CompletionSink<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CompletionSink")
    }
}
impl<T: 'static> CompletionSink<T> {
    /// Build a disposition. It must not block the engine that hands it an answer.
    pub fn new(sink: impl Fn(T) + Send + Sync + 'static) -> Self {
        Self(Arc::new(sink))
    }
    /// Poll `operation` with this sink installed, before its native handoff.
    /// The innermost sink of a matching type wins; other answer types are unchanged.
    pub async fn scope<F: Future>(self, operation: F) -> F::Output {
        let mut sinks = SINKS.try_with(Clone::clone).unwrap_or_default();
        sinks.push(Arc::new(self));
        SINKS.scope(sinks, operation).await
    }
    pub(crate) fn current() -> Option<Self> {
        SINKS
            .try_with(|sinks| {
                sinks
                    .iter()
                    .rev()
                    .find_map(|sink| sink.downcast_ref::<Self>().cloned())
            })
            .ok()
            .flatten()
    }
    pub(crate) fn map_input<U: 'static>(
        self,
        map: impl Fn(U) -> T + Send + Sync + 'static,
    ) -> CompletionSink<U> {
        CompletionSink::new(move |value| (self.0)(map(value)))
    }
}

type Disposition<T> = Arc<Mutex<Option<Box<dyn FnOnce(T) + Send>>>>;

pub(crate) struct Sender<T> {
    inner: oneshot::Sender<T>,
    disposition: Disposition<T>,
}
impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CompletionSender")
    }
}
pub(crate) struct Receiver<T> {
    inner: oneshot::Receiver<T>,
    disposition: Disposition<T>,
}
impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CompletionReceiver")
    }
}
impl<T> From<oneshot::Sender<T>> for Sender<T> {
    fn from(inner: oneshot::Sender<T>) -> Self {
        Self {
            inner,
            disposition: Arc::default(),
        }
    }
}
impl<T> From<oneshot::Receiver<T>> for Receiver<T> {
    fn from(inner: oneshot::Receiver<T>) -> Self {
        Self {
            inner,
            disposition: Arc::default(),
        }
    }
}
impl<T: 'static> Sender<T> {
    /// Add native resource retirement before the caller's disposition.
    pub(crate) fn on_untaken(&self, cleanup: impl FnOnce(&T) + Send + 'static) {
        let old = self.disposition.lock().take();
        *self.disposition.lock() = Some(Box::new(move |value| {
            cleanup(&value);
            if let Some(sink) = old {
                sink(value);
            }
        }));
    }
    pub(crate) fn send(self, value: T) -> Result<(), T> {
        match self.inner.send(value) {
            Ok(()) => Ok(()),
            Err(value) => dispose(&self.disposition, value),
        }
    }
}
fn dispose<T>(disposition: &Disposition<T>, value: T) -> Result<(), T> {
    let sink = disposition.lock().take();
    match sink {
        Some(sink) => {
            sink(value);
            Ok(())
        }
        None => Err(value),
    }
}
impl<T> Receiver<T> {
    pub(crate) fn try_recv(&mut self) -> Result<T, oneshot::error::TryRecvError> {
        self.inner.try_recv()
    }
    pub(crate) fn dispose(&self, value: T) {
        let _ = dispose(&self.disposition, value);
    }
    pub(crate) fn set_sink(&self, sink: CompletionSink<T>)
    where
        T: 'static,
    {
        *self.disposition.lock() = Some(Box::new(move |value| (sink.0)(value)));
    }
}
impl<T> Future for Receiver<T> {
    type Output = Result<T, oneshot::error::RecvError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.close();
        if let Ok(value) = self.inner.try_recv() {
            self.dispose(value);
        }
    }
}
pub(crate) fn channel<T: 'static>() -> (Sender<T>, Receiver<T>) {
    channel_with(CompletionSink::current())
}
pub(crate) fn channel_with<T: 'static>(
    sink: Option<CompletionSink<T>>,
) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = oneshot::channel();
    let disposition: Disposition<T> = Arc::new(Mutex::new(
        sink.map(|sink| Box::new(move |value| (sink.0)(value)) as _),
    ));
    (
        Sender {
            inner: tx,
            disposition: disposition.clone(),
        },
        Receiver {
            inner: rx,
            disposition,
        },
    )
}

/// Retirement is bounded by the engine's allocated population: each allocation
/// can enqueue once, and its slot cannot be reused until the engine drains it.
#[derive(Debug)]
pub(crate) struct Retired<T>(Arc<(Mutex<Vec<T>>, Notify)>);
impl<T> Clone for Retired<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> Default for Retired<T> {
    fn default() -> Self {
        Self(Arc::new((Mutex::new(Vec::new()), Notify::new())))
    }
}
impl<T> Retired<T> {
    pub(crate) fn push(&self, value: T) {
        self.0 .0.lock().push(value);
        self.0 .1.notify_one();
    }
    pub(crate) fn take(&self) -> Vec<T> {
        std::mem::take(&mut *self.0 .0.lock())
    }
    pub(crate) async fn ready(&self) {
        self.0 .1.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn both_untaken_orders_and_normal_take_have_one_owner() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let output = values.clone();
        CompletionSink::new(move |value: u32| output.lock().push(value))
            .scope(async {
                let (tx, rx) = channel();
                drop(rx);
                tx.send(1u32).unwrap();
                let (tx, rx) = channel();
                tx.send(2u32).unwrap();
                drop(rx);
                let (tx, rx) = channel();
                tx.send(3u32).unwrap();
                assert_eq!(rx.await.unwrap(), 3);
            })
            .await;
        assert_eq!(*values.lock(), [1, 2]);
    }
    #[tokio::test]
    async fn concurrent_send_and_drop_never_lose_or_duplicate_the_answer() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let count = Arc::new(AtomicUsize::new(0));
        let output = count.clone();
        CompletionSink::new(move |(): ()| {
            output.fetch_add(1, Ordering::SeqCst);
        })
        .scope(async {
            for _ in 0..128 {
                let (tx, rx) = channel();
                std::thread::scope(|scope| {
                    let barrier = Arc::new(std::sync::Barrier::new(2));
                    let other = barrier.clone();
                    scope.spawn(move || {
                        other.wait();
                        tx.send(()).unwrap();
                    });
                    barrier.wait();
                    drop(rx);
                });
            }
        })
        .await;
        assert_eq!(count.load(Ordering::SeqCst), 128);
    }
}
