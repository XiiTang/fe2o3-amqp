//! One terminal winner for an engine and its owner, independent of result observation.
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use tokio::{
    sync::oneshot,
    task::{JoinError, JoinHandle},
};

const RUNNING: u8 = 0;
const NATURAL: u8 = 1;
const STOPPED: u8 = 2;

#[derive(Debug)]
pub(crate) struct EngineOutcomeSender<E> {
    sender: oneshot::Sender<Result<(), E>>,
    state: Arc<AtomicU8>,
}

impl<E> EngineOutcomeSender<E> {
    pub(crate) fn complete(self, result: Result<(), E>, publish_reason: impl FnOnce()) {
        // No await can separate winning, publishing the reason and sending the result.
        if self
            .state
            .compare_exchange(RUNNING, NATURAL, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            publish_reason();
        }
        let _ = self.sender.send(result);
    }
}

pub(crate) struct EngineOutcome<E> {
    receiver: oneshot::Receiver<Result<(), E>>,
    state: Arc<AtomicU8>,
    buffered: Option<Result<(), E>>,
    consumed: bool,
    #[cfg(test)]
    pub(crate) before_stop: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl<E> EngineOutcome<E> {
    pub(crate) fn channel() -> (EngineOutcomeSender<E>, Self) {
        let (sender, receiver) = oneshot::channel();
        let state = Arc::new(AtomicU8::new(RUNNING));
        (
            EngineOutcomeSender {
                sender,
                state: state.clone(),
            },
            Self {
                receiver,
                state,
                buffered: None,
                consumed: false,
                #[cfg(test)]
                before_stop: None,
            },
        )
    }

    pub(crate) fn try_recv(&mut self) -> Result<Result<(), E>, oneshot::error::TryRecvError> {
        let result = match self.buffered.take() {
            Some(result) => Ok(result),
            None => self.receiver.try_recv(),
        };
        if result.is_ok() {
            self.consumed = true;
        }
        result
    }

    pub(crate) async fn recv(&mut self) -> Result<Result<(), E>, oneshot::error::RecvError> {
        let result = match self.buffered.take() {
            Some(result) => Ok(result),
            None => (&mut self.receiver).await,
        };
        self.consumed = true;
        result
    }

    fn probe(&mut self) {
        if !self.consumed && self.buffered.is_none() {
            self.buffered = self.receiver.try_recv().ok();
        }
    }

    pub(crate) async fn stop_and_join(
        &mut self,
        task: &mut JoinHandle<()>,
        joined: &mut bool,
        publish_stop: impl FnOnce(),
    ) -> Result<(), E>
    where
        E: From<JoinError>,
    {
        if *joined {
            return Ok(());
        }
        // Keep the first probe in the owner if this join waiter is cancelled.
        self.probe();
        #[cfg(test)]
        if let Some(before_stop) = self.before_stop.take() {
            before_stop();
        }
        let stopped = self
            .state
            .compare_exchange(RUNNING, STOPPED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            || self.state.load(Ordering::Acquire) == STOPPED;
        if stopped {
            publish_stop();
        }
        task.abort();
        let joined_result = task.await;
        *joined = true;
        // A natural result can be published after the first probe, even during join.
        self.probe();
        if let (true, Some(result)) = (
            self.state.load(Ordering::Acquire) == NATURAL,
            self.buffered.take(),
        ) {
            self.consumed = true;
            return result;
        }
        match joined_result {
            Err(error) if stopped && error.is_cancelled() => Ok(()),
            Err(error) => Err(error.into()),
            Ok(()) => Ok(()),
        }
    }
}
