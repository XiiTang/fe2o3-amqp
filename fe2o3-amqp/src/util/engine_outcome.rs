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
    sender: crate::completion::Sender<Result<(), E>>,
    state: Arc<AtomicU8>,
}

impl<E: 'static> EngineOutcomeSender<E> {
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
    receiver: crate::completion::Receiver<Result<(), E>>,
    state: Arc<AtomicU8>,
    buffered: Option<Result<(), E>>,
    consumed: bool,
    #[cfg(test)]
    pub(crate) before_stop: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl<E: 'static> EngineOutcome<E> {
    pub(crate) fn channel() -> (EngineOutcomeSender<E>, Self) {
        let (sender, receiver) = crate::completion::channel();
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
            None => self.receiver.recv().await,
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
        if let Some(sink) = crate::completion::CompletionSink::current() {
            self.receiver.set_sink(sink);
        }
        struct Waiting<'a, E> {
            outcome: &'a mut EngineOutcome<E>,
            armed: bool,
        }
        impl<E> Drop for Waiting<'_, E> {
            fn drop(&mut self) {
                if self.armed {
                    if let Some(value) = self.outcome.buffered.take() {
                        self.outcome.receiver.dispose(value);
                    }
                    self.outcome.receiver.close_and_take();
                }
            }
        }
        let armed = self.receiver.has_sink();
        let mut waiting = Waiting {
            outcome: self,
            armed,
        };
        let result = waiting
            .outcome
            .stop_and_join_inner(task, joined, publish_stop)
            .await;
        waiting.armed = false;
        result
    }

    async fn stop_and_join_inner(
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
        if let Some(result) = self.buffered.take() {
            self.consumed = true;
            if self.state.load(Ordering::Acquire) == NATURAL {
                return result;
            }
            self.receiver.dispose(result);
        }
        match joined_result {
            Err(error) if stopped && error.is_cancelled() => Ok(()),
            Err(error) => Err(error.into()),
            Ok(()) => Ok(()),
        }
    }
}

impl<E> Drop for EngineOutcome<E> {
    fn drop(&mut self) {
        if let Some(value) = self.buffered.take() {
            self.receiver.dispose(value);
        }
    }
}

#[cfg(test)]
mod completion_tests {
    use super::*;
    use crate::completion::CompletionSink;
    use parking_lot::Mutex;
    #[tokio::test]
    async fn every_untaken_engine_end_including_a_probed_result_reaches_sink() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        for stage in 0..3 {
            let output = seen.clone();
            CompletionSink::new(move |value: Result<(), &'static str>| output.lock().push(value))
                .scope(async {
                    let (tx, mut outcome) = EngineOutcome::channel();
                    if stage == 0 {
                        drop(outcome);
                        tx.complete(Err("native"), || {});
                    } else {
                        tx.complete(Err("native"), || {});
                        if stage == 2 {
                            outcome.probe();
                        }
                        drop(outcome);
                    }
                })
                .await;
        }
        assert_eq!(*seen.lock(), [Err("native"), Err("native"), Err("native")]);
    }
    #[tokio::test]
    async fn a_cancelled_engine_wait_hands_later_completion_to_its_installed_sink() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = seen.clone();
        let (tx, mut outcome) = EngineOutcome::channel();
        let wait =
            CompletionSink::new(move |value: Result<(), &'static str>| output.lock().push(value))
                .scope(outcome.recv());
        let mut wait = Box::pin(wait);
        assert!(futures_util::poll!(&mut wait).is_pending());
        drop(wait);
        tx.complete(Err("late"), || {});
        drop(outcome);
        assert_eq!(*seen.lock(), [Err("late")]);
    }
}
