//! Bound encoded transfers while a peer closes its session receive window.
//! Waiting publishers retain their own input; the session driver keeps running.
use crate::link::LinkFrame;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
const RETAINED_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct Sender {
    tx: mpsc::Sender<LinkFrame>,
    bytes: Arc<Semaphore>,
}
pub(crate) fn channel(capacity: usize) -> (Sender, mpsc::Receiver<LinkFrame>) {
    let (tx, rx) = mpsc::channel(capacity);
    (Sender::from(tx), rx)
}
impl From<mpsc::Sender<LinkFrame>> for Sender {
    fn from(tx: mpsc::Sender<LinkFrame>) -> Self {
        Self {
            tx,
            bytes: Arc::new(Semaphore::new(RETAINED_BYTES)),
        }
    }
}
impl Sender {
    pub async fn send(
        &self,
        mut frame: LinkFrame,
    ) -> Result<(), mpsc::error::SendError<LinkFrame>> {
        if let LinkFrame::Transfer {
            payload,
            queue_slot,
            ..
        } = &mut frame
        {
            if queue_slot.is_none() {
                let size = payload.len().saturating_add(1024);
                if size > RETAINED_BYTES {
                    return Err(mpsc::error::SendError(frame));
                }
                let permit = tokio::select! {
                    _ = self.tx.closed() => return Err(mpsc::error::SendError(frame)),
                    permit = self.bytes.clone().acquire_many_owned(size as u32) => permit,
                };
                match permit {
                    Ok(permit) => *queue_slot = Some(permit),
                    Err(_) => return Err(mpsc::error::SendError(frame)),
                }
            }
        }
        self.tx.send(frame).await
    }
    pub fn try_send(
        &self,
        mut frame: LinkFrame,
    ) -> Result<(), mpsc::error::TrySendError<LinkFrame>> {
        if let LinkFrame::Transfer {
            payload,
            queue_slot,
            ..
        } = &mut frame
        {
            if queue_slot.is_none() {
                let size = payload.len().saturating_add(1024);
                if size > RETAINED_BYTES {
                    return Err(mpsc::error::TrySendError::Full(frame));
                }
                match self.bytes.clone().try_acquire_many_owned(size as u32) {
                    Ok(permit) => *queue_slot = Some(permit),
                    Err(_) => return Err(mpsc::error::TrySendError::Full(frame)),
                }
            }
        }
        self.tx.try_send(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        endpoint::{InputHandle, OutgoingChannel, Session as _},
        session::{Session, SessionState},
        Payload,
    };
    use fe2o3_amqp_types::performatives::Detach;
    use std::sync::OnceLock;
    fn transfer(id: u32) -> LinkFrame {
        let mut performative: fe2o3_amqp_types::performatives::Transfer =
            serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x55, 0x43])
                .unwrap();
        performative.delivery_id = Some(id);
        performative.delivery_tag = Some(vec![id as u8].into());
        LinkFrame::Transfer {
            input_handle: InputHandle(0),
            performative,
            payload: Payload::from_static(b"data"),
            window_slot: None,
            queue_slot: None,
        }
    }
    #[tokio::test]
    async fn zero_peer_window_keeps_a_bounded_backlog_without_blocking_controls() {
        let (tx, mut rx) = mpsc::channel(2);
        let bytes = Arc::new(Semaphore::new(2 * 1028));
        let writer = Sender {
            tx,
            bytes: bytes.clone(),
        };
        let mut session = Session::builder().into_session(
            OutgoingChannel(0),
            SessionState::Mapped,
            Arc::new(OnceLock::new()),
        );
        for id in 0..2 {
            writer.send(transfer(id)).await.unwrap();
            let LinkFrame::Transfer {
                input_handle,
                performative,
                payload,
                queue_slot,
                ..
            } = rx.recv().await.unwrap()
            else {
                panic!()
            };
            assert!(session
                .on_outgoing_transfer(input_handle, performative, payload, queue_slot)
                .unwrap()
                .is_none());
        }
        assert_eq!(bytes.available_permits(), 0);
        assert_eq!(session.remote_incoming_window_exhausted_buffer.len(), 2);
        let waiting = writer.send(transfer(2));
        tokio::pin!(waiting);
        assert!(matches!(
            futures_util::poll!(&mut waiting),
            std::task::Poll::Pending
        ));
        // Controls never compete for the payload budget and the session driver
        // can consume them while waiting for an explicit peer window update.
        writer
            .send(LinkFrame::Detach(Detach {
                handle: 1u32.into(),
                closed: true,
                error: None,
            }))
            .await
            .unwrap();
        assert!(matches!(rx.recv().await.unwrap(), LinkFrame::Detach(_)));
        session.remote_incoming_window = 2;
        let frames = session
            .prepare_session_frames_from_buffered_transfers(Vec::new())
            .unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(
            bytes.available_permits(),
            0,
            "forwarding to the connection retains the reservation"
        );
        assert!(matches!(
            futures_util::poll!(&mut waiting),
            std::task::Poll::Pending
        ));
        drop(frames);
        waiting.await.unwrap();
        drop(rx.recv().await.unwrap());
        assert_eq!(bytes.available_permits(), 2 * 1028);
    }
    #[tokio::test]
    async fn cancellation_and_session_stop_release_waiters_and_byte_reservations() {
        let (tx, mut rx) = mpsc::channel(1);
        let bytes = Arc::new(Semaphore::new(1028));
        let writer = Sender {
            tx,
            bytes: bytes.clone(),
        };
        writer.send(transfer(0)).await.unwrap();
        let retained = rx.recv().await.unwrap();
        {
            let waiting = writer.send(transfer(1));
            tokio::pin!(waiting);
            assert!(matches!(
                futures_util::poll!(&mut waiting),
                std::task::Poll::Pending
            ));
        }
        assert_eq!(bytes.available_permits(), 0);
        let waiting = writer.send(transfer(2));
        tokio::pin!(waiting);
        assert!(matches!(
            futures_util::poll!(&mut waiting),
            std::task::Poll::Pending
        ));
        drop(rx);
        assert!(
            waiting.await.is_err(),
            "closed session must wake even while its retained payload still owns all bytes"
        );
        drop(retained);
        assert_eq!(bytes.available_permits(), 1028);
    }
}
