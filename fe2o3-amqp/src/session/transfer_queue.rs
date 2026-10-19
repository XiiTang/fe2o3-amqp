//! Bound encoded transfers while a peer closes its session receive window.
//! Waiting publishers retain their own input; the session driver keeps running.
use crate::link::LinkFrame;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
const RETAINED_BYTES: usize = 32 * 1024 * 1024;
/// What a transfer frame retains beyond its payload.
const FRAME_OVERHEAD: usize = 1024;
/// The largest payload one transfer frame may carry into the queue. A link
/// splits every delivery into frames no larger than this and the peer's
/// max-frame-size (`SenderLink::split_transfer`), so the queue never has to
/// refuse a frame of a message the link may carry.
pub(crate) const MAX_FRAME_PAYLOAD: usize = RETAINED_BYTES - FRAME_OVERHEAD;

#[derive(Clone, Debug)]
pub(crate) struct Sender {
    tx: mpsc::Sender<LinkFrame>,
    bytes: Arc<Semaphore>,
}
/// The queue refused a transfer frame: it is closed.
#[derive(Debug)]
pub(crate) struct Refused;
/// Room the queue holds for one transfer frame.
#[derive(Debug)]
pub(crate) struct Room {
    bytes: tokio::sync::OwnedSemaphorePermit,
    slot: mpsc::OwnedPermit<LinkFrame>,
}
impl Room {
    /// Hands `frame`, a transfer, to the queue.
    pub(crate) fn send(self, mut frame: LinkFrame) {
        if let LinkFrame::Transfer { queue_slot, .. } = &mut frame {
            *queue_slot = Some(self.bytes);
        }
        self.slot.send(frame);
    }
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
    pub async fn send(&self, frame: LinkFrame) -> Result<(), mpsc::error::SendError<LinkFrame>> {
        if let LinkFrame::Transfer {
            payload,
            queue_slot: None,
            ..
        } = &frame
        {
            return match self.reserve(payload.len()).await {
                Ok(room) => {
                    room.send(frame);
                    Ok(())
                }
                Err(Refused) => Err(mpsc::error::SendError(frame)),
            };
        }
        self.tx.send(frame).await
    }
    /// Room for one transfer frame of `payload_len` bytes: its share of the
    /// retained bytes, then a queue slot. Holding it takes nothing else; it
    /// is given back when dropped unsent.
    pub(crate) async fn reserve(&self, payload_len: usize) -> Result<Room, Refused> {
        let size = retained(payload_len).ok_or(Refused)?;
        let bytes = tokio::select! {
            _ = self.tx.closed() => return Err(Refused),
            permit = self.bytes.clone().acquire_many_owned(size as u32) => permit.map_err(|_| Refused)?,
        };
        let slot = self.tx.clone().reserve_owned().await.map_err(|_| Refused)?;
        Ok(Room { bytes, slot })
    }
    #[allow(
        clippy::result_large_err,
        reason = "preserve the unsent inline frame without allocating on queue refusal"
    )]
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
                let Some(size) = retained(payload.len()) else {
                    return Err(mpsc::error::TrySendError::Full(frame));
                };
                match self.bytes.clone().try_acquire_many_owned(size as u32) {
                    Ok(permit) => *queue_slot = Some(permit),
                    Err(_) => return Err(mpsc::error::TrySendError::Full(frame)),
                }
            }
        }
        self.tx.try_send(frame)
    }
}

/// The bytes a transfer frame of `payload_len` retains. Every frame was split
/// to at most `MAX_FRAME_PAYLOAD`; a larger one could never get its bytes, so
/// it is refused rather than left waiting.
fn retained(payload_len: usize) -> Option<usize> {
    debug_assert!(
        payload_len <= MAX_FRAME_PAYLOAD,
        "a transfer frame of {payload_len} bytes bypassed the link's frame split"
    );
    (payload_len <= MAX_FRAME_PAYLOAD).then_some(payload_len + FRAME_OVERHEAD)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        endpoint::{AttachmentId, InputHandle, OutgoingChannel, Session as _},
        session::{Session, SessionState},
        Payload,
    };
    use fe2o3_amqp_types::performatives::Detach;
    use std::sync::OnceLock;
    /// A transfer on output handle 0, of `attachment`.
    fn transfer_of(attachment: AttachmentId, id: u32) -> LinkFrame {
        let mut performative: fe2o3_amqp_types::performatives::Transfer =
            serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x55, 0x43])
                .unwrap();
        performative.delivery_id = Some(id);
        performative.delivery_tag = Some(vec![id as u8].into());
        LinkFrame::Transfer {
            input_handle: InputHandle(0),
            attachment,
            performative,
            payload: Payload::from_static(b"data"),
            window_slot: None,
            queue_slot: None,
        }
    }
    /// A transfer for queue tests, which no session sees.
    fn transfer(id: u32) -> LinkFrame {
        transfer_of(AttachmentId::allocate(), id)
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
        let attachment = session.allocate_link("link".into(), None).unwrap();
        for id in 0..2 {
            writer.send(transfer_of(attachment.id, id)).await.unwrap();
            let LinkFrame::Transfer {
                input_handle,
                attachment,
                performative,
                payload,
                queue_slot,
                ..
            } = rx.recv().await.unwrap()
            else {
                panic!()
            };
            assert!(session
                .on_outgoing_transfer(input_handle, attachment, performative, payload, queue_slot)
                .unwrap()
                .is_none());
        }
        assert_eq!(bytes.available_permits(), 0);
        assert_eq!(session.remote_incoming_window_exhausted_buffer.len(), 2);
        let waiting = writer.send(transfer_of(attachment.id, 2));
        tokio::pin!(waiting);
        assert!(matches!(
            futures_util::poll!(&mut waiting),
            std::task::Poll::Pending
        ));
        // Controls never compete for the payload budget and the session driver
        // can consume them while waiting for an explicit peer window update.
        writer
            .send(LinkFrame::Detach(
                Detach {
                    handle: 1u32.into(),
                    closed: true,
                    error: None,
                },
                crate::endpoint::AttachmentId::allocate(),
            ))
            .await
            .unwrap();
        assert!(matches!(rx.recv().await.unwrap(), LinkFrame::Detach(_, _)));
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
