//! Exact post-SASL incoming frame observations, produced by the transport decoder.
use crate::frames::amqp::{Frame, FrameBody};
use bytes::{BufMut, Bytes, BytesMut};
use fe2o3_amqp_types::performatives::Performative;
use std::sync::Arc;
/// One admitted frame. Raw bytes preserve original constructors and map ordering.
pub struct IncomingFrame {
    /// Entire wire frame including its four-byte size field.
    pub bytes: Bytes,
    /// Remote session channel.
    pub channel: u16,
    /// Decoded performative; None for an empty heartbeat.
    pub performative: Option<Performative>,
    payload_offset: usize,
}
impl std::fmt::Debug for IncomingFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingFrame")
            .field("length", &self.bytes.len())
            .field("channel", &self.channel)
            .finish()
    }
}
impl IncomingFrame {
    /// Decode exactly one complete wire frame using the transport's codec.
    /// This entry point accepts already bounded input and never reads a socket.
    pub fn decode(bytes: Bytes) -> Result<Self, std::io::Error> {
        use tokio_util::codec::Decoder;
        let invalid = || std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid AMQP frame");
        if bytes.len() < 8
            || usize::try_from(u32::from_be_bytes(
                bytes[..4].try_into().map_err(|_| invalid())?,
            ))
            .ok()
                != Some(bytes.len())
        {
            return Err(invalid());
        }
        let body = bytes.slice(4..);
        let frame = crate::frames::amqp::FrameDecoder {}
            .decode(&mut BytesMut::from(body.as_ref()))
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?;
        Ok(Self::from_decoded(body, &frame))
    }

    /// Unmodified transfer payload. Other frames have an empty payload.
    pub fn payload(&self) -> &[u8] {
        &self.bytes[self.payload_offset..]
    }
    pub(crate) fn from_decoded(body: Bytes, frame: &Frame) -> Self {
        let mut bytes = BytesMut::with_capacity(body.len() + 4);
        bytes.put_u32((body.len() + 4) as u32);
        bytes.extend_from_slice(&body);
        let mut payload_len = 0;
        let performative = match &frame.body {
            FrameBody::Open(p) => Some(Performative::Open(p.clone())),
            FrameBody::Begin(p) => Some(Performative::Begin(p.clone())),
            FrameBody::Attach(p) => Some(Performative::Attach(p.clone())),
            FrameBody::Flow(p) => Some(Performative::Flow(p.clone())),
            FrameBody::Transfer {
                performative,
                payload,
            } => {
                payload_len = payload.len();
                Some(Performative::Transfer(performative.clone()))
            }
            FrameBody::Disposition(p) => Some(Performative::Disposition(p.clone())),
            FrameBody::Detach(p) => Some(Performative::Detach(p.clone())),
            FrameBody::End(p) => Some(Performative::End(p.clone())),
            FrameBody::Close(p) => Some(Performative::Close(p.clone())),
            FrameBody::Empty => None,
        };
        let payload_offset = bytes.len() - payload_len;
        Self {
            bytes: bytes.freeze(),
            channel: frame.channel,
            performative,
            payload_offset,
        }
    }
}
/// Synchronous observation hook. Implementations must not block or perform I/O.
/// A bounded observer can end its own delivery while the protocol engine continues.
/// SASL tokens and TLS material are never delivered through this hook.
pub trait IncomingFrameObserver: Send + Sync + std::fmt::Debug {
    /// The peer's successfully validated post-SASL AMQP 1.0 protocol header.
    fn protocol_header(&self, _header: [u8; 8]) {}
    /// Observe an admitted incoming AMQP frame before the engine processes it.
    fn incoming(&self, frame: Arc<IncomingFrame>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::sync::Mutex;
    use tokio::io::AsyncWriteExt;
    #[derive(Debug, Default)]
    struct Observer(Mutex<Vec<Arc<IncomingFrame>>>);
    impl IncomingFrameObserver for Observer {
        fn incoming(&self, f: Arc<IncomingFrame>) {
            self.0.lock().unwrap().push(f);
        }
    }
    #[tokio::test]
    async fn hook_preserves_extended_header_original_constructors_channel_and_binary_payload() {
        let raw = [
            0, 0, 0, 25, 3, 0, 0, 9, 1, 2, 3, 4, 0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1,
            0x55, 0x43, 0x42,
        ];
        let (io, mut peer) = tokio::io::duplex(64);
        let observer = Arc::new(Observer::default());
        let mut transport = super::super::Transport::<_, Frame>::bind(io, 512, None);
        transport.set_incoming_frame_observer(Some(observer.clone()));
        peer.write_all(&raw).await.unwrap();
        let frame = transport.next().await.unwrap().unwrap();
        assert_eq!(frame.channel, 9);
        let observed = observer.0.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].bytes.as_ref(), raw);
        assert_eq!(observed[0].payload(), [0x42]);
        assert!(matches!(
            observed[0].performative,
            Some(Performative::Transfer(_))
        ));
    }
}
