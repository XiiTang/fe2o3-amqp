//! A connection engine's first failure is how it ended, whatever the cleanup
//! after it meets: a peer that ends its stream instead of answering the
//! engine's Close does not turn a protocol violation into that stream's end,
//! and a stream that ends on its own is still that end (Boundless, AMQP 1.0
//! stop reasons).
//!
//! The peer is scripted over an in-memory `tokio::io::duplex` stream.

use std::time::Duration;

use bytes::{BufMut, BytesMut};
use fe2o3_amqp::{
    connection::{Connection, Error, OpenError},
    frames::amqp::{Frame, FrameBody, FrameDecoder},
    transport,
    types::{
        definitions::{AmqpError, ErrorCondition},
        performatives::{Begin, End, Open},
    },
};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio_util::codec::Decoder;

const HEADER: [u8; 8] = *b"AMQP\x00\x01\x00\x00";

/// The peer's end of the connection.
struct Peer {
    io: DuplexStream,
}

impl Peer {
    async fn write(&mut self, channel: u16, performative: &impl Serialize) {
        let body = serde_amqp::to_vec(performative).unwrap();
        let mut frame = BytesMut::new();
        frame.put_u32(8 + body.len() as u32);
        frame.put_u8(2);
        frame.put_u8(0);
        frame.put_u16(channel);
        frame.put_slice(&body);
        self.io.write_all(&frame).await.unwrap();
    }

    /// The next frame, or `None` once the client's stream ended.
    async fn read(&mut self) -> Option<FrameBody> {
        let size = self.io.read_u32().await.ok()? as usize;
        let mut frame = BytesMut::zeroed(size - 4);
        self.io.read_exact(&mut frame).await.ok()?;
        let Frame { body, .. } = FrameDecoder {}.decode(&mut frame).unwrap().unwrap();
        Some(body)
    }

    /// Answers the protocol header and the client's Open.
    async fn open(io: DuplexStream) -> Self {
        let mut peer = Self { io };
        let mut header = [0; 8];
        peer.io.read_exact(&mut header).await.unwrap();
        assert_eq!(header, HEADER);
        peer.io.write_all(&HEADER).await.unwrap();
        assert!(matches!(peer.read().await, Some(FrameBody::Open(_))));
        peer.write(0, &open()).await;
        peer
    }

    /// The condition of the Close the client sends.
    async fn close_condition(&mut self) -> Option<ErrorCondition> {
        loop {
            match self.read().await? {
                FrameBody::Close(close) => return close.error.map(|error| error.condition),
                _ => continue,
            }
        }
    }
}

fn open() -> Open {
    Open {
        container_id: "peer".into(),
        hostname: None,
        max_frame_size: Default::default(),
        channel_max: Default::default(),
        idle_time_out: None,
        outgoing_locales: None,
        incoming_locales: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

/// What the peer sends on an opened connection before it ends its stream.
#[derive(Clone, Copy, Debug)]
enum Sent {
    /// Nothing: the stream just ends.
    Nothing,
    /// A second Open, illegal on an opened connection.
    SecondOpen,
    /// A Begin that opens a session, which the client does not take.
    RemoteBegin,
    /// An End on a channel no session holds.
    UnknownEnd,
}

/// The peer sends `sent` and ends its stream without answering the client's
/// Close. The client sends the Close its first failure calls for, and its
/// engine ends with that failure, not with the end of the stream that the
/// wait for the peer's Close then meets; with nothing sent, the end of the
/// stream is the engine's end.
async fn ends_with_its_first_failure(sent: Sent) {
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);

    let client = async move {
        let mut connection = Connection::builder()
            .container_id("client")
            .open_with_stream(client_io)
            .await
            .unwrap();
        let ended = connection.on_close().await;
        connection.stop_and_join().await.unwrap();
        ended
    };

    let peer = async move {
        let mut peer = Peer::open(peer_io).await;
        match sent {
            Sent::Nothing => {}
            Sent::SecondOpen => peer.write(0, &open()).await,
            Sent::RemoteBegin => {
                let begin = Begin {
                    remote_channel: None,
                    next_outgoing_id: 0,
                    incoming_window: 10,
                    outgoing_window: 10,
                    handle_max: Default::default(),
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                };
                peer.write(0, &begin).await;
            }
            Sent::UnknownEnd => peer.write(5, &End { error: None }).await,
        }
        peer.io.shutdown().await.unwrap();
        peer.close_condition().await
    };

    let (ended, condition) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(client, peer)
    })
    .await
    .expect("the exchange stalled");

    let expected = match sent {
        Sent::Nothing | Sent::SecondOpen => AmqpError::IllegalState,
        Sent::RemoteBegin => AmqpError::NotImplemented,
        Sent::UnknownEnd => AmqpError::NotFound,
    };
    assert_eq!(
        condition,
        Some(ErrorCondition::AmqpError(expected)),
        "{sent:?}"
    );
    match (sent, ended) {
        (Sent::Nothing, Err(Error::TransportError(transport::Error::Io(io))))
            if io.kind() == std::io::ErrorKind::UnexpectedEof => {}
        (Sent::SecondOpen, Err(Error::IllegalState))
        | (Sent::RemoteBegin, Err(Error::NotImplemented(_)))
        | (Sent::UnknownEnd, Err(Error::NotFound(_))) => {}
        (sent, ended) => panic!("{sent:?}: the engine ended with {ended:?}"),
    }
}

#[tokio::test]
async fn a_stream_that_ends_on_its_own_is_the_engine_s_end() {
    ends_with_its_first_failure(Sent::Nothing).await;
}

#[tokio::test]
async fn an_illegal_frame_before_the_stream_s_end_is_the_engine_s_end() {
    ends_with_its_first_failure(Sent::SecondOpen).await;
}

#[tokio::test]
async fn an_unsupported_frame_before_the_stream_s_end_is_the_engine_s_end() {
    ends_with_its_first_failure(Sent::RemoteBegin).await;
}

#[tokio::test]
async fn a_frame_for_no_session_before_the_stream_s_end_is_the_engine_s_end() {
    ends_with_its_first_failure(Sent::UnknownEnd).await;
}

/// The peer answers the client's Open with a Begin and ends its stream: the
/// opening fails as the illegal frame made it, not with the end of the
/// stream that its Close then meets.
#[tokio::test]
async fn an_opening_ends_with_its_first_failure() {
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::builder()
        .container_id("client")
        .open_with_stream(client_io);
    let peer = async move {
        let mut peer = Peer { io: peer_io };
        let mut header = [0; 8];
        peer.io.read_exact(&mut header).await.unwrap();
        peer.io.write_all(&HEADER).await.unwrap();
        assert!(matches!(peer.read().await, Some(FrameBody::Open(_))));
        let begin = Begin {
            remote_channel: None,
            next_outgoing_id: 0,
            incoming_window: 10,
            outgoing_window: 10,
            handle_max: Default::default(),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        peer.write(0, &begin).await;
        peer.io.shutdown().await.unwrap();
        loop {
            match peer.read().await {
                Some(FrameBody::Close(close)) => return Some(close),
                Some(_) => continue,
                None => return None,
            }
        }
    };
    let (opened, close) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(client, peer)
    })
    .await
    .expect("the exchange stalled");
    assert!(close.is_some(), "the client closes the connection");
    assert!(matches!(opened, Err(OpenError::IllegalState)), "{opened:?}");
}
