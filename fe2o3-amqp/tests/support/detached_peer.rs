#![allow(dead_code, unused_imports)]
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use fe2o3_amqp::{
    connection::{Connection, ConnectionHandle},
    frames::amqp::{Frame, FrameBody, FrameDecoder},
    link::{DetachError, LinkStateError, SendError},
    session::{Session, SessionHandle},
    types::{
        definitions::{AmqpError, DeliveryTag, Error, Handle, Role},
        messaging::{Accepted, AmqpValue, DeliveryState, Message, Outcome},
        performatives::{Attach, Begin, Detach, Disposition, Flow, Open, Transfer},
        primitives::{Binary, OrderedMap},
    },
    Receiver, Sendable, Sender,
};
use serde::Serialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::oneshot,
};
use tokio_util::codec::Decoder;

const HEADER: [u8; 8] = *b"AMQP\x00\x01\x00\x00";

/// The peer's end of the connection.
pub(super) struct Peer {
    io: DuplexStream,
}

impl Peer {
    pub(super) async fn write(&mut self, channel: u16, performative: &impl Serialize) {
        let body = serde_amqp::to_vec(performative).unwrap();
        let mut frame = BytesMut::new();
        frame.put_u32(8 + body.len() as u32);
        frame.put_u8(2);
        frame.put_u8(0);
        frame.put_u16(channel);
        frame.put_slice(&body);
        self.io.write_all(&frame).await.unwrap();
    }

    pub(super) async fn read(&mut self) -> FrameBody {
        let size = self.io.read_u32().await.unwrap() as usize;
        let mut frame = BytesMut::zeroed(size - 4);
        self.io.read_exact(&mut frame).await.unwrap();
        let Frame { body, .. } = FrameDecoder {}.decode(&mut frame).unwrap().unwrap();
        body
    }

    /// Reads frames until one `pick` takes, failing on any `forbid` matches.
    pub(super) async fn read_until<T>(
        &mut self,
        mut pick: impl FnMut(&FrameBody) -> Option<T>,
        forbid: impl Fn(&FrameBody) -> bool,
    ) -> T {
        loop {
            let body = self.read().await;
            assert!(!forbid(&body), "unexpected frame: {body:?}");
            if let Some(picked) = pick(&body) {
                return picked;
            }
        }
    }

    /// Answers the protocol header, Open and Begin, taking frames of up to
    /// `max_frame_size` bytes (unbounded when `None`) and opening its session
    /// window to `incoming_window` transfers.
    pub(super) async fn open(
        io: DuplexStream,
        max_frame_size: Option<u32>,
        incoming_window: u32,
    ) -> Self {
        let mut peer = Self { io };
        let mut header = [0; 8];
        peer.io.read_exact(&mut header).await.unwrap();
        assert_eq!(header, HEADER);
        peer.io.write_all(&HEADER).await.unwrap();
        assert!(matches!(peer.read().await, FrameBody::Open(_)));
        peer.write(
            0,
            &Open {
                container_id: "peer".into(),
                hostname: None,
                max_frame_size: max_frame_size.map(Into::into).unwrap_or_default(),
                channel_max: Default::default(),
                idle_time_out: None,
                outgoing_locales: None,
                incoming_locales: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            },
        )
        .await;
        assert!(matches!(peer.read().await, FrameBody::Begin(_)));
        peer.write(
            0,
            &Begin {
                remote_channel: Some(0),
                next_outgoing_id: 0,
                incoming_window,
                outgoing_window: 100,
                handle_max: Default::default(),
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            },
        )
        .await;
        peer
    }

    /// Answers the client's next attach as its receiver on the same handle,
    /// with `unsettled` as the peer's record of the link's deliveries.
    /// Returns the client's attach.
    pub(super) async fn answer_attach(&mut self, unsettled: Option<Unsettled>) -> Attach {
        let FrameBody::Attach(attach) = self.read().await else {
            panic!("expected an attach")
        };
        self.write(
            0,
            &Attach {
                role: Role::Receiver,
                initial_delivery_count: None,
                unsettled,
                incomplete_unsettled: false,
                ..attach.clone()
            },
        )
        .await;
        attach
    }

    /// Answers the client's next attach, a receiver's, as its sender on the
    /// same handle, with `unsettled` as the peer's record of the link's
    /// deliveries, and reads the receiver's credit. Returns the client's
    /// attach.
    pub(super) async fn answer_receiver_attach(&mut self, unsettled: Option<Unsettled>) -> Attach {
        let FrameBody::Attach(attach) = self.read().await else {
            panic!("expected an attach")
        };
        assert_eq!(attach.role, Role::Receiver);
        self.write(
            0,
            &Attach {
                role: Role::Sender,
                initial_delivery_count: Some(0),
                unsettled,
                incomplete_unsettled: false,
                ..attach.clone()
            },
        )
        .await;
        let handle = attach.handle.clone();
        self.read_until(
            |body| {
                matches!(body, FrameBody::Flow(f) if f.handle.as_ref() == Some(&handle))
                    .then_some(())
            },
            |_| false,
        )
        .await;
        attach
    }

    /// Sends an unsettled delivery of the string `value` on `handle`.
    pub(super) async fn transfer(
        &mut self,
        handle: &Handle,
        delivery_id: u32,
        tag: &DeliveryTag,
        value: &str,
    ) {
        let mut body = serde_amqp::to_vec(&Transfer {
            handle: handle.clone(),
            delivery_id: Some(delivery_id),
            delivery_tag: Some(tag.clone()),
            message_format: Some(0),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        })
        .unwrap();
        body.extend(serde_amqp::to_vec(&AmqpValue(value)).unwrap());
        let mut frame = BytesMut::new();
        frame.put_u32(8 + body.len() as u32);
        frame.put_u8(2);
        frame.put_u8(0);
        frame.put_u16(0);
        frame.put_slice(&body);
        self.io.write_all(&frame).await.unwrap();
    }

    /// Answers the client's next attach, granting it `credit` and keeping the
    /// session window at `incoming_window`.
    pub(super) async fn attach_receiver(&mut self, credit: u32, incoming_window: u32) -> Handle {
        let handle = self.answer_attach(None).await.handle;
        self.write(0, &flow(Some((handle.clone(), 0, credit)), incoming_window))
            .await;
        handle
    }

    /// Reads the client's detach, collecting the transfers before it, and
    /// answers it with a suspending one.
    pub(super) async fn answer_detach(&mut self) -> Vec<(Transfer, usize)> {
        let mut transfers = Vec::new();
        loop {
            match self.read().await {
                FrameBody::Transfer {
                    performative,
                    payload,
                } => transfers.push((performative, payload.len())),
                FrameBody::Detach(detach) => {
                    assert!(!detach.closed);
                    self.write(0, &detach).await;
                    return transfers;
                }
                _ => {}
            }
        }
    }
}

pub(super) type Unsettled = OrderedMap<DeliveryTag, Option<DeliveryState>>;

/// Stops and joins the client's engines, its session's first: neither ended
/// with a failure of its own.
pub(super) async fn stop_and_join(
    mut connection: ConnectionHandle<()>,
    mut session: SessionHandle<()>,
) {
    session.stop_and_join().await.unwrap();
    connection.stop_and_join().await.unwrap();
}

/// A flow opening the peer's session window to `incoming_window` transfers,
/// and granting `credit` to a link given as `(handle, delivery_count, credit)`.
pub(super) fn flow(link: Option<(Handle, u32, u32)>, incoming_window: u32) -> Flow {
    Flow {
        next_incoming_id: Some(0),
        incoming_window,
        next_outgoing_id: 0,
        outgoing_window: 100,
        handle: link.as_ref().map(|(handle, ..)| handle.clone()),
        delivery_count: link.as_ref().map(|(_, count, _)| *count),
        link_credit: link.as_ref().map(|(.., credit)| *credit),
        available: None,
        drain: false,
        echo: false,
        properties: None,
    }
}
