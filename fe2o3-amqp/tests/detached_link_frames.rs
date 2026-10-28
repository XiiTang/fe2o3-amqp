//! Frames of a link attachment the peer detached do not reach the wire after
//! the session's reply, and the deliveries of a closed link fail with the
//! peer's close (Boundless #87).
//!
//! The peer is scripted over an in-memory `tokio::io::duplex` stream: it reads
//! and writes AMQP frames itself, so the test controls the session window and
//! sees every frame the client writes, in order.

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

    async fn read(&mut self) -> FrameBody {
        let size = self.io.read_u32().await.unwrap() as usize;
        let mut frame = BytesMut::zeroed(size - 4);
        self.io.read_exact(&mut frame).await.unwrap();
        let Frame { body, .. } = FrameDecoder {}.decode(&mut frame).unwrap().unwrap();
        body
    }

    /// Reads frames until one `pick` takes, failing on any `forbid` matches.
    async fn read_until<T>(
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
    async fn open(io: DuplexStream, max_frame_size: Option<u32>, incoming_window: u32) -> Self {
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
    async fn answer_attach(&mut self, unsettled: Option<Unsettled>) -> Attach {
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
    async fn answer_receiver_attach(&mut self, unsettled: Option<Unsettled>) -> Attach {
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
    async fn transfer(
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
    async fn attach_receiver(&mut self, credit: u32, incoming_window: u32) -> Handle {
        let handle = self.answer_attach(None).await.handle;
        self.write(0, &flow(Some((handle.clone(), 0, credit)), incoming_window))
            .await;
        handle
    }

    /// Reads the client's detach, collecting the transfers before it, and
    /// answers it with a suspending one.
    async fn answer_detach(&mut self) -> Vec<(Transfer, usize)> {
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

type Unsettled = OrderedMap<DeliveryTag, Option<DeliveryState>>;

/// Stops and joins the client's engines, its session's first: neither ended
/// with a failure of its own.
async fn stop_and_join(mut connection: ConnectionHandle<()>, mut session: SessionHandle<()>) {
    session.stop_and_join().await.unwrap();
    connection.stop_and_join().await.unwrap();
}

/// A flow opening the peer's session window to `incoming_window` transfers,
/// and granting `credit` to a link given as `(handle, delivery_count, credit)`.
fn flow(link: Option<(Handle, u32, u32)>, incoming_window: u32) -> Flow {
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

/// Whether `body` is a link frame on `handle`.
fn on_handle(body: &FrameBody, handle: &Handle) -> bool {
    match body {
        FrameBody::Transfer { performative, .. } => &performative.handle == handle,
        FrameBody::Flow(flow) => flow.handle.as_ref() == Some(handle),
        FrameBody::Detach(detach) => &detach.handle == handle,
        _ => false,
    }
}

/// The peer closes link A while A's delivery waits for the closed session
/// window. A sibling link's delivery, sent after the close and released with
/// A's by the same window update, is the fence: no frame of A comes between
/// the session's reply and the sibling's transfer, A's send fails with the
/// peer's error, and the sibling and its session keep working.
#[tokio::test]
async fn a_transfer_waiting_for_the_window_is_not_written_after_the_reply_to_a_close() {
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let error = Error::new(
        AmqpError::InternalError,
        Some("closed by peer".into()),
        None,
    );
    let (a_handed_over, a_sent) = oneshot::channel();
    let (s_handed_over, s_sent) = oneshot::channel();

    let client = {
        let error = error.clone();
        async move {
            let mut connection = Connection::builder()
                .container_id("client")
                .open_with_stream(client_io)
                .await
                .unwrap();
            let mut session = Session::begin(&mut connection).await.unwrap();
            let mut a = Sender::attach(&mut session, "a", "q").await.unwrap();
            let mut s = Sender::attach(&mut session, "s", "q").await.unwrap();

            let delivery = a
                .send_batchable(Sendable::builder().message("a").build())
                .await
                .unwrap();
            a_handed_over.send(()).unwrap();
            match delivery.await {
                Err(SendError::LinkStateError(LinkStateError::RemoteClosedWithError(e))) => {
                    assert_eq!(e, error)
                }
                other => panic!("expected the peer's close error, got {other:?}"),
            }
            // Dropping A queues its own detach for the attachment that ended.
            drop(a);

            let delivery = s
                .send_batchable(Sendable::builder().message("s").build())
                .await
                .unwrap();
            s_handed_over.send(()).unwrap();
            let outcome = delivery.await.unwrap();
            assert!(matches!(outcome, Outcome::Accepted(_)));
            (connection, session, s)
        }
    };

    let peer = async move {
        let mut peer = Peer::open(peer_io, None, 0).await;
        let a = peer.attach_receiver(10, 0).await;
        let s = peer.attach_receiver(10, 0).await;
        a_sent.await.unwrap();
        peer.write(
            0,
            &Detach {
                handle: a.clone(),
                closed: true,
                error: Some(error),
            },
        )
        .await;
        peer.read_until(
            |body| matches!(body, FrameBody::Detach(d) if d.handle == a).then_some(()),
            |body| matches!(body, FrameBody::Transfer { .. }),
        )
        .await;
        s_sent.await.unwrap();
        peer.write(0, &flow(None, 10)).await;
        let delivery_id = peer
            .read_until(
                |body| match body {
                    FrameBody::Transfer { performative, .. } if performative.handle == s => {
                        performative.delivery_id
                    }
                    _ => None,
                },
                |body| on_handle(body, &a),
            )
            .await;
        peer.write(
            0,
            &Disposition {
                role: Role::Receiver,
                first: delivery_id,
                last: None,
                settled: true,
                state: Some(DeliveryState::Accepted(Accepted {})),
                batchable: false,
            },
        )
        .await;
        peer
    };

    let ((connection, session, _s), _peer) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(client, peer)
    })
    .await
    .expect("the exchange stalled");
    stop_and_join(connection, session).await;
}

/// How far A's delivery got when the peer paused A.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Paused {
    /// Registered, but waiting for the closed session window: the peer has
    /// no record of it, and it is sent again as a new delivery.
    NeverWritten,
    /// Its first frame written and a later one waiting for the session
    /// queue's room: the peer holds its tag with a null state, and it is
    /// resumed whole under that tag.
    LaterFragment,
    /// Written whole: the peer holds its tag with a null state, and it is
    /// resumed under that tag.
    Written,
}

/// A body spanning more frames of `FRAME` bytes than the session queue
/// retains.
const BIG: usize = 40 * 1024 * 1024;
const FRAME: u32 = 64 * 1024;

/// The peer pauses link A with one delivery outstanding, then resumes it.
/// Nothing of the paused attachment reaches the wire after the reply; the
/// resume sends the delivery once, whole, as AMQP 1.0 §2.6.13 says for the
/// peer's record of it, and resuming under the original tag takes no new
/// credit. The delivery's outcome is the one the peer reports for it.
#[allow(
    deprecated,
    reason = "`Sender::on_detach` still reports a pause as `DetachedByRemote`"
)]
async fn pause_and_resume(paused: Paused) {
    let (client_io, peer_io) = tokio::io::duplex(1024 * 1024);
    let (handed_over, sent) = oneshot::channel();
    let body = if paused == Paused::LaterFragment {
        BIG
    } else {
        1
    };

    let client = async move {
        let mut connection = Connection::builder()
            .container_id("client")
            .open_with_stream(client_io)
            .await
            .unwrap();
        let mut session = Session::begin(&mut connection).await.unwrap();
        let mut a = Sender::attach(&mut session, "a", "q").await.unwrap();
        let message = Message::builder()
            .data(Binary::from(vec![7u8; body]))
            .build();
        // A pause during the hand-over ends it, leaving the delivery
        // pending; otherwise the link reads the pause after it.
        let delivery = a
            .send_batchable(Sendable::builder().message(message).build())
            .await
            .unwrap();
        if paused != Paused::LaterFragment {
            handed_over.send(()).unwrap();
            assert!(matches!(a.on_detach().await, DetachError::DetachedByRemote));
        }
        let mut a = a.into_detached().unwrap().resume().await.unwrap();
        assert!(matches!(delivery.await.unwrap(), Outcome::Accepted(_)));
        let next = a
            .send_batchable(Sendable::builder().message("next").build())
            .await
            .unwrap();
        (connection, session, a, next)
    };

    let peer = async move {
        let (max_frame_size, window) = match paused {
            Paused::NeverWritten => (None, 0),
            Paused::LaterFragment => (Some(FRAME), 1),
            Paused::Written => (None, 10),
        };
        let mut peer = Peer::open(peer_io, max_frame_size, window).await;
        let a = peer.attach_receiver(1, window).await;
        let tag = DeliveryTag::from(vec![0; 4]);
        if paused == Paused::NeverWritten {
            sent.await.unwrap();
        } else {
            let FrameBody::Transfer { performative, .. } = peer.read().await else {
                panic!("expected A's transfer")
            };
            assert_eq!(performative.delivery_tag.as_ref(), Some(&tag));
            assert_eq!(performative.more, paused == Paused::LaterFragment);
            if paused == Paused::Written {
                sent.await.unwrap();
            }
        }
        peer.write(
            0,
            &Detach {
                handle: a.clone(),
                closed: false,
                error: None,
            },
        )
        .await;
        peer.read_until(
            |body| matches!(body, FrameBody::Detach(d) if d.handle == a).then_some(()),
            |body| matches!(body, FrameBody::Transfer { .. }),
        )
        .await;

        let only = |tag: &DeliveryTag, state: Option<DeliveryState>| {
            let mut map = Unsettled::new();
            map.insert(tag.clone(), state);
            map
        };
        let record = (paused != Paused::NeverWritten).then(|| only(&tag, None));
        let attach = peer.answer_attach(record).await;
        assert_eq!(attach.unsettled, Some(only(&tag, None)));
        // Only a delivery sent again as a new one takes credit; with none, a
        // resume that took any would wait for it.
        let credit = (paused == Paused::NeverWritten).then(|| (attach.handle.clone(), 1, 1));
        peer.write(0, &flow(credit, 100_000)).await;
        let transfers = peer.answer_detach().await;
        let (first, _) = &transfers[0];
        let resent = match paused {
            Paused::NeverWritten => DeliveryTag::from(vec![0, 0, 0, 1]),
            Paused::LaterFragment | Paused::Written => tag.clone(),
        };
        assert_eq!(first.delivery_tag.as_ref(), Some(&resent), "{paused:?}");
        assert_eq!(first.resume, paused != Paused::NeverWritten, "{paused:?}");
        assert!(
            transfers[1..]
                .iter()
                .all(|(transfer, _)| transfer.delivery_tag.is_none()),
            "{paused:?}: one delivery"
        );
        assert!(!transfers.last().unwrap().0.more);
        let len: usize = transfers.iter().map(|(_, len)| len).sum();
        assert!((body..body + 64).contains(&len), "{paused:?}: sent whole");

        let attach = peer
            .answer_attach(Some(only(&resent, Some(Accepted {}.into()))))
            .await;
        assert_eq!(attach.unsettled, Some(only(&resent, None)));
        assert!(peer.answer_detach().await.is_empty());
        let attach = peer.answer_attach(None).await;
        assert!(attach.unsettled.is_none_or(|map| map.is_empty()));

        // The next delivery's tag counts the credit taken so far.
        let count = if paused == Paused::NeverWritten { 2 } else { 1 };
        peer.write(0, &flow(Some((attach.handle, count, 1)), 100_000))
            .await;
        let next = peer
            .read_until(
                |body| match body {
                    FrameBody::Transfer { performative, .. } => performative.delivery_tag.clone(),
                    _ => None,
                },
                |_| false,
            )
            .await;
        assert_eq!(
            next,
            DeliveryTag::from(vec![0, 0, 0, count as u8]),
            "{paused:?}"
        );
        peer
    };

    let ((connection, session, _a, _next), _peer) =
        tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(client, peer)
        })
        .await
        .expect("the exchange stalled");
    stop_and_join(connection, session).await;
}

#[tokio::test]
async fn a_paused_delivery_never_written_is_sent_again_whole_once() {
    pause_and_resume(Paused::NeverWritten).await;
}

#[tokio::test]
async fn a_delivery_paused_in_a_later_fragment_is_resumed_whole_once() {
    pause_and_resume(Paused::LaterFragment).await;
}

#[tokio::test]
async fn a_delivery_the_peer_holds_is_resumed_under_its_tag_without_new_credit() {
    pause_and_resume(Paused::Written).await;
}

/// The peer pauses link A with a delivery unsettled, attaches B on the same
/// handle and then settles A's old delivery-id. The peer's dispositions for a
/// detached attachment no longer route through its handle, as qpid-proton
/// drops a released link's deliveries from its session: B's delivery, under
/// the same tag, keeps its own outcome, and A keeps its delivery for a resume.
#[tokio::test]
async fn a_disposition_for_a_detached_link_does_not_settle_the_handle_s_next_link() {
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let (b_settled, settled) = oneshot::channel();

    let client = async move {
        let mut connection = Connection::builder()
            .container_id("client")
            .open_with_stream(client_io)
            .await
            .unwrap();
        let mut session = Session::begin(&mut connection).await.unwrap();
        let mut a = Sender::attach(&mut session, "a", "q").await.unwrap();
        let mut a_delivery = a
            .send_batchable(Sendable::builder().message("a").build())
            .await
            .unwrap();
        #[allow(
            deprecated,
            reason = "`Sender::on_detach` still reports a pause this way"
        )]
        let paused = matches!(a.on_detach().await, DetachError::DetachedByRemote);
        assert!(paused);
        let mut b = Sender::attach(&mut session, "b", "q").await.unwrap();
        let b_delivery = b
            .send_batchable(Sendable::builder().message("b").build())
            .await
            .unwrap();
        assert!(matches!(b_delivery.await.unwrap(), Outcome::Accepted(_)));
        b_settled.send(()).unwrap();
        assert!(
            futures_util::poll!(&mut a_delivery).is_pending(),
            "A's delivery waits for its resume"
        );
        (connection, session, a, b, a_delivery)
    };

    let peer = async move {
        let mut peer = Peer::open(peer_io, None, 10).await;
        let a = peer.attach_receiver(10, 10).await;
        let FrameBody::Transfer { performative, .. } = peer.read().await else {
            panic!("expected A's transfer")
        };
        let (a_id, a_tag) = (performative.delivery_id.unwrap(), performative.delivery_tag);
        peer.write(
            0,
            &Detach {
                handle: a.clone(),
                closed: false,
                error: None,
            },
        )
        .await;
        peer.read_until(
            |body| matches!(body, FrameBody::Detach(d) if d.handle == a).then_some(()),
            |_| false,
        )
        .await;
        let b = peer.attach_receiver(10, 10).await;
        assert_eq!(b, a, "B reuses A's handle");
        let FrameBody::Transfer { performative, .. } = peer.read().await else {
            panic!("expected B's transfer")
        };
        assert_eq!(performative.delivery_tag, a_tag, "B's tag is A's");
        let settle = |first, state: DeliveryState| Disposition {
            role: Role::Receiver,
            first,
            last: None,
            settled: true,
            state: Some(state),
            batchable: false,
        };
        let rejected = fe2o3_amqp::types::messaging::Rejected { error: None };
        peer.write(0, &settle(a_id, rejected.into())).await;
        peer.write(
            0,
            &settle(performative.delivery_id.unwrap(), Accepted {}.into()),
        )
        .await;
        settled.await.unwrap();
        peer
    };

    let ((connection, session, _a, _b, _a_delivery), _peer) =
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(client, peer)
        })
        .await
        .expect("the exchange stalled");
    stop_and_join(connection, session).await;
}

/// The peer pauses receiving link A with a delivery unsettled, attaches B on
/// the same handle with a delivery under the same tag, and then settles A's
/// old delivery-id as its sender. That disposition no longer routes through
/// the handle: B's retained unsettled delivery is still the one its resume
/// reports.
#[tokio::test]
async fn a_sender_s_disposition_for_a_detached_link_leaves_the_handle_s_next_receiver() {
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let tag = DeliveryTag::from(vec![1]);

    let client = async move {
        let mut connection = Connection::builder()
            .container_id("client")
            .open_with_stream(client_io)
            .await
            .unwrap();
        let mut session = Session::begin(&mut connection).await.unwrap();
        let mut a = Receiver::attach(&mut session, "a", "q").await.unwrap();
        let a_delivery = a.recv::<String>().await.unwrap();
        assert!(a.recv::<String>().await.is_err(), "A is paused");
        let mut b = Receiver::attach(&mut session, "b", "q").await.unwrap();
        let b_delivery = b.recv::<String>().await.unwrap();
        assert!(b.recv::<String>().await.is_err(), "B is paused");
        let b = b.into_detached().unwrap().resume().await.unwrap();
        (connection, session, a, a_delivery, b, b_delivery)
    };

    let peer = async move {
        let mut peer = Peer::open(peer_io, None, 10).await;
        let a = peer.answer_receiver_attach(None).await.handle;
        peer.transfer(&a, 0, &tag, "a").await;
        let pause = |handle: &Handle| Detach {
            handle: handle.clone(),
            closed: false,
            error: None,
        };
        peer.write(0, &pause(&a)).await;
        peer.read_until(
            |body| matches!(body, FrameBody::Detach(d) if d.handle == a).then_some(()),
            |_| false,
        )
        .await;
        let b = peer.answer_receiver_attach(None).await.handle;
        assert_eq!(b, a, "B reuses A's handle");
        peer.transfer(&b, 1, &tag, "b").await;
        peer.write(
            0,
            &Disposition {
                role: Role::Sender,
                first: 0,
                last: None,
                settled: true,
                state: Some(Accepted {}.into()),
                batchable: false,
            },
        )
        .await;
        peer.write(0, &pause(&b)).await;
        peer.read_until(
            |body| matches!(body, FrameBody::Detach(d) if d.handle == b).then_some(()),
            |_| false,
        )
        .await;
        let resumed = peer.answer_receiver_attach(None).await;
        let unsettled = resumed.unsettled.expect("B's unsettled deliveries");
        assert_eq!(unsettled.keys().collect::<Vec<_>>(), [&tag]);
        peer
    };

    let ((connection, session, _a, _a_delivery, _b, _b_delivery), _peer) =
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(client, peer)
        })
        .await
        .expect("the exchange stalled");
    stop_and_join(connection, session).await;
}

/// A cancelled caller does not retract its Attach. The owned resume must consume
/// the late answer and finish suspension before its endpoint can be resumed again.
#[tokio::test]
async fn interrupted_resume_consumes_the_late_attach_and_returns_resumable_custody() {
    use fe2o3_amqp::link::{ReceiverResumeErrorKind, SenderResumeErrorKind};

    for receiving in [false, true] {
        let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
        let (interrupt, interrupted) = oneshot::channel();
        let client = async move {
            let mut connection = Connection::builder()
                .container_id("client")
                .open_with_stream(client_io)
                .await
                .unwrap();
            let mut session = Session::begin(&mut connection).await.unwrap();
            if receiving {
                let receiver = Receiver::attach(&mut session, "resume", "q").await.unwrap();
                let detached = receiver.detach().await.unwrap();
                let before = detached
                    .resume_on_session_until(&session, std::future::ready(()))
                    .await
                    .unwrap_err();
                assert!(matches!(before.kind, ReceiverResumeErrorKind::Interrupted));
                let after = before
                    .detached_recver
                    .resume_on_session_until(&session, async { interrupted.await.unwrap() })
                    .await
                    .unwrap_err();
                assert!(
                    matches!(after.kind, ReceiverResumeErrorKind::Interrupted),
                    "actual receiver cleanup: {:?}",
                    after.kind
                );
                let resumed = after.detached_recver.resume().await.unwrap();
                stop_and_join(connection, session).await;
                drop(resumed);
            } else {
                let sender = Sender::attach(&mut session, "resume", "q").await.unwrap();
                let detached = sender.detach().await.unwrap();
                let before = detached
                    .resume_on_session_until(&session, std::future::ready(()))
                    .await
                    .unwrap_err();
                assert!(matches!(before.kind, SenderResumeErrorKind::Interrupted));
                let after = before
                    .detached_sender
                    .resume_on_session_until(&session, async { interrupted.await.unwrap() })
                    .await
                    .unwrap_err();
                assert!(
                    matches!(after.kind, SenderResumeErrorKind::Interrupted),
                    "actual sender cleanup: {:?}",
                    after.kind
                );
                let resumed = after.detached_sender.resume().await.unwrap();
                stop_and_join(connection, session).await;
                drop(resumed);
            }
        };
        let peer = async move {
            let mut peer = Peer::open(peer_io, None, 10).await;
            if receiving {
                peer.answer_receiver_attach(None).await;
            } else {
                peer.answer_attach(None).await;
            }
            assert!(peer.answer_detach().await.is_empty());
            // A pre-dispatch interruption sends nothing: the next frame is
            // the subsequent attempt's Attach, never an unsolicited Detach.
            let FrameBody::Attach(attach) = peer.read().await else {
                panic!("expected only the next admitted resume Attach")
            };
            interrupt.send(()).unwrap();
            // Hold its answering Attach until cleanup has actually admitted
            // the nonclosing Detach; no sleep or timing assumption is needed.
            let detach = peer
                .read_until(
                    |frame| match frame {
                        FrameBody::Detach(detach) => Some(detach.clone()),
                        _ => None,
                    },
                    |frame| matches!(frame, FrameBody::Transfer { .. }),
                )
                .await;
            assert_eq!(detach.handle, attach.handle);
            assert!(!detach.closed);
            assert!(detach.error.is_none());
            peer.write(
                0,
                &Attach {
                    role: if receiving {
                        Role::Sender
                    } else {
                        Role::Receiver
                    },
                    initial_delivery_count: receiving.then_some(0),
                    ..attach
                },
            )
            .await;
            peer.write(0, &detach).await;
            if receiving {
                peer.answer_receiver_attach(None).await;
            } else {
                peer.answer_attach(None).await;
            }
            peer
        };
        let (_, _peer) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(client, peer)
        })
        .await
        .expect("the interrupted-resume cleanup stalled");
    }
}
