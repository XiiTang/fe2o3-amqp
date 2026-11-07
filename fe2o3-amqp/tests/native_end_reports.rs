//! A native end is reported to its owner before the engine or link sends the
//! reply or cleanup frame that a peer could answer: the Connection's accepted
//! Close or first failure, the Session's accepted End or first failure, and a
//! link's refusal of the peer's Attach answer. An acknowledgment of that
//! cleanup is not reported as a new end (Boundless B).
//!
//! The peer is scripted over an in-memory `tokio::io::duplex` stream. Each
//! check that a report came first runs when the peer has read the client's
//! frame, so the order is the stream's, not a time.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use fe2o3_amqp::{
    connection::{ConnectionInnerError, OpenError},
    frames::amqp::{Frame, FrameBody, FrameDecoder},
    link::{AttachRefusalReport, ReceiverAttachError, SenderAttachError},
    session::{error::SessionInnerError, ResultReport},
    transport::observation::{IncomingFrame, IncomingFrameAction, IncomingFrameObserver},
    types::{
        definitions::{AmqpError, ErrorCondition, Role, SessionError},
        messaging::TargetArchetype,
        performatives::{Attach, Begin, Close, Detach, End, Flow, Open},
    },
    Connection, Receiver, Sender, Session,
};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio_util::codec::Decoder;

const HEADER: [u8; 8] = *b"AMQP\x00\x01\x00\x00";

type Reports = Arc<Mutex<Vec<String>>>;

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

    async fn read(&mut self) -> Option<(u16, FrameBody)> {
        let size = self.io.read_u32().await.ok()? as usize;
        let mut frame = BytesMut::zeroed(size - 4);
        self.io.read_exact(&mut frame).await.ok()?;
        let Frame { channel, body } = FrameDecoder {}.decode(&mut frame).unwrap().unwrap();
        Some((channel, body))
    }

    async fn header(io: DuplexStream) -> Self {
        let mut peer = Self { io };
        let mut header = [0; 8];
        peer.io.read_exact(&mut header).await.unwrap();
        assert_eq!(header, HEADER);
        peer.io.write_all(&HEADER).await.unwrap();
        assert!(matches!(peer.read().await, Some((_, FrameBody::Open(_)))));
        peer
    }

    async fn open(io: DuplexStream) -> Self {
        let mut peer = Self::header(io).await;
        peer.write(0, &open()).await;
        peer
    }

    /// Answers the client's Begin on the same channel number.
    async fn begin(&mut self) -> u16 {
        let Some((channel, FrameBody::Begin(_))) = self.read().await else {
            panic!("the client begins its session");
        };
        let mut begin = begin();
        begin.remote_channel = Some(channel);
        self.write(channel, &begin).await;
        channel
    }

    /// The next frame of `kind` the client sends, skipping Flow.
    async fn next(&mut self) -> Option<(u16, FrameBody)> {
        loop {
            match self.read().await? {
                (_, FrameBody::Flow(_)) => continue,
                other => return Some(other),
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

fn begin() -> Begin {
    Begin {
        remote_channel: None,
        next_outgoing_id: 0,
        incoming_window: 10,
        outgoing_window: 10,
        handle_max: Default::default(),
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

#[derive(Debug, Default)]
struct Observer {
    reports: Reports,
}

impl IncomingFrameObserver for Observer {
    fn incoming(&self, _frame: Arc<IncomingFrame>) -> IncomingFrameAction {
        IncomingFrameAction::Forward
    }
    fn connection_open_failed(&self, error: &OpenError) {
        self.reports.lock().unwrap().push(format!("open {error:?}"));
    }
    fn connection_ended(&self, result: Result<(), &ConnectionInnerError>) {
        self.reports
            .lock()
            .unwrap()
            .push(format!("ended {result:?}"));
    }
}

fn reports(reports: &Reports) -> Vec<String> {
    reports.lock().unwrap().clone()
}

async fn bounded<T>(work: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), work)
        .await
        .expect("the exchange stalled")
}

/// A second Open fails the connection. Its failure is reported before the
/// error Close; the peer's clean acknowledgment of that Close is not.
#[tokio::test]
async fn a_connection_failure_is_reported_before_its_close_and_its_acknowledgment_is_not() {
    let observer = Arc::new(Observer::default());
    let seen = observer.reports.clone();
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let client = async move {
        let mut connection = Connection::builder()
            .container_id("client")
            .incoming_frame_observer(observer)
            .open_with_stream(client_io)
            .await
            .unwrap();
        connection.on_close().await
    };
    let peer = async move {
        let mut peer = Peer::open(peer_io).await;
        peer.write(0, &open()).await;
        let Some((_, FrameBody::Close(close))) = peer.next().await else {
            panic!("the client closes for the second Open");
        };
        assert_eq!(
            close.error.map(|error| error.condition),
            Some(ErrorCondition::AmqpError(AmqpError::IllegalState))
        );
        let before = reports(&seen);
        peer.write(0, &Close { error: None }).await;
        (before, seen)
    };
    let (ended, (before, seen)) = bounded(async { tokio::join!(client, peer) }).await;
    assert_eq!(before, ["ended Err(IllegalState)"]);
    assert!(
        matches!(ended, Err(fe2o3_amqp::connection::Error::IllegalState)),
        "{ended:?}"
    );
    assert_eq!(reports(&seen), ["ended Err(IllegalState)"]);
}

/// The peer's own Close is reported as accepted before the Close reply.
#[tokio::test]
async fn a_peer_close_is_reported_before_its_reply() {
    let observer = Arc::new(Observer::default());
    let seen = observer.reports.clone();
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let client = async move {
        let mut connection = Connection::builder()
            .container_id("client")
            .incoming_frame_observer(observer)
            .open_with_stream(client_io)
            .await
            .unwrap();
        connection.on_close().await
    };
    let peer = async move {
        let mut peer = Peer::open(peer_io).await;
        peer.write(0, &Close { error: None }).await;
        let Some((_, FrameBody::Close(_))) = peer.next().await else {
            panic!("the client answers the Close");
        };
        reports(&seen)
    };
    let (_, before) = bounded(async { tokio::join!(client, peer) }).await;
    assert_eq!(before, ["ended Err(RemoteClosed)"]);
}

/// An opening that fails is reported before its cleanup Close.
#[tokio::test]
async fn an_opening_failure_is_reported_before_its_cleanup_close() {
    let observer = Arc::new(Observer::default());
    let seen = observer.reports.clone();
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let client = Connection::builder()
        .container_id("client")
        .incoming_frame_observer(observer)
        .open_with_stream(client_io);
    let peer = async move {
        let mut peer = Peer::header(peer_io).await;
        peer.write(0, &begin()).await;
        let Some((_, FrameBody::Close(_))) = peer.next().await else {
            panic!("the opening's cleanup closes");
        };
        let before = reports(&seen);
        peer.write(0, &Close { error: None }).await;
        (before, seen)
    };
    let (opened, (before, seen)) = bounded(async { tokio::join!(client, peer) }).await;
    assert!(matches!(opened, Err(OpenError::IllegalState)), "{opened:?}");
    assert_eq!(before, ["open IllegalState"]);
    assert_eq!(reports(&seen), ["open IllegalState"]);
}

fn session_reports() -> (Reports, ResultReport) {
    let reports = Reports::default();
    let seen = reports.clone();
    (
        reports,
        Arc::new(move |result: &Result<(), SessionInnerError>| {
            seen.lock().unwrap().push(format!("{result:?}"))
        }),
    )
}

/// A Flow for no attached link fails the Session. Its failure is reported
/// before the error End; the peer's clean acknowledgment is not.
#[tokio::test]
async fn a_session_failure_is_reported_before_its_end_and_its_acknowledgment_is_not() {
    let (seen, report) = session_reports();
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let client = async move {
        let mut connection = Connection::builder()
            .container_id("client")
            .open_with_stream(client_io)
            .await
            .unwrap();
        let mut session = Session::builder()
            .result_reporter(report)
            .begin(&mut connection)
            .await
            .unwrap();
        let ended = session.on_end().await;
        let _ = connection.close().await;
        ended
    };
    let checked = seen.clone();
    let peer = async move {
        let mut peer = Peer::open(peer_io).await;
        let channel = peer.begin().await;
        let flow = Flow {
            next_incoming_id: Some(0),
            incoming_window: 10,
            next_outgoing_id: 0,
            outgoing_window: 10,
            handle: Some(999.into()),
            delivery_count: Some(0),
            link_credit: Some(1),
            available: None,
            drain: false,
            echo: false,
            properties: None,
        };
        peer.write(channel, &flow).await;
        let Some((_, FrameBody::End(end))) = peer.next().await else {
            panic!("the session ends for the unattached handle");
        };
        assert_eq!(
            end.error.map(|error| error.condition),
            Some(ErrorCondition::SessionError(SessionError::UnattachedHandle))
        );
        let before = reports(&checked);
        peer.write(channel, &End { error: None }).await;
        while let Some((_, body)) = peer.read().await {
            if let FrameBody::Close(close) = body {
                peer.write(0, &close).await;
            }
        }
        before
    };
    let (ended, before) = bounded(async { tokio::join!(client, peer) }).await;
    assert_eq!(before, ["Err(UnattachedHandle)"]);
    assert!(ended.is_err(), "{ended:?}");
    assert_eq!(reports(&seen), ["Err(UnattachedHandle)"]);
}

/// The peer's own End is reported as accepted before the End reply.
#[tokio::test]
async fn a_peer_end_is_reported_before_its_reply() {
    let (seen, report) = session_reports();
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let client = async move {
        let mut connection = Connection::builder()
            .container_id("client")
            .open_with_stream(client_io)
            .await
            .unwrap();
        let mut session = Session::builder()
            .result_reporter(report)
            .begin(&mut connection)
            .await
            .unwrap();
        let _ = session.on_end().await;
        let _ = connection.close().await;
    };
    let checked = seen.clone();
    let peer = async move {
        let mut peer = Peer::open(peer_io).await;
        let channel = peer.begin().await;
        peer.write(channel, &End { error: None }).await;
        let Some((_, FrameBody::End(_))) = peer.next().await else {
            panic!("the client answers the End");
        };
        let before = reports(&checked);
        while let Some((_, body)) = peer.read().await {
            if let FrameBody::Close(close) = body {
                peer.write(0, &close).await;
            }
        }
        before
    };
    let (_, before) = bounded(async { tokio::join!(client, peer) }).await;
    assert_eq!(before, ["Err(RemoteEnded)"]);
}

#[derive(Clone, Copy, Debug)]
enum Answer {
    /// The peer acknowledges the link's closing Detach cleanly.
    Detach,
    /// The peer closes the connection instead.
    Close,
}

fn attach_reports() -> (Reports, AttachRefusalReport) {
    let reports = Reports::default();
    let seen = reports.clone();
    (
        reports,
        Arc::new(move |error: &(dyn std::error::Error + 'static)| {
            let text = if let Some(error) = error.downcast_ref::<ReceiverAttachError>() {
                format!("receiver {error:?}")
            } else if let Some(error) = error.downcast_ref::<SenderAttachError>() {
                format!("sender {error:?}")
            } else {
                format!("other {error:?}")
            };
            seen.lock().unwrap().push(text)
        }),
    )
}

/// The peer answers the link's Attach in a way the library refuses, then
/// answers the link's closing Detach as `answer` says. The refusal is reported
/// before that Detach is sent; the attach still returns the library's own
/// result after it.
async fn attach_refusal(sender: bool, answer: Answer, configured: bool) {
    let (seen, report) = attach_reports();
    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
    let client = async move {
        let mut connection = Connection::builder()
            .container_id("client")
            .open_with_stream(client_io)
            .await
            .unwrap();
        let mut session = Session::begin(&mut connection).await.unwrap();
        let attached = if sender {
            let builder = Sender::builder().name("refused").target("q");
            let builder = match configured {
                true => builder.attach_refusal_reporter(report),
                false => builder,
            };
            builder
                .attach(&mut session)
                .await
                .map(drop)
                .map_err(|error| format!("{error:?}"))
        } else {
            let builder = Receiver::builder().name("refused").source("q");
            let builder = match configured {
                true => builder.attach_refusal_reporter(report),
                false => builder,
            };
            builder
                .attach(&mut session)
                .await
                .map(drop)
                .map_err(|error| format!("{error:?}"))
        };
        let _ = session.end().await;
        let _ = connection.close().await;
        attached
    };
    let checked = seen.clone();
    let peer = async move {
        let mut peer = Peer::open(peer_io).await;
        let channel = peer.begin().await;
        let Some((_, FrameBody::Attach(attach))) = peer.next().await else {
            panic!("the client attaches");
        };
        let mut answer_attach: Attach = attach;
        if sender {
            answer_attach.role = Role::Receiver;
            let Some(TargetArchetype::Target(target)) = answer_attach.target.as_deref_mut() else {
                panic!("a Target");
            };
            target.dynamic = true;
            target.address = None;
        } else {
            answer_attach.role = Role::Sender;
            answer_attach.initial_delivery_count = None;
        }
        let handle = answer_attach.handle.clone();
        peer.write(channel, &answer_attach).await;
        let Some((_, FrameBody::Detach(detach))) = peer.next().await else {
            panic!("the library detaches the refused link");
        };
        assert!(detach.closed);
        assert_eq!(
            detach.error.map(|error| error.condition),
            Some(ErrorCondition::AmqpError(AmqpError::InvalidField))
        );
        let before = reports(&checked);
        match answer {
            Answer::Detach => {
                let ack = Detach {
                    handle,
                    closed: true,
                    error: None,
                };
                peer.write(channel, &ack).await;
            }
            Answer::Close => peer.write(0, &Close { error: None }).await,
        }
        while let Some((channel, body)) = peer.read().await {
            match body {
                FrameBody::End(end) => peer.write(channel, &end).await,
                FrameBody::Close(close) => {
                    if matches!(answer, Answer::Detach) {
                        peer.write(0, &close).await
                    }
                }
                _ => {}
            }
        }
        before
    };
    let (attached, before) = bounded(async { tokio::join!(client, peer) }).await;
    let refusal = if sender {
        "sender TargetAddressIsNoneWhenDynamicIsTrue"
    } else {
        "receiver InitialDeliveryCountIsNone"
    };
    let expected: &[&str] = if configured { &[refusal] } else { &[] };
    assert_eq!(before, expected, "{answer:?}");
    assert_eq!(reports(&seen), expected, "{answer:?}");
    let attached = attached.unwrap_err();
    match answer {
        Answer::Detach => assert_eq!(attached, refusal.split_once(' ').unwrap().1),
        Answer::Close => assert!(attached.starts_with("SessionStopped"), "{attached}"),
    }
}

#[tokio::test]
async fn a_refused_receiver_attach_is_reported_before_its_closing_detach() {
    attach_refusal(false, Answer::Detach, true).await;
}

#[tokio::test]
async fn a_refused_sender_attach_is_reported_before_its_closing_detach() {
    attach_refusal(true, Answer::Detach, true).await;
}

#[tokio::test]
async fn a_refused_attach_report_survives_the_peer_s_close() {
    attach_refusal(false, Answer::Close, true).await;
}

/// Without a reporter the library sends the same closing Detach and returns
/// the same error.
#[tokio::test]
async fn an_unconfigured_refusal_keeps_the_library_s_exchange() {
    attach_refusal(false, Answer::Detach, false).await;
    attach_refusal(true, Answer::Detach, false).await;
}
