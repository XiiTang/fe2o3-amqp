use super::detached_peer::*;
use fe2o3_amqp::{
    frames::amqp::FrameBody,
    link::{DetachError, UnsettledLimits},
    types::{
        definitions::{AmqpError, Error, Role},
        messaging::{Accepted, Outcome},
        performatives::{Attach, Detach, Disposition, End},
        primitives::OrderedMap,
    },
    Connection, Receiver, Sender, Session,
};
use std::{future::Future, time::Duration};
use tokio::sync::oneshot;

pub(super) struct CapState {
    pub exhaust: Box<dyn FnOnce()>,
    pub check: Box<dyn Fn()>,
}
#[derive(Clone, Copy)]
pub(super) struct CapConstructors {
    pub sender: fn(&Sender) -> CapState,
    pub receiver: fn(&Receiver) -> CapState,
}
/// Cancellation or timeout cannot retract an Attach. The owned resume consumes
/// its late answer and completes suspension before explicit retry; a peer's
/// cleanup failure retains its original typed condition instead of Timeout.
pub(super) async fn run(cap_constructors: Option<CapConstructors>) {
    use fe2o3_amqp::link::{ReceiverResumeErrorKind, SenderResumeErrorKind};

    let mut cases = Vec::new();
    for receiving in [false, true] {
        for timed in [false, true] {
            if cap_constructors.is_some() {
                for phase in 0..3 {
                    for session_failure in [false, true] {
                        cases.push((receiving, timed, false, true, Some(phase), session_failure));
                    }
                }
            } else {
                for peer_error in [false, true] {
                    for crossed in [false, true] {
                        cases.push((receiving, timed, peer_error, crossed, None, false));
                    }
                }
            }
        }
    }
    for (receiving, timed, peer_error, crossed, phase, session_failure) in cases {
        let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
        let (interrupt, interrupted) = oneshot::channel();
        let (resumed_peer, resume_complete) = oneshot::channel();
        let (cap_gate, cap_ready) = oneshot::channel();
        let (cap_done, cap_complete) = oneshot::channel();
        let client = async move {
            let mut connection = Connection::builder()
                .container_id("client")
                .open_with_stream(client_io)
                .await
                .unwrap();
            let mut session = Session::begin(&mut connection).await.unwrap();
            if receiving {
                let builder = Receiver::builder().name("resume").source("q");
                let builder = if cap_constructors.is_some() {
                    builder.unsettled_limits(UnsettledLimits::new(2, 4096))
                } else {
                    builder
                };
                let mut receiver = builder.attach(&mut session).await.unwrap();
                let retained = if crossed && !peer_error {
                    let confirmed = receiver.recv::<String>().await.unwrap();
                    assert_eq!(confirmed.body(), "confirmed");
                    receiver.accept(&confirmed).await.unwrap();
                    let old = receiver.recv::<String>().await.unwrap();
                    assert_eq!(old.body(), "old");
                    Some(old)
                } else {
                    None
                };
                let cap = cap_constructors.map(|construct| (construct.receiver)(&receiver));
                let detached = receiver.detach().await.unwrap();
                let before = detached
                    .resume_on_session_until(&session, std::future::ready(()))
                    .await
                    .unwrap_err();
                assert!(matches!(before.kind, ReceiverResumeErrorKind::Interrupted));
                let attempt = async {
                    if timed {
                        before
                            .detached_recver
                            .resume_on_session_with_timeout(&session, Duration::from_secs(1))
                            .await
                    } else {
                        before
                            .detached_recver
                            .resume_on_session_until(&session, async { interrupted.await.unwrap() })
                            .await
                    }
                    .unwrap_err()
                };
                let after = finish_cleanup(attempt, cap, cap_ready, cap_done).await;
                if session_failure {
                    assert!(matches!(
                        after.kind,
                        ReceiverResumeErrorKind::DetachError(DetachError::SessionStopped(_))
                    ));
                    assert!(after.detached_recver.check_unsettled().is_err());
                    resume_complete.await.unwrap();
                    assert!(matches!(
                        session.stop_and_join().await,
                        Err(fe2o3_amqp::session::Error::RemoteEnded)
                    ));
                    connection.stop_and_join().await.unwrap();
                    return;
                } else if crossed && !peer_error {
                    assert!(matches!(
                        after.kind,
                        ReceiverResumeErrorKind::DetachError(DetachError::ClosedByRemote)
                    ));
                    assert_eq!(retained.unwrap().body(), "old");
                    let closed = after.detached_recver.resume().await.unwrap_err();
                    assert!(matches!(
                        closed.kind,
                        ReceiverResumeErrorKind::AttachError(_)
                    ));
                    resume_complete.await.unwrap();
                    stop_and_join(connection, session).await;
                    return;
                } else if peer_error {
                    assert!(matches!(&after.kind,
                        ReceiverResumeErrorKind::DetachError(DetachError::RemoteDetachedWithError(error))
                        if error.condition == AmqpError::InternalError.into()));
                } else if timed {
                    assert!(matches!(after.kind, ReceiverResumeErrorKind::Timeout));
                } else {
                    assert!(matches!(after.kind, ReceiverResumeErrorKind::Interrupted));
                }
                let resumed = after.detached_recver.resume().await.unwrap();
                // Keep the engine live until the peer saw the resumed credit;
                // stopping the fixture must not drop that queued Flow.
                resume_complete.await.unwrap();
                stop_and_join(connection, session).await;
                drop(resumed);
            } else {
                let builder = Sender::builder().name("resume").target("q");
                let builder = if cap_constructors.is_some() {
                    builder.unsettled_limits(UnsettledLimits::new(2, 4096))
                } else {
                    builder
                };
                let mut sender = builder.attach(&mut session).await.unwrap();
                let retained = if crossed && !peer_error {
                    assert!(matches!(
                        sender.send("confirmed").await.unwrap(),
                        Outcome::Accepted(_)
                    ));
                    Some(sender.send_batchable("old").await.unwrap())
                } else {
                    None
                };
                let cap = cap_constructors.map(|construct| (construct.sender)(&sender));
                let detached = sender.detach().await.unwrap();
                let before = detached
                    .resume_on_session_until(&session, std::future::ready(()))
                    .await
                    .unwrap_err();
                assert!(matches!(before.kind, SenderResumeErrorKind::Interrupted));
                let attempt = async {
                    if timed {
                        before
                            .detached_sender
                            .resume_on_session_with_timeout(&session, Duration::from_secs(1))
                            .await
                    } else {
                        before
                            .detached_sender
                            .resume_on_session_until(&session, async { interrupted.await.unwrap() })
                            .await
                    }
                    .unwrap_err()
                };
                let after = finish_cleanup(attempt, cap, cap_ready, cap_done).await;
                if session_failure {
                    assert!(matches!(
                        after.kind,
                        SenderResumeErrorKind::DetachError(DetachError::SessionStopped(_))
                    ));
                    assert!(after.detached_sender.check_unsettled().is_err());
                    resume_complete.await.unwrap();
                    assert!(matches!(
                        session.stop_and_join().await,
                        Err(fe2o3_amqp::session::Error::RemoteEnded)
                    ));
                    connection.stop_and_join().await.unwrap();
                    return;
                } else if crossed && !peer_error {
                    assert!(matches!(
                        after.kind,
                        SenderResumeErrorKind::DetachError(DetachError::ClosedByRemote)
                    ));
                    assert!(
                        !matches!(
                            retained.unwrap().await,
                            Ok(Outcome::Accepted(_) | Outcome::Rejected(_))
                        ),
                        "teardown Attach supplied a fictitious business outcome"
                    );
                    let closed = after.detached_sender.resume().await.unwrap_err();
                    assert!(matches!(closed.kind, SenderResumeErrorKind::AttachError(_)));
                    resume_complete.await.unwrap();
                    stop_and_join(connection, session).await;
                    return;
                } else if peer_error {
                    assert!(matches!(&after.kind,
                        SenderResumeErrorKind::DetachError(DetachError::RemoteDetachedWithError(error))
                        if error.condition == AmqpError::InternalError.into()));
                } else if timed {
                    assert!(matches!(after.kind, SenderResumeErrorKind::Timeout));
                } else {
                    assert!(matches!(after.kind, SenderResumeErrorKind::Interrupted));
                }
                let resumed = after.detached_sender.resume().await.unwrap();
                // Keep the engine live until the peer saw the resumed credit;
                // stopping the fixture must not drop that queued Flow.
                resume_complete.await.unwrap();
                stop_and_join(connection, session).await;
                drop(resumed);
            }
        };
        let peer = async move {
            let mut cap_gate = Some((cap_gate, cap_complete));
            let mut peer = Peer::open(peer_io, None, 10).await;
            if receiving {
                let attach = peer.answer_receiver_attach(None).await;
                if crossed && !peer_error {
                    peer.transfer(&attach.handle, 0, &vec![0].into(), "confirmed")
                        .await;
                    peer.read_until(|body| matches!(body, FrameBody::Disposition(d) if d.first == 0 && matches!(d.state, Some(fe2o3_amqp::types::messaging::DeliveryState::Accepted(_)))).then_some(()), |_| false).await;
                    peer.transfer(&attach.handle, 1, &vec![1].into(), "old")
                        .await;
                }
            } else if crossed && !peer_error {
                peer.attach_receiver(2, 10).await;
                let first = peer
                    .read_until(
                        |body| match body {
                            FrameBody::Transfer { performative, .. } => Some(performative.clone()),
                            _ => None,
                        },
                        |_| false,
                    )
                    .await;
                peer.write(
                    0,
                    &Disposition {
                        role: Role::Receiver,
                        first: first.delivery_id.unwrap(),
                        last: None,
                        settled: true,
                        state: Some(Accepted {}.into()),
                        batchable: false,
                    },
                )
                .await;
                peer.read_until(
                    |body| matches!(body, FrameBody::Transfer { .. }).then_some(()),
                    |_| false,
                )
                .await;
            } else {
                peer.answer_attach(None).await;
            }
            assert!(peer.answer_detach().await.is_empty());
            // A pre-dispatch interruption sends nothing: the next frame is
            // the subsequent attempt's Attach, never an unsolicited Detach.
            let FrameBody::Attach(attach) = peer.read().await else {
                panic!("expected only the next admitted resume Attach")
            };
            if timed {
                // Advance the actual native deadline while the late Attach
                // answer is held, with no elapsed wall-clock assumption.
                tokio::time::advance(Duration::from_secs(1)).await;
            } else {
                interrupt.send(()).unwrap();
            }
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
            if phase == Some(0) {
                held_cap(&mut cap_gate).await;
                if session_failure {
                    end_session(&mut peer).await;
                    resumed_peer.send(()).unwrap();
                    return peer;
                }
            }
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
            peer.write(
                0,
                &Detach {
                    closed: crossed && !peer_error,
                    error: peer_error.then(|| Error::new(AmqpError::InternalError, None, None)),
                    ..detach
                },
            )
            .await;
            if crossed && !peer_error {
                let FrameBody::Attach(close_attach) = peer.read().await else {
                    panic!("expected the crossed teardown Attach")
                };
                assert_eq!(close_attach.name, "resume");
                assert!(close_attach.unsettled.is_none());
                let mut old_outcome = OrderedMap::new();
                old_outcome.insert(vec![1].into(), Some(Accepted {}.into()));
                // A complete peer map is validation input for CloseOnly, not
                // evidence that this teardown accepted an old delivery.
                if phase == Some(1) {
                    held_cap(&mut cap_gate).await;
                    if session_failure {
                        end_session(&mut peer).await;
                        resumed_peer.send(()).unwrap();
                        return peer;
                    }
                }
                peer.write(
                    0,
                    &Attach {
                        role: if receiving {
                            Role::Sender
                        } else {
                            Role::Receiver
                        },
                        initial_delivery_count: receiving.then_some(0),
                        unsettled: Some(old_outcome),
                        incomplete_unsettled: false,
                        ..close_attach.clone()
                    },
                )
                .await;
                let FrameBody::Detach(close_detach) = peer.read().await else {
                    panic!("teardown must close without business replay")
                };
                assert_eq!(close_detach.handle, close_attach.handle);
                assert!(close_detach.closed);
                assert!(close_detach.error.is_none());
                if phase == Some(2) {
                    held_cap(&mut cap_gate).await;
                    if session_failure {
                        end_session(&mut peer).await;
                        resumed_peer.send(()).unwrap();
                        return peer;
                    }
                }
                peer.write(0, &close_detach).await;
                resumed_peer.send(()).unwrap();
                return peer;
            }
            if receiving {
                peer.answer_receiver_attach(None).await;
            } else {
                peer.answer_attach(None).await;
            }
            resumed_peer.send(()).unwrap();
            peer
        };
        let (_, _peer) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(client, peer)
        })
        .await
        .expect("the interrupted-resume cleanup stalled");
    }
}

// The same owned public resume runs in both the integration and private-Store
// cases. The latter calls the real checked mutation while a native answer is
// withheld; no product API or synthetic failure is installed.
async fn finish_cleanup<T>(
    attempt: impl Future<Output = T>,
    cap: Option<CapState>,
    ready: oneshot::Receiver<()>,
    done: oneshot::Sender<()>,
) -> T {
    let mut attempt = Box::pin(attempt);
    if let Some(cap) = cap {
        tokio::select! { biased;
            _ = &mut attempt => panic!("custody returned before the held cleanup answer"),
            _ = ready => {},
        }
        (cap.exhaust)();
        assert!(
            futures_util::poll!(&mut attempt).is_pending(),
            "Store exhaustion cancelled admitted cleanup"
        );
        done.send(()).unwrap();
        let result = attempt.await;
        (cap.check)();
        result
    } else {
        attempt.await
    }
}
async fn held_cap(gate: &mut Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>) {
    let (ready, done) = gate.take().unwrap();
    ready.send(()).unwrap();
    done.await.unwrap();
}
async fn end_session(peer: &mut Peer) {
    peer.write(0, &End { error: None }).await;
    assert!(matches!(peer.read().await, FrameBody::End(_)));
}
