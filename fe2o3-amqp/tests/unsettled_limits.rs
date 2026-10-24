//! Configured unsettled retention ends its native owner with a typed cause.
#![cfg(feature = "acceptor")]
use fe2o3_amqp::{
    acceptor::{ConnectionAcceptor, LinkAcceptor, LinkEndpoint, SessionAcceptor},
    link::{LinkStateError, RecvError, SendError, UnsettledLimits},
    session, Connection, Sender, Session,
};

#[tokio::test]
async fn configured_unsettled_limits_survive_replenished_windows_for_both_roles() {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        for sender_cap in [false, true] {
            let (client, server) = tokio::io::duplex(4096);
            let acceptor = ConnectionAcceptor::new("listener");
            let (client, server) = tokio::join!(
                Connection::builder()
                    .container_id("client")
                    .open_with_stream(client),
                acceptor.accept(server),
            );
            let mut client = client.unwrap();
            let mut server = server.unwrap();
            let session_acceptor = SessionAcceptor::builder().incoming_window(1).build();
            let (outgoing, incoming) = tokio::join!(
                Session::builder().incoming_window(1).begin(&mut client),
                session_acceptor.accept(&mut server),
            );
            let mut outgoing = outgoing.unwrap();
            let mut incoming = incoming.unwrap();
            let limits = UnsettledLimits::new(1, 4096);
            let builder = Sender::builder().name("bounded").target("queue");
            let builder = if sender_cap {
                builder.unsettled_limits(limits)
            } else {
                builder
            };
            let acceptor = LinkAcceptor::builder();
            let acceptor = if sender_cap {
                acceptor
            } else {
                acceptor.unsettled_limits(limits)
            };
            let acceptor = acceptor.build();
            let (sender, receiver) = tokio::join!(
                builder.attach(&mut outgoing),
                acceptor.accept(&mut incoming)
            );
            let mut sender = sender.unwrap();
            let LinkEndpoint::Receiver(mut receiver) = receiver.unwrap() else {
                panic!("receiver")
            };
            receiver.set_credit(1).await.unwrap();
            let first = sender.send_batchable("first").await.unwrap();
            let delivery = receiver.recv::<String>().await.unwrap();
            assert_eq!(delivery.body(), "first");
            // Receiving releases the one-frame session window. A fresh flow
            // grants another delivery, while the first remains unsettled.
            receiver.set_credit(1).await.unwrap();
            let second = sender.send_batchable("second").await;
            if sender_cap {
                assert!(matches!(
                    second,
                    Err(SendError::LinkStateError(
                        LinkStateError::UnsettledCapacityExceeded(_)
                    ))
                ));
                assert!(matches!(
                    first.await,
                    Err(SendError::LinkStateError(
                        LinkStateError::UnsettledCapacityExceeded(_)
                    ))
                ));
            } else {
                drop(second.unwrap());
                assert!(matches!(
                    incoming.on_end().await,
                    Err(session::Error::UnsettledCapacityExceeded(_))
                ));
                assert!(matches!(
                    receiver.recv::<String>().await,
                    Err(RecvError::LinkStateError(
                        LinkStateError::UnsettledCapacityExceeded(_)
                    ))
                ));
                drop(first);
            }
            assert!(!client.is_closed());
            assert!(!server.is_closed());
            let _ = outgoing.stop_and_join().await;
            let _ = incoming.stop_and_join().await;
            client.stop_and_join().await.unwrap();
            // Stopping the client can let the server observe EOF before its
            // local stop. That native result is unrelated to the cap assertions.
            let _ = server.stop_and_join().await;
        }
    })
    .await
    .expect("native unsettled exhaustion did not finish");
}
