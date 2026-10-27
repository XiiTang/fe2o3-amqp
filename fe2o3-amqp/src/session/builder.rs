//! Session builder

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, OnceLock};

use fe2o3_amqp_types::definitions::{Fields, Handle, TransferNumber};
use serde_amqp::primitives::Symbol;
use slab::Slab;
use tokio::sync::mpsc;

use crate::{
    connection::{AllocSessionError, ConnectionHandle, ConnectionStopReason},
    control::SessionControl,
    endpoint::{OutgoingChannel, Session as _},
    session::{engine::SessionEngine, SessionState},
    util::Constant,
    Session,
};

use super::{error::BeginError, SessionHandle, DEFAULT_WINDOW};

pub(crate) const DEFAULT_SESSION_CONTROL_BUFFER_SIZE: usize = 128;
pub(crate) const DEFAULT_SESSION_MUX_BUFFER_SIZE: usize = u16::MAX as usize;

/// Builder for [`crate::Session`]
#[derive(Debug, Clone)]
pub struct Builder {
    /// The transfer-id of the first transfer id the sender will send
    pub next_outgoing_id: TransferNumber,

    /// The initial incoming-window of the sender
    ///
    /// Defaults to [`DEFAULT_WINDOW`]. Once half of the window has been consumed
    /// by received transfers, the session re-advertises its window with a
    /// session-only flow (no link handle), so a peer sending continuously never
    /// stalls even when no link-level flow is generated.
    pub incoming_window: TransferNumber,

    /// The initial outgoing-window of the sender
    ///
    /// Defaults to [`DEFAULT_WINDOW`].
    pub outgoing_window: TransferNumber,

    /// The maximum handle value that can be used on the session
    pub handle_max: Handle,

    /// The extension capabilities the sender supports
    pub offered_capabilities: Option<Vec<Symbol>>,

    /// The extension capabilities the sender can use if the receiver supports them
    pub desired_capabilities: Option<Vec<Symbol>>,

    /// Session properties
    pub properties: Option<Fields>,

    /// Buffer size of the underlying [`tokio::sync::mpsc::channel`]
    /// that are used by links attached to the session
    pub buffer_size: usize,

    /// Acceptor for incoming transaction control links
    #[cfg(not(target_arch = "wasm32"))]
    #[cfg(all(feature = "transaction", feature = "acceptor"))]
    pub(crate) control_link_acceptor: Option<ControlLinkAcceptor>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            next_outgoing_id: 0,
            incoming_window: DEFAULT_WINDOW,
            outgoing_window: DEFAULT_WINDOW,
            handle_max: Default::default(),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
            buffer_size: DEFAULT_SESSION_MUX_BUFFER_SIZE,

            #[cfg(not(target_arch = "wasm32"))]
            #[cfg(all(feature = "transaction", feature = "acceptor"))]
            control_link_acceptor: None,
        }
    }
}

cfg_transaction! {
    cfg_acceptor! {
        use crate::transaction::{
            coordinator::ControlLinkAcceptor, manager::TransactionManager, session::TxnSession,
        };

        impl Builder {
            #[allow(clippy::too_many_arguments)]
            pub(crate) fn into_txn_session(
                self,
                control: mpsc::Sender<SessionControl>,
                outgoing: crate::session::transfer_queue::Sender,
                outgoing_channel: OutgoingChannel,
                control_link_acceptor: ControlLinkAcceptor,
                local_state: SessionState,
                connection_stop_reason: Arc<OnceLock<ConnectionStopReason>>,
                max_frame_size: usize,
            ) -> TxnSession<Session> {
                let txn_manager = TransactionManager::new(outgoing, control_link_acceptor);
                let session = Session {
                    // control,
                    outgoing_channel,
                    session_stop_reason: Arc::new(OnceLock::new()),
                    connection_stop_reason,
                    local_state,
                    initial_outgoing_id: Constant::new(self.next_outgoing_id),
                    next_outgoing_id: self.next_outgoing_id,
                    incoming_window: self.incoming_window,
                    outgoing_window: self.outgoing_window,
                    handle_max: self.handle_max,
                    incoming_channel: None,
                    next_incoming_id: 0,
                    need_flow_count: 0,
                    receive_window: super::receive_window::ReceiveWindow::new(self.incoming_window),
                    remote_incoming_window: 0,
                    remote_incoming_window_exhausted_buffer: VecDeque::new(),
                    remote_outgoing_window: 0,
                    offered_capabilities: self.offered_capabilities,
                    desired_capabilities: self.desired_capabilities,
                    properties: self.properties,

                    link_name_by_output_handle: Slab::new(),
                    link_by_name: HashMap::new(),
                    link_by_input_handle: HashMap::new(),
                    close_pending: HashSet::new(),
                    delivery_tag_by_id: HashMap::new(),
                };

                TxnSession {
                    control,
                    session,
                    txn_manager,
                    max_frame_size,
                }
            }
        }
    }
}

impl Builder {
    /// Creates a new builder for [`crate::Session`]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn into_session(
        self,
        outgoing_channel: OutgoingChannel,
        local_state: SessionState,
        connection_stop_reason: Arc<OnceLock<ConnectionStopReason>>,
    ) -> Session {
        Session {
            outgoing_channel,
            session_stop_reason: Arc::new(OnceLock::new()),
            connection_stop_reason,
            local_state,
            initial_outgoing_id: Constant::new(self.next_outgoing_id),
            next_outgoing_id: self.next_outgoing_id,
            incoming_window: self.incoming_window,
            outgoing_window: self.outgoing_window,
            handle_max: self.handle_max,
            incoming_channel: None,
            next_incoming_id: 0,
            need_flow_count: 0,
            receive_window: super::receive_window::ReceiveWindow::new(self.incoming_window),
            remote_incoming_window: 0,
            remote_incoming_window_exhausted_buffer: VecDeque::new(),
            remote_outgoing_window: 0,
            offered_capabilities: self.offered_capabilities,
            desired_capabilities: self.desired_capabilities,
            properties: self.properties,

            link_name_by_output_handle: Slab::new(),
            link_by_name: HashMap::new(),
            link_by_input_handle: HashMap::new(),
            close_pending: HashSet::new(),
            delivery_tag_by_id: HashMap::new(),
        }
    }

    /// The transfer-id of the first transfer id the sender will send
    pub fn next_outgoing_id(mut self, value: TransferNumber) -> Self {
        self.next_outgoing_id = value;
        self
    }

    /// The initial incoming-window of the sender
    ///
    /// Defaults to [`DEFAULT_WINDOW`]. Once half of the window has been consumed
    /// by received transfers, the session re-advertises its window with a
    /// session-only flow (no link handle), so a peer sending continuously never
    /// stalls even when no link-level flow is generated.
    pub fn incoming_window(mut self, value: TransferNumber) -> Self {
        self.incoming_window = value;
        self
    }

    /// The initial outgoing-window of the sender
    ///
    /// Defaults to [`DEFAULT_WINDOW`].
    pub fn outgoing_window(mut self, value: TransferNumber) -> Self {
        self.outgoing_window = value;
        self
    }

    /// The maximum handle value that can be used on the session
    pub fn handle_max(mut self, value: impl Into<Handle>) -> Self {
        self.handle_max = value.into();
        self
    }

    /// Add one extension capabilities the sender supports
    pub fn add_offered_capabilities(mut self, capability: impl Into<Symbol>) -> Self {
        match &mut self.offered_capabilities {
            Some(capabilities) => capabilities.push(capability.into()),
            None => self.offered_capabilities = Some(vec![capability.into()]),
        }
        self
    }

    /// Set the extension capabilities the sender supports
    pub fn set_offered_capabilities(mut self, capabilities: Vec<Symbol>) -> Self {
        self.offered_capabilities = Some(capabilities);
        self
    }

    /// Add one extension capabilities the sender can use if the receiver supports them
    pub fn add_desired_capabilities(mut self, capability: impl Into<Symbol>) -> Self {
        match &mut self.desired_capabilities {
            Some(capabilities) => capabilities.push(capability.into()),
            None => self.desired_capabilities = Some(vec![capability.into()]),
        }
        self
    }

    /// Set the extension capabilities the sender can use if the receiver supports them
    pub fn set_desired_capabilities(mut self, capabilities: Vec<Symbol>) -> Self {
        self.desired_capabilities = Some(capabilities);
        self
    }

    /// Session properties
    pub fn properties(mut self, properties: Fields) -> Self {
        self.properties = Some(properties);
        self
    }

    /// Buffer size of the underlying [`tokio::sync::mpsc::channel`]
    /// that are used by links attached to the session
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    // TODO
    // /// Enable handling remotely initiated control link and transaction by setting the
    // /// `control_link_acceptor` field
    // #[cfg(all(feature = "transaction", feature = "acceptor"))]
    // pub fn control_link_acceptor(
    //     mut self,
    //     control_link_acceptor: impl Into<Option<ControlLinkAcceptor>>,
    // ) -> Self {
    //     self.control_link_acceptor = control_link_acceptor.into();
    //     self
    // }

    cfg_not_wasm32! {
        /// Begins a new session
        ///
        /// # Example
        ///
        /// ```rust, ignore
        /// let session = Session::builder()
        ///     .handle_max(128u32)
        ///     .begin(&mut connection)
        ///     .await.unwrap();
        /// ```
        ///
        pub async fn begin(
            self,
            connection: &mut ConnectionHandle<()>,
        ) -> Result<SessionHandle<()>, BeginError> {
            self.begin_with_channel(connection, |_| {}).await
        }

        /// Associate the allocated outgoing channel before any Begin can be sent.
        /// The synchronous callback runs once after allocation succeeds.
        pub async fn begin_with_channel<F>(
            self,
            connection: &mut ConnectionHandle<()>,
            before_begin: F,
        ) -> Result<SessionHandle<()>, BeginError>
        where
            F: FnOnce(u16) + Send,
        {
            let local_state = SessionState::Unmapped;
            let (session_control_tx, session_control_rx) =
                mpsc::channel::<SessionControl>(DEFAULT_SESSION_CONTROL_BUFFER_SIZE);
            let (incoming_tx, incoming_rx) = mpsc::channel(self.buffer_size);
            let (outgoing_tx, outgoing_rx) = crate::session::transfer_queue::channel(self.buffer_size);

            // create session in connection::Engine
            let outgoing_channel = match connection.allocate_session(incoming_tx).await {
                Ok(channel) => channel,
                Err(alloc_error) => match alloc_error {
                    AllocSessionError::ChannelMaxReached => {
                        // Locally initiating session exceeded channel max
                        return Err(BeginError::LocalChannelMaxReached);
                    }
                    other => return Err(other.into()),
                },
            };

            before_begin(outgoing_channel.0);

            #[cfg(not(all(feature = "transaction", feature = "acceptor")))]
            let (engine_handle, outcome, session_stop_reason, max_frame_size) = {
                let max_frame_size = connection.max_frame_size();
                let session = self.into_session(
                    outgoing_channel,
                    local_state,
                    connection.connection_stop_reason.clone(),
                );
                let session_stop_reason = session.session_stop_reason().clone();
                let (handle, outcome) = SessionEngine::begin_client_session(
                    connection.control.clone(),
                    session,
                    session_control_rx,
                    incoming_rx,
                    connection.outgoing.clone(),
                    outgoing_rx,
                )
                .await?
                .spawn();
                (handle, outcome, session_stop_reason, max_frame_size)
            };

            #[cfg(all(feature = "transaction", feature = "acceptor"))]
            let (engine_handle, outcome, session_stop_reason, max_frame_size) = {
                let mut this = self;
                match this.control_link_acceptor.take() {
                    Some(control_link_acceptor) => {
                        let max_frame_size = connection.max_frame_size();
                        let session = this.into_txn_session(
                            session_control_tx.clone(),
                            outgoing_tx.clone(),
                            outgoing_channel,
                            control_link_acceptor,
                            local_state,
                            connection.connection_stop_reason.clone(),
                            max_frame_size,
                        );
                        let session_stop_reason = session.session_stop_reason().clone();
                        let (handle, outcome) = SessionEngine::begin_client_session(
                            connection.control.clone(),
                            session,
                            session_control_rx,
                            incoming_rx,
                            connection.outgoing.clone(),
                            outgoing_rx,
                        )
                        .await?
                        .spawn();
                        (handle, outcome, session_stop_reason, max_frame_size)
                    }
                    None => {
                        let max_frame_size = connection.max_frame_size();
                        let session = this.into_session(
                            outgoing_channel,
                            local_state,
                            connection.connection_stop_reason.clone(),
                        );
                        let session_stop_reason = session.session_stop_reason().clone();
                        let (handle, outcome) = SessionEngine::begin_client_session(
                            connection.control.clone(),
                            session,
                            session_control_rx,
                            incoming_rx,
                            connection.outgoing.clone(),
                            outgoing_rx,
                        )
                        .await?
                        .spawn();
                        (handle, outcome, session_stop_reason, max_frame_size)
                    }
                }
            };

            let handle = SessionHandle {
                is_ended: false,
            engine_joined: false,
                control: session_control_tx,
                engine_handle,
                outcome,
                outgoing: outgoing_tx,
                session_stop_reason,
                max_frame_size,
                link_listener: (),
            };
            Ok(handle)
        }
    }

    cfg_wasm32! {
        /// Begins a new session on a local set
        ///
        /// # Example
        ///
        /// ```rust, ignore
        /// let session = Session::builder()
        ///     .handle_max(128u32)
        ///     .begin(&mut connection)
        ///     .await.unwrap();
        /// ```
        ///
        pub async fn begin_on_local_set(
            self,
            connection: &mut ConnectionHandle<()>,
            local_set: &tokio::task::LocalSet,
        ) -> Result<SessionHandle<()>, BeginError> {
            let local_state = SessionState::Unmapped;
            let (session_control_tx, session_control_rx) =
                mpsc::channel::<SessionControl>(DEFAULT_SESSION_CONTROL_BUFFER_SIZE);
            let (incoming_tx, incoming_rx) = mpsc::channel(self.buffer_size);
            let (outgoing_tx, outgoing_rx) = crate::session::transfer_queue::channel(self.buffer_size);

            // create session in connection::Engine
            let outgoing_channel = match connection.allocate_session(incoming_tx).await {
                Ok(channel) => channel,
                Err(alloc_error) => match alloc_error {
                    AllocSessionError::ChannelMaxReached => {
                        // Locally initiating session exceeded channel max
                        return Err(BeginError::LocalChannelMaxReached);
                    }
                    other => return Err(other.into()),
                },
            };

            let (engine_handle, outcome, session_stop_reason, max_frame_size) = {
                let max_frame_size = connection.max_frame_size();
                let session = self.into_session(
                    outgoing_channel,
                    local_state,
                    connection.connection_stop_reason.clone(),
                );
                let session_stop_reason = session.session_stop_reason().clone();
                let (handle, outcome) = SessionEngine::begin_client_session(
                    connection.control.clone(),
                    session,
                    session_control_rx,
                    incoming_rx,
                    connection.outgoing.clone(),
                    outgoing_rx,
                )
                .await?
                .spawn_on_local_set(local_set);
                (handle, outcome, session_stop_reason, max_frame_size)
            };

            let handle = SessionHandle {
                is_ended: false,
            engine_joined: false,
                control: session_control_tx,
                engine_handle,
                outcome,
                outgoing: outgoing_tx,
                session_stop_reason,
                max_frame_size,
                link_listener: (),
            };
            Ok(handle)
        }


        /// Begins a new session on the current local set. This internally uses [`tokio::task::spawn_local()`]
        /// and must be called within a [`tokio::task::LocalSet`].
        ///
        /// # Example
        ///
        /// ```rust, ignore
        /// let session = Session::builder()
        ///     .handle_max(128u32)
        ///     .begin(&mut connection)
        ///     .await.unwrap();
        /// ```
        ///
        pub async fn begin_on_current_local_set(
            self,
            connection: &mut ConnectionHandle<()>,
        ) -> Result<SessionHandle<()>, BeginError> {
            let local_state = SessionState::Unmapped;
            let (session_control_tx, session_control_rx) =
                mpsc::channel::<SessionControl>(DEFAULT_SESSION_CONTROL_BUFFER_SIZE);
            let (incoming_tx, incoming_rx) = mpsc::channel(self.buffer_size);
            let (outgoing_tx, outgoing_rx) = crate::session::transfer_queue::channel(self.buffer_size);

            // create session in connection::Engine
            let outgoing_channel = match connection.allocate_session(incoming_tx).await {
                Ok(channel) => channel,
                Err(alloc_error) => match alloc_error {
                    AllocSessionError::ChannelMaxReached => {
                        // Locally initiating session exceeded channel max
                        return Err(BeginError::LocalChannelMaxReached);
                    }
                    other => return Err(other.into()),
                },
            };

            let (engine_handle, outcome, session_stop_reason, max_frame_size) = {
                let max_frame_size = connection.max_frame_size();
                let session = self.into_session(
                    outgoing_channel,
                    local_state,
                    connection.connection_stop_reason.clone(),
                );
                let session_stop_reason = session.session_stop_reason().clone();
                let (handle, outcome) = SessionEngine::begin_client_session(
                    connection.control.clone(),
                    session,
                    session_control_rx,
                    incoming_rx,
                    connection.outgoing.clone(),
                    outgoing_rx,
                )
                .await?
                .spawn_local();
                (handle, outcome, session_stop_reason, max_frame_size)
            };

            let handle = SessionHandle {
                is_ended: false,
            engine_joined: false,
                control: session_control_tx,
                engine_handle,
                outcome,
                outgoing: outgoing_tx,
                session_stop_reason,
                max_frame_size,
                link_listener: (),
            };
            Ok(handle)
        }
    }
}

#[cfg(all(test, feature = "acceptor", not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::{
        acceptor::{ConnectionAcceptor, ListenerConnectionHandle, SessionAcceptor},
        link::SessionStopReason,
        Connection,
    };
    use std::{
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
        task::{Context, Poll},
    };
    use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
    use tokio::sync::oneshot;

    #[derive(Debug)]
    struct Tap {
        inner: DuplexStream,
        bytes: Arc<Mutex<Vec<u8>>>,
    }
    impl AsyncRead for Tap {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }
    impl AsyncWrite for Tap {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let result = Pin::new(&mut self.inner).poll_write(cx, buf);
            if let Poll::Ready(Ok(n)) = &result {
                self.bytes.lock().unwrap().extend_from_slice(&buf[..*n]);
            }
            result
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }
    async fn connections() -> (
        ConnectionHandle<()>,
        ListenerConnectionHandle,
        Arc<Mutex<Vec<u8>>>,
    ) {
        let (client, server) = tokio::io::duplex(8192);
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let acceptor = ConnectionAcceptor::new("begin-peer");
        let (client, server) = tokio::join!(
            Connection::builder()
                .container_id("begin-client")
                .channel_max(0u16)
                .open_with_stream(Tap {
                    inner: client,
                    bytes: bytes.clone()
                }),
            acceptor.accept(server)
        );
        (client.unwrap(), server.unwrap(), bytes)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_begin_channel_is_associated_before_any_begin_can_escape() {
        let (mut connection, mut peer, wire) = connections().await;
        let before = wire.lock().unwrap().len();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let (entered, at) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let (returned, at_return) = oneshot::channel();
        let (take, wait_take) = oneshot::channel();
        let task = tokio::spawn(async move {
            let result = Builder::new()
                .begin_with_channel(&mut connection, move |channel| {
                    count.fetch_add(1, Ordering::AcqRel);
                    entered.send(channel).unwrap();
                    wait.recv_timeout(std::time::Duration::from_secs(10))
                        .expect("callback gate released");
                })
                .await;
            returned.send(()).unwrap();
            wait_take.await.unwrap();
            (connection, result)
        });
        let channel = tokio::time::timeout(std::time::Duration::from_secs(10), at)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(wire.lock().unwrap().len(), before);
        {
            let incoming = peer.next_incoming_session();
            tokio::pin!(incoming);
            tokio_test::assert_pending!(futures_util::poll!(incoming.as_mut()));
        }
        release.send(()).unwrap();
        let incoming = peer.next_incoming_session().await.unwrap();
        assert_eq!(incoming.channel, channel);
        let mut peer_session = SessionAcceptor::new()
            .accept_incoming_session(incoming, &mut peer)
            .await
            .unwrap();
        at_return.await.unwrap();
        peer_session.end().await.unwrap();
        take.send(()).unwrap();
        let (mut connection, result) = task.await.unwrap();
        let mut session = result.unwrap();
        assert!(matches!(
            session.session_stop_reason.get(),
            Some(SessionStopReason::RemoteEnded)
        ));
        assert!(matches!(
            session.stop_and_join().await,
            Err(crate::session::Error::RemoteEnded)
        ));
        let _ = connection.stop_and_join().await;
        let _ = peer.stop_and_join().await;

        // Allocation refusal never calls the callback; ordinary begin still works.
        let (mut connection, mut peer, wire) = connections().await;
        let acceptor = SessionAcceptor::new();
        let (session, peer_session) = tokio::join!(
            Builder::new().begin(&mut connection),
            acceptor.accept(&mut peer)
        );
        let mut session = session.unwrap();
        let mut peer_session = peer_session.unwrap();
        let before = wire.lock().unwrap().len();
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        assert!(matches!(
            Builder::new()
                .begin_with_channel(&mut connection, move |_| {
                    calls.fetch_add(1, Ordering::AcqRel);
                })
                .await,
            Err(BeginError::LocalChannelMaxReached)
        ));
        assert_eq!(count.load(Ordering::Acquire), 0);
        assert_eq!(wire.lock().unwrap().len(), before);
        session.stop_and_join().await.unwrap();
        peer_session.stop_and_join().await.unwrap();
        let _ = connection.stop_and_join().await;
        let _ = peer.stop_and_join().await;

        // A stop after allocation still invokes the callback exactly once;
        // only the native dispatch/answer fails, with its original error.
        let (mut connection, mut peer, _) = connections().await;
        let control = connection.control.clone();
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let (entered, at) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            let result = Builder::new()
                .begin_with_channel(&mut connection, move |channel| {
                    calls.fetch_add(1, Ordering::AcqRel);
                    entered.send(channel).unwrap();
                    wait.recv_timeout(std::time::Duration::from_secs(10))
                        .unwrap();
                })
                .await;
            (connection, result)
        });
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), at)
            .await
            .unwrap()
            .unwrap();
        peer.stop_and_join().await.unwrap();
        control.closed().await;
        release.send(()).unwrap();
        let (mut connection, result) = task.await.unwrap();
        assert!(matches!(result, Err(BeginError::ConnectionStopped(_))));
        assert_eq!(count.load(Ordering::Acquire), 1);
        let _ = connection.stop_and_join().await;
    }
}
