//! Implements AMQP1.0 Session

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, OnceLock},
};

use fe2o3_amqp_types::{
    definitions::{
        self, DeliveryNumber, DeliveryTag, Fields, Handle, Role, SequenceNo, TransferNumber,
    },
    performatives::{Attach, Begin, Detach, Disposition, End, Flow, Transfer},
    primitives::{Symbol, Uint},
    states::SessionState,
};
use slab::Slab;
use tokio::{
    sync::{
        mpsc::{self},
        oneshot::{self, error::TryRecvError},
    },
    task::JoinHandle,
};

use crate::{
    connection::ConnectionStopReason,
    control::SessionControl,
    endpoint::{self, IncomingChannel, InputHandle, LinkFlow, OutgoingChannel, OutputHandle},
    link::{LinkFrame, LinkRelay, SessionStopReason},
    util::{is_consecutive, Constant},
    Payload,
};

cfg_transaction! {
    use fe2o3_amqp_types::{messaging::Accepted, transaction::TransactionError};

    use crate::{
        endpoint::{HandleDeclare, HandleDischarge},
        transaction::AllocTxnIdError,
    };
}

pub(crate) mod engine;
pub(crate) mod frame;
pub(crate) mod receive_window;
pub(crate) mod transfer_queue;

pub mod error;
use error::{
    connection_stop_reason_or_closed, AllocLinkError, SessionInnerError, SessionStateError,
};
pub use error::{BeginError, Error, TryEndError};

mod builder;
pub use builder::*;

use self::frame::{SessionFrame, SessionFrameBody, SessionOutgoingItem};

/// Default incoming_window and outgoing_window
pub const DEFAULT_WINDOW: Uint = 5000;

/// A handle to the [`Session`] event loop
///
/// Dropping the handle will also stop the [`Session`] event loop
///
/// # Generic Parameters
///
/// - `R`: The type of the listener for the link. This will be `()` on the client side.
#[allow(dead_code)]
pub struct SessionHandle<R> {
    /// This value should only be changed in the `on_end` method
    pub(crate) is_ended: bool,
    pub(crate) engine_joined: bool,
    pub(crate) control: mpsc::Sender<SessionControl>,
    pub(crate) engine_handle: JoinHandle<()>,
    pub(crate) outcome: oneshot::Receiver<Result<(), Error>>,

    // outgoing for Link
    pub(crate) outgoing: crate::session::transfer_queue::Sender,
    /// Why the session (or its connection) stopped, shared with the links
    pub(crate) session_stop_reason: Arc<OnceLock<SessionStopReason>>,
    /// The negotiated max frame size (encoder max frame length), shared from
    /// the connection and with the links
    pub(crate) max_frame_size: usize,
    pub(crate) link_listener: R,
}

impl<R> std::fmt::Debug for SessionHandle<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionHandle").finish()
    }
}

impl<R> Drop for SessionHandle<R> {
    fn drop(&mut self) {
        if self.is_ended {
            return;
        }
        if let Err(_error) = self.control.try_send(SessionControl::End(None)) {
            #[cfg(any(feature = "log", feature = "tracing"))]
            {
                let reason = match &_error {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => "control channel is full",
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        "control channel is closed"
                    }
                };
                #[cfg(feature = "tracing")]
                tracing::warn!(reason, "Failed to enqueue End frame on session drop");
                #[cfg(feature = "log")]
                log::warn!("Failed to enqueue End frame on session drop: {reason}");
            }
        }
    }
}

impl<R> SessionHandle<R> {
    /// Stop the local engine and join its task, without sending protocol cleanup.
    /// The owner must separately stop its transport/children. Repeated calls are safe.
    pub async fn stop_and_join(&mut self) -> Result<(), tokio::task::JoinError> {
        self.is_ended = true;
        let _ = self.session_stop_reason.set(SessionStopReason::Stopped);
        if self.engine_joined {
            return Ok(());
        }
        self.engine_handle.abort();
        let result = (&mut self.engine_handle).await;
        self.engine_joined = true;
        match result {
            Err(error) if error.is_cancelled() => Ok(()),
            other => other,
        }
    }

    /// The shared stop reason cell, used by links to observe why the session stopped
    pub(crate) fn session_stop_reason(&self) -> &Arc<OnceLock<SessionStopReason>> {
        &self.session_stop_reason
    }

    /// The negotiated max frame size (encoder max frame length) of the
    /// session's connection, used by links to split transfers and attach
    /// frames
    pub(crate) fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }

    /// Checks if the underlying event loop has stopped
    pub fn is_ended(&self) -> bool {
        match self.is_ended {
            true => true,
            false => self.control.is_closed(),
        }
    }

    /// Tries to end the session
    ///
    /// # Returns
    ///
    /// - `Ok(Ok(()))` if the session has ended successfully
    /// - `Ok(Err(_))` if an error occurred during the session ending on either side
    /// - `Err(TryEndError::AlreadyEnded)` if the session has already ended
    /// - `Err(TryEndError::RemoteEndNotReceived)` if the remote end has not been received yet
    pub fn try_end(&mut self) -> Result<Result<(), Error>, TryEndError> {
        if self.is_ended {
            return Err(TryEndError::AlreadyEnded);
        }

        let _ = self.control.try_send(SessionControl::End(None));
        match self.outcome.try_recv() {
            Ok(res) => {
                self.is_ended = true;
                Ok(res)
            }
            Err(TryRecvError::Empty) => Err(TryEndError::RemoteEndNotReceived),
            Err(TryRecvError::Closed) => {
                self.is_ended = true;
                Ok(Err(Error::IllegalState))
            }
        }
    }

    cfg_not_wasm32! {
        /// End the session
        ///
        /// If the connection stopped before the session, the session ends with it and
        /// this method returns `Ok`; connection-level errors are reported through the
        /// [`ConnectionHandle`](crate::connection::ConnectionHandle).
        ///
        /// An `Error::IllegalState` will be returned if called after any of [`end`](#method.end),
        /// [`end_with_error`](#method.end_with_error), [`on_end`](#on_end) has beend executed. This
        /// will cause the JoinHandle to be polled after completion, which causes a panic.
        ///
        /// # wasm32 support
        ///
        /// This method is not supported on wasm32 targets, please use `drop()` instead.
        pub async fn end(&mut self) -> Result<(), Error> {
            // If sending is unsuccessful, the `SessionEngine` event loop is
            // already dropped, this should be reflected by `JoinError` then.
            let _ = self.control.send(SessionControl::End(None)).await;
            self.on_end().await
        }

        /// Alias for [`end`](#method.end)
        ///
        /// # wasm32 support
        ///
        /// This method is not supported on wasm32 targets, please use `drop()` instead.
        pub async fn close(&mut self) -> Result<(), Error> {
            self.end().await
        }

        /// End the session with an error
        ///
        /// An `Error::IllegalState` will be returned if called after any of [`end`](#method.end),
        /// [`end_with_error`](#method.end_with_error), [`on_end`](#on_end) has beend executed.
        /// This will cause the JoinHandle to be polled after completion, which causes a panic.
        ///
        /// # wasm32 support
        ///
        /// This method is not supported on wasm32 targets, please use `drop()` instead.
        pub async fn end_with_error(
            &mut self,
            error: impl Into<definitions::Error>,
        ) -> Result<(), Error> {
            // If sending is unsuccessful, the `SessionEngine` event loop is
            // already dropped, this should be reflected by `JoinError` then.
            let _ = self
                .control
                .send(SessionControl::End(Some(error.into())))
                .await;
            self.on_end().await
        }
    }

    /// Returns when the underlying event loop has stopped
    ///
    /// An `Error::IllegalState` will be returned if called after any of [`end`](#method.end),
    /// [`end_with_error`](#method.end_with_error), [`on_end`](#on_end) has beend executed. This
    /// will cause the JoinHandle to be polled after completion, which causes a panic.
    pub async fn on_end(&mut self) -> Result<(), Error> {
        if self.is_ended {
            return Err(Error::IllegalState);
        }

        match (&mut self.outcome).await {
            Ok(res) => {
                self.is_ended = true;
                res
            }
            Err(_) => {
                self.is_ended = true;
                Err(Error::IllegalState)
            }
        }
    }
}

/// Size the link inbox for every transfer promised by the session, plus the
/// attach/detach control frames. Tokio allocates queue blocks only as used.
pub(crate) async fn link_incoming_capacity(
    control: &mpsc::Sender<SessionControl>,
    minimum: usize,
    reason: &OnceLock<SessionStopReason>,
) -> Result<usize, AllocLinkError> {
    let (tx, rx) = oneshot::channel();
    let stopped = || {
        AllocLinkError::SessionStopped(reason.get().cloned().unwrap_or(SessionStopReason::Ended))
    };
    control
        .send(SessionControl::GetIncomingWindow(tx))
        .await
        .map_err(|_| stopped())?;
    let window = rx.await.map_err(|_| stopped())?;
    (window as usize)
        .checked_add(2)
        .map(|n| n.max(minimum))
        .filter(|n| *n <= tokio::sync::Semaphore::MAX_PERMITS)
        .ok_or(AllocLinkError::NativeBufferTooLarge)
}

/// # Cancel safety
///
/// It internally `.await` on a send on `tokio::mpsc::Sender` and on a `oneshot::Receiver`.
/// This should be cancel safe
pub(crate) async fn allocate_link(
    control: &mpsc::Sender<SessionControl>,
    link_name: String,
    link_relay: LinkRelay<()>,
    session_stop_reason: &Arc<OnceLock<SessionStopReason>>,
) -> Result<OutputHandle, AllocLinkError> {
    let (responder, resp_rx) = oneshot::channel();

    let reason = || match session_stop_reason.get() {
        Some(reason) => reason.clone(),
        None => {
            // The session engine should always record a stop reason before its
            // channels close; an unset cell here is a defensive fallback.
            #[cfg(feature = "tracing")]
            tracing::warn!(
                "allocate_link: session stop reason not recorded; reporting SessionStopped(Ended)"
            );
            #[cfg(feature = "log")]
            log::warn!(
                "allocate_link: session stop reason not recorded; reporting SessionStopped(Ended)"
            );
            SessionStopReason::Ended
        }
    };

    control
        .send(SessionControl::AllocateLink {
            link_name,
            link_relay,
            responder,
        })
        .await // cancel safe
        // The `SendError` could only happen when the receiving half is
        // dropped, meaning the `SessionEngine::event_loop` has stopped.
        // This would also mean the `Session` is Unmapped, and thus it
        // may be treated as illegal state
        .map_err(|_| AllocLinkError::SessionStopped(reason()))?;
    resp_rx
        .await // FIXME: Is oneshot channel cancel safe?
        // The error could only occur when the sending half is dropped,
        // indicating the `SessionEngine::even_loop` has stopped or
        // unmapped. Thus it could be considered as illegal state
        .map_err(|_| AllocLinkError::SessionStopped(reason()))?
}

/// AMQP1.0 Session
///
/// # Begin a new Session with default configuration
///
/// ```rust,ignore
/// use fe2o3_amqp::Session;
///
/// let session = Session::begin(&mut connection).await.unwrap();
/// ```
///
/// ## Default configuration
///
/// | Field | Default Value |
/// |-------|---------------|
/// |`next_outgoing_id`| 0 |
/// |`incoming_window`| [`DEFAULT_WINDOW`] |
/// |`outgoing_window`| [`DEFAULT_WINDOW`] |
/// |`handle_max`| `u32::MAX` |
/// |`offered_capabilities` | `None` |
/// |`desired_capabilities`| `None` |
/// |`Properties`| `None` |
///
/// # Customize configuration with [`Builder`]
///
/// The builder should be used if the user would like to customize the configuration
/// for the session.
///
/// ```rust, ignore
/// let session = Session::builder()
///     .handle_max(128)
///     .begin(&mut connection)
///     .await.unwrap();
/// ```
///
#[derive(Debug)]
pub struct Session {
    pub(crate) outgoing_channel: OutgoingChannel,

    /// Why this session (or its connection) stopped, shared with the links
    /// and the session handle
    pub(crate) session_stop_reason: Arc<OnceLock<SessionStopReason>>,

    /// Why the connection stopped, shared with the connection engine and the
    /// connection handle
    pub(crate) connection_stop_reason: Arc<OnceLock<ConnectionStopReason>>,

    // local amqp states
    pub(crate) local_state: SessionState,
    pub(crate) initial_outgoing_id: Constant<TransferNumber>,
    pub(crate) next_outgoing_id: TransferNumber,
    pub(crate) incoming_window: TransferNumber,
    pub(crate) outgoing_window: TransferNumber,
    pub(crate) handle_max: Handle,

    // remote amqp states
    pub(crate) incoming_channel: Option<IncomingChannel>,
    // initialize with 0 first and change after receiving the remote Begin
    pub(crate) next_incoming_id: TransferNumber,
    // Consumed transfer slots since the last session-only window update.
    pub(crate) need_flow_count: u32,
    pub(crate) receive_window: Arc<receive_window::ReceiveWindow>,
    pub(crate) remote_incoming_window: SequenceNo,
    // Outgoing transfers that are blocked by the remote-incoming-window
    pub(crate) remote_incoming_window_exhausted_buffer: VecDeque<(
        InputHandle,
        Transfer,
        Payload,
        Option<tokio::sync::OwnedSemaphorePermit>,
    )>,

    // The remote-outgoing-window reflects the maximum number of incoming transfers that MAY
    // arrive without exceeding the remote endpoint’s outgoing-window. This value MUST be
    // decremented after every incoming transfer frame is received, and recomputed when in-
    // formed of the remote session endpoint state. When this window shrinks, it is an
    // indication of outstanding transfers. Settling outstanding transfers can cause the window
    // to grow.
    pub(crate) remote_outgoing_window: SequenceNo,

    // capabilities
    pub(crate) offered_capabilities: Option<Vec<Symbol>>,
    pub(crate) desired_capabilities: Option<Vec<Symbol>>,
    pub(crate) properties: Option<Fields>,

    // local links by output handle
    pub(crate) link_name_by_output_handle: Slab<String>,
    pub(crate) link_by_name: HashMap<String, Option<LinkRelay<OutputHandle>>>,
    pub(crate) link_by_input_handle: HashMap<InputHandle, LinkRelay<OutputHandle>>,
    // Output handles of links whose own detach/close has been sent and is
    // still awaiting the peer's answer
    pub(crate) close_pending: HashSet<OutputHandle>,
    // Maps from DeliveryId to link.DeliveryCount
    pub(crate) delivery_tag_by_id: HashMap<(Role, DeliveryNumber), (InputHandle, DeliveryTag)>, // Role must be the remote peer's role
}

impl Session {
    /// Creates a builder for [`Session`]
    pub fn builder() -> builder::Builder {
        builder::Builder::new()
    }

    cfg_not_wasm32! {
        /// Begins a new session with the default configurations
        ///
        /// # Default configuration
        ///
        /// | Field | Default Value |
        /// |-------|---------------|
        /// |`next_outgoing_id`| 0 |
        /// |`incoming_window`| [`DEFAULT_WINDOW`] |
        /// |`outgoing_window`| [`DEFAULT_WINDOW`] |
        /// |`handle_max`| `u32::MAX` |
        /// |`offered_capabilities` | `None` |
        /// |`desired_capabilities`| `None` |
        /// |`Properties`| `None` |
        ///
        /// # Example
        ///
        /// ```rust,ignore
        /// use fe2o3_amqp::Session;
        ///
        /// let session = Session::begin(&mut connection).await.unwrap();
        /// ```
        pub async fn begin(
            conn: &mut crate::connection::ConnectionHandle<()>,
        ) -> Result<SessionHandle<()>, BeginError> {
            Session::builder().begin(conn).await
        }
    }

    fn on_outgoing_transfer_inner(
        &mut self,
        input_handle: InputHandle,
        mut transfer: Transfer,
        payload: Payload,
        queue_slot: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<SessionFrame, SessionInnerError> {
        // Upon sending a transfer, the sending endpoint will increment its next-outgoing-id, decre-
        // ment its remote-incoming-window, and MAY (depending on policy) decrement its outgoing-
        // window.

        // TODO: What policy would result in a decrement in outgoing-window?

        // If not set on the first (or only) transfer for a (multi-transfer)
        // delivery, then the settled flag MUST be interpreted as being false.
        //
        // If the negotiated value for snd-settle-mode at attachment is settled,
        // then this field MUST be true on at least one transfer frame for a
        // delivery
        //
        // If the negotiated value for snd-settle-mode at attachment is unsettled,
        // then this field MUST be false (or unset) on every transfer frame for a
        // delivery
        let settled = transfer.settled.unwrap_or(false);

        // Only the first transfer is required to have delivery_tag and delivery_id
        if let Some(delivery_tag) = &transfer.delivery_tag {
            // The next-outgoing-id is the transfer-id to assign to the next transfer frame.
            let delivery_id = self.next_outgoing_id;
            transfer.delivery_id = Some(delivery_id);

            // Disposition doesn't carry delivery tag
            if !settled {
                self.delivery_tag_by_id.insert(
                    (Role::Receiver, delivery_id),
                    (input_handle, delivery_tag.clone()),
                );
            }
        }

        self.next_outgoing_id = self.next_outgoing_id.wrapping_add(1);

        // The remote-incoming-window reflects the maximum number of outgoing
        // transfers that can be sent without exceeding the remote endpoint’s
        // incoming-window. This value MUST be decremented after every transfer
        // frame is sent, and recomputed when informed of the remote session
        // endpoint state.
        self.remote_incoming_window = self.remote_incoming_window.saturating_sub(1);

        let body = SessionFrameBody::Transfer {
            performative: transfer,
            payload,
        };
        let mut frame = SessionFrame::new(self.outgoing_channel, body);
        frame.queue_slot = queue_slot;
        Ok(frame)
    }

    /// Build a session-only flow that re-advertises the session window. It carries no link
    /// state (`handle` and the link fields are unset), so it only updates the peer's view of
    /// our session window via the advanced `next-incoming-id`.
    fn on_outgoing_session_flow(&self) -> SessionFrame {
        let flow = Flow {
            // Session flow states
            next_incoming_id: Some(self.next_incoming_id),
            incoming_window: self.receive_window.available(),
            next_outgoing_id: self.next_outgoing_id,
            outgoing_window: self.outgoing_window,
            // No link flow states: this is a session-only flow
            handle: None,
            delivery_count: None,
            link_credit: None,
            available: None,
            drain: false,
            echo: false,
            properties: None,
        };

        let body = SessionFrameBody::Flow(flow);
        SessionFrame::new(self.outgoing_channel, body)
    }

    async fn on_incoming_flow_inner(
        &mut self,
        flow: Flow,
    ) -> Result<Option<LinkFlow>, SessionInnerError> {
        // Handle session flow control
        //
        // When the endpoint receives a flow frame from its peer, it MUST update
        // the next-incoming-id directly from the next-outgoing-id of the frame,
        // and it MUST update the remote-outgoing- window directly from the
        // outgoing-window of the frame.
        self.next_incoming_id = flow.next_outgoing_id;
        self.remote_outgoing_window = flow.outgoing_window;

        // Sequence numbers wrap at 2^32. Subtract the transfers already sent
        // beyond the peer's acknowledgement; ordinary saturating addition would
        // grant the wrong window when either sequence crosses that boundary.
        let peer_next = flow
            .next_incoming_id
            .unwrap_or_else(|| *self.initial_outgoing_id.value());
        let in_flight = self.next_outgoing_id.wrapping_sub(peer_next);
        self.remote_incoming_window = flow.incoming_window.saturating_sub(in_flight);

        // Handle link flow control
        if let Ok(link_flow) = LinkFlow::try_from(flow) {
            let input_handle = InputHandle::from(link_flow.handle.clone());
            match self.link_by_input_handle.get_mut(&input_handle) {
                Some(link_relay) => {
                    return link_relay
                        .on_incoming_flow(link_flow)
                        .await
                        .map_err(Into::into);
                }
                None => return Err(SessionInnerError::UnattachedHandle), // End session with unattached handle?
            }
        }

        Ok(None)
    }

    fn prepare_session_frames_from_buffered_transfers(
        &mut self,
        mut output_frame_buffer: Vec<SessionFrame>,
    ) -> Result<Vec<SessionFrame>, SessionInnerError> {
        // Drain the buffered transfers as much as possible
        while self.remote_incoming_window > 0 {
            if let Some((input_handle, transfer, payload, queue_slot)) =
                self.remote_incoming_window_exhausted_buffer.pop_front()
            {
                let frame =
                    self.on_outgoing_transfer_inner(input_handle, transfer, payload, queue_slot)?;
                output_frame_buffer.push(frame);
            } else {
                break;
            }
        }
        Ok(output_frame_buffer)
    }

    /// Drain the buffered transfers frames and current transfer frame as much as possible
    fn prepare_session_frames_from_buffered_and_current_transfers(
        &mut self,
        output_frame_buffer: Vec<SessionFrame>,
        cur_input_handle: InputHandle,
        cur_transfer: Transfer,
        cur_payload: Payload,
        cur_queue_slot: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<Vec<SessionFrame>, SessionInnerError> {
        // Drain the buffered transfers first
        let mut frames =
            self.prepare_session_frames_from_buffered_transfers(output_frame_buffer)?;

        // Then process the current transfer if there is still space in the
        // remote-incoming-window
        if self.remote_incoming_window > 0 {
            let frame = self.on_outgoing_transfer_inner(
                cur_input_handle,
                cur_transfer,
                cur_payload,
                cur_queue_slot,
            )?;
            frames.push(frame);
        } else {
            self.remote_incoming_window_exhausted_buffer.push_back((
                cur_input_handle,
                cur_transfer,
                cur_payload,
                cur_queue_slot,
            ));
        }
        Ok(frames)
    }
}

impl endpoint::Session for Session {
    type AllocError = AllocLinkError;
    type BeginError = SessionStateError;
    type EndError = SessionStateError;
    type Error = SessionInnerError;
    type State = SessionState;

    fn local_state(&self) -> &Self::State {
        &self.local_state
    }

    fn set_session_stop_reason(&mut self, reason: SessionStopReason) {
        let _ = self.session_stop_reason.set(reason);
    }

    fn session_stop_reason(&self) -> &Arc<OnceLock<SessionStopReason>> {
        &self.session_stop_reason
    }

    fn connection_stop_reason(&self) -> &Arc<OnceLock<ConnectionStopReason>> {
        &self.connection_stop_reason
    }

    fn outgoing_channel(&self) -> OutgoingChannel {
        self.outgoing_channel
    }

    fn allocate_link(
        &mut self,
        link_name: String,
        link_relay: Option<LinkRelay<()>>, // TODO: why is this `Option`?
    ) -> Result<OutputHandle, Self::AllocError> {
        match &self.local_state {
            SessionState::Mapped => {}
            _ => {
                return Err(match self.session_stop_reason.get() {
                    // The session (or its connection) stopped; the reason is
                    // recorded before the session becomes Unmapped
                    Some(reason) => AllocLinkError::SessionStopped(reason.clone()),
                    None => match &self.local_state {
                        // The session is ending while the engine still runs; the
                        // stop reason is only recorded at engine exit, so `Ended`
                        // is accurate here (defensive fallback)
                        SessionState::EndSent
                        | SessionState::EndReceived
                        | SessionState::Discarding => {
                            #[cfg(feature = "tracing")]
                            tracing::warn!(
                                "allocate_link: session stop reason not recorded; reporting SessionStopped(Ended)"
                            );
                            #[cfg(feature = "log")]
                            log::warn!(
                                "allocate_link: session stop reason not recorded; reporting SessionStopped(Ended)"
                            );
                            AllocLinkError::SessionStopped(SessionStopReason::Ended)
                        }
                        // Not begun yet (or fully ended without a recorded stop):
                        // the session exists but is not mapped
                        _ => AllocLinkError::SessionNotMapped,
                    },
                });
            }
        };

        // check whether link name is duplciated
        if self.link_by_name.contains_key(&link_name) {
            return Err(AllocLinkError::DuplicatedLinkName);
        }

        // get a new entry index
        let entry = self.link_name_by_output_handle.vacant_entry();
        let handle = OutputHandle(entry.key() as u32);

        entry.insert(link_name.clone());
        let value = link_relay.map(|val| val.with_output_handle(handle.clone()));
        self.link_by_name.insert(link_name, value);
        Ok(handle)
    }

    fn allocate_incoming_link(
        &mut self,
        link_name: String,
        link_relay: LinkRelay<()>,
        input_handle: InputHandle,
    ) -> Result<OutputHandle, Self::AllocError> {
        match self.allocate_link(link_name, None) {
            Ok(output_handle) => {
                let value = link_relay.with_output_handle(output_handle.clone());
                self.link_by_input_handle.insert(input_handle, value);
                Ok(output_handle)
            }
            Err(err) => Err(err),
        }
    }

    /// This should only deallocate the output handle. Returns whether the
    /// link's bookkeeping was still present (i.e. the detach is the first for
    /// the link rather than a duplicate).
    fn deallocate_link(&mut self, output_handle: OutputHandle) -> bool {
        if let Some(name) = self
            .link_name_by_output_handle
            .try_remove(output_handle.0 as usize)
        {
            let _ = self.link_by_name.remove(&name);
            true
        } else {
            false
        }
    }

    fn on_incoming_begin(
        &mut self,
        channel: IncomingChannel,
        begin: Begin,
    ) -> Result<(), Self::BeginError> {
        match self.local_state {
            SessionState::Unmapped => self.local_state = SessionState::BeginReceived,
            SessionState::BeginSent => self.local_state = SessionState::Mapped,
            _ => return Err(SessionStateError::IllegalState), // End session with unattached handle?
        }

        self.incoming_channel = Some(channel);
        self.next_incoming_id = begin.next_outgoing_id;
        self.remote_incoming_window = begin.incoming_window;
        self.remote_outgoing_window = begin.outgoing_window;

        Ok(())
    }

    async fn on_incoming_attach(&mut self, attach: Attach) -> Result<(), Self::Error> {
        match self.link_by_name.get_mut(&attach.name) {
            Some(link) => match link.take() {
                Some(mut relay) => {
                    // Only Sender need to update the receiver settle mode
                    // because the sender needs to echo a disposition if
                    // rcv-settle-mode is 1
                    if let LinkRelay::Sender {
                        receiver_settle_mode,
                        ..
                    } = &mut relay
                    {
                        *receiver_settle_mode = attach.rcv_settle_mode.clone();
                    }

                    let input_handle = InputHandle::from(attach.handle.clone()); // handle is just a wrapper around u32
                    relay
                        .send(LinkFrame::Attach(attach))
                        .await
                        .map_err(|_| SessionInnerError::UnattachedHandle)?;
                    self.link_by_input_handle.insert(input_handle, relay);

                    Ok(())
                }
                None => {
                    // Link name is found but is already in use
                    Err(SessionInnerError::HandleInUse)
                }
            },
            None => Err(SessionInnerError::RemoteAttachingLinkNameNotFound), // End session with unattached handle?,
        }
    }

    async fn on_incoming_flow(
        &mut self,
        flow: Flow,
    ) -> Result<Option<SessionOutgoingItem>, Self::Error> {
        let outgoing_link_flow = self.on_incoming_flow_inner(flow).await?;
        let outgoing_session_flow = outgoing_link_flow
            .map(|flow| self.on_outgoing_flow(flow))
            .transpose()?;

        // Process buffered outgoing transfer frames if the updated remote-incoming-window is
        // greater than 0
        if self.remote_incoming_window > 0
            && !self.remote_incoming_window_exhausted_buffer.is_empty()
        {
            let mut output_frame_buffer = Vec::with_capacity(
                self.remote_incoming_window_exhausted_buffer
                    .len()
                    .saturating_add(1),
            );
            if let Some(outgoing_session_flow) = outgoing_session_flow {
                output_frame_buffer.push(outgoing_session_flow);
            }
            let frames =
                self.prepare_session_frames_from_buffered_transfers(output_frame_buffer)?;
            Ok(Some(SessionOutgoingItem::MultipleFrames(frames)))
        } else {
            Ok(outgoing_session_flow.map(SessionOutgoingItem::SingleFrame))
        }
    }

    /// Handle an incoming transfer.
    ///
    /// Always returns `Ok(None)`: settlement of non-transactional deliveries is
    /// handled at the link level, so this session never produces an immediate
    /// disposition. The `Option<Disposition>` return is only used by the
    /// transactional session ([`crate::transaction::session::TxnSession`]) for
    /// the presumptive-outcome reply.
    async fn on_incoming_transfer(
        &mut self,
        transfer: Transfer,
        payload: Payload,
    ) -> Result<Option<Disposition>, Self::Error> {
        // Upon receiving a transfer, the receiving endpoint will increment the next-incoming-id to
        // match the implicit transfer-id of the incoming transfer plus one, as well as decrementing the
        // remote-outgoing-window, and MAY (depending on policy) decrement its incoming-window.
        self.next_incoming_id = self.next_incoming_id.wrapping_add(1);
        self.remote_outgoing_window = self.remote_outgoing_window.saturating_sub(1);
        let slot = self
            .receive_window
            .reserve()
            .ok_or(SessionInnerError::WindowViolation)?;

        let input_handle = InputHandle::from(transfer.handle.clone());
        match self.link_by_input_handle.get_mut(&input_handle) {
            Some(link_relay) => {
                let id_and_tag = link_relay
                    .on_incoming_transfer(transfer, payload, Some(slot))
                    .await?;

                // FIXME: If the unsettled map needs this
                if let Some((delivery_id, delivery_tag)) = id_and_tag {
                    self.delivery_tag_by_id
                        .insert((Role::Sender, delivery_id), (input_handle, delivery_tag));
                }
            }
            None => return Err(SessionInnerError::UnattachedHandle),
        };

        Ok(None)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    fn on_incoming_disposition(
        &mut self,
        disposition: Disposition,
    ) -> Result<Option<Vec<Disposition>>, Self::Error> {
        let first = disposition.first;
        let last = disposition.last.unwrap_or(first);

        // The peer chooses a serial-number interval, not a loop bound. Work is
        // proportional to our actual unsettled identities, including wraparound.
        let width = last.wrapping_sub(first);
        let mut matching_ids: Vec<_> = self
            .delivery_tag_by_id
            .keys()
            .filter_map(|(role, id)| {
                (role == &disposition.role && id.wrapping_sub(first) <= width).then_some(*id)
            })
            .collect();
        matching_ids.sort_unstable_by_key(|id| id.wrapping_sub(first));

        // A disposition frame may refer to deliveries on multiple links, each may be running
        // in different mode. This counts the largest sections that can be echoed back together
        if disposition.settled {
            // If it is alrea
            for delivery_id in matching_ids {
                let key = (disposition.role.clone(), delivery_id);
                if let Some((handle, delivery_tag)) = self.delivery_tag_by_id.remove(&key) {
                    if let Some(link_handle) = self.link_by_input_handle.get_mut(&handle) {
                        let _echo = link_handle.on_incoming_disposition(
                            disposition.role.clone(),
                            disposition.settled,
                            disposition.state.clone(),
                            delivery_tag,
                        );
                    }
                }
            }

            Ok(None)
        } else {
            let mut delivery_ids = Vec::new();
            for delivery_id in matching_ids {
                let key = (disposition.role.clone(), delivery_id);
                if let Some((handle, delivery_tag)) = self.delivery_tag_by_id.get(&key) {
                    if let Some(link_handle) = self.link_by_input_handle.get_mut(handle) {
                        // In mode Second, the receiver will first send a non-settled disposition,
                        // and wait for sender's settled disposition
                        let echo = link_handle.on_incoming_disposition(
                            disposition.role.clone(),
                            disposition.settled,
                            disposition.state.clone(),
                            delivery_tag.clone(),
                        );

                        if echo {
                            delivery_ids.push(delivery_id);
                        }
                    }
                }
            }

            // Chunking expects numeric order and must not span uint wraparound.
            delivery_ids.sort_unstable();
            let chunk_inds = consecutive_chunk_indices(&delivery_ids[..]);

            let mut dispositions = Vec::with_capacity(chunk_inds.len());
            let mut prev_ind = 0;
            for ind in chunk_inds {
                let slice = &delivery_ids[prev_ind..ind];
                let disposition = Disposition {
                    role: Role::Sender,
                    first: slice[0],
                    last: slice.last().copied(),
                    settled: true,
                    state: disposition.state.clone(),
                    batchable: false,
                };
                dispositions.push(disposition);
                prev_ind = ind;
            }
            Ok(Some(dispositions))
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_incoming_detach(&mut self, detach: Detach) -> Result<Option<Detach>, Self::Error> {
        #[cfg(feature = "tracing")]
        tracing::trace!(frame = ?detach);
        #[cfg(feature = "log")]
        log::trace!("RECV frame = {:?}", detach);
        // Remove the link by input handle
        match self
            .link_by_input_handle
            .remove(&InputHandle::from(detach.handle.clone()))
        {
            Some(mut link) => {
                let output_handle = match &link {
                    LinkRelay::Sender { output_handle, .. }
                    | LinkRelay::Receiver { output_handle, .. } => output_handle.clone(),
                };
                // If the link is in `close_pending`, this detach answers the
                // one the link sent itself; otherwise the peer detached the
                // link on its own.
                let remote_initiated = !self.close_pending.remove(&output_handle);

                // Forward the detach into the link engine. If the peer
                // detached the link on its own, the relay returns the reply
                // detach; it is sent out via `on_outgoing_detach` (see the
                // trait doc), which releases the link bookkeeping.
                let response = link.on_incoming_detach(detach, remote_initiated).await;

                Ok(response)
            }
            None => {
                // The link is no longer registered: both sides closed it at
                // the same time, our reply to the peer's first detach removed
                // it, and the peer then answered the detach we sent for our
                // own close. That second detach is for a handle that no
                // longer exists, so it is dropped instead of ending the
                // session.
                #[cfg(feature = "tracing")]
                tracing::trace!(frame = ?detach, "incoming detach for an unattached handle is ignored");
                #[cfg(feature = "log")]
                log::trace!(
                    "incoming detach for an unattached handle is ignored: {:?}",
                    detach
                );
                Ok(None)
            }
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    fn on_incoming_end(
        &mut self,
        _channel: IncomingChannel,
        end: End,
    ) -> Result<(), Self::EndError> {
        #[cfg(feature = "tracing")]
        tracing::trace!(end = ?end);
        #[cfg(feature = "log")]
        log::trace!("RECV end = {:?}", end);
        match self.local_state {
            SessionState::BeginSent | SessionState::BeginReceived | SessionState::Mapped => {
                self.local_state = SessionState::EndReceived;

                match end.error {
                    Some(err) => Err(SessionStateError::RemoteEndedWithError(err)),
                    None => Err(SessionStateError::RemoteEnded),
                }
            }
            SessionState::EndSent | SessionState::Discarding => {
                self.local_state = SessionState::Unmapped;

                if let Some(error) = end.error {
                    #[cfg(feature = "tracing")]
                    tracing::error!(remote_error = ?error);
                    #[cfg(feature = "log")]
                    log::error!("remote_error = {:?}", error);
                    return Err(SessionStateError::RemoteEndedWithError(error));
                }
                Ok(())
            }
            _ => Err(SessionStateError::IllegalState), // End session with illegal state?
        }
    }

    async fn send_begin(
        &mut self,
        writer: &mpsc::Sender<SessionFrame>,
    ) -> Result<(), Self::BeginError> {
        let begin = Begin {
            remote_channel: self.incoming_channel.map(Into::into),
            next_outgoing_id: self.next_outgoing_id,
            incoming_window: self.receive_window.available(),
            outgoing_window: self.outgoing_window,
            handle_max: self.handle_max.clone(),
            offered_capabilities: self.offered_capabilities.clone().map(Into::into),
            desired_capabilities: self.desired_capabilities.clone().map(Into::into),
            properties: self.properties.clone(),
        };
        let frame = SessionFrame::new(self.outgoing_channel, SessionFrameBody::Begin(begin));

        // check local states
        match &self.local_state {
            SessionState::Unmapped => {
                writer
                    .send(frame)
                    .await
                    // The receiving half must have dropped, and thus the `Connection`
                    // event loop has stopped.
                    .map_err(|_| {
                        SessionStateError::ConnectionStopped(connection_stop_reason_or_closed(
                            &self.connection_stop_reason,
                        ))
                    })?;
                self.local_state = SessionState::BeginSent;
            }
            SessionState::BeginReceived => {
                writer.send(frame).await.map_err(|_| {
                    SessionStateError::ConnectionStopped(connection_stop_reason_or_closed(
                        &self.connection_stop_reason,
                    ))
                })?;
                self.local_state = SessionState::Mapped;
            }
            _ => return Err(SessionStateError::IllegalState),
        }

        Ok(())
    }

    async fn send_end(
        &mut self,
        writer: &mpsc::Sender<SessionFrame>,
        error: Option<definitions::Error>,
    ) -> Result<(), Self::EndError> {
        match self.local_state {
            SessionState::Mapped => match error.is_some() {
                true => self.local_state = SessionState::Discarding,
                false => self.local_state = SessionState::EndSent,
            },
            SessionState::EndReceived => self.local_state = SessionState::Unmapped,
            _ => return Err(SessionStateError::IllegalState),
        }

        let frame = SessionFrame::new(self.outgoing_channel, SessionFrameBody::End(End { error }));
        writer
            .send(frame)
            .await
            // The receiving half must have dropped, and thus the `Connection`
            // event loop has stopped.
            .map_err(|_| {
                SessionStateError::ConnectionStopped(connection_stop_reason_or_closed(
                    &self.connection_stop_reason,
                ))
            })?;
        Ok(())
    }

    fn on_outgoing_attach(&mut self, attach: Attach) -> Result<SessionFrame, Self::Error> {
        let body = SessionFrameBody::Attach(attach);
        let frame = SessionFrame::new(self.outgoing_channel, body);
        Ok(frame)
    }

    fn on_outgoing_flow(&mut self, flow: LinkFlow) -> Result<SessionFrame, Self::Error> {
        let flow = Flow {
            // Session flow states
            next_incoming_id: Some(self.next_incoming_id),
            incoming_window: self.receive_window.available(),
            next_outgoing_id: self.next_outgoing_id,
            outgoing_window: self.outgoing_window,
            // Link flow states
            handle: Some(flow.handle),
            delivery_count: flow.delivery_count,
            link_credit: flow.link_credit,
            available: flow.available,
            drain: flow.drain,
            echo: flow.echo,
            properties: flow.properties,
        };

        let body = SessionFrameBody::Flow(flow);
        let frame = SessionFrame::new(self.outgoing_channel, body);
        Ok(frame)
    }

    /// Slots remain occupied while a frame waits in a link inbox. Consumption
    /// wakes the engine so it can replenish the peer's window even without a
    /// new inbound frame or application command.
    fn receive_window(&self) -> &Arc<receive_window::ReceiveWindow> {
        &self.receive_window
    }

    fn maybe_outgoing_session_flow(&mut self) -> Option<SessionOutgoingItem> {
        self.need_flow_count = self
            .need_flow_count
            .saturating_add(self.receive_window.take_released());
        // Only send while the session is fully mapped; no flows are emitted during teardown.
        if !matches!(self.local_state, SessionState::Mapped) {
            return None;
        }

        if self.need_flow_count >= (self.incoming_window / 2).max(1) {
            self.need_flow_count = 0;
            Some(SessionOutgoingItem::SingleFrame(
                self.on_outgoing_session_flow(),
            ))
        } else {
            None
        }
    }

    fn on_outgoing_transfer(
        &mut self,
        input_handle: InputHandle,
        transfer: Transfer,
        payload: Payload,
        queue_slot: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<Option<SessionOutgoingItem>, Self::Error> {
        // Check if remote-incoming-window is exhausted
        if self.remote_incoming_window == 0 {
            // exhausted
            self.remote_incoming_window_exhausted_buffer.push_back((
                input_handle,
                transfer,
                payload,
                queue_slot,
            ));
            Ok(None)
        } else if self.remote_incoming_window_exhausted_buffer.is_empty() {
            // no buffered transfer
            let frame =
                self.on_outgoing_transfer_inner(input_handle, transfer, payload, queue_slot)?;
            Ok(Some(SessionOutgoingItem::SingleFrame(frame)))
        } else {
            let output_frame_buffer = Vec::with_capacity(
                self.remote_incoming_window_exhausted_buffer
                    .len()
                    .saturating_add(1),
            );
            self.prepare_session_frames_from_buffered_and_current_transfers(
                output_frame_buffer,
                input_handle,
                transfer,
                payload,
                queue_slot,
            )
            .map(SessionOutgoingItem::MultipleFrames)
            .map(Some)
        }
    }

    fn on_outgoing_disposition(
        &mut self,
        disposition: Disposition,
    ) -> Result<SessionFrame, Self::Error> {
        // Currently the sender cannot actively dispose any message
        // because the sender doesn't have access to the delivery_id

        // The remote-outgoing-window reflects the maximum number of incoming transfers that MAY
        // arrive without exceeding the remote endpoint’s outgoing-window. This value MUST be
        // decremented after every incoming transfer frame is received, and recomputed when in-
        // formed of the remote session endpoint state. When this window shrinks, it is an
        // indication of outstanding transfers. Settling outstanding transfers can cause the window
        // to grow.
        if disposition
            .state
            .as_ref()
            .map(|s| s.is_terminal())
            .unwrap_or(false)
        {
            let count = num_messages_settled_by_disposition(disposition.first, disposition.last);
            self.remote_outgoing_window = self.remote_outgoing_window.saturating_add(count);
        }

        let body = SessionFrameBody::Disposition(disposition);
        let frame = SessionFrame::new(self.outgoing_channel, body);
        Ok(frame)
    }

    /// The single outbound path for detach frames (see the trait doc).
    fn on_outgoing_detach(&mut self, detach: Detach, expects_echo: bool) -> Option<SessionFrame> {
        let deallocated = self.deallocate_link(detach.handle.clone().into());
        if expects_echo {
            // Only a detach the link sent itself will be answered by the
            // peer. An entry for any other detach would stay stale and could
            // make a later detach on a reused handle look like it is waiting
            // for an answer.
            if !deallocated {
                // Both sides closed at the same time: the relay already sent
                // the closing detach for this link, so this one would only
                // repeat it. The engine's own close/detach still completes —
                // it consumes the peer's detach that the relay forwarded.
                #[cfg(feature = "tracing")]
                tracing::debug!("Suppressing duplicate locally initiated detach");
                #[cfg(feature = "log")]
                log::debug!("Suppressing duplicate locally initiated detach");
                return None;
            }
            self.close_pending.insert(detach.handle.clone().into());
        }
        let body = SessionFrameBody::Detach(detach);
        Some(SessionFrame::new(self.outgoing_channel, body))
    }
}

fn num_messages_settled_by_disposition(first: u32, last: Option<u32>) -> u32 {
    last.and_then(|last| last.checked_sub(first)).unwrap_or(0) + 1
}

cfg_transaction! {
    impl HandleDeclare for Session {
        // This should be unreachable, but an error is probably a better way
        fn allocate_transaction_id(
            &mut self,
        ) -> Result<fe2o3_amqp_types::transaction::TransactionId, AllocTxnIdError> {
            // Err(Error::amqp_error(AmqpError::NotImplemented, "Resource side transaction is not enabled".to_string()))
            Err(AllocTxnIdError::NotImplemented)
        }
    }

    impl HandleDischarge for Session {
        async fn commit_transaction(
            &mut self,
            _txn_id: fe2o3_amqp_types::transaction::TransactionId,
        ) -> Result<Result<Accepted, TransactionError>, Self::Error> {
            // FIXME: This should be impossible
            Ok(Err(TransactionError::UnknownId))
        }

        fn rollback_transaction(
            &mut self,
            _txn_id: fe2o3_amqp_types::transaction::TransactionId,
        ) -> Result<Result<Accepted, TransactionError>, Self::Error> {
            // FIXME: This should be impossible
            Ok(Err(TransactionError::UnknownId))
        }
    }
}

fn consecutive_chunk_indices(delivery_ids: &[DeliveryNumber]) -> Vec<usize> {
    delivery_ids
        .windows(2)
        .enumerate()
        .filter_map(|(i, id)| {
            if is_consecutive(&id[0], &id[1]) {
                None
            } else {
                Some(i + 1)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, OnceLock};

    use super::{
        num_messages_settled_by_disposition, Builder, Session, SessionFrame, SessionFrameBody,
        SessionOutgoingItem, SessionState, DEFAULT_WINDOW,
    };
    use crate::endpoint::{OutgoingChannel, Session as _};

    fn mapped_session() -> Session {
        Builder::new()
            .incoming_window(DEFAULT_WINDOW)
            .outgoing_window(DEFAULT_WINDOW)
            .into_session(
                OutgoingChannel(0),
                SessionState::Mapped,
                Arc::new(OnceLock::new()),
            )
    }

    #[test]
    fn peer_disposition_ranges_visit_only_live_ids_and_cover_serial_wrap() {
        use crate::endpoint::InputHandle;
        use fe2o3_amqp_types::{definitions::Role, performatives::Disposition};
        let mut session = mapped_session();
        for id in [0, 1, 4, u32::MAX - 1, u32::MAX] {
            session
                .delivery_tag_by_id
                .insert((Role::Sender, id), (InputHandle(0), vec![1].into()));
        }
        session
            .delivery_tag_by_id
            .insert((Role::Receiver, 0), (InputHandle(0), vec![2].into()));
        let mut frame = Disposition {
            role: Role::Sender,
            first: u32::MAX - 1,
            last: Some(1),
            settled: true,
            state: None,
            batchable: false,
        };
        session.on_incoming_disposition(frame.clone()).unwrap();
        assert_eq!(session.delivery_tag_by_id.len(), 2);
        assert!(session.delivery_tag_by_id.contains_key(&(Role::Sender, 4)));
        assert!(session
            .delivery_tag_by_id
            .contains_key(&(Role::Receiver, 0)));
        // This used to enumerate all 2^32 possible IDs on the session driver.
        frame.first = 0;
        frame.last = Some(u32::MAX);
        session.on_incoming_disposition(frame).unwrap();
        assert_eq!(session.delivery_tag_by_id.len(), 1);
    }

    #[tokio::test]
    async fn unread_transfers_hold_session_window_but_do_not_block_settlement() {
        use crate::{
            endpoint::{InputHandle, OutputHandle},
            link::{
                state::{LinkFlowState, LinkFlowStateInner},
                unsettled_store::Store,
                LinkFrame, LinkRelay,
            },
            Payload,
        };
        use fe2o3_amqp_types::{
            definitions::{ReceiverSettleMode, Role},
            performatives::{Disposition, Transfer},
        };
        let mut session = Builder::new().incoming_window(4).into_session(
            OutgoingChannel(0),
            SessionState::Mapped,
            Arc::new(OnceLock::new()),
        );
        let (tx, mut inbox) = tokio::sync::mpsc::channel(6);
        let unsettled = Arc::new(Store::new(None));
        let flow = Arc::new(LinkFlowState::receiver(LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 8,
            available: 0,
            drain: false,
            properties: None,
        }));
        let relay =
            LinkRelay::new_receiver(tx, flow, unsettled.clone(), ReceiverSettleMode::Second)
                .with_output_handle(OutputHandle(0));
        session.link_by_input_handle.insert(InputHandle(0), relay);
        let template: Transfer =
            serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x55, 0x43])
                .unwrap();
        for id in 0..4 {
            let mut transfer = template.clone();
            transfer.delivery_id = Some(id);
            transfer.delivery_tag = Some(vec![id as u8].into());
            session
                .on_incoming_transfer(transfer, Payload::new())
                .await
                .unwrap();
            assert!(session.maybe_outgoing_session_flow().is_none());
        }
        assert_eq!(session.receive_window.available(), 0);
        session
            .on_incoming_disposition(Disposition {
                role: Role::Sender,
                first: 0,
                last: Some(3),
                settled: true,
                state: None,
                batchable: false,
            })
            .unwrap();
        assert!(unsettled.read().as_ref().unwrap().is_empty());
        assert_eq!(
            session.receive_window.available(),
            0,
            "settlement is not frame consumption"
        );
        let first = inbox.recv().await.unwrap();
        assert!(matches!(first, LinkFrame::Transfer { .. }));
        assert_eq!(
            session.receive_window.available(),
            0,
            "dequeue alone must retain the slot during processing"
        );
        drop(first);
        assert_eq!(session.receive_window.available(), 1);
        assert!(session.maybe_outgoing_session_flow().is_none());
        drop(inbox.recv().await.unwrap());
        let Some(SessionOutgoingItem::SingleFrame(SessionFrame {
            body: SessionFrameBody::Flow(flow),
            ..
        })) = session.maybe_outgoing_session_flow()
        else {
            panic!("consumption must reopen window")
        };
        assert_eq!(flow.incoming_window, 2);
        assert_eq!(flow.next_incoming_id, Some(4));
        drop(inbox);
        assert_eq!(
            session.receive_window.available(),
            4,
            "closing a child releases its queued slots"
        );
    }

    #[tokio::test]
    async fn excess_transfer_is_rejected_without_creating_an_unsettled_identity() {
        use crate::Payload;
        let mut session = Builder::new().incoming_window(0).into_session(
            OutgoingChannel(0),
            SessionState::Mapped,
            Arc::new(OnceLock::new()),
        );
        let transfer =
            serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x55, 0x43])
                .unwrap();
        assert!(matches!(
            session.on_incoming_transfer(transfer, Payload::new()).await,
            Err(super::SessionInnerError::WindowViolation)
        ));
        assert!(session.delivery_tag_by_id.is_empty());
        assert_eq!(session.receive_window.available(), 0);
    }

    #[tokio::test]
    async fn peer_window_accounts_for_in_flight_transfers_across_sequence_wrap() {
        use fe2o3_amqp_types::performatives::Flow;
        let mut session = mapped_session();
        session.next_outgoing_id = 1;
        let mut flow = Flow {
            next_incoming_id: Some(u32::MAX - 1),
            incoming_window: 8,
            next_outgoing_id: 0,
            outgoing_window: 8,
            handle: None,
            delivery_count: None,
            link_credit: None,
            available: None,
            drain: false,
            echo: false,
            properties: None,
        };
        session.on_incoming_flow_inner(flow.clone()).await.unwrap();
        assert_eq!(session.remote_incoming_window, 5);
        flow.incoming_window = 2;
        session.on_incoming_flow_inner(flow.clone()).await.unwrap();
        assert_eq!(session.remote_incoming_window, 0);
        flow.next_incoming_id = Some(1);
        session.on_incoming_flow_inner(flow).await.unwrap();
        assert_eq!(session.remote_incoming_window, 2);
    }

    #[test]
    fn number_of_message_settled_by_disposition() {
        let first = 1;
        let last = Some(3);
        let count = num_messages_settled_by_disposition(first, last);
        assert_eq!(count, 3);

        let first = 1;
        let last = None;
        let count = num_messages_settled_by_disposition(first, last);
        assert_eq!(count, 1);

        // This should be impossible as the Link will sort the delivery_ids first
        let first = 3;
        let last = Some(1);
        let count = num_messages_settled_by_disposition(first, last);
        assert_eq!(count, 1);
    }

    #[test]
    fn maybe_outgoing_session_flow_fires_at_half_window() {
        let mut session = mapped_session();

        // Below half of the incoming window: no flow is due.
        session.need_flow_count = DEFAULT_WINDOW / 2 - 1;
        assert!(session.maybe_outgoing_session_flow().is_none());

        // At half of the incoming window: a session-only flow is due and the
        // counter is reset.
        session.need_flow_count = DEFAULT_WINDOW / 2;
        let item = session
            .maybe_outgoing_session_flow()
            .expect("session flow should be due");
        let flow = match item {
            SessionOutgoingItem::SingleFrame(SessionFrame {
                body: SessionFrameBody::Flow(flow),
                ..
            }) => flow,
            _ => panic!("expected a single session flow frame"),
        };

        // Session-only flow: no link handle and no link fields.
        assert!(flow.handle.is_none());
        assert!(flow.delivery_count.is_none());
        assert!(flow.link_credit.is_none());

        // The session window is re-advertised from the current session state.
        assert_eq!(flow.next_incoming_id, Some(session.next_incoming_id));
        assert_eq!(flow.incoming_window, session.incoming_window);
        assert_eq!(flow.next_outgoing_id, session.next_outgoing_id);
        assert_eq!(flow.outgoing_window, session.outgoing_window);

        // The counter must be reset after the flow is emitted, so no flow is
        // due immediately after.
        assert_eq!(session.need_flow_count, 0);
        assert!(session.maybe_outgoing_session_flow().is_none());
    }

    #[test]
    fn maybe_outgoing_session_flow_skips_when_not_mapped() {
        let mut session = mapped_session();
        session.local_state = SessionState::EndSent;
        session.need_flow_count = u32::MAX;

        assert!(session.maybe_outgoing_session_flow().is_none());
    }
}

#[cfg(test)]
mod local_stop_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Released(Arc<AtomicBool>);
    impl Drop for Released {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    #[tokio::test]
    async fn stop_joins_a_blocked_engine_without_protocol_cleanup_or_detached_work() {
        let released = Arc::new(AtomicBool::new(false));
        let owned = released.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (outcome_tx, outcome) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _released = Released(owned);
            let _outcome = outcome_tx;
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.unwrap();
        let (control, mut commands) = tokio::sync::mpsc::channel(1);
        let (outgoing, _outgoing_rx) = tokio::sync::mpsc::channel(1);
        let mut owner = SessionHandle {
            is_ended: false,
            engine_joined: false,
            engine_handle: task,
            outcome,
            control,
            outgoing: outgoing.into(),
            session_stop_reason: Arc::new(OnceLock::new()),
            link_listener: (),
            max_frame_size: 65532,
        };
        owner.stop_and_join().await.unwrap();
        owner.stop_and_join().await.unwrap();
        assert!(released.load(Ordering::Acquire));
        assert!(matches!(
            owner.session_stop_reason.get(),
            Some(SessionStopReason::Stopped)
        ));
        drop(owner);
        assert!(matches!(
            commands.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));
    }
}
