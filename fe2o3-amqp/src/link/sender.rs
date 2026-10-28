//! Implementation of AMQP1.0 sender

use std::sync::{Arc, OnceLock};

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

cfg_not_wasm32! {
    use std::time::Duration;
    use tokio::time::{error::Elapsed, timeout};
}

use fe2o3_amqp_types::{
    definitions::{self, DeliveryTag, Fields, MessageFormat, SenderSettleMode},
    messaging::{
        message::__private::Serializable, Address, DeliveryState, Outcome, SerializableBody,
        Source, Target,
    },
    performatives::{Attach, Detach, Transfer},
};

use crate::{
    control::SessionControl,
    endpoint::{self, LinkAttach, LinkDetach, LinkExt, Settlement},
    session::SessionHandle,
    Payload,
};

use super::{
    builder::{self, WithSource, WithoutName, WithoutTarget},
    delivery::{DeliveryFut, Sendable},
    error::DetachError,
    resumption::ResumingDelivery,
    role,
    shared_inner::{
        recv_remote_detach, LinkEndpointInner, LinkEndpointInnerDetach, LinkEndpointInnerReattach,
    },
    ArcSenderUnsettledMap, DetachThenResumeSenderError, LinkFrame, LinkRelay, LinkStateError,
    MessageSizeExceeded, SendError, SenderAttachError, SenderAttachExchange, SenderFlowState,
    SenderLink, SenderResumeError, SenderResumeErrorKind, SessionStopReason,
};

#[cfg(docsrs)]
use fe2o3_amqp_types::messaging::{
    AmqpSequence, AmqpValue, Batch, Body, Data, IntoBody, Message, MESSAGE_FORMAT,
};

/// An AMQP1.0 sender
///
/// # Attach a new sender with default configurations
///
/// ```rust,ignore
/// let mut sender = Sender::attach(
///     &mut session,           // mutable reference to SessionHandle
///     "rust-sender-link-1",   // link name
///     "q1"                    // Target address
/// ).await.unwrap();
///
/// let outcome = sender.send("hello AMQP").await.unwrap();
///
/// // Checks the outcome of delivery
/// if outcome.is_accepted() {
///     tracing::info!("Outcome: {:?}", outcome)
/// } else {
///     tracing::error!("Outcome: {:?}", outcome)
/// }
///
/// sender.close().await.unwrap();
/// ```
///
/// ## Default configuration
///
/// | Field | Default Value |
/// |-------|---------------|
/// |`name`|`String::default()`|
/// |`snd_settle_mode`|`SenderSettleMode::Mixed`|
/// |`rcv_settle_mode`|`ReceiverSettleMode::First`|
/// |`source`|`Some(Source)` |
/// |`target`| `None` |
/// |`initial_delivery_count`| `0` |
/// |`max_message_size`| `None` |
/// |`offered_capabilities`| `None` |
/// |`desired_capabilities`| `None` |
/// |`Properties`| `None` |
/// |`buffer_size`| `u16::MAX` |
/// |`role`| `role::Sender` |
///
/// # Customize configuration with [`builder::Builder`]
///
/// ```rust,ignore
/// let mut sender = Sender::builder()
///     .name("rust-sender-link-1")
///     .target("q1")
///     .sender_settle_mode(SenderSettleMode::Mixed)
///     .attach(&mut session)
///     .await
///     .unwrap();
/// ```
pub struct Sender {
    pub(crate) inner: SenderInner<SenderLink<Target>>,
}

impl std::fmt::Debug for Sender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").finish()
    }
}

impl Sender {
    /// Close in place, arming correlation before every internal reattachment.
    pub async fn close_in_place_exact<F, G, E>(
        &mut self,
        peer_suspended: bool,
        mut before: F,
    ) -> Result<(), super::LinkExchangeError<SenderAttachError, E>>
    where
        F: FnMut() -> Result<G, E> + Send,
        G: Send,
        E: Send,
    {
        super::shared_inner::close_exact(&mut self.inner, peer_suspended, &mut before).await
    }

    /// Suspend in place and retain correlation through a crossed mandatory close.
    pub async fn detach_borrowed_exact<F, G, E>(
        &mut self,
        mut before: F,
    ) -> Result<(), super::LinkExchangeError<SenderAttachError, E>>
    where
        F: FnMut() -> Result<G, E> + Send,
        G: Send,
        E: Send,
    {
        super::shared_inner::detach_exact(&mut self.inner, &mut before).await
    }

    /// Creates a builder for [`Sender`] link
    pub fn builder(
    ) -> builder::Builder<role::SenderMarker, Target, WithoutName, WithSource, WithoutTarget> {
        builder::Builder::<role::SenderMarker, Target, _, _, _>::new()
    }

    /// Get the name of the link
    pub fn name(&self) -> &str {
        self.inner.link.name()
    }

    /// Returns the `max_message_size` of the link. A value of zero indicates that the link has no
    /// maximum message size, and thus a zero value is turned into a `None`
    pub fn max_message_size(&self) -> Option<u64> {
        self.inner.link.max_message_size()
    }

    /// Get a reference to the link's source field
    pub fn source(&self) -> &Option<Source> {
        &self.inner.link.source
    }

    /// Get a mutable reference to the link's source field
    pub fn source_mut(&mut self) -> &mut Option<Source> {
        &mut self.inner.link.source
    }

    /// Get a reference to the link's target field
    pub fn target(&self) -> &Option<Target> {
        &self.inner.link.target
    }

    /// Get a mutable reference to the link's target field
    pub fn target_mut(&mut self) -> &mut Option<Target> {
        &mut self.inner.link.target
    }

    /// Get a reference to the link's properties field in the op
    pub fn properties<F, O>(&self, op: F) -> O
    where
        F: FnOnce(&Option<Fields>) -> O,
    {
        self.inner.link.properties(op)
    }

    /// Get a mutable reference to the link's properties field in the op
    pub fn properties_mut<F, O>(&mut self, op: F) -> O
    where
        F: FnOnce(&mut Option<Fields>) -> O,
    {
        self.inner.link.properties_mut(op)
    }

    /// Attach the sender link to a session with default configuration
    /// with the `name` and `target` address set to the specified values
    ///
    /// ## Default configuration
    ///
    /// | Field | Default Value |
    /// |-------|---------------|
    /// |`name`|`String::default()`|
    /// |`snd_settle_mode`|`[SenderSettleMode]::Mixed`|
    /// |`rcv_settle_mode`|`ReceiverSettleMode::First`|
    /// |`source`|`Some(Source)` |
    /// |`target`| `None` |
    /// |`initial_delivery_count`| `0` |
    /// |`max_message_size`| `None` |
    /// |`offered_capabilities`| `None` |
    /// |`desired_capabilities`| `None` |
    /// |`Properties`| `None` |
    /// |`buffer_size`| `u16::MAX` |
    /// |`role`| `role::Sender` |
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let sender = Sender::attach(
    ///     &mut session,           // mutable reference to SessionHandle
    ///     "rust-sender-link-1",   // link name
    ///     "q1"                    // Target address
    /// ).await.unwrap();
    /// ```
    ///
    pub async fn attach<R>(
        session: &mut SessionHandle<R>,
        name: impl Into<String>,
        addr: impl Into<Address>,
    ) -> Result<Sender, SenderAttachError> {
        Self::builder()
            .name(name)
            .target(addr)
            .attach(session)
            .await
    }

    /// Preserve an endpoint after its session stopped, without sending frames.
    /// Already closed links and queued peer-closing Detach frames cannot resume.
    #[allow(
        clippy::result_large_err,
        reason = "return the original endpoint without allocating while it remains attached"
    )]
    pub fn into_detached(mut self) -> Result<DetachedSender, Self> {
        use super::state::LinkState;
        if matches!(
            self.inner.link.local_state,
            LinkState::Closed | LinkState::CloseSent
        ) {
            return Err(self);
        }
        if !matches!(self.inner.link.local_state, LinkState::Detached)
            && self.inner.link.session_stop_reason.get().is_none()
        {
            return Err(self);
        }
        while let Ok(frame) = self.inner.incoming.try_recv() {
            if let LinkFrame::Detach(detach, _) = frame {
                if detach.closed {
                    self.inner.link.local_state = LinkState::Closed;
                    self.inner.link.output_handle.take();
                    return Err(self);
                }
            }
        }
        // A stopped session has no live link handles. The endpoint and its
        // unsettled map survive, but attachment belongs to the next session.
        self.inner.link.local_state = LinkState::Detached;
        self.inner.link.output_handle.take();
        self.inner.link.input_handle.take();
        Ok(DetachedSender::new(self.inner))
    }

    /// Send a non-closing Detach and wait for the reply while retaining ownership.
    /// Cancellation preserves the in-progress exchange. A later call continues
    /// waiting without sending another Detach. After success, `into_detached`
    /// transfers the suspended endpoint to the caller.
    pub async fn detach_borrowed(&mut self) -> Result<(), DetachError> {
        if matches!(
            self.inner.link.local_state,
            super::state::LinkState::Detached
        ) {
            return Ok(());
        }
        self.inner.detach_with_error(None).await.map(|_| ())
    }

    /// Detach and transfer ownership of the suspended endpoint to the caller.
    pub async fn detach(mut self) -> Result<DetachedSender, (DetachedSender, DetachError)> {
        match self.inner.detach_with_error(None).await {
            Ok(_) => Ok(DetachedSender::new(self.inner)),
            Err(err) => Err((DetachedSender::new(self.inner), err)),
        }
    }

    /// Detach the link with an error
    pub async fn detach_with_error(
        mut self,
        error: impl Into<definitions::Error>,
    ) -> Result<DetachedSender, (DetachedSender, DetachError)> {
        match self.inner.detach_with_error(Some(error.into())).await {
            Ok(_) => Ok(DetachedSender::new(self.inner)),
            Err(err) => Err((DetachedSender::new(self.inner), err)),
        }
    }

    cfg_not_wasm32! {
        /// Detach the link with a timeout
        ///
        /// This simply wraps [`detach`](#method.detach) with a `timeout`
        pub async fn detach_with_timeout(
            self,
            duration: Duration,
        ) -> Result<Result<DetachedSender, (DetachedSender, DetachError)>, Elapsed> {
            timeout(duration, self.detach()).await
        }
    }

    /// Detach and re-attach the link to a new session
    ///
    /// This will still attempt to re-attach even if detaching fails.
    /// `DetachThenResumeSenderError::Resume` will be returned if the detach succeeds but re-attach
    /// fails. `DetachThenResumeSenderError::Detach` will be returned if both the detach and
    /// re-attach fails.
    pub async fn detach_then_resume_on_session<R>(
        &mut self,
        new_session: &SessionHandle<R>,
    ) -> Result<(), DetachThenResumeSenderError> {
        // Detach the link
        let detach_result = self.inner.detach_with_error(None).await;

        // Re-attach the link
        let is_reattaching = self.inner.switch_session(new_session);
        let attach_result = self
            .inner
            .resume_incoming_attach(None, is_reattaching)
            .await;

        match (detach_result, attach_result) {
            (_, Ok(())) => Ok(()),
            (Ok(()), Err(e)) => Err(DetachThenResumeSenderError::Resume(e)),
            (Err(e), Err(_)) => Err(DetachThenResumeSenderError::Detach(e)),
        }
    }

    /// Close the link.
    ///
    /// This will set the `closed` field in the Detach performative to true
    pub async fn close(mut self) -> Result<(), DetachError> {
        self.close_in_place().await
    }

    /// Drive a closing Detach without surrendering endpoint ownership on interruption.
    pub async fn close_in_place(&mut self) -> Result<(), DetachError> {
        self.inner.close_with_error(None).await
    }

    /// Whether the closing Detach exchange completed and released its output handle.
    pub fn is_closed(&self) -> bool {
        matches!(self.inner.link.local_state, super::state::LinkState::Closed)
            && self.inner.link.output_handle.is_none()
    }

    /// Detach the link with an error
    pub async fn close_with_error(
        mut self,
        error: impl Into<definitions::Error>,
    ) -> Result<(), DetachError> {
        self.inner.close_with_error(Some(error.into())).await
    }

    /// Send a message and wait for acknowledgement (disposition)
    ///
    /// # Use custom types as argument
    ///
    /// The AMQP 1.0 protocol requires user to choose the body section type:
    ///
    /// - [`AmqpValue`]
    /// - [`AmqpSequence`] / [`Batch<AmqpSequence>`]
    /// - [`Data`] / [`Batch<Data>`]
    ///
    /// Below shows some ways to use custom types.
    ///
    /// ## 1. Wrap custom type in [`AmqpValue`]
    ///
    /// The easiest way to use a custom type is probably to define a custom type that implements
    /// [`serde::Serialize`] and then wrap the type in [`AmqpValue`].
    ///
    /// ### Example
    ///
    /// ```rust,ignore
    /// #[derive(Serialize)]
    /// struct Foo {
    ///     a: i32
    /// }
    ///
    /// sender.send(AmqpValue(Foo{a: 3})).await.unwrap();
    /// ```
    ///
    /// ## 2. Implement [`IntoBody`] trait for the custom type
    ///
    /// Another option is to implement the [`IntoBody`] trait, which essentially asks the user to
    /// choose which body section type should be used. Please see documention on [`IntoBody`] for
    /// more information on the available body section types.
    ///
    /// ### Example
    ///
    /// ```rust,ignore
    /// #[derive(Serialize)]
    /// struct Foo {
    ///     a: i32
    /// }
    ///
    /// impl IntoBody for Foo {
    ///     type Body = AmqpValue<Foo>;
    ///
    ///     fn into_body(self) -> Self::Body {
    ///         AmqpValue(self)
    ///     }
    /// }
    ///
    /// sender.send(Foo{a: 5}).await.unwrap();
    /// ```
    ///
    /// ## 3. Use message builder
    ///
    /// One could still use the message builder to not only set other non-body fields of
    /// [`Message`] but also choosing the type of body section.
    ///
    /// ```rust,ignore
    /// #[derive(Serialize)]
    /// struct Foo {
    ///     a: i32
    /// }
    ///
    /// let message = Message::builder()
    ///     .properties(
    ///         Properties::builder()
    ///         .group_id(String::from("send_to_event_hub"))
    ///         .build(),
    ///     )
    ///     .value(Foo {a: 8}) // set body section to `AmqpValue<Foo>`
    ///     .build();
    /// sender.send(message).await.unwrap();
    /// ```
    ///
    /// # Pre-settle a message with [`Sendable`] builder
    ///
    /// The user can pre-settle a message by specifying `settled` field in the [`Sendable`]. Unless
    /// an explicity [`Sendable`] is passed to the argument, the `message_format` field is set to
    /// [`MESSAGE_FORMAT`] and the `settled` field is set to `None`.
    ///
    /// ## Example
    ///
    /// Send message pre-settled if the sender's settle mode is set to `SenderSettleMode::Mixed`.
    /// Please note that the field `settled` will be ***ignored*** if the negotiated
    /// `SenderSettleMode` is set to either `SenderSettleMode::Settled` or
    /// `SenderSettleMode::Unsettled`
    ///
    /// ```rust,ignore
    /// let sendable = Sendable::builder()
    ///     .message("hello AMQP")
    ///     .settled(true)
    ///     .build();
    /// let outcome = sender.send(sendable).await.unwrap();
    /// ```
    ///
    /// # Cancel safety
    ///
    /// This function is cancel-safe. See [#22](https://github.com/minghuaw/fe2o3-amqp/issues/22)
    /// for more details.
    pub async fn send<T: SerializableBody>(
        &mut self,
        sendable: impl Into<Sendable<T>>,
    ) -> Result<Outcome, SendError> {
        let fut = self
            .inner
            .send_with_state::<T, SendError>(sendable.into(), None, false)
            .await
            .map(|settlement| {
                DeliveryFut::new(settlement, self.inner.link.session_stop_reason.clone())
            })?;
        fut.await
    }

    /// Like [`send()`](#method.send) but takes a reference to the message
    ///
    /// This is useful when the message is large and you want to avoid cloning it because the
    /// message may be used again after the send operation.
    pub async fn send_ref<T: SerializableBody>(
        &mut self,
        sendable: &Sendable<T>,
    ) -> Result<Outcome, SendError> {
        let fut = self
            .inner
            .send_ref_with_state::<T, SendError>(sendable, None, false)
            .await
            .map(|settlement| {
                DeliveryFut::new(settlement, self.inner.link.session_stop_reason.clone())
            })?;
        fut.await
    }

    cfg_not_wasm32! {
        /// Send a message and wait for acknowledgement (disposition) with a timeout.
        ///
        /// This simply wraps [`send`](#method.send) inside a [`tokio::time::timeout`]
        pub async fn send_with_timeout<T: SerializableBody>(
            &mut self,
            sendable: impl Into<Sendable<T>>,
            duration: Duration,
        ) -> Result<Result<Outcome, SendError>, Elapsed> {
            timeout(duration, self.send(sendable)).await
        }
    }

    /// Send a message without waiting for the acknowledgement.
    ///
    /// This will set the batchable field of the `Transfer` performative to true. Please see
    /// [`send()`](#method.send) for information on how to use custom type as argument.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let fut = sender.send_batchable("HELLO AMQP").await.unwrap();
    /// let result = fut.await;
    /// ```
    pub async fn send_batchable<T: SerializableBody>(
        &mut self,
        sendable: impl Into<Sendable<T>>,
    ) -> Result<DeliveryFut<Result<Outcome, SendError>>, SendError> {
        self.inner
            .send_with_state(sendable.into(), None, true)
            .await
            .map(|settlement| {
                DeliveryFut::new(settlement, self.inner.link.session_stop_reason.clone())
            })
    }

    /// Like [`send_batchable()`](#method.send_batchable) but this only takes a reference.
    ///
    /// This is useful when the message is large and you want to avoid cloning it because the
    /// message may be used again after the send operation.
    pub async fn send_batchable_ref<T: SerializableBody>(
        &mut self,
        sendable: &Sendable<T>,
    ) -> Result<DeliveryFut<Result<Outcome, SendError>>, SendError> {
        self.inner
            .send_ref_with_state(sendable, None, true)
            .await
            .map(|settlement| {
                DeliveryFut::new(settlement, self.inner.link.session_stop_reason.clone())
            })
    }

    /// Returns when the remote peer detach/close the link
    ///
    /// The peer's detach has already been answered when this returns, so the
    /// link is left `Closed` or `Detached` (its output handle is released),
    /// and a later `close()` finishes without sending another detach.
    #[allow(deprecated)]
    pub async fn on_detach(&mut self) -> DetachError {
        match recv_remote_detach(&mut self.inner).await {
            Ok(detach) => {
                let closed = detach.closed;
                match self.inner.link.apply_remote_detach_outcome(detach) {
                    Ok(_) => {
                        if closed {
                            DetachError::ClosedByRemote
                        } else {
                            DetachError::DetachedByRemote
                        }
                    }
                    Err(err) => err.into(),
                }
            }
            Err(err) => err,
        }
    }
}

/// This is so that the transaction controller can re-use
/// the sender
#[derive(Debug)]
pub(crate) struct SenderInner<L>
where
    L: endpoint::SenderLink + LinkExt<Unsettled = ArcSenderUnsettledMap>,
{
    // The SenderLink manages the state
    pub(crate) link: L,
    pub(crate) buffer_size: usize,

    // Control sender to the session
    pub(crate) session: mpsc::Sender<SessionControl>,

    // Outgoing mpsc channel to send the Link frames
    pub(crate) outgoing: crate::session::transfer_queue::Sender,
    pub(crate) incoming: mpsc::Receiver<LinkFrame>,
}

impl<L> Drop for SenderInner<L>
where
    L: endpoint::SenderLink + LinkExt<Unsettled = ArcSenderUnsettledMap>,
{
    fn drop(&mut self) {
        if matches!(self.link.local_state(), super::state::LinkState::DetachSent) {
            // A declared suspension must not become a closing detach on drop.
            return;
        }
        // A detach the relay already answered may be waiting in the engine's
        // channel. Apply it so the link state matches the detach; and once
        // any detach was drained, the peer has already ended the link, so
        // the closing detach this drop would otherwise send would be a
        // duplicate.
        let mut remote_detach_received = false;
        let mut detach_error: Option<LinkStateError> = None;
        while let Ok(frame) = self.incoming.try_recv() {
            if let LinkFrame::Detach(detach, _) = frame {
                remote_detach_received = true;
                let closed = detach.closed;
                let error = detach.error.clone();
                // If the state change fails, ignore it: the engine is being
                // dropped anyway, and the deliveries below are failed from
                // the detach itself.
                if self.link.apply_remote_detach_outcome(detach).is_err() {
                    #[cfg(feature = "tracing")]
                    tracing::debug!("failed to apply remote detach outcome on sender drop");
                    #[cfg(feature = "log")]
                    log::debug!("failed to apply remote detach outcome on sender drop");
                }
                detach_error = Some(match (closed, error) {
                    (true, Some(error)) => LinkStateError::RemoteClosedWithError(error),
                    (true, None) => LinkStateError::RemoteClosed,
                    (false, Some(error)) => LinkStateError::RemoteDetachedWithError(error),
                    (false, None) => LinkStateError::RemoteDetached,
                });
            }
            // Any other frame (e.g. an attach response left behind by an
            // interrupted reattach) is superseded by the drop.
        }

        // Whether the peer can still be reached with a closing detach
        let mut detach_sent = false;
        if !remote_detach_received {
            if let Some(attachment) = self.link.output_handle_mut().take() {
                let detach = Detach {
                    handle: attachment.handle.into(),
                    closed: true,
                    error: None,
                };
                match self
                    .outgoing
                    .try_send(LinkFrame::Detach(detach, attachment.id))
                {
                    Ok(()) => detach_sent = true,
                    Err(_error) => {
                        #[cfg(any(feature = "log", feature = "tracing"))]
                        {
                            let reason = match &_error {
                                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                                    "control channel is full"
                                }
                                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                    "control channel is closed"
                                }
                            };
                            #[cfg(feature = "tracing")]
                            tracing::warn!(reason, "Failed to enqueue Detach frame on sender drop");
                            #[cfg(feature = "log")]
                            log::warn!("Failed to enqueue Detach frame on sender drop: {reason}");
                        }
                    }
                }
            }
        }

        // Fail the deliveries that can no longer be settled: the peer closed
        // or detached the link without settling them (the entries are
        // removed first, so each is failed once, even if the relay already
        // failed it), or the session stopped so the closing detach could not
        // be sent. Deliveries on a still-open link stay pending: the relay
        // settles them as the peer's dispositions arrive.
        let error = detach_error.or_else(|| {
            if detach_sent {
                None
            } else {
                match self.link.session_stop_reason().get() {
                    Some(reason) => Some(LinkStateError::SessionStopped(reason.clone())),
                    None => Some(LinkStateError::IllegalState), // defensive: no stop reason recorded; failure is link-local
                }
            }
        });
        if let Some(error) = error {
            self.link.unsettled().close(error);
        }
    }
}

impl<L> LinkEndpointInner for SenderInner<L>
where
    L: endpoint::SenderLink<AttachError = SenderAttachError, DetachError = DetachError>
        + LinkExt<FlowState = SenderFlowState, Unsettled = ArcSenderUnsettledMap>
        + LinkAttach<AttachExchange = SenderAttachExchange>
        + Send
        + Sync,
{
    type Link = L;

    fn link(&self) -> &Self::Link {
        &self.link
    }

    fn link_mut(&mut self) -> &mut Self::Link {
        &mut self.link
    }

    fn reader_mut(&mut self) -> &mut mpsc::Receiver<LinkFrame> {
        &mut self.incoming
    }

    fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    fn as_new_link_relay(&self, tx: mpsc::Sender<LinkFrame>) -> LinkRelay<()> {
        LinkRelay::Sender {
            tx,
            output_handle: (),
            flow_state: self.link.flow_state().producer(),
            // TODO: what else to do during re-attaching
            unsettled: self.link.unsettled().clone(),
            receiver_settle_mode: self.link.rcv_settle_mode().clone(),
        }
    }

    fn session_control(&self) -> &mpsc::Sender<SessionControl> {
        &self.session
    }

    fn session_stop_reason(&self) -> &Arc<OnceLock<SessionStopReason>> {
        self.link().session_stop_reason()
    }

    async fn exchange_attach(
        &mut self,
        is_reattaching: bool,
    ) -> Result<SenderAttachExchange, <Self::Link as LinkAttach>::AttachError> {
        self.link
            .exchange_attach(&self.outgoing, &mut self.incoming, is_reattaching)
            .await
    }

    async fn exchange_attach_for_close(
        &mut self,
    ) -> Result<(), <Self::Link as LinkAttach>::AttachError> {
        self.link
            .exchange_attach_for_close(&self.outgoing, &mut self.incoming)
            .await
    }

    async fn handle_attach_error(
        &mut self,
        attach_error: <Self::Link as LinkAttach>::AttachError,
    ) -> <Self::Link as LinkAttach>::AttachError {
        self.link
            .handle_attach_error(
                attach_error,
                &self.outgoing,
                &mut self.incoming,
                &self.session,
            )
            .await
    }

    async fn send_detach(
        &mut self,
        closed: bool,
        error: Option<definitions::Error>,
    ) -> Result<(), <Self::Link as LinkDetach>::DetachError> {
        self.link.send_detach(&self.outgoing, closed, error).await
    }
}

impl<L> LinkEndpointInnerReattach for SenderInner<L>
where
    L: endpoint::SenderLink<AttachError = SenderAttachError, DetachError = DetachError>
        + LinkExt<FlowState = SenderFlowState, Unsettled = ArcSenderUnsettledMap>
        + LinkAttach<AttachExchange = SenderAttachExchange>
        + Send
        + Sync,
{
    fn handle_reattach_outcome(
        &mut self,
        outcome: SenderAttachExchange,
    ) -> Result<&mut Self, L::AttachError> {
        match outcome {
            SenderAttachExchange::Complete => Ok(self),
            //  Re-attach should have None valued unsettled, so this should be invalid
            SenderAttachExchange::IncompleteUnsettled(_) | SenderAttachExchange::Resume(_) => {
                Err(SenderAttachError::IllegalState)
            }
        }
    }
}

impl<L> SenderInner<L>
where
    L: endpoint::SenderLink<
            TransferError = LinkStateError,
            AttachError = SenderAttachError,
            DetachError = DetachError,
        > + LinkExt<FlowState = SenderFlowState, Unsettled = ArcSenderUnsettledMap>
        + Send
        + Sync,
{
    pub(crate) async fn send_with_state<T, E>(
        &mut self,
        sendable: Sendable<T>,
        state: Option<DeliveryState>,
        batchable: bool,
    ) -> Result<Settlement, E>
    where
        T: SerializableBody,
        E: From<L::TransferError> + From<serde_amqp::Error> + From<MessageSizeExceeded>,
    {
        use bytes::BufMut;
        use serde::Serialize;
        use serde_amqp::ser::Serializer;

        let Sendable {
            message,
            message_format,
            settled,
        } = sendable;

        // serialize message
        let mut payload = BytesMut::new();
        let mut serializer = Serializer::from((&mut payload).writer());
        Serializable(message).serialize(&mut serializer)?;
        let payload = payload.freeze();

        self.send_payload(payload, message_format, settled, state, batchable)
            .await
    }

    pub(crate) async fn send_ref_with_state<T, E>(
        &mut self,
        sendable: &Sendable<T>,
        state: Option<DeliveryState>,
        batchable: bool,
    ) -> Result<Settlement, E>
    where
        T: SerializableBody,
        E: From<L::TransferError> + From<serde_amqp::Error> + From<MessageSizeExceeded>,
    {
        use bytes::BufMut;
        use serde::Serialize;
        use serde_amqp::ser::Serializer;

        let Sendable {
            message,
            message_format,
            settled,
        } = sendable;

        // serialize message
        let mut payload = BytesMut::new();
        let mut serializer = Serializer::from((&mut payload).writer());
        Serializable(message).serialize(&mut serializer)?;
        let payload = payload.freeze();

        self.send_payload(payload, *message_format, *settled, state, batchable)
            .await
    }

    pub(crate) async fn send_payload<E>(
        &mut self,
        payload: Payload,
        message_format: MessageFormat,
        settled: Option<bool>,
        state: Option<DeliveryState>,
        batchable: bool,
    ) -> Result<Settlement, E>
    where
        E: From<L::TransferError> + From<serde_amqp::Error> + From<MessageSizeExceeded>,
    {
        // Reject the message locally before any transfer frame is sent when it
        // exceeds the negotiated max-message-size of the link. The payload is
        // the already-encoded message, so its length is the encoded message
        // size. A max-message-size of zero means no limit is imposed.
        if let Some(max_size) = self.link.max_message_size() {
            let size = payload.len() as u64;
            if size > max_size {
                return Err(E::from(MessageSizeExceeded { size, max_size }));
            }
        }

        // send a transfer, checking state will be implemented in SenderLink
        let detached_fut = self.incoming.recv(); // cancel safe
        let settlement = self
            .link
            .send_payload(
                &self.outgoing,
                detached_fut,
                payload,
                message_format,
                settled,
                state,
                batchable,
            )
            .await?;
        Ok(settlement)
    }
}

impl SenderInner<SenderLink<Target>> {
    /// Switch sessions while preserving the endpoint's unsettled state.
    ///
    /// The new session may belong to a different connection whose negotiated
    /// max frame size differs, so the link's `max_frame_size` is refreshed
    /// from the new session before any attach/transfer frame is sent.
    pub(crate) fn switch_session<R>(&mut self, new_session: &SessionHandle<R>) -> bool {
        self.link.session_stop_reason = new_session.session_stop_reason().clone();
        self.session = new_session.control.clone();
        self.outgoing = new_session.outgoing.clone();
        self.link.max_frame_size = new_session.max_frame_size();
        false
    }

    /// Carries out what the resume decided for the delivery under
    /// `delivery_tag`. Every local delivery stays in the store until its step
    /// has been handed to the session, so a resume cut short, by an error,
    /// the peer's detach or its future being dropped, leaves the ones it has
    /// not finished for the next resume, or the close, to settle.
    ///
    /// Resent deliveries wait in `resend` until every other one is done.
    async fn handle_resuming_delivery(
        &mut self,
        delivery_tag: DeliveryTag,
        resuming: ResumingDelivery,
        resend: &mut Vec<DeliveryTag>,
    ) -> Result<(), SendError> {
        #[cfg(feature = "tracing")]
        tracing::debug!("Resuming delivery: delivery_tag: {:?}", delivery_tag);
        #[cfg(feature = "log")]
        log::debug!("Resuming delivery: delivery_tag: {:?}", delivery_tag);
        match resuming {
            ResumingDelivery::Abort { message_format } => {
                self.abort(delivery_tag, message_format).await?
            }
            ResumingDelivery::Resend => {
                if !resend.contains(&delivery_tag) {
                    resend.push(delivery_tag)
                }
            }
            ResumingDelivery::Resume { remaining } => self.resume(delivery_tag, remaining).await?,
            ResumingDelivery::RestateOutcome => self.restate_outcome(delivery_tag).await?,
        }

        Ok(())
    }

    /// The local delivery's message format, delivery state and payload.
    fn local_delivery(
        &self,
        delivery_tag: &DeliveryTag,
    ) -> Option<(MessageFormat, Option<DeliveryState>, Payload)> {
        let guard = self.link.unsettled.read();
        let local = guard.as_ref()?.get(delivery_tag)?;
        Some((
            local.message_format,
            local.state.clone(),
            local.payload.clone(),
        ))
    }

    async fn abort(
        &mut self,
        delivery_tag: DeliveryTag,
        message_format: MessageFormat,
    ) -> Result<(), SendError> {
        let handle = self
            .link
            .output_handle
            .clone()
            .ok_or(LinkStateError::IllegalState)?
            .into();
        let transfer = Transfer {
            handle,
            delivery_id: None,
            delivery_tag: Some(delivery_tag.clone()),
            message_format: Some(message_format),
            settled: None,
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: true,
            aborted: true,
            batchable: false,
        };
        let payload = Bytes::new();

        self.link
            .send_resuming_transfer(&self.outgoing, self.incoming.recv(), transfer, payload)
            .await?;
        // An aborted delivery is implicitly settled (AMQP 1.0 §2.7.5): the
        // local outcome is None.
        if let Some(local) = self.link.unsettled.withdraw(&delivery_tag) {
            let _ = local.settle_with_state(None);
        }

        Ok(())
    }

    /// Sends the part of the delivery the peer has yet to receive with its
    /// original tag and `resume` set. It takes no new link credit: it is not a
    /// new delivery (AMQP 1.0 §2.6.13).
    async fn resume(
        &mut self,
        delivery_tag: DeliveryTag,
        remaining: Payload,
    ) -> Result<(), SendError> {
        let Some((message_format, state, _)) = self.local_delivery(&delivery_tag) else {
            return Ok(());
        };
        let handle = self
            .link
            .output_handle
            .clone()
            .ok_or(LinkStateError::IllegalState)?
            .into();
        let settled = match self.link.snd_settle_mode {
            SenderSettleMode::Settled => true,
            SenderSettleMode::Unsettled => false,
            SenderSettleMode::Mixed => false,
        };
        let transfer = Transfer {
            handle,
            delivery_id: None,
            delivery_tag: Some(delivery_tag.clone()),
            message_format: Some(message_format),
            settled: Some(settled),
            more: false, // This will be determined in `split_transfer`
            rcv_settle_mode: None,
            state,
            resume: true,
            aborted: false,
            batchable: false,
        };

        self.send_transfer_with_outcome(transfer, delivery_tag, remaining)
            .await
    }

    /// Restates the delivery's local terminal outcome, which is definitive
    /// (AMQP 1.0 §2.6.13, delivery-tag 13).
    async fn restate_outcome(&mut self, delivery_tag: DeliveryTag) -> Result<(), SendError> {
        let Some((message_format, state, payload)) = self.local_delivery(&delivery_tag) else {
            return Ok(());
        };
        let handle = self
            .link
            .output_handle
            .clone()
            .ok_or(LinkStateError::IllegalState)?
            .into();
        let transfer = Transfer {
            handle,
            delivery_id: None,
            delivery_tag: Some(delivery_tag.clone()),
            message_format: Some(message_format),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state,
            resume: true,
            aborted: false,
            batchable: false,
        };

        self.send_transfer_with_outcome(transfer, delivery_tag, payload)
            .await
    }

    /// Sends `payload` for a resumed delivery, which resolves with its
    /// outcome: at once when the transfer goes out settled, otherwise when the
    /// peer settles it, through the unsettled map.
    async fn send_transfer_with_outcome(
        &mut self,
        transfer: Transfer,
        delivery_tag: DeliveryTag,
        payload: Payload,
    ) -> Result<(), SendError> {
        let settled = self.link.is_settled_on_send(&transfer);
        self.link
            .send_resuming_transfer(&self.outgoing, self.incoming.recv(), transfer, payload)
            .await?;
        if settled {
            if let Some(local) = self.link.unsettled.withdraw(&delivery_tag) {
                let _ = local.settle();
            }
        }
        Ok(())
    }

    /// Carries out every delivery a resume decided on, and the resends last
    /// when `resend_now`.
    async fn resume_deliveries(
        &mut self,
        resuming_deliveries: Vec<(DeliveryTag, ResumingDelivery)>,
        resend: &mut Vec<DeliveryTag>,
        resend_now: bool,
    ) -> Result<(), SendError> {
        for (delivery_tag, resuming) in resuming_deliveries {
            self.handle_resuming_delivery(delivery_tag, resuming, resend)
                .await?;
        }
        if resend_now {
            while let Some(delivery_tag) = resend.first().cloned() {
                let detached = self.incoming.recv();
                self.link
                    .resend(&self.outgoing, detached, delivery_tag)
                    .await?;
                resend.remove(0);
            }
        }
        Ok(())
    }

    async fn resume_incoming_attach(
        &mut self,
        mut initial_remote_attach: Option<Attach>,
        is_reattaching: bool,
    ) -> Result<(), SenderResumeErrorKind> {
        self.reallocate_output_handle().await?;
        let mut resend = Vec::new();

        loop {
            let attach_exchange = match initial_remote_attach.take() {
                Some(remote_attach) => {
                    self.link
                        .send_attach(&self.outgoing, is_reattaching)
                        .await?;
                    self.link.on_incoming_attach(remote_attach)?
                }
                None => self.exchange_attach(is_reattaching).await?,
            };

            match attach_exchange {
                SenderAttachExchange::Complete => break,
                SenderAttachExchange::IncompleteUnsettled(resuming_deliveries) => {
                    self.resume_deliveries(resuming_deliveries, &mut resend, false)
                        .await?;
                }
                SenderAttachExchange::Resume(resuming_deliveries) => {
                    // Resend buffered payloads last
                    self.resume_deliveries(resuming_deliveries, &mut resend, true)
                        .await?;

                    // Upon completion of this reduction of state, the two parties MUST suspend and
                    // re-attempt to resume the link. The detach consumes the output handle,
                    // so reallocate it for the retry.
                    self.detach_with_error(None).await?;
                    self.reallocate_output_handle().await?;
                }
            }
        }

        Ok(())
    }
}

/// A detached sender
///
/// # Example
///
/// Link re-attachment
///
/// ```rust,ignore
/// let detached = sender.detach().await.unwrap();
/// let sender = detached.resume().await.unwrap();
/// ```
#[derive(Debug)]
pub struct DetachedSender {
    // Boxed to keep the link-reattachment result small
    inner: Box<SenderInner<SenderLink<Target>>>,
}

macro_rules! try_as_sender {
    ($self:ident, $f:expr) => {
        match $f {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(SenderResumeError {
                    detached_sender: $self,
                    kind: error.into(),
                })
            }
        }
    };
}

impl DetachedSender {
    /// Resume with an explicit interruption future, returning the native endpoint.
    /// An interrupted admitted Attach completes upstream's nonclosing Detach
    /// cleanup before custody returns; an already Detached link sends nothing.
    pub async fn resume_on_session_until<R>(
        mut self,
        session: &SessionHandle<R>,
        interrupt: impl std::future::Future<Output = ()>,
    ) -> Result<Sender, SenderResumeError> {
        if self.inner.link.session_stop_reason.get().is_some() {
            self = match (Sender { inner: *self.inner }).into_detached() {
                Ok(endpoint) => endpoint,
                Err(endpoint) => {
                    return Err(SenderResumeError {
                        detached_sender: DetachedSender {
                            inner: Box::new(endpoint.inner),
                        },
                        kind: SenderAttachError::IllegalState.into(),
                    });
                }
            };
        }
        self.inner.switch_session(session);
        let result = tokio::select! { biased;
            _ = interrupt => Err(SenderResumeErrorKind::Interrupted),
            result = self.inner.resume_incoming_attach(None, false) => result,
        };
        if matches!(&result, Err(SenderResumeErrorKind::Interrupted)) {
            try_as_sender!(self, self.inner.detach_with_error(None).await);
        }
        match result {
            Ok(()) => Ok(Sender { inner: *self.inner }),
            Err(kind) => Err(SenderResumeError {
                detached_sender: self,
                kind,
            }),
        }
    }
    fn new(inner: SenderInner<SenderLink<Target>>) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }

    /// Get a reference to the link's source field
    pub fn source(&self) -> &Option<Source> {
        &self.inner.link.source
    }

    /// Get a mutable reference to the link's source field
    pub fn source_mut(&mut self) -> &mut Option<Source> {
        &mut self.inner.link.source
    }

    /// Get a reference to the link's target field
    pub fn target(&self) -> &Option<Target> {
        &self.inner.link.target
    }

    /// Get a mutable reference to the link's target field
    pub fn target_mut(&mut self) -> &mut Option<Target> {
        &mut self.inner.link.target
    }

    async fn resume_inner(mut self, is_reattaching: bool) -> Result<Sender, SenderResumeError> {
        try_as_sender!(
            self,
            self.inner
                .resume_incoming_attach(None, is_reattaching)
                .await
        );
        Ok(Sender { inner: *self.inner })
    }

    /// Resume the sender link on the original session
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn resume(self) -> Result<Sender, SenderResumeError> {
        self.resume_inner(false).await
    }

    /// Resume the sender link on the original session with an Attach sent by the remote peer
    pub async fn resume_incoming_attach(
        mut self,
        remote_attach: Attach,
    ) -> Result<Sender, SenderResumeError> {
        try_as_sender!(
            self,
            self.inner
                .resume_incoming_attach(Some(remote_attach), false)
                .await
        );
        Ok(Sender { inner: *self.inner })
    }

    cfg_not_wasm32! {
        async fn resume_with_timeout_inner(
            mut self,
            duration: Duration,
            is_reattaching: bool,
        ) -> Result<Sender, SenderResumeError> {
            let fut = self.inner.resume_incoming_attach(None, is_reattaching);

            match tokio::time::timeout(duration, fut).await {
                Ok(Ok(_)) => Ok(Sender { inner: *self.inner }),
                Ok(Err(kind)) => Err(SenderResumeError {
                    detached_sender: self,
                    kind,
                }),
                Err(_) => {
                    try_as_sender!(self, self.inner.detach_with_error(None).await);
                    Err(SenderResumeError {
                        detached_sender: self,
                        kind: SenderResumeErrorKind::Timeout,
                    })
                }
            }
        }

        /// Resume the sender link with a timeout
        #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
        pub async fn resume_with_timeout(
            self,
            duration: Duration,
        ) -> Result<Sender, SenderResumeError> {
            self.resume_with_timeout_inner(duration, false).await
        }

        async fn resume_incoming_attach_with_timeout_inner(
            mut self,
            remote_attach: Attach,
            duration: Duration,
            is_reattaching: bool,
        ) -> Result<Sender, SenderResumeError> {
            let fut = self.inner.resume_incoming_attach(Some(remote_attach), is_reattaching);

            match tokio::time::timeout(duration, fut).await {
                Ok(Ok(_)) => Ok(Sender { inner: *self.inner }),
                Ok(Err(kind)) => Err(SenderResumeError {
                    detached_sender: self,
                    kind,
                }),
                Err(_) => {
                    // Keep ownership and the actual in-progress state. A timeout
                    // does not authorize a new protocol exchange.
                    Err(SenderResumeError {
                        detached_sender: self,
                        kind: SenderResumeErrorKind::Timeout,
                    })
                }
            }
        }

        /// Resume the sender link on the original session with an Attach sent by the remote peer
        pub async fn resume_incoming_attach_with_timeout(
            self,
            remote_attach: Attach,
            duration: Duration,
        ) -> Result<Sender, SenderResumeError> {
            self.resume_incoming_attach_with_timeout_inner(remote_attach, duration, false).await
        }
    }

    /// Resume the sender on a specific session
    pub async fn resume_on_session<R>(
        mut self,
        session: &SessionHandle<R>,
    ) -> Result<Sender, SenderResumeError> {
        let is_reattaching = self.inner.switch_session(session);
        self.resume_inner(is_reattaching).await
    }

    /// Resume the sender on a specific session
    pub async fn resume_incoming_attach_on_session<R>(
        mut self,
        remote_attach: Attach,
        session: &SessionHandle<R>,
    ) -> Result<Sender, SenderResumeError> {
        let is_reattaching = self.inner.switch_session(session);

        try_as_sender!(
            self,
            self.inner
                .resume_incoming_attach(Some(remote_attach), is_reattaching)
                .await
        );
        Ok(Sender { inner: *self.inner })
    }

    cfg_not_wasm32! {
        /// Resume the sender on a specific session with timeout
        pub async fn resume_on_session_with_timeout<R>(
            mut self,
            session: &SessionHandle<R>,
            duration: Duration,
        ) -> Result<Sender, SenderResumeError> {
            let is_reattaching = self.inner.switch_session(session);
            self.resume_with_timeout_inner(duration, is_reattaching).await
        }

        /// Resume the sender on a specific session with timeout
        pub async fn resume_incoming_attach_on_session_with_timeout<R>(
            mut self,
            remote_attach: Attach,
            session: &SessionHandle<R>,
            duration: Duration,
        ) -> Result<Sender, SenderResumeError> {
            let is_reattaching = self.inner.switch_session(session);
            self.resume_incoming_attach_with_timeout_inner(remote_attach, duration, is_reattaching)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn every_crossed_exchange_arms_exactly_one_reattach() {
        use crate::link::test_util::{drive_exact_end, CorrelationGuard};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let (inner, control, outgoing, incoming) = make_sender_inner_with_channels(4096);
        let mut sender = Sender { inner };
        let calls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let (result, detaches) = tokio::join!(
            sender.detach_borrowed_exact(|| {
                calls.fetch_add(1, Ordering::AcqRel);
                Ok::<_, std::io::Error>(CorrelationGuard(dropped.clone()))
            }),
            drive_exact_end(
                control,
                outgoing,
                incoming,
                peer_receiver_attach(),
                || {
                    assert_eq!(calls.load(Ordering::Acquire), 1);
                    assert!(!dropped.load(Ordering::Acquire));
                },
                || {
                    assert!(!dropped.load(Ordering::Acquire));
                    None
                }
            )
        );
        assert!(matches!(
            result,
            Err(super::super::LinkExchangeError::Detach(
                DetachError::ClosedByRemote
            ))
        ));
        assert_eq!(detaches, 2);
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn reattach_errors_keep_phase_and_drop_correlation() {
        use super::super::LinkExchangeError;
        use crate::link::test_util::CorrelationGuard;
        use std::sync::atomic::{AtomicBool, Ordering};
        let (mut inner, mut controls, mut outgoing, _incoming) =
            make_sender_inner_with_channels(4096);
        inner.link.local_state = LinkState::Detached;
        inner.link.output_handle.take();
        let mut endpoint = Sender { inner };
        assert!(matches!(
            endpoint
                .close_in_place_exact(false, || Err::<(), _>(std::io::Error::other("refused")))
                .await,
            Err(LinkExchangeError::Correlation(_))
        ));
        assert!(controls.try_recv().is_err());
        assert!(outgoing.try_recv().is_err());
        let dropped = Arc::new(AtomicBool::new(false));
        let (result, ()) = tokio::join!(
            endpoint.close_in_place_exact(false, || Ok::<_, std::io::Error>(CorrelationGuard(
                dropped.clone()
            ))),
            async {
                let SessionControl::GetIncomingWindow(reply) = controls.recv().await.unwrap()
                else {
                    panic!("window allocation")
                };
                assert!(!dropped.load(Ordering::Acquire));
                reply.send(16).unwrap();
                let SessionControl::AllocateLink { responder, .. } = controls.recv().await.unwrap()
                else {
                    panic!("link allocation")
                };
                assert!(!dropped.load(Ordering::Acquire));
                responder
                    .send(Err(crate::session::error::AllocLinkError::SessionNotMapped))
                    .unwrap();
            }
        );
        assert!(matches!(
            result,
            Err(LinkExchangeError::Attach(
                SenderAttachError::SessionNotMapped
            ))
        ));
        assert!(dropped.load(Ordering::Acquire));
        assert!(outgoing.try_recv().is_err());
    }

    #[tokio::test]
    async fn exact_attach_and_detach_failures_keep_their_original_phase() {
        use super::super::LinkExchangeError;
        use crate::link::test_util::{drive_exact_end, CorrelationGuard};
        use std::sync::atomic::{AtomicBool, Ordering};
        for refusal in [false, true] {
            let (mut inner, control, outgoing, incoming) = make_sender_inner_with_channels(4096);
            inner.link.local_state = LinkState::Detached;
            inner.link.output_handle.take();
            let mut endpoint = Sender { inner };
            let dropped = Arc::new(AtomicBool::new(false));
            let mut attach = peer_receiver_attach();
            if refusal {
                attach.target = None;
            }
            let error = definitions::Error::new(definitions::AmqpError::NotFound, None, None);
            let (result, _) = tokio::join!(
                endpoint.close_in_place_exact(false, || Ok::<_, std::io::Error>(CorrelationGuard(
                    dropped.clone()
                ))),
                drive_exact_end(
                    control,
                    outgoing,
                    incoming,
                    attach,
                    || {
                        assert!(!dropped.load(Ordering::Acquire));
                    },
                    || {
                        assert!(!dropped.load(Ordering::Acquire));
                        Some(error.clone())
                    }
                )
            );
            if refusal {
                assert!(
                    matches!(result,Err(LinkExchangeError::Attach(SenderAttachError::RemoteClosedWithError(actual))) if actual.condition==error.condition)
                );
            } else {
                assert!(
                    matches!(result,Err(LinkExchangeError::Detach(DetachError::RemoteClosedWithError(actual))) if actual.condition==error.condition)
                );
            }
            assert!(dropped.load(Ordering::Acquire));
        }
    }

    #[tokio::test]
    async fn closing_a_detached_endpoint_reattaches_without_replay() {
        use crate::link::test_util::{drive_exact_end, CorrelationGuard};
        use std::sync::atomic::{AtomicBool, Ordering};
        let (mut inner, control, outgoing, incoming) = make_sender_inner_with_channels(4096);
        inner.link.local_state = LinkState::Detached;
        inner.link.output_handle.take();
        let (tx, mut rx) = oneshot::channel();
        inner
            .link
            .unsettled
            .register(
                vec![42].into(),
                UnsettledMessage::new(Payload::from_static(b"pending"), None, 0, tx),
            )
            .unwrap();
        let mut sender = Sender { inner };
        let dropped = Arc::new(AtomicBool::new(false));
        let (result, detaches) = tokio::join!(
            sender.close_in_place_exact(false, || Ok::<_, std::io::Error>(CorrelationGuard(
                dropped.clone()
            ))),
            drive_exact_end(
                control,
                outgoing,
                incoming,
                peer_receiver_attach(),
                || assert!(!dropped.load(Ordering::Acquire)),
                || {
                    assert!(!dropped.load(Ordering::Acquire));
                    None
                }
            )
        );
        result.unwrap();
        assert_eq!(detaches, 1);
        assert!(dropped.load(Ordering::Acquire));
        // Null-map close may clear native unsettled state. Logical protection
        // decides Pending, not this native sender's destruction timing.
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }

    use std::marker::PhantomData;

    use fe2o3_amqp_types::{
        definitions::ReceiverSettleMode,
        messaging::{Accepted, Received},
        primitives::OrderedMap,
    };
    use tokio::sync::Notify;

    use super::*;
    use crate::endpoint::{InputHandle, SenderLink as _};
    use crate::link::delivery::UnsettledMessage;
    use crate::{
        link::state::{LinkFlowState, LinkFlowStateInner, LinkState},
        util::Consumer,
    };
    use tokio::sync::oneshot;

    fn make_sender_inner_with_channels(
        max_frame_size: usize,
    ) -> (
        SenderInner<SenderLink<Target>>,
        mpsc::Receiver<SessionControl>,
        mpsc::Receiver<LinkFrame>,
        mpsc::Sender<LinkFrame>,
    ) {
        let (session_tx, session_rx) = mpsc::channel::<SessionControl>(16);
        let (outgoing_tx, outgoing_rx) = mpsc::channel::<LinkFrame>(16);
        let (incoming_tx, incoming_rx) = mpsc::channel::<LinkFrame>(16);

        let flow_state_inner = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 0,
            available: 0,
            drain: false,
            properties: None,
        };
        let flow_state = Arc::new(LinkFlowState::sender(flow_state_inner));
        let consumer = Consumer::new(Arc::new(Notify::new()), flow_state);

        let link = SenderLink::<Target> {
            operation_dispatch: None,
            role: PhantomData,
            local_state: LinkState::Attached,
            name: String::from("test-sender"),
            output_handle: Some(endpoint::OutputAttachment::for_tests(0)),
            input_handle: None,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: None,
            max_message_size: 0,
            offered_capabilities: None,
            desired_capabilities: None,
            flow_state: consumer,
            unsettled: Arc::new(super::super::unsettled_store::Store::new(None)),
            session_stop_reason: Arc::new(OnceLock::new()),
            max_frame_size,
            verify_incoming_source: true,
            verify_incoming_target: true,
        };
        let inner = SenderInner {
            link,
            buffer_size: 16,
            session: session_tx,
            outgoing: outgoing_tx.into(),
            incoming: incoming_rx,
        };
        (inner, session_rx, outgoing_rx, incoming_tx)
    }

    fn make_sender_inner(max_frame_size: usize) -> SenderInner<SenderLink<Target>> {
        make_sender_inner_with_channels(max_frame_size).0
    }

    /// The minimal `Attach` a receiver peer sends for this sender link: it
    /// must carry a target so `on_incoming_attach` accepts it.
    fn peer_receiver_attach() -> Attach {
        Attach {
            name: String::from("test-sender"),
            handle: fe2o3_amqp_types::definitions::Handle(0),
            role: fe2o3_amqp_types::definitions::Role::Receiver,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: Some(Box::new(
                fe2o3_amqp_types::messaging::TargetArchetype::Target(Target::default()),
            )),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    fn make_session_handle(max_frame_size: usize) -> SessionHandle<()> {
        let (control, _control_rx) = mpsc::channel::<SessionControl>(16);
        let (outgoing_tx, _outgoing_rx) = mpsc::channel::<LinkFrame>(16);
        let (outcome_tx, outcome) = crate::util::engine_outcome::EngineOutcome::channel();
        drop(outcome_tx);
        SessionHandle {
            is_ended: false,
            control,
            engine_handle: tokio::spawn(async {}),
            engine_joined: false,
            outcome,
            outgoing: outgoing_tx.into(),
            session_stop_reason: Arc::new(OnceLock::new()),
            max_frame_size,
            link_listener: (),
        }
    }

    #[tokio::test]
    async fn switch_session_refreshes_max_frame_size() {
        let mut inner = make_sender_inner(4092);
        let session_b = make_session_handle(1020);

        // A new session refreshes frame limits without discarding recovery state.
        let is_reattaching = inner.switch_session(&session_b);
        assert!(!is_reattaching);
        assert_eq!(inner.link.max_frame_size, 1020);

        // Switching to the same session does not mark the link as reattaching
        let is_reattaching = inner.switch_session(&session_b);
        assert!(!is_reattaching);
        assert_eq!(inner.link.max_frame_size, 1020);
    }

    #[tokio::test]
    async fn borrowed_detach_survives_timeout_without_replaying_detach() {
        let (inner, _session, mut outgoing, incoming) = make_sender_inner_with_channels(4096);
        let mut sender = Sender { inner };
        assert!(
            tokio::time::timeout(Duration::from_millis(10), sender.detach_borrowed())
                .await
                .is_err()
        );
        assert!(matches!(
            outgoing.recv().await.unwrap(),
            LinkFrame::Detach(_, _)
        ));
        incoming
            .send(LinkFrame::Detach(
                fe2o3_amqp_types::performatives::Detach {
                    handle: 0.into(),
                    closed: false,
                    error: None,
                },
                crate::endpoint::AttachmentId::allocate(),
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), sender.detach_borrowed())
            .await
            .unwrap()
            .unwrap();
        assert!(outgoing.try_recv().is_err());
        assert!(sender.into_detached().is_ok());
    }

    /// A peer that suspends (non-closing detach) while this side is closing
    /// triggers the AMQP 1.0 §2.6.6 simultaneous-detach handshake.
    ///
    /// The spec assigns the reattach to the non-closing (suspending) side and
    /// only requires the closing side to complete the exchange. This
    /// implementation drives the reattach from both sides:
    ///
    /// - sending our closing detach releases the link from the session
    ///   (`Session::on_outgoing_detach`), so the link must be reattached
    ///   (`reattach_then_close` -> `reallocate_output_handle` ->
    ///   `allocate_link`) to re-register it; otherwise the peer's crossed
    ///   `Attach`/`Detach` could not be routed to the link and would end the
    ///   session;
    /// - with both sides reattaching, each side's attach exchange accepts the
    ///   peer's `Attach` as its answer, so the crossed detaches converge
    ///   symmetrically without depending on whether the peer drives its
    ///   reattach.
    ///
    /// The peer's frames are scripted, so this is deterministic.
    #[tokio::test]
    async fn close_reattaches_and_closes_on_simultaneous_suspend() {
        let (mut inner, session_rx, outgoing_rx, incoming_tx) =
            make_sender_inner_with_channels(4096);

        let (result, (saw_attach, detaches)) = tokio::join!(
            inner.close_with_error(None),
            crate::link::test_util::drive_simultaneous_detach_race(
                session_rx,
                outgoing_rx,
                incoming_tx,
                peer_receiver_attach(),
            ),
        );

        assert!(saw_attach, "the closing side must reattach");
        assert_eq!(
            detaches, 2,
            "expected a detach before and after the reattach"
        );
        assert!(result.is_ok(), "close must complete: {result:?}");
        assert!(matches!(&inner.link.local_state, LinkState::Closed));
    }

    /// A peer that closes (closing detach) while this side is suspending
    /// triggers the AMQP 1.0 §2.6.6 simultaneous-detach handshake. The spec
    /// assigns the reattach to this side (the non-closing/suspending side),
    /// which reattaches and then sends a closing detach; the link ends
    /// `Closed`, so the local detach fails with `ClosedByRemote`.
    ///
    /// The peer's frames are scripted, so this is deterministic.
    #[tokio::test]
    async fn detach_reattaches_and_closes_on_simultaneous_close() {
        let (mut inner, session_rx, outgoing_rx, incoming_tx) =
            make_sender_inner_with_channels(4096);

        let (result, (saw_attach, detaches)) = tokio::join!(
            inner.detach_with_error(None),
            crate::link::test_util::drive_simultaneous_detach_race(
                session_rx,
                outgoing_rx,
                incoming_tx,
                peer_receiver_attach(),
            ),
        );

        assert!(saw_attach, "the suspending side must reattach");
        assert_eq!(
            detaches, 2,
            "expected a detach before and after the reattach"
        );
        assert!(matches!(result, Err(DetachError::ClosedByRemote)));
        assert!(matches!(&inner.link.local_state, LinkState::Closed));
    }

    fn is_registered(link: &SenderLink<Target>, tag: &DeliveryTag) -> bool {
        link.unsettled
            .read()
            .as_ref()
            .is_some_and(|map| map.contains_key(tag))
    }

    /// The link's credit and delivery-count.
    fn credit(link: &SenderLink<Target>) -> (u32, u32) {
        let state = link.flow_state.state().lock.read();
        (state.link_credit, state.delivery_count)
    }

    /// A sender with one link credit.
    fn credited_sender(max_frame_size: usize) -> SenderInner<SenderLink<Target>> {
        let mut inner = make_sender_inner(max_frame_size);
        inner.link.input_handle = Some(endpoint::InputHandle(0));
        inner.link.flow_state.state().lock.write().link_credit = 1;
        inner
    }

    /// A channel to the session with no room left, so a hand-over stays pending until the
    /// session takes the frame in front of it.
    async fn full_session_channel() -> (
        crate::session::transfer_queue::Sender,
        mpsc::Receiver<LinkFrame>,
    ) {
        let (writer, session) = mpsc::channel::<LinkFrame>(1);
        let placeholder = Detach {
            handle: fe2o3_amqp_types::definitions::Handle(1),
            closed: false,
            error: None,
        };
        writer
            .send(LinkFrame::Detach(
                placeholder,
                crate::endpoint::AttachmentId::allocate(),
            ))
            .await
            .expect("the session end is open");
        (writer.into(), session)
    }

    /// The session engine is a task of its own: it can write a transfer, receive the peer's
    /// settled disposition and look the delivery up in the unsettled map before the sending task
    /// resumes. A disposition that finds no entry is dropped and the outcome never resolves, so
    /// the delivery is registered by the time its first frame reaches the session.
    ///
    /// The delivery spans frames and the session's channel holds one, so the send is pending on
    /// its second frame with the first in the session's hands.
    #[tokio::test]
    async fn a_delivery_is_registered_when_its_first_frame_reaches_the_session() {
        let mut inner = credited_sender(512);
        let (writer, mut session) = mpsc::channel::<LinkFrame>(1);
        let writer = crate::session::transfer_queue::Sender::from(writer);
        let unsettled = inner.link.unsettled.clone();
        let registered = |tag: &DeliveryTag| {
            unsettled
                .read()
                .as_ref()
                .is_some_and(|map| map.contains_key(tag))
        };
        let tag = DeliveryTag::from([0; 4]);

        let send = inner.link.send_payload(
            &writer,
            std::future::pending(),
            Payload::from(vec![7; 2048]),
            0,
            Some(false),
            None,
            false,
        );
        tokio::pin!(send);
        assert!(futures_util::poll!(&mut send).is_pending());
        assert!(matches!(
            session.try_recv(),
            Ok(LinkFrame::Transfer { performative, .. }) if performative.more
        ));
        assert!(registered(&tag));

        while !matches!(futures_util::poll!(&mut send), std::task::Poll::Ready(_)) {
            session.recv().await.expect("the send is still running");
        }
        assert!(registered(&tag));
    }

    /// A send cut short while the session queue has no room takes nothing: the link keeps its
    /// credit and delivery-count, nothing is registered, and the next send uses the credit.
    #[tokio::test]
    async fn a_send_cut_short_before_the_queue_takes_it_keeps_its_credit() {
        let mut inner = credited_sender(4096);
        let (writer, mut session) = full_session_channel().await;

        {
            let send = inner.link.send_payload(
                &writer,
                std::future::pending(),
                Payload::from_static(b"m"),
                0,
                Some(false),
                None,
                false,
            );
            tokio::pin!(send);
            assert!(futures_util::poll!(&mut send).is_pending());
        }
        assert_eq!(credit(&inner.link), (1, 0));
        assert!(!is_registered(&inner.link, &DeliveryTag::from([0; 4])));

        assert!(matches!(
            session.recv().await,
            Some(LinkFrame::Detach(_, _))
        ));
        let settlement = inner
            .link
            .send_payload(
                &writer,
                std::future::pending(),
                Payload::from_static(b"m"),
                0,
                Some(false),
                None,
                false,
            )
            .await
            .expect("the transfer is handed over");
        assert!(
            matches!(settlement, Settlement::Unsettled { delivery_tag, .. } if delivery_tag == DeliveryTag::from([0; 4]))
        );
        assert_eq!(credit(&inner.link), (0, 1));
    }

    /// The payload sizes of the frames one send of `len` bytes goes out in, received as the
    /// session takes them: a frame holds its share of the queue's bytes until the session is done
    /// with it. Every frame but the first has no delivery tag of its own.
    async fn frames_of_one_send(
        inner: &mut SenderInner<SenderLink<Target>>,
        len: usize,
    ) -> Vec<usize> {
        let (writer, mut session) = mpsc::channel::<LinkFrame>(1);
        let writer = crate::session::transfer_queue::Sender::from(writer);
        let send = inner.link.send_payload(
            &writer,
            std::future::pending(),
            Payload::from(vec![7; len]),
            0,
            Some(false),
            None,
            false,
        );
        let session = async {
            let mut frames = Vec::new();
            loop {
                let Some(LinkFrame::Transfer {
                    performative,
                    payload,
                    ..
                }) = session.recv().await
                else {
                    panic!("expected a transfer frame");
                };
                assert_eq!(performative.delivery_tag.is_some(), frames.is_empty());
                frames.push(payload.len());
                if !performative.more {
                    return frames;
                }
            }
        };
        let (sent, frames) = tokio::join!(send, session);
        sent.expect("the delivery is handed over");
        frames
    }

    /// A peer that omits max-frame-size takes frames of any size, but the session queue retains
    /// a bounded payload per frame. A delivery goes out in frames of the smaller of the two, and
    /// takes one credit however many frames it spans.
    #[tokio::test]
    async fn a_delivery_is_split_to_what_the_peer_and_the_session_queue_take() {
        use crate::session::transfer_queue::MAX_FRAME_PAYLOAD;
        let omitted = u32::MAX as usize;
        for (len, expected) in [
            (MAX_FRAME_PAYLOAD, vec![MAX_FRAME_PAYLOAD]),
            (MAX_FRAME_PAYLOAD + 1, vec![MAX_FRAME_PAYLOAD, 1]),
            (
                2 * MAX_FRAME_PAYLOAD + 2,
                vec![MAX_FRAME_PAYLOAD, MAX_FRAME_PAYLOAD, 2],
            ),
        ] {
            let mut inner = credited_sender(omitted);
            assert_eq!(frames_of_one_send(&mut inner, len).await, expected);
            assert_eq!(credit(&inner.link), (0, 1));
        }

        let mut inner = credited_sender(4096);
        let frames = frames_of_one_send(&mut inner, 10_000).await;
        assert_eq!(frames.iter().sum::<usize>(), 10_000);
        assert!(frames.len() > 2 && frames.iter().all(|len| *len < 4096));
        assert_eq!(credit(&inner.link), (0, 1));
    }

    /// A send whose session ended takes nothing.
    #[tokio::test]
    async fn a_send_to_an_ended_session_keeps_its_credit() {
        let mut inner = credited_sender(4096);
        let (writer, session) = mpsc::channel::<LinkFrame>(1);
        let writer = crate::session::transfer_queue::Sender::from(writer);
        drop(session);

        let result = inner
            .link
            .send_payload(
                &writer,
                std::future::pending(),
                Payload::from_static(b"m"),
                0,
                Some(false),
                None,
                false,
            )
            .await;

        assert!(result.is_err());
        assert_eq!(credit(&inner.link), (1, 0));
        assert!(!is_registered(&inner.link, &DeliveryTag::from([0; 4])));
    }

    /// Aborting a delivery sends a resuming, aborted transfer; the delivery is
    /// implicitly settled, so the application future resolves and nothing is
    /// registered in the unsettled map.
    #[tokio::test]
    async fn abort_sends_aborted_resuming_transfer_and_settles() {
        let (mut inner, _session_rx, mut outgoing_rx, _incoming_tx) =
            make_sender_inner_with_channels(4096);
        inner.link.input_handle = Some(InputHandle(0));

        let tag = DeliveryTag::from(vec![0x01]);
        let (sender, mut settled) = oneshot::channel();
        inner
            .link
            .unsettled
            .register(
                tag.clone(),
                UnsettledMessage::new(Payload::new(), None, 0, sender),
            )
            .unwrap();
        inner.abort(tag.clone(), 0).await.unwrap();

        match outgoing_rx.try_recv().expect("expected a transfer") {
            LinkFrame::Transfer {
                performative,
                payload,
                ..
            } => {
                assert!(performative.resume);
                assert!(performative.aborted);
                assert!(!performative.more);
                assert_eq!(performative.delivery_tag, Some(tag.clone()));
                assert!(payload.is_empty());
            }
            other => panic!("expected Transfer, got {other:?}"),
        }

        assert!(matches!(settled.try_recv(), Ok(Ok(None))));
        assert!(!inner
            .link
            .unsettled
            .read()
            .as_ref()
            .is_some_and(|map| map.contains_key(&tag)));
    }

    /// A delivery known only from the remote unsettled map has no local future
    /// to resolve, and is not registered either.
    #[tokio::test]
    async fn abort_without_local_sender_does_not_register() {
        let (mut inner, _session_rx, mut outgoing_rx, _incoming_tx) =
            make_sender_inner_with_channels(4096);
        inner.link.input_handle = Some(InputHandle(0));

        let tag = DeliveryTag::from(vec![0x02]);
        inner.abort(tag.clone(), 0).await.unwrap();

        assert!(matches!(
            outgoing_rx.try_recv(),
            Ok(LinkFrame::Transfer { .. })
        ));
        assert!(inner.link.unsettled.read().is_none());
    }

    /// Full resume→abort flow with a scripted peer: the local delivery is
    /// terminal (`Accepted`) while the peer's attach reports it as partially
    /// received, so the sender aborts it, settles the application future, and
    /// completes the mandatory detach/re-attach cycle.
    #[tokio::test]
    async fn resuming_a_terminal_delivery_against_remote_received_aborts() {
        let (mut inner, mut session_rx, mut outgoing_rx, _incoming_tx) =
            make_sender_inner_with_channels(4096);

        // The terminal local state is seeded directly: `resume_delivery`
        // handles the spec's resumption "spontaneous terminal outcome" case
        // (delivery-tag 11/14 in §2.6.13's examples), where the sender reached
        // a terminal outcome while the receiver reports partial receipt. The
        // public API does not produce that combination today; the reachable
        // abort arm is `Received(local) > Received(remote)`.
        let tag = DeliveryTag::from(vec![0x01]);
        let (sender, mut settled) = oneshot::channel();
        inner
            .link
            .unsettled
            .register(
                tag.clone(),
                UnsettledMessage::new(
                    Payload::new(),
                    Some(DeliveryState::Accepted(Accepted {})),
                    0,
                    sender,
                ),
            )
            .unwrap();
        inner.link.local_state = LinkState::Detached;
        inner.link.output_handle = None;

        let mut remote_attach = peer_receiver_attach();
        let mut remote_unsettled = OrderedMap::new();
        remote_unsettled.insert(
            tag.clone(),
            Some(DeliveryState::Received(Received {
                section_number: 0,
                section_offset: 0,
            })),
        );
        remote_attach.unsettled = Some(remote_unsettled);

        let driver = async move {
            let mut incoming_tx: Option<mpsc::Sender<LinkFrame>> = None;
            let mut attaches = 0usize;
            let mut detaches = 0usize;
            let mut saw_abort = false;

            loop {
                tokio::select! {
                    ctrl = session_rx.recv() => match ctrl {
                        Some(SessionControl::AllocateLink {
                            link_relay,
                            responder,
                            ..
                        }) => {
                            incoming_tx = Some(match link_relay {
                                LinkRelay::Sender { tx, .. } => tx,
                                LinkRelay::Receiver { tx, .. } => tx,
                            });
                            let _ = responder.send(Ok(crate::endpoint::OutputAttachment::for_tests(1)));
                        }
                        Some(SessionControl::GetIncomingWindow(responder)) => {
                            let _ = responder.send(16);
                        }
                        Some(_) => {}
                        None => break,
                    },
                    frame = outgoing_rx.recv() => match frame {
                        Some(LinkFrame::Attach(_)) => {
                            attaches += 1;
                            // The first attach is answered by
                            // `initial_remote_attach`; the second is the
                            // re-attach after the Resume exchange.
                            if attaches == 2 {
                                let tx =
                                    incoming_tx.as_ref().expect("allocate was answered");
                                let _ = tx
                                    .send(LinkFrame::Attach(peer_receiver_attach()))
                                    .await;
                            }
                        }
                        Some(LinkFrame::Transfer { performative, .. }) => {
                            assert!(performative.aborted);
                            assert!(performative.resume);
                            saw_abort = true;
                        }
                        Some(LinkFrame::Detach(detach, _)) => {
                            assert!(!detach.closed);
                            detaches += 1;
                            let tx = incoming_tx.as_ref().expect("allocate was answered");
                            let _ = tx
                                .send(LinkFrame::Detach(Detach {
                                    handle: fe2o3_amqp_types::definitions::Handle(0),
                                    closed: false,
                                    error: None,
                                }, crate::endpoint::AttachmentId::allocate()))
                                .await;
                        }
                        Some(_) => {}
                        None => break,
                    },
                }

                if saw_abort && detaches >= 1 && attaches >= 2 {
                    break;
                }
            }
        };

        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(
                inner.resume_incoming_attach(Some(remote_attach), false),
                driver
            )
        })
        .await
        .expect("the resume flow timed out");
        result.unwrap();

        assert!(matches!(settled.try_recv(), Ok(Ok(None))));
    }

    /// The hand-over a send is waiting in when the peer detaches.
    #[derive(Clone, Copy, Debug)]
    enum Stage {
        /// A new delivery's first frame, which takes its credit.
        FirstFrame,
        /// A later frame of a delivery spanning several.
        LaterFragment,
        /// A resumed delivery, which keeps its tag and takes no credit.
        Resume,
    }

    fn peer_detach(closed: bool) -> LinkFrame {
        LinkFrame::Detach(
            Detach {
                handle: 0.into(),
                closed,
                error: None,
            },
            crate::endpoint::AttachmentId::allocate(),
        )
    }

    /// The transfer that resumes the delivery under `tag` with its original tag.
    fn resuming_transfer(tag: DeliveryTag) -> Transfer {
        Transfer {
            handle: 0.into(),
            delivery_id: None,
            delivery_tag: Some(tag),
            message_format: Some(0),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: true,
            aborted: false,
            batchable: false,
        }
    }

    fn pending_delivery(
        tag: &DeliveryTag,
        inner: &SenderInner<SenderLink<Target>>,
    ) -> oneshot::Receiver<Result<Option<DeliveryState>, LinkStateError>> {
        let (reply, outcome) = oneshot::channel();
        inner
            .link
            .unsettled
            .register(
                tag.clone(),
                UnsettledMessage::new(Payload::from_static(b"m"), None, 0, reply),
            )
            .unwrap();
        outcome
    }

    /// The peer's detach and the queue's room become ready together while a send waits for the
    /// room: the detach wins, nothing more goes out, and a delivery whose first frame was not
    /// handed over keeps the link's credit.
    #[tokio::test]
    async fn a_detach_ready_with_the_queue_room_stops_the_hand_over() {
        for stage in [Stage::FirstFrame, Stage::LaterFragment, Stage::Resume] {
            let mut inner = credited_sender(512);
            let (writer, mut session) = match stage {
                // The first frame of a 2048-byte delivery fills the channel.
                Stage::LaterFragment => {
                    let (writer, session) = mpsc::channel::<LinkFrame>(1);
                    (writer.into(), session)
                }
                Stage::FirstFrame | Stage::Resume => full_session_channel().await,
            };
            let tag = DeliveryTag::from(vec![9]);
            let _outcome = matches!(stage, Stage::Resume).then(|| pending_delivery(&tag, &inner));
            let (detach, detached) = oneshot::channel();
            let detached = async { detached.await.ok() };
            {
                let send = async {
                    match stage {
                        Stage::Resume => {
                            inner
                                .link
                                .send_resuming_transfer(
                                    &writer,
                                    detached,
                                    resuming_transfer(tag.clone()),
                                    Payload::from_static(b"m"),
                                )
                                .await
                        }
                        Stage::FirstFrame | Stage::LaterFragment => {
                            let len = if matches!(stage, Stage::LaterFragment) {
                                2048
                            } else {
                                1
                            };
                            inner
                                .link
                                .send_payload(
                                    &writer,
                                    detached,
                                    Payload::from(vec![7; len]),
                                    0,
                                    Some(false),
                                    None,
                                    false,
                                )
                                .await
                                .map(|_| ())
                        }
                    }
                };
                tokio::pin!(send);
                assert!(futures_util::poll!(&mut send).is_pending(), "{stage:?}");
                detach.send(peer_detach(true)).unwrap();
                // The frame in front of the send leaves: the queue has room.
                let ahead = session.try_recv().unwrap();
                if matches!(stage, Stage::LaterFragment) {
                    assert!(
                        matches!(ahead, LinkFrame::Transfer { performative, .. } if performative.more)
                    );
                }
                assert!(
                    matches!(send.await, Err(LinkStateError::RemoteClosed)),
                    "{stage:?}"
                );
            }
            assert!(
                session.try_recv().is_err(),
                "{stage:?}: nothing more is handed over"
            );
            let expected = match stage {
                Stage::FirstFrame | Stage::Resume => (1, 0),
                Stage::LaterFragment => (0, 1),
            };
            assert_eq!(credit(&inner.link), expected, "{stage:?}");
            assert_eq!(
                is_registered(&inner.link, &tag),
                matches!(stage, Stage::Resume),
                "{stage:?}: a resumed delivery stays for the close or the next resume"
            );
        }
    }

    /// The relay closes the store for the peer's closing detach under the lock a registration
    /// takes. Closed first, a new delivery is refused with the close's error before it takes
    /// credit or queue room, and a resend finds nothing to send; registered first, the delivery is
    /// failed by the close.
    #[tokio::test]
    async fn the_store_orders_a_registration_with_the_relay_close() {
        use fe2o3_amqp_types::definitions::{AmqpError, Error};
        let error = Error::new(AmqpError::InternalError, Some("gone".into()), None);
        let close = |inner: &SenderInner<SenderLink<Target>>| {
            inner
                .link
                .unsettled
                .close(LinkStateError::RemoteClosedWithError(error.clone()))
        };

        // Closed before a new delivery registers.
        let mut inner = credited_sender(4096);
        let (writer, mut session) = mpsc::channel::<LinkFrame>(1);
        let writer = crate::session::transfer_queue::Sender::from(writer);
        close(&inner);
        let refused = inner
            .link
            .send_payload(
                &writer,
                std::future::pending(),
                Payload::from_static(b"m"),
                0,
                Some(false),
                None,
                false,
            )
            .await;
        assert!(matches!(refused, Err(LinkStateError::RemoteClosedWithError(e)) if e == error));
        assert_eq!(credit(&inner.link), (1, 0));
        assert!(session.try_recv().is_err());
        assert!(
            futures_util::poll!(std::pin::pin!(writer.reserve(1))).is_ready(),
            "the queue room is released"
        );

        // Closed before a resend moves the delivery to its new tag.
        let mut inner = credited_sender(4096);
        let (writer, mut session) = mpsc::channel::<LinkFrame>(1);
        let writer = crate::session::transfer_queue::Sender::from(writer);
        let old = DeliveryTag::from(vec![9]);
        let outcome = pending_delivery(&old, &inner);
        close(&inner);
        assert!(
            matches!(outcome.await, Ok(Err(LinkStateError::RemoteClosedWithError(e))) if e == error)
        );
        inner
            .link
            .resend(&writer, std::future::pending(), old)
            .await
            .unwrap();
        assert!(session.try_recv().is_err());
        assert_eq!(credit(&inner.link), (1, 0));

        // Registered before the close.
        let mut inner = credited_sender(4096);
        let (writer, mut session) = mpsc::channel::<LinkFrame>(1);
        let writer = crate::session::transfer_queue::Sender::from(writer);
        let Settlement::Unsettled { outcome, .. } = inner
            .link
            .send_payload(
                &writer,
                std::future::pending(),
                Payload::from_static(b"m"),
                0,
                Some(false),
                None,
                false,
            )
            .await
            .unwrap()
        else {
            panic!("an unsettled delivery")
        };
        assert!(matches!(session.try_recv(), Ok(LinkFrame::Transfer { .. })));
        close(&inner);
        assert!(
            matches!(outcome.await, Ok(Err(LinkStateError::RemoteClosedWithError(e))) if e == error)
        );
    }

    /// A resend cut short after its first frame, as a resume's interrupt drops it, keeps the
    /// delivery the store held before it, under its new tag, with its outcome still to come.
    #[tokio::test]
    async fn a_resend_cut_short_after_its_first_frame_keeps_the_delivery() {
        let mut inner = credited_sender(512);
        let (writer, mut session) = mpsc::channel::<LinkFrame>(1);
        let writer = crate::session::transfer_queue::Sender::from(writer);
        let old = DeliveryTag::from(vec![9]);
        let (tx, mut outcome) = oneshot::channel();
        let pending = UnsettledMessage::new(Payload::from(vec![7; 2048]), None, 0, tx);
        inner.link.unsettled.register(old.clone(), pending).unwrap();
        {
            let resend = inner
                .link
                .resend(&writer, std::future::pending(), old.clone());
            tokio::pin!(resend);
            assert!(futures_util::poll!(&mut resend).is_pending());
            let Ok(LinkFrame::Transfer { performative, .. }) = session.try_recv() else {
                panic!("the first frame")
            };
            assert!(performative.more, "later frames wait for room");
        }
        let held = inner.link.unsettled.read();
        let held = held.as_ref().unwrap();
        assert_eq!(held.len(), 1);
        assert!(!held.contains_key(&old));
        assert_eq!(held.values().next().unwrap().payload.len(), 2048);
        assert!(matches!(
            outcome.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }

    /// A drain that takes the credit while the first frame waits for queue room leaves the send
    /// waiting for new credit: it never goes out on credit it no longer has.
    #[tokio::test]
    async fn credit_drained_while_the_first_frame_waits_for_room_is_not_taken() {
        use crate::{endpoint::LinkFlow, util::Produce};
        let mut inner = credited_sender(4096);
        let mut producer = inner.link.flow_state.producer();
        let (writer, mut session) = full_session_channel().await;
        let send = inner.link.send_payload(
            &writer,
            std::future::pending(),
            Payload::from_static(b"m"),
            0,
            Some(false),
            None,
            false,
        );
        tokio::pin!(send);
        assert!(futures_util::poll!(&mut send).is_pending());
        let drain = LinkFlow {
            handle: 0.into(),
            delivery_count: Some(0),
            link_credit: Some(1),
            drain: true,
            ..Default::default()
        };
        producer.produce((drain, endpoint::OutputHandle(0))).await;
        assert!(matches!(session.try_recv(), Ok(LinkFrame::Detach(_, _))));
        assert!(futures_util::poll!(&mut send).is_pending());
        assert!(session.try_recv().is_err(), "no credit, no transfer");
        let grant = LinkFlow {
            handle: 0.into(),
            delivery_count: Some(1),
            link_credit: Some(1),
            ..Default::default()
        };
        producer.produce((grant, endpoint::OutputHandle(0))).await;
        let Settlement::Unsettled { delivery_tag, .. } = send.await.unwrap() else {
            panic!("an unsettled delivery")
        };
        assert_eq!(delivery_tag, DeliveryTag::from([0, 0, 0, 1]));
        assert!(matches!(session.try_recv(), Ok(LinkFrame::Transfer { .. })));
    }
}
