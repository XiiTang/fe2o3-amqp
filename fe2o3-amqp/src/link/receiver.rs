//! Implementation of AMQP1.0 receiver

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use fe2o3_amqp_types::{
    definitions::{
        self, DeliveryTag, Fields, Handle, LinkError, ReceiverSettleMode, Role, SequenceNo,
    },
    messaging::{
        Accepted, Address, DeliveryState, FromBody, Modified, Rejected, Released, Source, Target,
    },
    performatives::{Attach, Detach, Disposition, Transfer},
};
use tokio::sync::mpsc;

cfg_not_wasm32! {
    use std::time::Duration;
    use tokio::time::{error::Elapsed, timeout};
}

use crate::{
    control::SessionControl,
    endpoint::{self, LinkAttach, LinkDetach, LinkExt, LinkFlow},
    session::SessionHandle,
    Payload,
};

use super::{
    builder::{self, WithTarget, WithoutName, WithoutSource},
    delivery::{Delivery, DeliveryInfo},
    error::DetachError,
    incomplete_transfer::IncompleteTransfer,
    receiver_link::count_number_of_sections_and_offset,
    role,
    shared_inner::{LinkEndpointInner, LinkEndpointInnerDetach, LinkEndpointInnerReattach},
    ArcReceiverUnsettledMap, DetachThenResumeReceiverError, DispositionError, FlowError,
    IllegalLinkStateError, LinkFrame, LinkRelay, LinkStateError, MessageSizeExceeded,
    ReceiverAttachError, ReceiverAttachExchange, ReceiverFlowState, ReceiverLink,
    ReceiverResumeError, ReceiverResumeErrorKind, ReceiverTransferError, RecvError,
    SessionStopReason, DEFAULT_CREDIT,
};

cfg_transaction! {
    use fe2o3_amqp_types::definitions::AmqpError;
}

#[cfg(docsrs)]
use fe2o3_amqp_types::{
    messaging::{AmqpSequence, AmqpValue, Batch, Body},
    primitives::{LazyValue, Value},
};

/// Credit mode for the link
#[derive(Debug, Clone)]
pub enum CreditMode {
    /// Manual mode will require the user to manually allocate credit whenever
    /// the available credits are depleted
    Manual,

    /// The receiver will automatically re-fill the credit
    Auto(SequenceNo),
}

impl Default for CreditMode {
    fn default() -> Self {
        // Default credit
        Self::Auto(DEFAULT_CREDIT)
    }
}

/// A receive result before a saved protocol-fault close is dispatched.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "the native Delivery is the hot path and must move without an extra allocation"
)]
pub enum ReceiverReadStep<T> {
    /// A complete delivery from the normal receiver parser.
    Delivery(Delivery<T>),
    /// The receiver saved its fault; its owner must finish the close.
    SelfClosing,
}

/// A receive-preserving step of an explicit suspension.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "the native Delivery is the hot path and must move without an extra allocation"
)]
pub enum ReceiverDetachStep<T> {
    /// A complete delivery preceding the Detach reply.
    Delivery(Delivery<T>),
    /// A saved protocol fault requires its owned close.
    SelfClosing,
    /// The peer suspended this attachment.
    Suspended {
        /// The original peer error, if any.
        error: Option<definitions::Error>,
    },
    /// The crossed mandatory closing handshake completed.
    Closed {
        /// The original closing Detach's error, if any.
        error: Option<definitions::Error>,
    },
}

/// A queued receive result after the original native session stopped.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "the native Delivery is the hot path and must move without an extra allocation"
)]
pub enum ReceiverCollectionStep<T> {
    /// A complete delivery from the normal parser.
    Delivery(Delivery<T>),
    /// The native queue is exhausted.
    Drained {
        /// The original session's immutable stop reason.
        reason: SessionStopReason,
    },
    /// A queued closing Detach prevents recovery.
    Closed {
        /// The original peer error, if any.
        error: Option<definitions::Error>,
    },
}

/// The receive or exchange phase that prevented suspension.
#[derive(Debug, thiserror::Error)]
pub enum ReceiverDetachError<E> {
    /// A native receive fault.
    #[error("receive failed during suspension")]
    Receive(#[source] RecvError),
    /// A crossed end exchange failed.
    #[error("crossed exchange failed during suspension")]
    Exchange(#[source] super::LinkExchangeError<ReceiverAttachError, E>),
}

/// The parser's decision for exactly one consumed Transfer.
#[derive(Debug)]
pub enum ReceiverTransferEffect {
    /// A payload fragment is retained for completion.
    PayloadIncomplete,
    /// A complete delivery was constructed.
    Delivered {
        /// That delivery's exact native identity.
        info: DeliveryInfo,
    },
    /// The peer aborted the delivery.
    Aborted,
    /// The native recovery parser ignored an unknown resumed delivery.
    Ignored {
        /// Whether the ignored delivery continues.
        more: bool,
    },
    /// A retained complete delivery was resumed without redelivery.
    StateOnly {
        /// The first frame's exact new identity, if one was installed.
        info: Option<DeliveryInfo>,
        /// Whether the tag remains in the native store.
        retained: bool,
        /// Whether this resumed delivery continues.
        more: bool,
    },
    /// This frame failed native validation or parsing.
    Failed,
}

/// One native decision for the next admitted Transfer.
#[derive(Debug)]
pub struct ReceiverTransferProgress {
    effect: ReceiverTransferEffect,
}

impl ReceiverTransferProgress {
    /// The native parser's authoritative decision.
    pub fn effect(&self) -> &ReceiverTransferEffect {
        &self.effect
    }
}

/// Synchronous progress sink; callbacks run outside native map and flow locks.
pub trait ReceiverTransferObserver: Send {
    /// Runs once after the native semantic decision, before another frame.
    fn transfer(&mut self, progress: ReceiverTransferProgress);
}

struct NoopTransferObserver;
impl ReceiverTransferObserver for NoopTransferObserver {
    fn transfer(&mut self, _: ReceiverTransferProgress) {}
}

#[derive(Clone, Copy)]
enum ReceiveContext {
    Live,
    Stopped,
}

#[derive(Debug)]
enum ReceiveFault {
    Error(RecvError),
    SelfClosing,
}
impl From<RecvError> for ReceiveFault {
    fn from(error: RecvError) -> Self {
        Self::Error(error)
    }
}

struct TransferCommit {
    committed: bool,
}
impl TransferCommit {
    fn commit<O: ReceiverTransferObserver + ?Sized>(
        &mut self,
        effect: ReceiverTransferEffect,
        observer: &mut O,
    ) {
        assert!(!self.committed, "each Transfer commits once");
        self.committed = true;
        observer.transfer(ReceiverTransferProgress { effect });
    }
}

/// An AMQP1.0 receiver
///
/// # Attach a new receiver with default configurations
///
/// ```rust, ignore
/// let mut receiver = Receiver::attach(
///     &mut session,           // mutable reference to SessionHandle
///     "rust-receiver-link-1", // link name
///     "q1"                    // Source address
/// ).await.unwrap();
///
/// // Receiver defaults to `ReceiverSettleMode::First` which spontaneously settles incoming delivery
/// let delivery: Delivery<String> = receiver.recv::<String>().await.unwrap();
///
/// receiver.close().await.unwrap();
/// ```
///
/// ## Default configuration
///
/// | Field | Default Value |
/// |-------|---------------|
/// |`name`|`String::default()`|
/// |`snd_settle_mode`|`SenderSettleMode::Mixed`|
/// |`rcv_settle_mode`|`ReceiverSettleMode::First`|
/// |`source`|`None` |
/// |`target`| `Some(Target)` |
/// |`initial_delivery_count`| `0` |
/// |`max_message_size`| `None` |
/// |`offered_capabilities`| `None` |
/// |`desired_capabilities`| `None` |
/// |`Properties`| `None` |
/// |`buffer_size`| `u16::MAX` |
/// |`role`| `role::Sender` |
/// |`auto_accept`|`false`|
///
/// # Customize configuration with [`builder::Builder`]
///
/// ```rust, ignore
/// let mut receiver = Receiver::builder()
///     .name("rust-receiver-link-1")
///     .source("q1")
///     .attach(&mut session)
///     .receiver_settle_mode(ReceiverSettleMode::Second)
///     .await
///     .unwrap();
/// ```
#[derive(Debug)]
pub struct Receiver {
    pub(crate) inner: ReceiverInner<ReceiverLink<Target>>,
}

impl Receiver {
    /// Receive with authoritative per-Transfer progress, before protocol-close wire work.
    pub async fn recv_observed<T, O>(
        &mut self,
        observer: &mut O,
    ) -> Result<ReceiverReadStep<T>, RecvError>
    where
        for<'de> T: FromBody<'de> + Send,
        O: ReceiverTransferObserver + ?Sized,
    {
        self.inner.recv_observed(observer).await
    }

    /// The saved native cause of a signalled protocol-fault close.
    pub fn self_close_cause(&self) -> Option<&RecvError> {
        self.inner.self_close.as_ref().map(|close| &close.cause)
    }

    /// Finish the saved protocol close without replay, keeping scoped correlation.
    pub async fn finish_self_close_exact<F, G, E>(
        &mut self,
        before: &mut F,
    ) -> Result<(), super::LinkExchangeError<ReceiverAttachError, E>>
    where
        F: FnMut() -> Result<G, E> + Send,
        G: Send,
        E: Send,
    {
        use super::{state::LinkState, LinkExchangeError as X};
        let error = self
            .inner
            .self_close
            .as_ref()
            .ok_or(X::Detach(DetachError::IllegalState))?
            .error
            .clone();
        match self.inner.link.local_state() {
            LinkState::Closed => {}
            LinkState::Detached => {
                super::shared_inner::reattach_then_close_exact(
                    &mut self.inner,
                    before,
                    Some(error),
                )
                .await?;
            }
            state => {
                let was_detaching = matches!(state, LinkState::DetachSent);
                if !matches!(state, LinkState::DetachSent | LinkState::CloseSent) {
                    self.inner
                        .send_detach(true, Some(error.clone()))
                        .await
                        .map_err(X::Detach)?;
                }
                let reply = super::shared_inner::recv_remote_detach(&mut self.inner)
                    .await
                    .map_err(X::Detach)?;
                if was_detaching {
                    if !reply.closed {
                        let _ = self.inner.link.apply_remote_detach_outcome(reply);
                    }
                    super::shared_inner::reattach_then_close_exact(
                        &mut self.inner,
                        before,
                        Some(error),
                    )
                    .await?;
                } else if reply.closed {
                    self.inner.link.on_detach_reply(reply).map_err(X::Detach)?;
                } else {
                    let _ = self.inner.link.apply_remote_detach_outcome(reply);
                    super::shared_inner::reattach_then_close_exact(
                        &mut self.inner,
                        before,
                        Some(error),
                    )
                    .await?;
                }
            }
        }
        self.inner.self_close.take();
        Ok(())
    }

    /// Drain in-flight deliveries while waiting for an explicit suspension reply.
    pub async fn recv_detaching_exact<T, O, F, G, E>(
        &mut self,
        observer: &mut O,
        before: &mut F,
    ) -> Result<ReceiverDetachStep<T>, ReceiverDetachError<E>>
    where
        for<'de> T: FromBody<'de> + Send,
        O: ReceiverTransferObserver + ?Sized,
        F: FnMut() -> Result<G, E> + Send,
        G: Send,
        E: Send,
    {
        self.recv_end_prefix(observer, before, false).await
    }

    async fn recv_end_prefix<T, O, F, G, E>(
        &mut self,
        observer: &mut O,
        before: &mut F,
        closing: bool,
    ) -> Result<ReceiverDetachStep<T>, ReceiverDetachError<E>>
    where
        for<'de> T: FromBody<'de> + Send,
        O: ReceiverTransferObserver + ?Sized,
        F: FnMut() -> Result<G, E> + Send,
        G: Send,
        E: Send,
    {
        loop {
            if self.inner.self_close.is_some() {
                return Ok(ReceiverDetachStep::SelfClosing);
            }
            let state = self.inner.link.local_state();
            if !(matches!(state, super::state::LinkState::DetachSent)
                || closing
                    && matches!(
                        state,
                        super::state::LinkState::Attached | super::state::LinkState::CloseSent
                    ))
            {
                return Err(ReceiverDetachError::Receive(
                    LinkStateError::IllegalState.into(),
                ));
            }
            let frame = self
                .inner
                .incoming
                .recv()
                .await
                .ok_or_else(|| ReceiverDetachError::Receive(self.inner.stopped()))?;
            match frame {
                LinkFrame::Transfer {
                    performative,
                    payload,
                    window_slot,
                    ..
                } => {
                    match self
                        .inner
                        .receive_transfer(
                            performative,
                            payload,
                            window_slot,
                            ReceiveContext::Live,
                            observer,
                        )
                        .await
                    {
                        Ok(Some(delivery)) => return Ok(ReceiverDetachStep::Delivery(delivery)),
                        Ok(None) => {}
                        Err(ReceiveFault::SelfClosing) => {
                            return Ok(ReceiverDetachStep::SelfClosing)
                        }
                        Err(ReceiveFault::Error(error)) => {
                            return Err(ReceiverDetachError::Receive(error))
                        }
                    }
                }
                LinkFrame::Detach(detach, _) => {
                    let closed = detach.closed;
                    let error = detach.error.clone();
                    if closed {
                        if closing && matches!(state, super::state::LinkState::CloseSent) {
                            self.inner.link.on_detach_reply(detach).map_err(|error| {
                                ReceiverDetachError::Exchange(super::LinkExchangeError::Detach(
                                    error,
                                ))
                            })?;
                            return Ok(ReceiverDetachStep::Closed { error });
                        }
                        if closing && matches!(state, super::state::LinkState::Attached) {
                            let applied = self.inner.link.apply_remote_detach_outcome(detach);
                            if matches!(applied, Err(super::ApplyRemoteDetachError::IllegalState)) {
                                return Err(ReceiverDetachError::Receive(
                                    LinkStateError::IllegalState.into(),
                                ));
                            }
                            return Ok(ReceiverDetachStep::Closed { error });
                        }
                        super::shared_inner::reattach_then_close_exact(
                            &mut self.inner,
                            before,
                            None,
                        )
                        .await
                        .map_err(ReceiverDetachError::Exchange)?;
                        return Ok(ReceiverDetachStep::Closed { error });
                    }
                    let result = self.inner.link.apply_remote_detach_outcome(detach);
                    if matches!(result, Err(super::ApplyRemoteDetachError::IllegalState)) {
                        return Err(ReceiverDetachError::Receive(
                            LinkStateError::IllegalState.into(),
                        ));
                    }
                    return Ok(ReceiverDetachStep::Suspended { error });
                }
                LinkFrame::Attach(_) => {
                    return Err(ReceiverDetachError::Receive(
                        LinkStateError::IllegalState.into(),
                    ))
                }
                LinkFrame::Flow(..) | LinkFrame::Disposition(_) => {
                    unreachable!("handled by LinkRelay")
                }
                #[cfg(feature = "transaction")]
                LinkFrame::Acquisition(_) => {
                    self.inner.close_itself(
                        definitions::Error::new(
                            AmqpError::NotImplemented,
                            "Transactional acquisition is not implemented".to_string(),
                            None,
                        ),
                        RecvError::TransactionalAcquisitionIsNotImeplemented,
                    );
                    return Ok(ReceiverDetachStep::SelfClosing);
                }
            }
        }
    }

    /// Consume only input queued by the original, already-stopped native session.
    pub async fn recv_collecting_stopped<T, O>(
        &mut self,
        observer: &mut O,
    ) -> Result<ReceiverCollectionStep<T>, RecvError>
    where
        for<'de> T: FromBody<'de> + Send,
        O: ReceiverTransferObserver + ?Sized,
    {
        let reason = self
            .inner
            .link
            .session_stop_reason
            .get()
            .cloned()
            .ok_or(RecvError::LinkStateError(LinkStateError::IllegalState))?;
        if let Some(close) = self.inner.self_close.take() {
            return Err(close.cause);
        }
        loop {
            let frame = match self.inner.incoming.try_recv() {
                Ok(frame) => frame,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Ok(ReceiverCollectionStep::Drained { reason })
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    return Err(LinkStateError::IllegalState.into())
                }
            };
            match frame {
                LinkFrame::Transfer {
                    performative,
                    payload,
                    window_slot,
                    ..
                } => {
                    match self
                        .inner
                        .receive_transfer(
                            performative,
                            payload,
                            window_slot,
                            ReceiveContext::Stopped,
                            observer,
                        )
                        .await
                    {
                        Ok(Some(delivery)) => {
                            return Ok(ReceiverCollectionStep::Delivery(delivery))
                        }
                        Ok(None) => {}
                        Err(ReceiveFault::Error(error)) => return Err(error),
                        Err(ReceiveFault::SelfClosing) => {
                            unreachable!("stopped collection cannot initiate close")
                        }
                    }
                }
                LinkFrame::Detach(detach, _) => {
                    let closed = detach.closed;
                    let error = detach.error.clone();
                    let _ = self.inner.link.apply_remote_detach_outcome(detach);
                    if closed {
                        return Ok(ReceiverCollectionStep::Closed { error });
                    }
                }
                LinkFrame::Attach(_) => return Err(LinkStateError::IllegalState.into()),
                LinkFrame::Flow(..) | LinkFrame::Disposition(_) => {
                    unreachable!("handled by LinkRelay")
                }
                #[cfg(feature = "transaction")]
                LinkFrame::Acquisition(_) => {
                    return Err(RecvError::TransactionalAcquisitionIsNotImeplemented)
                }
            }
        }
    }

    /// Close while preserving input admitted on the original attachment.
    ///
    /// Each Delivery must be accounted before requesting another step. A
    /// Suspended boundary ends the old receiving attachment before CloseOnly.
    pub async fn close_in_place_exact<T, O, F, G, E>(
        &mut self,
        peer_suspended: bool,
        observer: &mut O,
        before: &mut F,
    ) -> Result<ReceiverDetachStep<T>, ReceiverDetachError<E>>
    where
        for<'de> T: FromBody<'de> + Send,
        O: ReceiverTransferObserver + ?Sized,
        F: FnMut() -> Result<G, E> + Send,
        G: Send,
        E: Send,
    {
        use super::state::LinkState;
        match self.inner.link.local_state() {
            LinkState::Closed => return Ok(ReceiverDetachStep::Closed { error: None }),
            LinkState::Detached => {
                super::shared_inner::close_exact(&mut self.inner, false, before)
                    .await
                    .map_err(ReceiverDetachError::Exchange)?;
                return Ok(ReceiverDetachStep::Closed { error: None });
            }
            LinkState::Attached if !peer_suspended => {
                self.inner.send_detach(true, None).await.map_err(|error| {
                    ReceiverDetachError::Exchange(super::LinkExchangeError::Detach(error))
                })?;
            }
            _ => {}
        }
        self.recv_end_prefix(observer, before, true).await
    }

    /// Creates a builder for the [`Receiver`]
    pub fn builder(
    ) -> builder::Builder<role::ReceiverMarker, Target, WithoutName, WithoutSource, WithTarget>
    {
        builder::Builder::<role::ReceiverMarker, Target, _, _, _>::new()
    }

    /// Get the name of the link
    pub fn name(&self) -> &str {
        self.inner.link.name()
    }

    /// Whether the closing Detach exchange completed and released this link's handle.
    pub fn is_closed(&self) -> bool {
        matches!(self.inner.link.local_state, super::state::LinkState::Closed)
            && self.inner.link.output_handle.is_none()
    }

    /// Observe the engine's unsettled map without a second disposition ledger.
    pub fn settlement_view(&self) -> ReceiverSettlementView {
        ReceiverSettlementView {
            changed: self.inner.link.unsettled.subscribe(),
            unsettled: self.inner.link.unsettled.clone(),
        }
    }

    /// Set the shared native budget before receiving any message fragments.
    /// Existing retained fragments cannot be silently moved to another budget.
    pub fn set_receive_budget(
        &mut self,
        budget: Arc<super::receive_budget::ReceiveBudget>,
    ) -> Result<(), RecvError> {
        if self.inner.incomplete_transfer.is_some() {
            return Err(LinkStateError::IllegalState.into());
        }
        self.inner.receive_budget = budget;
        Ok(())
    }

    /// Returns the `max_message_size` of the link. A value of zero indicates that the link has no
    /// maximum message size, and thus a zero value is turned into a `None`
    ///
    /// When this value is set, deliveries larger than it are rejected on
    /// receive with `amqp:link:message-size-exceeded` and [`recv`](#method.recv)
    /// returns [`RecvError::MessageSizeExceeded`]; the link is closed with an error.
    pub fn max_message_size(&self) -> Option<u64> {
        self.inner.link.max_message_size()
    }

    /// Get the configured credit mode of the link
    pub fn credit_mode(&self) -> &CreditMode {
        &self.inner.credit_mode
    }

    /// Set the credit mode
    ///
    /// This will not send a flow to the remote peer even if credits in `CreditMode::Auto` is changed.
    pub fn set_credit_mode(&mut self, credit_mode: CreditMode) {
        self.inner.credit_mode = credit_mode;
    }

    /// Get the current credit of the link
    pub fn credit(&self) -> u32 {
        self.inner.link.flow_state.link_credit()
    }

    /// Get the `auto_accept` field of receiver
    pub fn auto_accept(&self) -> bool {
        self.inner.auto_accept
    }

    /// Set `auto_accept` to `value`
    pub fn set_auto_accept(&mut self, value: bool) {
        self.inner.auto_accept = value;
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

    /// Attach the receiver link to a session with the default configuration
    /// with the `name` and `source` address set the specified value
    ///
    /// # Default configuration
    ///
    /// | Field | Default Value |
    /// |-------|---------------|
    /// |`name`|`String::default()`|
    /// |`snd_settle_mode`|`SenderSettleMode::Mixed`|
    /// |`rcv_settle_mode`|`ReceiverSettleMode::First`|
    /// |`source`|`None` |
    /// |`target`| `Some(Target)` |
    /// |`initial_delivery_count`| `0` |
    /// |`max_message_size`| `None` |
    /// |`offered_capabilities`| `None` |
    /// |`desired_capabilities`| `None` |
    /// |`Properties`| `None` |
    /// |`buffer_size`| `u16::MAX` |
    /// |`role`| `role::Sender` |
    /// |`auto_accept`|`false`|
    ///
    /// # Example
    ///
    /// ```rust, ignore
    /// let mut receiver = Receiver::attach(
    ///     &mut session,           // mutable reference to SessionHandle
    ///     "rust-receiver-link-1", // link name
    ///     "q1"                    // Source address
    /// ).await.unwrap();
    /// ```
    pub async fn attach<R>(
        session: &mut SessionHandle<R>,
        name: impl Into<String>,
        addr: impl Into<Address>,
    ) -> Result<Receiver, ReceiverAttachError> {
        Self::builder()
            .name(name)
            .source(addr)
            .attach(session)
            .await
    }

    /// Receive a message from the link
    ///
    /// # Example
    ///
    /// ```rust, ignore
    /// let delivery: Delivery<String> = receiver.recv::<String>().await.unwrap();
    /// receiver.accept(&delivery).await.unwrap();
    /// ```
    ///
    /// # The receive type `Delivery<T>`
    ///
    /// If the user is not certain the exact type (including the exact body section type) to
    /// receive, [`Body<Value>`] is probably the safest bet. [`Body<T>`] covers all possible body
    /// section types, including an empty body, and `Value` covers all possible AMQP 1.0 types.
    ///
    /// ```rust,ignore
    /// // `Body<Value>` covers all possibilities
    /// let delivery: Delivery<Body<Value>> = receiver.recv::<Body<Value>>().await.unwrap();
    /// receiver.accept(&delivery).await.unwrap();
    /// ```
    ///
    /// Lazy deserialization is supported with [`Body<LazyValue>`]. The [`LazyValue`] type is a
    /// a thin wrapper around the encoded bytes and can be deserialized lazily.
    ///
    /// ```rust,ignore
    /// // `Body<LazyValue>` covers all possibilities and can be deserialized lazily
    /// let delivery: Delivery<Body<LazyValue>> = receiver.recv::<Body<LazyValue>>().await.unwrap();
    /// receiver.accept(&delivery).await.unwrap();
    /// ```
    ///
    /// If the user is certain an [`AmqpValue`] body section is expected, then the user could use
    /// [`AmqpValue<KnownType>`] if the exact message type `KnownType` is known and implements
    /// [`serde::Deserialize`]. If the user is not sure about the exact message type, one could use
    /// [`AmqpValue<Value>`] to cover the most general cases. This also applies to [`AmqpSequence`]
    /// or [`Batch<AmqpSequence>`].
    ///
    /// ```rust,ignore
    /// // `KnownType` must implement `serde::Deserialize`
    /// let delivery: Delivery<AmqpValue<KnownType>> = receiver.recv::<AmqpValue<KnownType>>().await.unwrap();
    /// receiver.accept(&delivery).await.unwrap();
    /// ```
    ///
    /// Another option to use a custom type is to implement the [`FromBody`] trait on a custom type.
    ///
    /// ```rust,ignore
    /// #[derive(Deserialize)]
    /// struct Foo {
    ///     a: i32
    /// }
    ///
    /// impl FromBody<'_> for Foo {
    ///     type Body = AmqpValue<Foo>;
    ///
    ///     fn from_body(body: Self::Body) -> Self {
    ///         body.0
    ///     }
    /// }
    ///
    /// let delivery: Delivery<Foo> = receiver.recv::<Foo>().await.unwrap();
    /// receiver.accept(&delivery).await.unwrap();
    /// ```
    ///
    /// # Message size
    ///
    /// When the link advertises a `max_message_size` (see
    /// [`max_message_size`](#method.max_message_size)) and the peer sends a
    /// delivery larger than it, the receiver rejects the delivery with a
    /// `Rejected` disposition carrying `amqp:link:message-size-exceeded` and
    /// this method returns [`RecvError::MessageSizeExceeded`] after closing the link
    /// with the corresponding error. Other links on the session remain usable.
    ///
    /// # Cancel safety
    ///
    /// This function is cancel-safe. See [#22](https://github.com/minghuaw/fe2o3-amqp/issues/22)
    /// for more details.
    pub async fn recv<T>(&mut self) -> Result<Delivery<T>, RecvError>
    where
        for<'de> T: FromBody<'de> + Send,
    {
        self.inner.recv().await
    }

    /// Set the link credit. This will stop draining if the link is in a draining cycle
    pub async fn set_credit(&mut self, credit: SequenceNo) -> Result<(), IllegalLinkStateError> {
        self.inner.set_credit(credit).await
    }

    /// Drain the link.
    ///
    /// This will send a `Flow` performative with the `drain` field set to true.
    /// Setting the credit will set the `drain` field to false and stop draining
    pub async fn drain(&mut self) -> Result<(), IllegalLinkStateError> {
        self.inner.drain().await
    }

    /// Send the link properties to the remote peer via a `Flow` performative
    pub async fn send_properties(&self) -> Result<(), FlowError> {
        self.inner.send_properties().await
    }

    /// Begin an explicit non-closing Detach while retaining the receive owner.
    /// Continue calling recv() to deliver in-flight messages and preserve partial
    /// transfers until it reports RemoteDetached, then call into_detached().
    pub async fn request_detach(&mut self) -> Result<(), DetachError> {
        self.inner.send_detach(false, None).await
    }

    /// Preserve the actual endpoint after Detach or a stopped parent session.
    /// A still-active endpoint is returned unchanged as an error; this method
    /// does not perform I/O or manufacture an attached link's suspension.
    #[allow(
        clippy::result_large_err,
        reason = "return the original endpoint without allocating while it remains attached"
    )]
    pub fn into_detached(mut self) -> Result<DetachedReceiver, Self> {
        if matches!(
            self.inner.link.local_state,
            super::state::LinkState::Closed | super::state::LinkState::CloseSent
        ) {
            return Err(self);
        }
        if matches!(
            self.inner.link.local_state,
            super::state::LinkState::Detached
        ) || self.inner.link.session_stop_reason.get().is_some()
        {
            self.inner.link.local_state = super::state::LinkState::Detached;
            self.inner.link.output_handle.take();
            self.inner.link.input_handle.take();
            Ok(DetachedReceiver {
                inner: Box::new(self.inner),
            })
        } else {
            Err(self)
        }
    }

    /// Detach the link.
    ///
    /// This will send a `Detach` performative with the `closed` field set to false. If the remote
    /// peer responds with a Detach performative whose `closed` field is set to true, the link will
    /// re-attach and then close by exchanging closing Detach performatives.
    pub async fn detach(mut self) -> Result<DetachedReceiver, (DetachedReceiver, DetachError)> {
        match self.inner.detach_with_error(None).await {
            Ok(_) => Ok(DetachedReceiver {
                inner: Box::new(self.inner),
            }),
            Err(err) => Err((
                DetachedReceiver {
                    inner: Box::new(self.inner),
                },
                err,
            )),
        }
    }

    /// Detach the link with an error.
    ///
    /// This will send a `Detach` performative with the `closed` field set to false. If the remote
    /// peer responds with a Detach performative whose `closed` field is set to true, the link will
    /// re-attach and then close by exchanging closing Detach performatives.
    pub async fn detach_with_error(
        mut self,
        error: impl Into<definitions::Error>,
    ) -> Result<DetachedReceiver, (DetachedReceiver, DetachError)> {
        match self.inner.detach_with_error(Some(error.into())).await {
            Ok(_) => Ok(DetachedReceiver {
                inner: Box::new(self.inner),
            }),
            Err(err) => Err((
                DetachedReceiver {
                    inner: Box::new(self.inner),
                },
                err,
            )),
        }
    }

    cfg_not_wasm32! {
        /// Detach the link with a timeout
        ///
        /// This simply wraps [`detach`](#method.detach) with a `timeout`
        pub async fn detach_with_timeout(
            self,
            duration: Duration,
        ) -> Result<Result<DetachedReceiver, (DetachedReceiver, DetachError)>, Elapsed> {
            timeout(duration, self.detach()).await
        }
    }

    /// Detach the link and then resume on a new session.
    ///
    /// This will still attemt to re-attach even if the detach fails.
    /// `DetachThenResumeReceiverError::Resume` will be returned if detach succeeds but re-attach
    /// fails. `DetachThenResumeReceiverError::Detach` will be returned if both detach and re-attach
    /// fails.
    pub async fn detach_then_resume_on_session<R>(
        &mut self,
        new_session: &SessionHandle<R>,
    ) -> Result<ReceiverAttachExchange, DetachThenResumeReceiverError> {
        // detach the link
        let detach_result = self
            .inner
            .detach_with_error(None)
            .await
            .map_err(DetachThenResumeReceiverError::from);

        // re-attach the link
        let is_reattaching = self.inner.switch_session(new_session);
        let exchange_result = self
            .inner
            .resume_incoming_attach(None, is_reattaching)
            .await
            .map_err(DetachThenResumeReceiverError::from);

        match (detach_result, exchange_result) {
            (_, Ok(exchange)) => Ok(exchange),
            (Ok(_), Err(err)) => Err(err),
            (Err(err), Err(_)) => Err(err),
        }
    }

    /// Close the link.
    ///
    /// This will send a Detach performative with the `closed` field set to true.
    pub async fn close(mut self) -> Result<(), DetachError> {
        self.close_in_place().await
    }

    /// Complete closing without consuming the receiver, so its owner can keep
    /// driving the same exchange after a caller's wait expires.
    pub async fn close_in_place(&mut self) -> Result<(), DetachError> {
        self.inner.close_with_error(None).await
    }

    /// Close the link with an error.
    ///
    /// This will send a Detach performative with the `closed` field set to true.
    pub async fn close_with_error(
        mut self,
        error: impl Into<definitions::Error>,
    ) -> Result<(), DetachError> {
        // Stop link transfer before closing
        self.set_credit(0).await?;
        self.inner.close_with_error(Some(error.into())).await
    }

    /// Accept the message by sending a disposition with the `delivery_state` field set
    /// to `Accept`.
    ///
    /// This will not send disposition if the delivery is not found in the local unsettled map.
    ///
    /// # Example
    ///
    /// The code of the example below can be found in the [GitHub repo](https://github.com/minghuaw/fe2o3-amqp/blob/main/examples/receiver/src/main.rs)
    ///
    /// ```rust,ignore
    /// let delivery: Delivery<Value> = receiver.recv().await.unwrap();
    /// receiver.accept(&delivery).await.unwrap();
    /// ```
    pub async fn accept(
        &self,
        delivery_info: impl Into<DeliveryInfo>,
    ) -> Result<(), DispositionError> {
        let state = TerminalDeliveryState::Accepted(Accepted {});
        self.dispose(delivery_info, state).await
    }

    /// Accept the message by sending one or more disposition(s) with the `delivery_state` field set
    /// to `Accept`
    ///
    /// Only deliveries that are found in the local unsettled map will be included in the disposition frame(s).
    ///
    /// # Example
    ///
    /// The code of the example below can be found in the [GitHub repo](https://github.com/minghuaw/fe2o3-amqp/blob/main/examples/dispose_multiple/src/main.rs)
    ///
    /// ```rust,ignore
    /// let delivery1: Delivery<Value> = receiver.recv().await.unwrap();
    /// let delivery2: Delivery<Value> = receiver.recv().await.unwrap();
    /// receiver.accept_all(vec![&delivery1, &delivery2]).await.unwrap();
    /// ```
    pub async fn accept_all(
        &self,
        deliveries: impl IntoIterator<Item = impl Into<DeliveryInfo>>,
    ) -> Result<(), DispositionError> {
        let state = TerminalDeliveryState::Accepted(Accepted {});
        self.dispose_all(deliveries, state).await
    }

    /// Reject the message by sending a disposition with the `delivery_state` field set
    /// to `Reject`
    ///
    /// This will not send disposition if the delivery is not found in the local unsettled map.
    pub async fn reject(
        &self,
        delivery_info: impl Into<DeliveryInfo>,
        error: impl Into<Option<definitions::Error>>,
    ) -> Result<(), DispositionError> {
        let state = TerminalDeliveryState::Rejected(Rejected {
            error: error.into(),
        });
        self.dispose(delivery_info, state).await
    }

    /// Reject the message by sending one or more disposition(s) with the `delivery_state` field set
    /// to `Reject`
    ///
    /// Only deliveries that are found in the local unsettled map will be included in the disposition frame(s).
    pub async fn reject_all(
        &self,
        deliveries: impl IntoIterator<Item = impl Into<DeliveryInfo>>,
        error: impl Into<Option<definitions::Error>>,
    ) -> Result<(), DispositionError> {
        let state = TerminalDeliveryState::Rejected(Rejected {
            error: error.into(),
        });
        self.dispose_all(deliveries, state).await
    }

    /// Release the message by sending a disposition with the `delivery_state` field set
    /// to `Release`
    ///
    /// This will not send disposition if the delivery is not found in the local unsettled map.
    pub async fn release(
        &self,
        delivery_info: impl Into<DeliveryInfo>,
    ) -> Result<(), DispositionError> {
        let state = TerminalDeliveryState::Released(Released {});
        self.dispose(delivery_info, state).await
    }

    /// Release the message by sending one or more disposition(s) with the `delivery_state` field set
    /// to `Release`
    ///
    /// Only deliveries that are found in the local unsettled map will be included in the disposition frame(s).
    pub async fn release_all(
        &self,
        deliveries: impl IntoIterator<Item = impl Into<DeliveryInfo>>,
    ) -> Result<(), DispositionError> {
        let state = TerminalDeliveryState::Released(Released {});
        self.dispose_all(deliveries, state).await
    }

    /// Modify the message by sending a disposition with the `delivery_state` field set
    /// to `Modify`
    ///
    /// This will not send disposition if the delivery is not found in the local unsettled map.
    pub async fn modify(
        &self,
        delivery_info: impl Into<DeliveryInfo>,
        modified: Modified,
    ) -> Result<(), DispositionError> {
        let state = TerminalDeliveryState::Modified(modified);
        self.dispose(delivery_info, state).await
    }

    /// Modify the message by sending one or more disposition(s) with the `delivery_state` field set
    /// to `Modify`
    ///
    /// Only deliveries that are found in the local unsettled map will be included in the disposition frame(s).
    pub async fn modify_all(
        &self,
        deliveries: impl IntoIterator<Item = impl Into<DeliveryInfo>>,
        modified: Modified,
    ) -> Result<(), DispositionError> {
        let state = TerminalDeliveryState::Modified(modified);
        self.dispose_all(deliveries, state).await
    }

    /// Dispose the message by sending a disposition with the provided state
    ///
    /// This will not send disposition if the delivery is not found in the local unsettled map.
    pub async fn dispose(
        &self,
        delivery_info: impl Into<DeliveryInfo>,
        state: impl Into<TerminalDeliveryState>,
    ) -> Result<(), DispositionError> {
        let state: TerminalDeliveryState = state.into();
        self.inner.dispose(delivery_info, None, state.into()).await
    }

    /// Dispose the message by sending one or more disposition(s) with the provided state
    ///
    /// Only deliveries that are found in the local unsettled map will be included in the disposition frame(s).
    pub async fn dispose_all(
        &self,
        deliveries: impl IntoIterator<Item = impl Into<DeliveryInfo>>,
        state: impl Into<TerminalDeliveryState>,
    ) -> Result<(), DispositionError> {
        let state: TerminalDeliveryState = state.into();
        let delivery_infos = deliveries.into_iter().map(|d| d.into()).collect();
        self.inner
            .dispose_all(delivery_infos, None, state.into())
            .await
    }

    /// Returns a [`ReceiverDisposer`] that can settle deliveries received by this
    /// receiver without holding the receiver itself.
    ///
    /// The disposer shares the underlying AMQP link state (unsettled map, flow state,
    /// outgoing channel) with the receiver. Settlement calls on the disposer are
    /// lock-free with respect to `recv()` — concurrent disposal and receive are safe.
    pub fn disposer(&self) -> ReceiverDisposer {
        ReceiverDisposer {
            outgoing: self.inner.outgoing.clone(),
            unsettled: self.inner.link.unsettled.clone(),
            rcv_settle_mode: self.inner.link.rcv_settle_mode.clone(),
            flow_state: self.inner.link.flow_state.clone(),
            output_handle: self.inner.link.output_handle.clone(),
            processed: Arc::clone(&self.inner.processed),
            credit_mode: self.inner.credit_mode.clone(),
            session_stop_reason: self.inner.link.session_stop_reason.clone(),
        }
    }
}

/// A lightweight helper that can settle deliveries previously received on a [`Receiver`].
///
/// Obtained via [`Receiver::disposer()`]. Settlement operations on this type do NOT
/// acquire the mutex guarding `recv()`, so calling them concurrently with an ongoing
/// receive is fully safe and will not block.
///
/// Cloning a `ReceiverDisposer` is cheap: all internal state is reference-counted.
#[derive(Clone, Debug)]
pub struct ReceiverDisposer {
    outgoing: crate::session::transfer_queue::Sender,
    unsettled: ArcReceiverUnsettledMap,
    rcv_settle_mode: ReceiverSettleMode,
    flow_state: ReceiverFlowState,
    output_handle: Option<crate::endpoint::OutputAttachment>,
    processed: Arc<AtomicU32>,
    credit_mode: CreditMode,
    session_stop_reason: Arc<OnceLock<SessionStopReason>>,
}

impl ReceiverDisposer {
    /// Accept the delivery (Accepted disposition).
    pub async fn accept(
        &self,
        delivery_info: impl Into<DeliveryInfo>,
    ) -> Result<(), DispositionError> {
        let info = delivery_info.into();
        self.dispose(info, DeliveryState::Accepted(Accepted {}))
            .await
    }

    /// Release the delivery (Released disposition — equivalent to `abandon`).
    pub async fn release(
        &self,
        delivery_info: impl Into<DeliveryInfo>,
    ) -> Result<(), DispositionError> {
        let info = delivery_info.into();
        self.dispose(info, DeliveryState::Released(Released {}))
            .await
    }

    async fn dispose(
        &self,
        delivery_info: DeliveryInfo,
        state: DeliveryState,
    ) -> Result<(), DispositionError> {
        let settled = match delivery_info
            .rcv_settle_mode
            .as_ref()
            .unwrap_or(&self.rcv_settle_mode)
        {
            ReceiverSettleMode::First => true,
            ReceiverSettleMode::Second => false,
        };

        let unsettled_state = {
            let mut lock = self.unsettled.write();
            let Some(map) = lock.as_mut() else {
                return Ok(());
            };
            let Some(entry) = map.get(&delivery_info.delivery_tag) else {
                return Ok(());
            };
            if !entry
                .info
                .as_ref()
                .is_some_and(|current| current.same_delivery(&delivery_info))
            {
                return Err(DispositionError::IllegalState);
            }
            if settled {
                map.swap_remove(&delivery_info.delivery_tag)
            } else {
                let entry = map.get_mut(&delivery_info.delivery_tag).unwrap();
                entry.local_state(state.clone());
                Some(entry.clone())
            }
        };

        if unsettled_state.is_some() {
            let disposition = Disposition {
                role: Role::Receiver,
                first: delivery_info.delivery_id,
                last: None,
                settled,
                state: Some(state),
                batchable: false,
            };
            self.outgoing
                .send(LinkFrame::Disposition(disposition))
                .await
                .map_err(|_| match self.session_stop_reason.get() {
                    Some(reason) => DispositionError::SessionStopped(reason.clone()),
                    None => DispositionError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                })?;
        }

        let prev = self.processed.fetch_add(1, Ordering::Release);
        self.refresh_credit_if_needed(prev + 1).await
    }

    async fn refresh_credit_if_needed(&self, processed: u32) -> Result<(), DispositionError> {
        if let CreditMode::Auto(max_credit) = self.credit_mode {
            if processed >= max_credit / 2 {
                self.processed.store(0, Ordering::Release);
                let attachment = self
                    .output_handle
                    .clone()
                    .ok_or(DispositionError::IllegalState)?;
                let handle: Handle = attachment.handle.into();
                let delivery_count = {
                    let mut guard = self.flow_state.lock.write();
                    guard.link_credit = max_credit;
                    guard.drain = false;
                    guard.delivery_count
                };
                let flow = LinkFlow {
                    handle,
                    delivery_count: Some(delivery_count),
                    link_credit: Some(max_credit),
                    available: None,
                    drain: false,
                    echo: false,
                    properties: None,
                };
                self.outgoing
                    .send(LinkFrame::Flow(flow, attachment.id))
                    .await
                    .map_err(|_| match self.session_stop_reason.get() {
                        Some(reason) => DispositionError::SessionStopped(reason.clone()),
                        None => DispositionError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                    })?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
/// Terminal delivery states that can be used by the receiver to dispose of a delivery
pub enum TerminalDeliveryState {
    /// 3.4.2 Accepted
    Accepted(Accepted),

    /// 3.4.3 Rejected
    Rejected(Rejected),

    /// 3.4.4 Released
    Released(Released),

    /// 3.4.5 Modified
    Modified(Modified),
}

impl From<TerminalDeliveryState> for DeliveryState {
    fn from(value: TerminalDeliveryState) -> Self {
        match value {
            TerminalDeliveryState::Accepted(val) => Self::Accepted(val),
            TerminalDeliveryState::Rejected(val) => Self::Rejected(val),
            TerminalDeliveryState::Released(val) => Self::Released(val),
            TerminalDeliveryState::Modified(val) => Self::Modified(val),
        }
    }
}

impl TryFrom<DeliveryState> for TerminalDeliveryState {
    type Error = DeliveryState;

    fn try_from(value: DeliveryState) -> Result<Self, Self::Error> {
        match value {
            DeliveryState::Accepted(val) => Ok(Self::Accepted(val)),
            DeliveryState::Rejected(val) => Ok(Self::Rejected(val)),
            DeliveryState::Released(val) => Ok(Self::Released(val)),
            DeliveryState::Modified(val) => Ok(Self::Modified(val)),
            _ => Err(value),
        }
    }
}

impl From<Accepted> for TerminalDeliveryState {
    fn from(value: Accepted) -> Self {
        Self::Accepted(value)
    }
}

impl From<Rejected> for TerminalDeliveryState {
    fn from(value: Rejected) -> Self {
        Self::Rejected(value)
    }
}

impl From<Released> for TerminalDeliveryState {
    fn from(value: Released) -> Self {
        Self::Released(value)
    }
}

impl From<Modified> for TerminalDeliveryState {
    fn from(value: Modified) -> Self {
        Self::Modified(value)
    }
}

/// A read-only view of the receiver engine's unsettled delivery state.
#[derive(Debug)]
pub struct ReceiverSettlementView {
    unsettled: ArcReceiverUnsettledMap,
    changed: tokio::sync::watch::Receiver<u64>,
}
impl ReceiverSettlementView {
    /// Whether this exact tag remains unsettled in the engine.
    pub fn contains(&self, tag: &DeliveryTag) -> bool {
        self.unsettled
            .read()
            .as_ref()
            .is_some_and(|map| map.contains_key(tag))
    }
    /// The current session delivery identity, only after a complete message or
    /// its state-only resumed Transfer has been received. Attach alone cannot
    /// supply a delivery-id; an identity from the prior session is never returned.
    pub fn delivery_info(&self, tag: &DeliveryTag) -> Option<DeliveryInfo> {
        self.unsettled
            .read()
            .as_ref()?
            .get(tag)
            .filter(|entry| entry.received)?
            .info
            .clone()
    }
    /// Current identity bound by an explicit resumed Transfer, never an ordinary tag reuse.
    pub fn resumed_delivery_info(&self, tag: &DeliveryTag) -> Option<DeliveryInfo> {
        self.unsettled
            .read()
            .as_ref()?
            .get(tag)
            .filter(|entry| entry.received && entry.resumed)?
            .info
            .clone()
    }
    /// Whether this exact physical delivery binding is still unsettled.
    pub fn contains_delivery(&self, info: &DeliveryInfo) -> bool {
        self.delivery_info(info.delivery_tag())
            .is_some_and(|current| current.same_delivery(info))
    }
    /// Wait for any map change. Updates coalesce and never form a frame queue.
    pub async fn changed(&mut self) -> Result<(), tokio::sync::watch::error::RecvError> {
        self.changed.changed().await
    }
}

#[derive(Debug)]
pub(crate) struct ReceiverInner<L: endpoint::ReceiverLink> {
    pub(crate) link: L,
    pub(crate) buffer_size: usize,
    pub(crate) credit_mode: CreditMode,
    pub(crate) processed: Arc<AtomicU32>, // SequenceNo,
    pub(crate) auto_accept: bool,

    // Control sender to the session
    pub(crate) session: mpsc::Sender<SessionControl>,

    // Outgoing mpsc channel to send the Link Frames
    pub(crate) outgoing: crate::session::transfer_queue::Sender,
    pub(crate) incoming: mpsc::Receiver<LinkFrame>,

    // Wrap in a box to avoid clippy warning large_enum_variant on link acceptor's output
    pub(crate) incomplete_transfer: Option<Box<IncompleteTransfer>>,
    pub(crate) incoming_recovery: super::incoming_recovery::IncomingRecovery,
    pub(crate) receive_budget: Arc<super::receive_budget::ReceiveBudget>,

    /// A close this receiver started itself, recorded before its Detach is
    /// sent, so a `recv` dropped mid-close resumes it and the peer's answer
    /// returns its cause.
    pub(crate) self_close: Option<SelfClose>,
}

/// A close the receiver started itself: the error its closing Detach
/// carries, and the error `recv` returns once the peer answers it.
#[derive(Debug)]
pub(crate) struct SelfClose {
    error: definitions::Error,
    cause: RecvError,
}

impl<L: endpoint::ReceiverLink> Drop for ReceiverInner<L> {
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
        while let Ok(frame) = self.incoming.try_recv() {
            if let LinkFrame::Detach(detach, _) = frame {
                remote_detach_received = true;
                // If the state change fails, ignore it: the engine is being
                // dropped anyway.
                if self.link.apply_remote_detach_outcome(detach).is_err() {
                    #[cfg(feature = "tracing")]
                    tracing::debug!("failed to apply remote detach outcome on receiver drop");
                    #[cfg(feature = "log")]
                    log::debug!("failed to apply remote detach outcome on receiver drop");
                }
            }
            // Any other frame (e.g. a partially received transfer or an attach
            // response left behind by an interrupted reattach) is superseded
            // by the drop.
        }

        if !remote_detach_received {
            if let Some(attachment) = self.link.output_handle_mut().take() {
                let detach = Detach {
                    handle: attachment.handle.into(),
                    closed: true,
                    error: None,
                };
                if let Err(_error) = self
                    .outgoing
                    .try_send(LinkFrame::Detach(detach, attachment.id))
                {
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
                        tracing::warn!(reason, "Failed to enqueue Detach frame on receiver drop");
                        #[cfg(feature = "log")]
                        log::warn!("Failed to enqueue Detach frame on receiver drop: {reason}");
                    }
                }
            }
        }
    }
}

impl<L> LinkEndpointInner for ReceiverInner<L>
where
    L: endpoint::ReceiverLink<AttachError = ReceiverAttachError, DetachError = DetachError>
        + LinkExt<FlowState = ReceiverFlowState, Unsettled = ArcReceiverUnsettledMap>
        + LinkAttach<AttachExchange = ReceiverAttachExchange>
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
        LinkRelay::Receiver {
            tx,
            output_handle: (),
            flow_state: self.link.flow_state().clone(),
            unsettled: self.link.unsettled().clone(),
            receiver_settle_mode: self.link.rcv_settle_mode().clone(),
            // This only controls whether a multi-transfer delivery id
            // will be added to sessions map
            more: false,
            current_tag: None,
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
    ) -> Result<ReceiverAttachExchange, <Self::Link as LinkAttach>::AttachError> {
        self.incoming_recovery.reset();
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

    /// # Cancel safety
    async fn send_detach(
        &mut self,
        closed: bool,
        error: Option<definitions::Error>,
    ) -> Result<(), <Self::Link as LinkDetach>::DetachError> {
        self.link.send_detach(&self.outgoing, closed, error).await // cancel safe
    }
}

impl<L> LinkEndpointInnerReattach for ReceiverInner<L>
where
    L: endpoint::ReceiverLink<AttachError = ReceiverAttachError, DetachError = DetachError>
        + LinkExt<FlowState = ReceiverFlowState, Unsettled = ArcReceiverUnsettledMap>
        + LinkAttach<AttachExchange = ReceiverAttachExchange>
        + Send
        + Sync,
{
    fn handle_reattach_outcome(
        &mut self,
        outcome: ReceiverAttachExchange,
    ) -> Result<&mut Self, L::AttachError> {
        match outcome {
            ReceiverAttachExchange::Complete => Ok(self),
            //  Re-attach should have None valued unsettled, so this should be invalid
            ReceiverAttachExchange::IncompleteUnsettled | ReceiverAttachExchange::Resume => {
                Err(ReceiverAttachError::IllegalState)
            }
        }
    }
}

impl<L> ReceiverInner<L>
where
    L: endpoint::ReceiverLink<
            FlowError = IllegalLinkStateError,
            TransferError = ReceiverTransferError,
            DispositionError = IllegalLinkStateError,
            AttachError = ReceiverAttachError,
            DetachError = DetachError,
        > + LinkExt<FlowState = ReceiverFlowState, Unsettled = ArcReceiverUnsettledMap>
        + LinkAttach<AttachExchange = ReceiverAttachExchange>
        + Send
        + Sync,
{
    pub(crate) async fn recv<T>(&mut self) -> Result<Delivery<T>, RecvError>
    where
        for<'de> T: FromBody<'de> + Send,
    {
        match self.recv_observed(&mut NoopTransferObserver).await? {
            ReceiverReadStep::Delivery(delivery) => Ok(delivery),
            ReceiverReadStep::SelfClosing => Err(self.finish_self_close().await),
        }
    }

    async fn recv_observed<T, O>(
        &mut self,
        observer: &mut O,
    ) -> Result<ReceiverReadStep<T>, RecvError>
    where
        for<'de> T: FromBody<'de> + Send,
        O: ReceiverTransferObserver + ?Sized,
    {
        loop {
            match self.recv_inner(ReceiveContext::Live, observer).await {
                Ok(Some(delivery)) => return Ok(ReceiverReadStep::Delivery(delivery)),
                Ok(None) => {}
                Err(ReceiveFault::Error(error)) => return Err(error),
                Err(ReceiveFault::SelfClosing) => return Ok(ReceiverReadStep::SelfClosing),
            }
        }
    }

    async fn recv_inner<T, O>(
        &mut self,
        context: ReceiveContext,
        observer: &mut O,
    ) -> Result<Option<Delivery<T>>, ReceiveFault>
    where
        for<'de> T: FromBody<'de> + Send,
        O: ReceiverTransferObserver + ?Sized,
    {
        if self.self_close.is_some() {
            return Err(ReceiveFault::SelfClosing);
        }
        let frame = self.incoming.recv().await.ok_or_else(|| self.stopped())?;
        match frame {
            LinkFrame::Transfer {
                performative,
                payload,
                window_slot,
                ..
            } => {
                self.receive_transfer(performative, payload, window_slot, context, observer)
                    .await
            }
            LinkFrame::Detach(detach, _) => {
                let closed = detach.closed;
                let error = match self.link.apply_remote_detach_outcome(detach) {
                    Err(error) => RecvError::from(error),
                    Ok(()) if closed => RecvError::from(LinkStateError::RemoteClosed),
                    Ok(()) => RecvError::from(LinkStateError::RemoteDetached),
                };
                Err(error.into())
            }
            LinkFrame::Attach(_) => Err(RecvError::from(LinkStateError::IllegalState).into()),
            LinkFrame::Flow(..) | LinkFrame::Disposition(_) => unreachable!("handled by LinkRelay"),
            #[cfg(feature = "transaction")]
            LinkFrame::Acquisition(_) => {
                let cause = RecvError::TransactionalAcquisitionIsNotImeplemented;
                if matches!(context, ReceiveContext::Stopped) {
                    return Err(cause.into());
                }
                Err(self.close_itself(
                    definitions::Error::new(
                        AmqpError::NotImplemented,
                        "Transactional acquisition is not implemented".to_string(),
                        None,
                    ),
                    cause,
                ))
            }
        }
    }

    async fn receive_transfer<T, O>(
        &mut self,
        performative: Transfer,
        payload: Payload,
        window_slot: Option<crate::session::receive_window::Slot>,
        context: ReceiveContext,
        observer: &mut O,
    ) -> Result<Option<Delivery<T>>, ReceiveFault>
    where
        for<'de> T: FromBody<'de> + Send,
        O: ReceiverTransferObserver + ?Sized,
    {
        let mut commit = TransferCommit { committed: false };
        let result = self
            .on_incoming_transfer(performative, payload, context, observer, &mut commit)
            .await;
        if result.is_err() && !commit.committed {
            commit.commit(ReceiverTransferEffect::Failed, observer);
        }
        drop(window_slot);
        result
    }

    /// The one place a Detach the link receives is told apart. One that
    /// arrives while this receiver's own closing Detach is out answers it:
    /// the close completes and the recorded cause is returned. Any other is
    /// a Detach the peer sent on its own, which the relay already answered;
    /// the link records the outcome here. Reported as-is even when the
    /// session is stopping: a stop without a detach shows up as the channel
    /// closing.
    async fn on_incoming_detach(&mut self, detach: Detach) -> RecvError {
        use super::state::LinkState;
        if let Some(close) = self.self_close.take() {
            if matches!(self.link.local_state(), LinkState::CloseSent) {
                let completed = if detach.closed {
                    self.link.on_detach_reply(detach)
                } else {
                    // The peer suspended while we closed: reattach and close
                    // (AMQP 1.0 §2.6.6). The close completes.
                    let _ = self.link.apply_remote_detach_outcome(detach);
                    super::shared_inner::reattach_then_close(self).await
                };
                return match completed {
                    Err(DetachError::SessionStopped(reason)) => {
                        LinkStateError::SessionStopped(reason).into()
                    }
                    _ => close.cause,
                };
            }
            // Its Detach never went out: the peer ended the link first.
        }
        let closed = detach.closed;
        match self.link.apply_remote_detach_outcome(detach) {
            Err(error) => error.into(),
            Ok(()) if closed => LinkStateError::RemoteClosed.into(),
            Ok(()) => LinkStateError::RemoteDetached.into(),
        }
    }

    /// Closes the link because of `cause`, sending `error` in its closing
    /// Detach. The cause is recorded before the Detach is sent, so a `recv`
    /// dropped at any point resumes the close and returns the cause.
    fn close_itself(&mut self, error: definitions::Error, cause: RecvError) -> ReceiveFault {
        self.self_close = Some(SelfClose { error, cause });
        ReceiveFault::SelfClosing
    }

    /// Sends the closing Detach if it is not out yet, then reads the link's
    /// frames until the one Detach that ends the close. Frames of the closing
    /// link are superseded by it.
    ///
    /// # Cancel safety
    ///
    /// Cancel safe: the close is recorded in `self_close` and the link state,
    /// and every `.await` is on a `tokio::sync::mpsc` channel.
    async fn finish_self_close(&mut self) -> RecvError {
        use super::state::LinkState;
        loop {
            let Some(close) = &self.self_close else {
                return LinkStateError::IllegalState.into();
            };
            if matches!(
                self.link.local_state(),
                LinkState::Unattached
                    | LinkState::AttachSent
                    | LinkState::IncompleteAttachSent
                    | LinkState::IncompleteAttachReceived
                    | LinkState::IncompleteAttachExchanged
                    | LinkState::AttachReceived
                    | LinkState::Attached
            ) {
                let error = close.error.clone();
                if let Err(error) = self.send_detach(true, Some(error)).await {
                    self.self_close = None;
                    return error.into();
                }
            }
            match self.incoming.recv().await {
                Some(LinkFrame::Detach(detach, _)) => return self.on_incoming_detach(detach).await,
                Some(_) => continue,
                None => return self.stopped(),
            }
        }
    }

    /// Why the link's channel closed: its session or connection stopped.
    fn stopped(&self) -> RecvError {
        match self.link().session_stop_reason().get() {
            Some(reason) => {
                RecvError::LinkStateError(LinkStateError::SessionStopped(reason.clone()))
            }
            // defensive: no stop reason recorded; failure is link-local
            None => RecvError::LinkStateError(LinkStateError::IllegalState),
        }
    }

    /// Close the link because a transfer violated the multi-frame delivery
    /// rules. AMQP 1.0 §2.6.5 requires the endpoint to be detached with error
    /// information and then destroyed.
    fn close_on_malformed_delivery(
        &mut self,
        error: ReceiverTransferError,
        context: ReceiveContext,
    ) -> Result<(), ReceiveFault> {
        if matches!(context, ReceiveContext::Stopped) {
            return Err(RecvError::from(error).into());
        }
        let detach_error = definitions::Error::new(
            definitions::AmqpError::NotAllowed,
            Some(error.to_string()),
            None,
        );
        Err(self.close_itself(detach_error, error.into()))
    }

    fn on_transfer_state<O: ReceiverTransferObserver + ?Sized>(
        &mut self,
        delivery_tag: &Option<DeliveryTag>,
        settled: Option<bool>,
        state: DeliveryState,
        context: ReceiveContext,
        _observer: &mut O,
        _commit: &mut TransferCommit,
    ) -> Result<(), ReceiveFault> {
        let effective_tag = delivery_tag.clone().or_else(|| {
            self.incomplete_transfer
                .as_ref()
                .and_then(|partial| partial.performative.delivery_tag.clone())
        });
        if effective_tag.is_none() {
            return self
                .close_on_malformed_delivery(ReceiverTransferError::DeliveryTagIsNone, context);
        }
        if let Some(partial) = &mut self.incomplete_transfer {
            if partial.performative.delivery_tag == effective_tag {
                if let DeliveryState::Received(received) = &state {
                    partial
                        .keep_buffer_till_section_number_and_offset(
                            received.section_number,
                            received.section_offset,
                        )
                        .map_err(RecvError::from)?;
                }
            }
        }
        self.link
            .on_transfer_state(&effective_tag, settled, state)
            .map_err(|error| ReceiveFault::from(RecvError::from(error)))
    }

    fn on_incomplete_transfer<O: ReceiverTransferObserver + ?Sized>(
        &mut self,
        transfer: Transfer,
        payload: Payload,
        context: ReceiveContext,
        observer: &mut O,
        commit: &mut TransferCommit,
    ) -> Result<(), ReceiveFault> {
        // Partial transfer of the delivery
        match &mut self.incomplete_transfer {
            Some(incomplete) => {
                if let Err(error) = incomplete.or_assign(transfer) {
                    return self.close_on_malformed_delivery(error, context);
                }
                incomplete.append(payload).map_err(RecvError::from)?;

                if let Some(delivery_tag) = incomplete.performative.delivery_tag.clone() {
                    // Update unsettled map in the link
                    self.link.on_incomplete_transfer(
                        delivery_tag,
                        incomplete.section_number.unwrap_or(0),
                        incomplete.section_offset,
                    );
                }
            }
            None => {
                let incomplete =
                    match IncompleteTransfer::new(transfer, payload, &self.receive_budget) {
                        Ok(partial) => partial,
                        Err(
                            error @ (ReceiverTransferError::DeliveryIdIsNone
                            | ReceiverTransferError::DeliveryTagIsNone),
                        ) => return self.close_on_malformed_delivery(error, context),
                        Err(error) => return Err(RecvError::from(error).into()),
                    };
                if let Some(delivery_tag) = incomplete.performative.delivery_tag.clone() {
                    // Update unsettled map in the link
                    self.link.on_incomplete_transfer(
                        delivery_tag,
                        incomplete.section_number.unwrap_or(0),
                        incomplete.section_offset,
                    );
                }
                self.incomplete_transfer = Some(Box::new(incomplete));
            }
        }

        commit.commit(ReceiverTransferEffect::PayloadIncomplete, observer);
        Ok(())
    }

    /// The bytes of the delivery accumulated so far that belong to the same
    /// delivery as `transfer` (i.e. the buffered chunks of the incomplete
    /// multi-frame delivery, when the transfer is a continuation of it).
    fn accumulated_message_size(&self, transfer: &Transfer) -> u64 {
        match &self.incomplete_transfer {
            Some(incomplete)
                if transfer.delivery_tag.is_none()
                    || incomplete.performative.delivery_tag == transfer.delivery_tag =>
            {
                incomplete.buffer.len() as u64
            }
            _ => 0,
        }
    }

    /// Detach the link because a delivery exceeds the negotiated
    /// max-message-size, as required for a `message-size-exceeded` link error
    /// (AMQP 1.0 §2.6.5). The buffered chunks are discarded and the link can
    /// only be restored by resuming it.
    fn close_on_message_size_exceeded(
        &mut self,
        total_size: u64,
        max_size: u64,
        context: ReceiveContext,
    ) -> ReceiveFault {
        #[cfg(feature = "tracing")]
        tracing::warn!(
            "Detaching link: received message of {total_size} bytes exceeds the max message size of {max_size}"
        );
        #[cfg(feature = "log")]
        log::warn!(
            "Detaching link: received message of {total_size} bytes exceeds the max message size of {max_size}"
        );

        self.incomplete_transfer.take();

        let error = definitions::Error::new(
            LinkError::MessageSizeExceeded,
            Some(format!(
                "received message larger than max size of {max_size}"
            )),
            None,
        );
        let cause = RecvError::MessageSizeExceeded(MessageSizeExceeded {
            size: total_size,
            max_size,
        });
        if matches!(context, ReceiveContext::Stopped) {
            cause.into()
        } else {
            self.close_itself(error, cause)
        }
    }

    /// # Cancel safety
    ///
    /// This is cancel safe because all internal `.await` point(s) are cancel safe
    async fn on_complete_transfer<T, O>(
        &mut self,
        transfer: Transfer,
        payload: Payload,
        context: ReceiveContext,
        observer: &mut O,
        commit: &mut TransferCommit,
    ) -> Result<Option<Delivery<T>>, ReceiveFault>
    where
        for<'de> T: FromBody<'de> + Send,
        O: ReceiverTransferObserver + ?Sized,
    {
        // Enforce the negotiated max-message-size of the link before
        // assembling the delivery
        if let Some(max_size) = self.link.max_message_size() {
            let total = self.accumulated_message_size(&transfer) + payload.len() as u64;
            if total > max_size {
                return Err(self.close_on_message_size_exceeded(total, max_size, context));
            }
        }

        let delivery = match self.incomplete_transfer.take() {
            Some(mut incomplete) => {
                if let Err(error) = incomplete.or_assign(transfer) {
                    return self
                        .close_on_malformed_delivery(error, context)
                        .map(|()| None);
                }
                incomplete.append(payload).map_err(RecvError::from)?; // This also computes the section number and offset incrementally

                self.link
                    .on_complete_transfer(
                        incomplete.performative,
                        incomplete.buffer,
                        incomplete.section_number.unwrap_or(0),
                        incomplete.section_offset,
                    )
                    .map_err(RecvError::from)?
            }
            None => {
                let (section_number, section_offset) =
                    count_number_of_sections_and_offset(&payload).map_err(RecvError::from)?;
                self.link
                    .on_complete_transfer(transfer, &payload, section_number, section_offset)
                    .map_err(RecvError::from)?
            }
        };

        commit.commit(
            ReceiverTransferEffect::Delivered {
                info: DeliveryInfo::from(&delivery),
            },
            observer,
        );

        // Auto accept the message and leave settled to be determined based on rcv_settle_mode
        if self.auto_accept && matches!(context, ReceiveContext::Live) {
            self.dispose(&delivery, None, Accepted {}.into())
                .await
                .map_err(RecvError::from)?; // cancel safe
        }

        Ok(Some(delivery))
    }

    /// # Cancel safety
    ///
    /// This is cancel safe because all internal `.await` point(s) are cancel safe
    #[inline]
    async fn on_incoming_transfer<T, O>(
        &mut self,
        mut transfer: Transfer,
        payload: Payload,
        context: ReceiveContext,
        observer: &mut O,
        commit: &mut TransferCommit,
    ) -> Result<Option<Delivery<T>>, ReceiveFault>
    where
        for<'de> T: FromBody<'de> + Send,
        O: ReceiverTransferObserver + ?Sized,
    {
        use super::incoming_recovery::Kind;
        let continuation = match self.incoming_recovery.continuation(&mut transfer) {
            Ok(kind) => kind,
            Err(RecvError::InconsistentFieldInMultiFrameDelivery) => {
                return self
                    .close_on_malformed_delivery(
                        ReceiverTransferError::InconsistentFieldInMultiFrameDelivery,
                        context,
                    )
                    .map(|()| None)
            }
            Err(error) => return Err(error.into()),
        };
        if transfer.aborted {
            if let Some(partial) = &self.incomplete_transfer {
                if transfer
                    .delivery_tag
                    .as_ref()
                    .is_some_and(|tag| partial.performative.delivery_tag.as_ref() != Some(tag))
                    || transfer
                        .delivery_id
                        .zip(partial.performative.delivery_id)
                        .is_some_and(|(a, b)| a != b)
                {
                    return self
                        .close_on_malformed_delivery(
                            ReceiverTransferError::InconsistentFieldInMultiFrameDelivery,
                            context,
                        )
                        .map(|()| None);
                }
            }
            let tag = transfer.delivery_tag.clone().or_else(|| {
                self.incomplete_transfer
                    .as_ref()
                    .and_then(|partial| partial.performative.delivery_tag.clone())
            });
            self.incomplete_transfer.take();
            if let Some(tag) = tag {
                if let Some(map) = self.link.unsettled().write().as_mut() {
                    map.swap_remove(&tag);
                }
            }
            commit.commit(ReceiverTransferEffect::Aborted, observer);
            return Ok(None);
        }
        let mut kind = continuation;
        if continuation.is_none() && transfer.resume {
            if transfer.delivery_tag.is_none() {
                return self
                    .close_on_malformed_delivery(ReceiverTransferError::DeliveryTagIsNone, context)
                    .map(|()| None);
            }
            if transfer.delivery_id.is_none() {
                return self
                    .close_on_malformed_delivery(ReceiverTransferError::DeliveryIdIsNone, context)
                    .map(|()| None);
            }
            let tag = transfer.delivery_tag.as_ref().unwrap();
            let known = self
                .link
                .unsettled()
                .read()
                .as_ref()
                .and_then(|m| m.get(tag))
                .cloned();
            kind = Some(match known {
                None => Kind::Ignore,
                Some(entry) if entry.received => Kind::StateOnly,
                Some(_) => Kind::Payload,
            });
            if kind == Some(Kind::Payload) {
                let (number, offset) = match &transfer.state {
                    Some(DeliveryState::Received(r)) => (r.section_number, r.section_offset),
                    _ => (0, 0),
                };
                if let Some(partial) = self.incomplete_transfer.as_mut() {
                    if partial.performative.delivery_tag.as_ref() != Some(tag) {
                        return self
                            .close_on_malformed_delivery(
                                ReceiverTransferError::InconsistentFieldInMultiFrameDelivery,
                                context,
                            )
                            .map(|()| None);
                    }
                    partial
                        .keep_buffer_till_section_number_and_offset(number, offset)
                        .map_err(RecvError::from)?;
                    partial.performative.delivery_id = transfer.delivery_id;
                } else if number != 0 || offset != 0 {
                    return Err(
                        RecvError::InvalidMessageEncoding(serde_amqp::Error::InvalidValue).into(),
                    );
                }
            }
            self.incoming_recovery
                .start(&transfer, kind.expect("resume classification"));
        }
        if kind == Some(Kind::Ignore) {
            commit.commit(
                ReceiverTransferEffect::Ignored {
                    more: transfer.more,
                },
                observer,
            );
            return Ok(None);
        }
        if kind == Some(Kind::StateOnly) {
            let mut info = None;
            if continuation.is_none() {
                if let Some(entry) = self.link.unsettled().write().as_mut().and_then(|map| {
                    transfer
                        .delivery_tag
                        .as_ref()
                        .and_then(|tag| map.get_mut(tag))
                }) {
                    entry.resumed = true;
                    entry.info = Some(DeliveryInfo {
                        identity: std::sync::Arc::new(()),
                        delivery_id: transfer.delivery_id.ok_or(RecvError::DeliveryIdIsNone)?,
                        delivery_tag: transfer
                            .delivery_tag
                            .clone()
                            .ok_or(RecvError::DeliveryTagIsNone)?,
                        rcv_settle_mode: transfer.rcv_settle_mode.clone(),
                        _sealed: crate::util::Sealed {},
                    });
                    info = entry.info.clone();
                }
            }
            if transfer.aborted || transfer.settled == Some(true) {
                if let Some(map) = self.link.unsettled().write().as_mut() {
                    if let Some(tag) = &transfer.delivery_tag {
                        map.swap_remove(tag);
                    }
                }
            } else if let Some(state) = transfer.state {
                self.on_transfer_state(
                    &transfer.delivery_tag,
                    transfer.settled,
                    state,
                    context,
                    observer,
                    commit,
                )?;
            }
            let retained = transfer.delivery_tag.as_ref().is_some_and(|tag| {
                self.link
                    .unsettled()
                    .read()
                    .as_ref()
                    .is_some_and(|map| map.contains_key(tag))
            });
            commit.commit(
                ReceiverTransferEffect::StateOnly {
                    info,
                    retained,
                    more: transfer.more,
                },
                observer,
            );
            return Ok(None);
        }
        if self.incomplete_transfer.is_none() {
            if transfer.delivery_id.is_none() {
                return self
                    .close_on_malformed_delivery(ReceiverTransferError::DeliveryIdIsNone, context)
                    .map(|()| None);
            }
            if transfer.delivery_tag.is_none() {
                return self
                    .close_on_malformed_delivery(ReceiverTransferError::DeliveryTagIsNone, context)
                    .map(|()| None);
            }
            if transfer.message_format.is_none() {
                return Err(
                    RecvError::InvalidMessageEncoding(serde_amqp::Error::InvalidValue).into(),
                );
            }
        } else if transfer.delivery_tag.is_none() {
            transfer.delivery_tag = self
                .incomplete_transfer
                .as_ref()
                .and_then(|p| p.performative.delivery_tag.clone());
        }
        if let Some(partial) = self.incomplete_transfer.as_mut() {
            if let Err(error) = partial.or_assign(transfer.clone()) {
                return self
                    .close_on_malformed_delivery(error, context)
                    .map(|()| None);
            }
        }
        if let Some(state) = transfer.state.clone() {
            // Setting the state
            // on the transfer can be thought of as being equivalent to sending a disposition immediately before
            // the transfer performative, i.e., it is the state of the delivery (not the transfer) that existed at the
            // point the frame was sent.
            self.on_transfer_state(
                &transfer.delivery_tag,
                transfer.settled,
                state,
                context,
                observer,
                commit,
            )?;
        }

        if let Some(max_size) = self.link.max_message_size() {
            let total = self.accumulated_message_size(&transfer) + payload.len() as u64;
            if total > max_size {
                return Err(self.close_on_message_size_exceeded(total, max_size, context));
            }
        }
        if transfer
            .delivery_tag
            .as_ref()
            .is_some_and(|tag| tag.len() > 32)
        {
            return Err(RecvError::InvalidMessageEncoding(serde_amqp::Error::InvalidValue).into());
        }

        if transfer.more {
            // Partial transfer of the delivery
            // There is only ONE incomplet transfer locally, so the partial transfer must belong to the
            // same delivery
            self.on_incomplete_transfer(transfer, payload, context, observer, commit)?;
            // Partial delivery doesn't yield a complete message
            Ok(None)
        } else {
            // Final transfer of the delivery
            self.on_complete_transfer(transfer, payload, context, observer, commit)
                .await // cancel safe
        }
    }

    /// Set the link credit. This will stop draining if the link is in a draining cycle
    ///
    /// # Cancel safety
    ///
    /// This is cancel safe as internanlly it only `.await` on sending over `tokio::mpsc::Sender`
    #[inline]
    pub async fn set_credit(&mut self, credit: SequenceNo) -> Result<(), IllegalLinkStateError> {
        self.processed.store(0, Ordering::Release);
        if let CreditMode::Auto(_) = self.credit_mode {
            self.credit_mode = CreditMode::Auto(credit)
        }

        self.link
            .send_flow(&self.outgoing, Some(credit), Some(false), false, false)
            .await // cancel safe
    }

    /// This is cancel safe because all internal `.await` points are cancel safe
    #[inline]
    pub(crate) async fn dispose(
        &self,
        delivery_info: impl Into<DeliveryInfo>,
        settled: Option<bool>,
        state: DeliveryState,
    ) -> Result<(), DispositionError> {
        let delivery_info = delivery_info.into();
        self.link
            .dispose(&self.outgoing, delivery_info, settled, state, false)
            .await?; // cancel safe

        let prev = self.processed.fetch_add(1, Ordering::Release);
        self.update_credit_if_auto(prev + 1).await?; // cancel safe
        Ok(())
    }

    /// This is cancel safe because all internal `.await` points are cancel safe
    #[inline]
    pub(crate) async fn dispose_all(
        &self,
        delivery_infos: Vec<DeliveryInfo>,
        settled: Option<bool>,
        state: DeliveryState,
    ) -> Result<(), DispositionError> {
        let total = delivery_infos.len() as u32;
        self.link
            .dispose_all(&self.outgoing, delivery_infos, settled, state, false)
            .await?; // cancel safe

        let prev = self.processed.fetch_add(total, Ordering::Release);
        self.update_credit_if_auto(prev + total).await?; // cancel safe
        Ok(())
    }

    /// This is cancel safe because it only `.await` on a cancel safe future
    #[inline]
    async fn update_credit_if_auto(&self, processed: u32) -> Result<(), DispositionError> {
        if let CreditMode::Auto(max_credit) = self.credit_mode {
            if processed >= max_credit / 2 {
                // Reset link credit
                self.processed.store(0, Ordering::Release);
                self.link
                    .send_flow(&self.outgoing, Some(max_credit), Some(false), false, false)
                    .await?; // cancel safe
            }
        }
        Ok(())
    }

    /// Drain the link.
    ///
    /// This will send a `Flow` performative with the `drain` field set to true.
    /// Setting the credit will set the `drain` field to false and stop draining
    #[inline]
    pub async fn drain(&mut self) -> Result<(), DispositionError> {
        self.processed.store(0, Ordering::Release);

        // Return if already draining
        if self.link.flow_state().drain() {
            return Ok(());
        }

        // Send a flow with Drain set to true
        self.link
            .send_flow(&self.outgoing, None, Some(true), false, false)
            .await
    }

    /// Send the properties of the link via a Flow frame
    #[inline]
    pub async fn send_properties(&self) -> Result<(), FlowError> {
        self.link
            .send_flow(&self.outgoing, None, None, false, true)
            .await
    }
}

impl ReceiverInner<ReceiverLink<Target>> {
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

    pub(crate) async fn resume_incoming_attach(
        &mut self,
        mut initial_remote_attach: Option<Attach>,
        is_reattaching: bool,
    ) -> Result<ReceiverAttachExchange, ReceiverResumeErrorKind> {
        if let Some(map) = self.link.unsettled.write().as_mut() {
            for entry in map.values_mut() {
                entry.info = None;
                entry.resumed = false;
            }
        }
        self.reallocate_output_handle().await?;

        let exchange = match initial_remote_attach.take() {
            Some(remote_attach) => {
                self.link
                    .send_attach(&self.outgoing, is_reattaching)
                    .await?;
                self.link.on_incoming_attach(remote_attach)?
            }
            None => self.exchange_attach(is_reattaching).await?,
        };
        #[cfg(feature = "tracing")]
        tracing::debug!(?exchange);
        #[cfg(feature = "log")]
        log::debug!("exchange = {:?}", exchange);

        self.discard_settled_partial();
        let credit = self.link.flow_state.link_credit();
        self.set_credit(credit).await?;

        Ok(exchange)
    }

    fn discard_settled_partial(&mut self) {
        // Attach reconciliation can settle the old delivery without resuming
        // its payload. Its prefix must not be prepended to a new delivery.
        let retained = self.incomplete_transfer.as_ref().is_some_and(|partial| {
            partial
                .performative
                .delivery_tag
                .as_ref()
                .is_some_and(|tag| {
                    self.link
                        .unsettled
                        .read()
                        .as_ref()
                        .is_some_and(|map| map.contains_key(tag))
                })
        });
        if !retained {
            self.incomplete_transfer = None;
        }
    }
}

/// A detached receiver
///
/// # Example
///
/// Link re-attachment
///
/// ```rust,ignore
/// let detached = receiver.detach().await.unwrap();
/// let resuming_receiver = detached.resume().await.unwrap();
/// ```
#[derive(Debug)]
pub struct DetachedReceiver {
    inner: Box<ReceiverInner<ReceiverLink<Target>>>,
}

macro_rules! try_as_recver {
    ($self:ident, $f:expr) => {
        match $f {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(ReceiverResumeError {
                    detached_recver: $self,
                    kind: error.into(),
                })
            }
        }
    };
}

/// The outcome of a resuming receiver
#[derive(Debug)]
pub enum ResumingReceiver {
    /// The resumption is complete with no unsettled deliveries
    Complete(Receiver),

    /// At least one side sent an Attach with an incomplete unsettled map
    ///
    /// Please note that additional detach-resume may be necessary when there are
    /// unsettled deliveries
    IncompleteUnsettled(Receiver),

    /// The link is attached to resume partial deliveries
    ///
    /// The current implementation only allows one partial delivery
    ///
    /// Please note that additional detach-resume may be necessary when there are
    /// unsettled deliveries
    Resume(Receiver),
}

impl ResumingReceiver {
    /// Returns `Ok(Receiver)` if value is `Complete` otherwise returns `op(self)`
    pub fn complete_or<E>(self, err: E) -> Result<Receiver, E> {
        match self {
            ResumingReceiver::Complete(receiver) => Ok(receiver),
            _ => Err(err),
        }
    }

    /// Returns `Ok(Receiver)` if value is `Complete` otherwise returns `op(self)`
    pub fn complete_or_else<F, E>(self, op: F) -> Result<Receiver, E>
    where
        F: FnOnce(Self) -> E,
    {
        match self {
            ResumingReceiver::Complete(receiver) => Ok(receiver),
            _ => Err((op)(self)),
        }
    }

    /// Consumes the enum and get the receiver
    pub fn into_receiver(self) -> Receiver {
        match self {
            ResumingReceiver::Complete(receiver) => receiver,
            ResumingReceiver::IncompleteUnsettled(receiver) => receiver,
            ResumingReceiver::Resume(receiver) => receiver,
        }
    }

    /// Get a reference to the receiver
    pub fn as_receiver(&self) -> &Receiver {
        self.as_ref()
    }

    /// Get a mutable reference to the receiver
    pub fn as_receiver_mut(&mut self) -> &mut Receiver {
        self.as_mut()
    }
}

impl AsRef<Receiver> for ResumingReceiver {
    fn as_ref(&self) -> &Receiver {
        match self {
            ResumingReceiver::Complete(receiver) => receiver,
            ResumingReceiver::IncompleteUnsettled(receiver) => receiver,
            ResumingReceiver::Resume(receiver) => receiver,
        }
    }
}

impl AsMut<Receiver> for ResumingReceiver {
    fn as_mut(&mut self) -> &mut Receiver {
        match self {
            ResumingReceiver::Complete(receiver) => receiver,
            ResumingReceiver::IncompleteUnsettled(receiver) => receiver,
            ResumingReceiver::Resume(receiver) => receiver,
        }
    }
}

impl From<ResumingReceiver> for Receiver {
    fn from(value: ResumingReceiver) -> Self {
        value.into_receiver()
    }
}

impl DetachedReceiver {
    /// Resume with an explicit interruption future while retaining ownership.
    /// An interrupted admitted Attach completes upstream's nonclosing Detach
    /// cleanup before custody returns; an already Detached link sends nothing.
    pub async fn resume_on_session_until<R>(
        mut self,
        session: &SessionHandle<R>,
        interrupt: impl std::future::Future<Output = ()>,
    ) -> Result<ResumingReceiver, ReceiverResumeError> {
        if self.inner.link.session_stop_reason.get().is_some() {
            self = match (Receiver { inner: *self.inner }).into_detached() {
                Ok(endpoint) => endpoint,
                Err(endpoint) => {
                    return Err(ReceiverResumeError {
                        detached_recver: DetachedReceiver {
                            inner: Box::new(endpoint.inner),
                        },
                        kind: ReceiverAttachError::IllegalState.into(),
                    });
                }
            };
        }
        self.inner.switch_session(session);
        let result = tokio::select! { biased;
            _ = interrupt => Err(ReceiverResumeErrorKind::Interrupted),
            result = self.inner.resume_incoming_attach(None, false) => result,
        };
        if matches!(&result, Err(ReceiverResumeErrorKind::Interrupted)) {
            try_as_recver!(self, self.inner.detach_with_error(None).await);
        }
        match result {
            Ok(exchange) => {
                let receiver = Receiver { inner: *self.inner };
                Ok(match exchange {
                    ReceiverAttachExchange::Complete => ResumingReceiver::Complete(receiver),
                    ReceiverAttachExchange::IncompleteUnsettled => {
                        ResumingReceiver::IncompleteUnsettled(receiver)
                    }
                    ReceiverAttachExchange::Resume => ResumingReceiver::Resume(receiver),
                })
            }
            Err(kind) => Err(ReceiverResumeError {
                detached_recver: self,
                kind,
            }),
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

    async fn resume_inner(
        mut self,
        is_reattaching: bool,
    ) -> Result<ResumingReceiver, ReceiverResumeError> {
        let exchange = try_as_recver!(
            self,
            self.inner
                .resume_incoming_attach(None, is_reattaching)
                .await
        );
        let receiver = Receiver { inner: *self.inner };
        let resuming_receiver = match exchange {
            ReceiverAttachExchange::Complete => ResumingReceiver::Complete(receiver),
            ReceiverAttachExchange::IncompleteUnsettled => {
                ResumingReceiver::IncompleteUnsettled(receiver)
            }
            ReceiverAttachExchange::Resume => ResumingReceiver::Resume(receiver),
        };
        Ok(resuming_receiver)
    }

    /// Resume the receiver link
    ///
    /// Please note that the link may need to be detached and then resume multiple
    /// times if there are unsettled deliveries.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn resume(self) -> Result<ResumingReceiver, ReceiverResumeError> {
        self.resume_inner(false).await
    }

    cfg_not_wasm32! {
        async fn resume_with_timeout_inner(
            mut self,
            duration: Duration,
            is_reattaching: bool,
        ) -> Result<ResumingReceiver, ReceiverResumeError> {
            let fut = self.inner.resume_incoming_attach(None, is_reattaching);

            match tokio::time::timeout(duration, fut).await {
                Ok(Ok(exchange)) => {
                    let receiver = Receiver { inner: *self.inner };
                    let resuming_receiver = match exchange {
                        ReceiverAttachExchange::Complete => ResumingReceiver::Complete(receiver),
                        ReceiverAttachExchange::IncompleteUnsettled => {
                            ResumingReceiver::IncompleteUnsettled(receiver)
                        }
                        ReceiverAttachExchange::Resume => ResumingReceiver::Resume(receiver),
                    };
                    Ok(resuming_receiver)
                }
                Ok(Err(kind)) => Err(ReceiverResumeError {
                    detached_recver: self,
                    kind,
                }),
                Err(_) => {
                    try_as_recver!(self, self.inner.detach_with_error(None).await);
                    Err(ReceiverResumeError {
                        detached_recver: self,
                        kind: ReceiverResumeErrorKind::Timeout,
                    })
                }
            }
        }

        /// Resume the receiver link with a timeout.
        ///
        /// Upon failure, the detached receiver can be accessed via `error.detached_recver`
        ///
        /// Please note that the link may need to be detached and then resume multiple
        /// times if there are unsettled deliveries. For more details please see [`resume`](./#method.resume)
        #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
        pub async fn resume_with_timeout(
            self,
            duration: Duration,
        ) -> Result<ResumingReceiver, ReceiverResumeError> {
            self.resume_with_timeout_inner(duration, false).await
        }
    }

    /// Resume the receiver on a specific session
    ///
    /// Please note that the link may need to be detached and then resume multiple
    /// times if there are unsettled deliveries. For more details please see [`resume`](./#method.resume)
    pub async fn resume_on_session<R>(
        mut self,
        session: &SessionHandle<R>,
    ) -> Result<ResumingReceiver, ReceiverResumeError> {
        let is_reattaching = self.inner.switch_session(session);

        self.resume_inner(is_reattaching).await
    }

    /// Resume the receiver link on the original session with an Attach sent by the remote peer
    ///
    /// Please note that the link may need to be detached and then resume multiple
    /// times if there are unsettled deliveries. For more details please see [`resume`](./#method.resume)
    pub async fn resume_incoming_attach(
        mut self,
        remote_attach: Attach,
    ) -> Result<ResumingReceiver, ReceiverResumeError> {
        let exchange = try_as_recver!(
            self,
            self.inner
                .resume_incoming_attach(Some(remote_attach), false)
                .await
        );
        let receiver = Receiver { inner: *self.inner };
        let resuming_receiver = match exchange {
            ReceiverAttachExchange::Complete => ResumingReceiver::Complete(receiver),
            ReceiverAttachExchange::IncompleteUnsettled => {
                ResumingReceiver::IncompleteUnsettled(receiver)
            }
            ReceiverAttachExchange::Resume => ResumingReceiver::Resume(receiver),
        };
        Ok(resuming_receiver)
    }

    /// Resume the receiver on a specific session
    ///
    /// Please note that the link may need to be detached and then resume multiple
    /// times if there are unsettled deliveries. For more details please see [`resume`](./#method.resume)
    pub async fn resume_incoming_attach_on_session<R>(
        mut self,
        remote_attach: Attach,
        session: &SessionHandle<R>,
    ) -> Result<ResumingReceiver, ReceiverResumeError> {
        let is_reattaching = self.inner.switch_session(session);

        let exchange = try_as_recver!(
            self,
            self.inner
                .resume_incoming_attach(Some(remote_attach), is_reattaching)
                .await
        );
        let receiver = Receiver { inner: *self.inner };
        let resuming_receiver = match exchange {
            ReceiverAttachExchange::Complete => ResumingReceiver::Complete(receiver),
            ReceiverAttachExchange::IncompleteUnsettled => {
                ResumingReceiver::IncompleteUnsettled(receiver)
            }
            ReceiverAttachExchange::Resume => ResumingReceiver::Resume(receiver),
        };
        Ok(resuming_receiver)
    }

    cfg_not_wasm32! {
        /// Resume the receiver on a specific session with timeout
        ///
        /// Please note that the link may need to be detached and then resume multiple
        /// times if there are unsettled deliveries. For more details please see [`resume`](./#method.resume)
        pub async fn resume_on_session_with_timeout<R>(
            mut self,
            session: &SessionHandle<R>,
            duration: Duration,
        ) -> Result<ResumingReceiver, ReceiverResumeError> {
            let is_reattaching = self.inner.switch_session(session);
            self.resume_with_timeout_inner(duration, is_reattaching).await
        }

        /// Resume the receiver link on the original session with an Attach sent by the remote peer
        ///
        /// Please note that the link may need to be detached and then resume multiple
        /// times if there are unsettled deliveries. For more details please see [`resume`](./#method.resume)
        pub async fn resume_incoming_attach_with_timeout(
            mut self,
            remote_attach: Attach,
            duration: Duration,
        ) -> Result<ResumingReceiver, ReceiverResumeError> {
            let fut = self.inner.resume_incoming_attach(Some(remote_attach), false);

            match tokio::time::timeout(duration, fut).await {
                Ok(Ok(exchange)) => {
                    let receiver = Receiver { inner: *self.inner };
                    let resuming_receiver = match exchange {
                        ReceiverAttachExchange::Complete => ResumingReceiver::Complete(receiver),
                        ReceiverAttachExchange::IncompleteUnsettled => {
                            ResumingReceiver::IncompleteUnsettled(receiver)
                        }
                        ReceiverAttachExchange::Resume => ResumingReceiver::Resume(receiver),
                    };
                    Ok(resuming_receiver)
                }
                Ok(Err(kind)) => Err(ReceiverResumeError {
                    detached_recver: self,
                    kind,
                }),
                Err(_) => {
                    try_as_recver!(self, self.inner.detach_with_error(None).await);
                    Err(ReceiverResumeError {
                        detached_recver: self,
                        kind: ReceiverResumeErrorKind::Timeout,
                    })
                }
            }
        }

        /// Resume the receiver on a specific session with timeout
        ///
        /// Please note that the link may need to be detached and then resume multiple
        /// times if there are unsettled deliveries. For more details please see [`resume`](./#method.resume)
        pub async fn resume_incoming_attach_on_session_with_timeout<R>(
            mut self,
            remote_attach: Attach,
            session: &SessionHandle<R>,
            duration: Duration,
        ) -> Result<ResumingReceiver, ReceiverResumeError> {
            let is_reattaching = self.inner.switch_session(session);

            let fut = self.inner.resume_incoming_attach(Some(remote_attach), is_reattaching);

            match tokio::time::timeout(duration, fut).await {
                Ok(Ok(exchange)) => {
                    let receiver = Receiver { inner: *self.inner };
                    let resuming_receiver = match exchange {
                        ReceiverAttachExchange::Complete => ResumingReceiver::Complete(receiver),
                        ReceiverAttachExchange::IncompleteUnsettled => {
                            ResumingReceiver::IncompleteUnsettled(receiver)
                        }
                        ReceiverAttachExchange::Resume => ResumingReceiver::Resume(receiver),
                    };
                    Ok(resuming_receiver)
                }
                Ok(Err(kind)) => Err(ReceiverResumeError {
                    detached_recver: self,
                    kind,
                }),
                Err(_) => {
                    try_as_recver!(self, self.inner.detach_with_error(None).await);
                    Err(ReceiverResumeError {
                        detached_recver: self,
                        kind: ReceiverResumeErrorKind::Timeout,
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn state_only_settlement_before_the_native_write_supplies_no_identity() {
        use crate::{
            endpoint::{InputHandle, OutgoingChannel, OutputAttachment, Session as _},
            session::Builder,
        };
        use fe2o3_amqp_types::{performatives::Disposition, states::SessionState};
        let (inner, _control, _outgoing, incoming) = make_receiver_inner_with_channels(4096);
        seed_unsettled(inner.link.unsettled(), &[vec![9]]);
        let tag: DeliveryTag = vec![9].into();
        {
            let mut guard = inner.link.unsettled().write();
            let entry = guard.as_mut().unwrap().get_mut(&tag).unwrap();
            entry.received = true;
            entry.info = Some(make_delivery_info(1, tag.to_vec()));
        }
        let mut session = Builder::new().into_session(
            OutgoingChannel(0),
            SessionState::Mapped,
            Arc::new(OnceLock::new()),
        );
        let (frames, _inbox) = mpsc::channel(2);
        session.link_by_input_handle.insert(
            InputHandle(0),
            LinkRelay::new_receiver(
                frames,
                inner.link.flow_state.clone(),
                inner.link.unsettled.clone(),
                ReceiverSettleMode::First,
            )
            .with_output_handle(OutputAttachment::for_tests(0)),
        );
        session
            .delivery_tag_by_id
            .insert((Role::Sender, 1), (InputHandle(0), tag.clone()));
        let session = Arc::new(std::sync::Mutex::new(session));
        let owner = Arc::downgrade(&session);
        inner.link.unsettled.before_write_once(move || {
            owner
                .upgrade()
                .unwrap()
                .lock()
                .unwrap()
                .on_incoming_disposition(Disposition {
                    role: Role::Sender,
                    first: 1,
                    last: None,
                    settled: true,
                    state: Some(Accepted {}.into()),
                    batchable: false,
                })
                .unwrap();
        });
        let mut resumed = make_incoming_transfer(9, Some(tag.to_vec()), false, false);
        resumed.resume = true;
        incoming
            .send(queued_transfer(resumed, Payload::new()))
            .await
            .unwrap();
        incoming
            .send(queued_transfer(
                make_incoming_transfer(10, Some(vec![10]), false, false),
                Payload::from_static(&[0, 0x53, 0x75, 0xa0, 2, 7, 8]),
            ))
            .await
            .unwrap();
        let mut receiver = Receiver { inner };
        let mut progress = Progress::default();
        let delivery = observed_delivery(0, &mut receiver, &mut progress).await;
        assert_eq!(*delivery.delivery_id(), 10);

        assert!(matches!(
            progress.steps[0].effect(),
            ReceiverTransferEffect::StateOnly {
                info: None,
                retained: false,
                more: false
            }
        ));
        assert!(!receiver
            .inner
            .link
            .unsettled
            .read()
            .as_ref()
            .unwrap()
            .contains_key(&tag));
    }

    #[tokio::test]
    async fn detached_dispositions_use_only_the_original_live_session() {
        for transactional in [false, true] {
            let (mut inner, _control, mut outgoing, _incoming) =
                make_receiver_inner_with_channels(4096);
            inner.credit_mode = CreditMode::Manual;
            inner.link.local_state = LinkState::Detached;
            inner.link.output_handle.take();
            inner.link.input_handle.take();
            let tag: DeliveryTag = vec![42].into();
            seed_unsettled(inner.link.unsettled(), &[tag.to_vec()]);
            let info = make_delivery_info(42, tag.to_vec());
            {
                inner
                    .link
                    .unsettled()
                    .write()
                    .as_mut()
                    .unwrap()
                    .get_mut(&tag)
                    .unwrap()
                    .info = Some(info.clone());
            }
            let state = if transactional {
                #[cfg(feature = "transaction")]
                {
                    DeliveryState::TransactionalState(
                        fe2o3_amqp_types::transaction::TransactionalState {
                            txn_id: vec![3].into(),
                            outcome: Some(fe2o3_amqp_types::messaging::Outcome::Accepted(
                                Accepted {},
                            )),
                        },
                    )
                }
                #[cfg(not(feature = "transaction"))]
                {
                    continue;
                }
            } else {
                Accepted {}.into()
            };
            inner
                .dispose(info.clone(), None, state.clone())
                .await
                .unwrap();
            let LinkFrame::Disposition(disposition) = outgoing.recv().await.unwrap() else {
                panic!("session disposition")
            };
            assert_eq!(disposition.first, 42);
            assert_eq!(disposition.state, Some(state.clone()));
            assert!(outgoing.try_recv().is_err());
            // Exact native identity still excludes a reused tag and delivery number.
            seed_unsettled(inner.link.unsettled(), &[tag.to_vec()]);
            let fresh = make_delivery_info(42, tag.to_vec());
            {
                inner
                    .link
                    .unsettled()
                    .write()
                    .as_mut()
                    .unwrap()
                    .get_mut(&tag)
                    .unwrap()
                    .info = Some(fresh.clone());
            }
            assert!(matches!(
                inner.dispose(info, None, state.clone()).await,
                Err(IllegalLinkStateError::IllegalState)
            ));
            assert!(outgoing.try_recv().is_err());
            inner
                .link
                .session_stop_reason
                .set(SessionStopReason::Stopped)
                .unwrap();
            outgoing.close();
            assert!(matches!(
                inner.dispose(fresh, None, state).await,
                Err(IllegalLinkStateError::SessionStopped(
                    SessionStopReason::Stopped
                ))
            ));
            assert!(outgoing.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn explicit_exact_detach_delivers_inflight_and_retains_partial() {
        use crate::link::test_util::{drive_exact_end, CorrelationGuard};
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        for closed in [false, true] {
            let (inner, control, mut outgoing, incoming) = make_receiver_inner_with_channels(4096);
            let mut receiver = Receiver { inner };
            receiver.request_detach().await.unwrap();
            assert!(matches!(
                outgoing.recv().await.unwrap(),
                LinkFrame::Detach(Detach { closed: false, .. }, _)
            ));
            let body = Payload::from_static(&[0, 0x53, 0x75, 0xa0, 2, 7, 8]);
            incoming
                .send(queued_transfer(
                    make_incoming_transfer(1, Some(vec![1]), false, false),
                    body.clone(),
                ))
                .await
                .unwrap();
            incoming
                .send(queued_transfer(
                    make_incoming_transfer(2, Some(vec![2]), true, false),
                    body.slice(..6),
                ))
                .await
                .unwrap();
            let mut continuation = make_incoming_transfer(2, None, false, false);
            continuation.message_format = None;
            incoming
                .send(queued_transfer(continuation, body.slice(6..)))
                .await
                .unwrap();
            incoming
                .send(queued_transfer(
                    make_incoming_transfer(3, Some(vec![3]), true, false),
                    body.slice(..6),
                ))
                .await
                .unwrap();
            incoming
                .send(LinkFrame::Detach(
                    Detach {
                        handle: 0u32.into(),
                        closed,
                        error: None,
                    },
                    crate::endpoint::AttachmentId::allocate(),
                ))
                .await
                .unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicBool::new(false));
            let mut progress = Progress::default();
            let mut before = || {
                calls.fetch_add(1, Ordering::AcqRel);
                Ok::<_, std::io::Error>(CorrelationGuard(dropped.clone()))
            };
            for id in [1, 2] {
                let ReceiverDetachStep::Delivery(delivery)=receiver.recv_detaching_exact::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>,_,_,_,_>(&mut progress,&mut before).await.unwrap() else {panic!("in-flight complete delivery")};
                assert_eq!(*delivery.delivery_id(), id);
                let data: Vec<u8> = delivery
                    .body()
                    .try_as_data()
                    .unwrap()
                    .flat_map(|binary| binary.iter().copied())
                    .collect();
                assert_eq!(data, [7, 8]);
            }
            if closed {
                let (result,_)=tokio::join!(receiver.recv_detaching_exact::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>,_,_,_,_>(&mut progress,&mut before),
                    drive_exact_end(control,outgoing,incoming,peer_sender_attach(),|| {assert_eq!(calls.load(Ordering::Acquire),1);assert!(!dropped.load(Ordering::Acquire));},|| {assert!(!dropped.load(Ordering::Acquire));None}));
                assert!(matches!(
                    result.unwrap(),
                    ReceiverDetachStep::Closed { error: None }
                ));
                assert!(dropped.load(Ordering::Acquire));
            } else {
                assert!(matches!(receiver.recv_detaching_exact::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>,_,_,_,_>(&mut progress,&mut before).await.unwrap(),ReceiverDetachStep::Suspended {error:None}));
                assert_eq!(calls.load(Ordering::Acquire), 0);
                assert!(outgoing.try_recv().is_err());
            }

            assert_eq!(progress.steps.len(), 4);
            assert!(receiver.inner.incomplete_transfer.is_some());
            assert_eq!(receiver.inner.receive_budget.retained_bytes(), 6);
        }
    }

    #[tokio::test]
    async fn reattach_errors_keep_phase_and_drop_correlation() {
        use super::super::LinkExchangeError;
        use crate::link::test_util::CorrelationGuard;
        use std::sync::atomic::AtomicBool;
        let (mut inner, mut controls, mut outgoing, _incoming) =
            make_receiver_inner_with_channels(4096);
        inner.link.local_state = LinkState::Detached;
        inner.link.output_handle.take();
        let mut endpoint = Receiver { inner };
        assert!(matches!(
            close_receiver_exact(&mut endpoint, false, || Err::<(), _>(
                std::io::Error::other("refused")
            ))
            .await,
            Err(LinkExchangeError::Correlation(_))
        ));
        assert!(controls.try_recv().is_err());
        assert!(outgoing.try_recv().is_err());
        let dropped = Arc::new(AtomicBool::new(false));
        let (result, ()) = tokio::join!(
            close_receiver_exact(&mut endpoint, false, || Ok::<_, std::io::Error>(
                CorrelationGuard(dropped.clone())
            )),
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
                ReceiverAttachError::SessionNotMapped
            ))
        ));
        assert!(dropped.load(Ordering::Acquire));
        assert!(outgoing.try_recv().is_err());
    }

    #[tokio::test]
    async fn exact_attach_and_detach_failures_keep_their_original_phase() {
        use super::super::LinkExchangeError;
        use crate::link::test_util::{drive_exact_end, CorrelationGuard};
        use std::sync::atomic::AtomicBool;
        for refusal in [false, true] {
            let (mut inner, control, outgoing, incoming) = make_receiver_inner_with_channels(4096);
            inner.link.local_state = LinkState::Detached;
            inner.link.output_handle.take();
            let mut endpoint = Receiver { inner };
            let dropped = Arc::new(AtomicBool::new(false));
            let mut attach = peer_sender_attach();
            if refusal {
                attach.source = None;
            }
            let error = definitions::Error::new(definitions::AmqpError::NotFound, None, None);
            let (result, _) = tokio::join!(
                close_receiver_exact(&mut endpoint, false, || Ok::<_, std::io::Error>(
                    CorrelationGuard(dropped.clone())
                )),
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
                    matches!(result,Err(LinkExchangeError::Attach(ReceiverAttachError::RemoteClosedWithError(actual))) if actual.condition==error.condition)
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
    async fn exact_close_preserves_the_original_receiving_prefix() {
        use crate::link::test_util::{drive_exact_end, CorrelationGuard};
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        // Active Close with closing reply, active Close crossed by suspension,
        // and a suspension observed before Close but still queued natively.
        for mode in 0..3 {
            let (mut inner, control, mut outgoing, incoming) =
                make_receiver_inner_with_channels(4096);
            inner.credit_mode = CreditMode::Manual;
            let original_handle: fe2o3_amqp_types::definitions::Handle = inner
                .link
                .output_handle
                .as_ref()
                .unwrap()
                .handle
                .clone()
                .into();
            let body = Payload::from_static(&[0, 0x53, 0x75, 0xa0, 2, 7, 8]);
            for (id, more, payload) in [
                (1, false, body.clone()),
                (2, true, body.slice(..6)),
                (2, false, body.slice(6..)),
                (3, true, body.slice(..6)),
            ] {
                let mut transfer = make_incoming_transfer(
                    id,
                    (id != 2 || more).then_some(vec![id as u8]),
                    more,
                    false,
                );
                if id == 2 && !more {
                    transfer.message_format = None;
                }
                incoming
                    .send(queued_transfer(transfer, payload))
                    .await
                    .unwrap();
            }
            // Hold the peer's Detach until both complete deliveries escape the
            // real Close parser; the closing frame has already been admitted.
            let mut receiver = Receiver { inner };
            let mut progress = Progress::default();
            let calls = AtomicUsize::new(0);
            let dropped = Arc::new(AtomicBool::new(false));
            let mut before = || {
                calls.fetch_add(1, Ordering::AcqRel);
                Ok::<_, std::io::Error>(CorrelationGuard(dropped.clone()))
            };
            for id in [1, 2] {
                let ReceiverDetachStep::Delivery(delivery) = receiver.close_in_place_exact::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>,_,_,_,_>(mode == 2, &mut progress, &mut before).await.unwrap() else { panic!("admitted complete delivery"); };
                assert_eq!(*delivery.delivery_id(), id);
                assert_eq!(delivery.handle(), &original_handle);
                assert_eq!(
                    delivery
                        .body()
                        .try_as_data()
                        .unwrap()
                        .flat_map(|b| b.iter().copied())
                        .collect::<Vec<_>>(),
                    [7, 8]
                );
                assert!(if mode == 2 {
                    matches!(receiver.inner.link.local_state, LinkState::Attached)
                } else {
                    matches!(receiver.inner.link.local_state, LinkState::CloseSent)
                });
            }
            if mode != 2 {
                let LinkFrame::Detach(detach, _) = outgoing.recv().await.unwrap() else {
                    panic!("actual closing Detach");
                };
                assert!(detach.closed);
                assert_eq!(detach.handle, original_handle);
            } else {
                assert!(outgoing.try_recv().is_err());
            }
            incoming
                .send(LinkFrame::Detach(
                    Detach {
                        handle: 0u32.into(),
                        closed: mode == 0,
                        error: None,
                    },
                    crate::endpoint::AttachmentId::allocate(),
                ))
                .await
                .unwrap();
            let boundary = receiver.close_in_place_exact::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>,_,_,_,_>(mode == 2, &mut progress, &mut before).await.unwrap();
            assert_eq!(progress.steps.len(), 4);
            if mode == 0 {
                assert!(matches!(
                    boundary,
                    ReceiverDetachStep::Closed { error: None }
                ));
            } else {
                assert!(matches!(
                    boundary,
                    ReceiverDetachStep::Suspended { error: None }
                ));
                let (result, count) = tokio::join!(
                    receiver.close_in_place_exact::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>,_,_,_,_>(true, &mut progress, &mut before),
                    drive_exact_end(control, outgoing, incoming, peer_sender_attach(), || assert!(!dropped.load(Ordering::Acquire)), || None)
                );
                assert!(matches!(
                    result.unwrap(),
                    ReceiverDetachStep::Closed { error: None }
                ));
                assert_eq!(count, 1);
                assert!(dropped.load(Ordering::Acquire));
            }
            assert!(receiver.is_closed());
        }
    }

    #[tokio::test]
    async fn incomplete_close_attach_is_not_business_recovery() {
        let (mut inner, control, outgoing, incoming) = make_receiver_inner_with_channels(4096);
        seed_unsettled(inner.link.unsettled(), &[vec![9]]);
        inner.link.local_state = LinkState::Detached;
        inner.link.output_handle.take();
        let mut receiver = Receiver { inner };
        let mut attach = peer_sender_attach();
        attach.incomplete_unsettled = true;
        let (result, _) = tokio::join!(
            close_receiver_exact(&mut receiver, true, || Ok::<_, std::io::Error>(())),
            crate::link::test_util::drive_exact_end(
                control,
                outgoing,
                incoming,
                attach,
                || {},
                || None
            )
        );
        assert!(matches!(
            result,
            Err(super::super::LinkExchangeError::Attach(
                ReceiverAttachError::IllegalState
            ))
        ));
    }

    async fn close_receiver_exact<F, G, E>(
        receiver: &mut Receiver,
        suspended: bool,
        mut before: F,
    ) -> Result<(), super::super::LinkExchangeError<ReceiverAttachError, E>>
    where
        F: FnMut() -> Result<G, E> + Send,
        G: Send,
        E: Send,
    {
        loop {
            match receiver.close_in_place_exact::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>,_,_,_,_>(suspended, &mut NoopTransferObserver, &mut before).await {
                Ok(ReceiverDetachStep::Closed { .. }) => return Ok(()),
                Ok(ReceiverDetachStep::Suspended { .. }) => {},
                Ok(_) => panic!("fixture has no deliveries or protocol fault"),
                Err(ReceiverDetachError::Exchange(error)) => return Err(error),
                Err(ReceiverDetachError::Receive(_)) => panic!("fixture has no receive error"),
            }
        }
    }

    #[derive(Default)]
    struct Progress {
        steps: Vec<ReceiverTransferProgress>,
    }
    impl ReceiverTransferObserver for Progress {
        fn transfer(&mut self, progress: ReceiverTransferProgress) {
            self.steps.push(progress);
        }
    }
    fn queued_transfer(performative: Transfer, payload: Payload) -> LinkFrame {
        LinkFrame::Transfer {
            input_handle: crate::endpoint::InputHandle(0),
            attachment: crate::endpoint::AttachmentId::allocate(),
            performative,
            payload,
            window_slot: None,
            queue_slot: None,
        }
    }
    async fn observed_delivery(
        mode: u8,
        receiver: &mut Receiver,
        observer: &mut Progress,
    ) -> Delivery<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>> {
        match mode {
            0 => match receiver.recv_observed(observer).await.unwrap() {
                ReceiverReadStep::Delivery(delivery) => delivery,
                other => panic!("{other:?}"),
            },
            1 => match receiver
                .recv_detaching_exact(observer, &mut || Ok::<_, std::io::Error>(()))
                .await
                .unwrap()
            {
                ReceiverDetachStep::Delivery(delivery) => delivery,
                other => panic!("{other:?}"),
            },
            _ => match receiver.recv_collecting_stopped(observer).await.unwrap() {
                ReceiverCollectionStep::Delivery(delivery) => delivery,
                other => panic!("{other:?}"),
            },
        }
    }

    #[tokio::test]
    async fn observed_abort_then_delivery_retires_only_the_partial() {
        for mode in 0..3 {
            for standalone in [false, true] {
                let (inner, _control, mut outgoing, incoming) =
                    make_receiver_inner_with_channels(4096);
                let budget = inner.receive_budget.clone();
                let mut receiver = Receiver { inner };
                if mode == 1 {
                    receiver.request_detach().await.unwrap();
                    outgoing.recv().await.unwrap();
                }
                let mut abort = make_incoming_transfer(1, Some(vec![1]), false, false);
                abort.aborted = true;
                if !standalone {
                    incoming
                        .send(queued_transfer(
                            make_incoming_transfer(1, Some(vec![1]), true, false),
                            Payload::from_static(&[0, 0x53, 0x75, 0xa0, 2, 0]),
                        ))
                        .await
                        .unwrap();
                    abort.delivery_id = None;
                    abort.delivery_tag = None;
                    abort.message_format = None;
                }
                incoming
                    .send(queued_transfer(abort, Payload::new()))
                    .await
                    .unwrap();
                incoming
                    .send(queued_transfer(
                        make_incoming_transfer(2, Some(vec![2]), false, false),
                        Payload::from_static(&[0, 0x53, 0x75, 0xa0, 2, 7, 8]),
                    ))
                    .await
                    .unwrap();
                if mode == 2 {
                    receiver
                        .inner
                        .link
                        .session_stop_reason
                        .set(SessionStopReason::Stopped)
                        .unwrap();
                }
                drop(incoming);
                let mut progress = Progress::default();
                let delivery = observed_delivery(mode, &mut receiver, &mut progress).await;
                assert_eq!(*delivery.delivery_id(), 2);
                assert_eq!(budget.retained_bytes(), 0);

                let abort_index = usize::from(!standalone);
                assert!(matches!(
                    progress.steps[abort_index].effect(),
                    ReceiverTransferEffect::Aborted
                ));
                if !standalone {
                    assert!(matches!(
                        progress.steps[0].effect(),
                        ReceiverTransferEffect::PayloadIncomplete
                    ));
                }
                assert!(
                    matches!(progress.steps.last().unwrap().effect(),ReceiverTransferEffect::Delivered {info} if info.same_delivery(&DeliveryInfo::from(&delivery)))
                );
            }
        }
    }

    #[tokio::test]
    async fn observed_ignored_resume_consumes_every_fragment_without_a_delivery() {
        for mode in 0..3 {
            let (inner, _control, mut outgoing, incoming) = make_receiver_inner_with_channels(4096);
            let mut receiver = Receiver { inner };
            if mode == 1 {
                receiver.request_detach().await.unwrap();
                outgoing.recv().await.unwrap();
            }
            let mut first = make_incoming_transfer(9, Some(vec![9]), true, false);
            first.resume = true;
            incoming
                .send(queued_transfer(first.clone(), Payload::new()))
                .await
                .unwrap();
            first.delivery_id = None;
            first.delivery_tag = None;
            first.message_format = None;
            first.more = false;
            first.resume = false;
            incoming
                .send(queued_transfer(first, Payload::new()))
                .await
                .unwrap();
            incoming
                .send(queued_transfer(
                    make_incoming_transfer(10, Some(vec![10]), false, false),
                    Payload::from_static(&[0, 0x53, 0x75, 0xa0, 2, 7, 8]),
                ))
                .await
                .unwrap();
            if mode == 2 {
                receiver
                    .inner
                    .link
                    .session_stop_reason
                    .set(SessionStopReason::Stopped)
                    .unwrap();
            }
            drop(incoming);
            let mut progress = Progress::default();
            let delivery = observed_delivery(mode, &mut receiver, &mut progress).await;
            assert_eq!(*delivery.delivery_id(), 10);

            assert!(matches!(
                progress.steps[0].effect(),
                ReceiverTransferEffect::Ignored { more: true }
            ));
            assert!(matches!(
                progress.steps[1].effect(),
                ReceiverTransferEffect::Ignored { more: false }
            ));

            assert!(receiver.inner.incomplete_transfer.is_none());
            assert_eq!(receiver.inner.receive_budget.retained_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn observed_state_only_supplies_the_exact_native_identity_once() {
        for mode in 0..3 {
            for settled in [false, true] {
                let (inner, _control, mut outgoing, incoming) =
                    make_receiver_inner_with_channels(4096);
                seed_unsettled(inner.link.unsettled(), &[vec![9]]);
                let old = make_delivery_info(1, vec![9]);
                {
                    let mut guard = inner.link.unsettled().write();
                    let entry = guard
                        .as_mut()
                        .unwrap()
                        .get_mut(&DeliveryTag::from(vec![9]))
                        .unwrap();
                    entry.received = true;
                    entry.info = Some(old.clone());
                }
                let mut receiver = Receiver { inner };
                if mode == 1 {
                    receiver.request_detach().await.unwrap();
                    outgoing.recv().await.unwrap();
                }
                let mut first = make_incoming_transfer(9, Some(vec![9]), true, false);
                first.resume = true;
                incoming
                    .send(queued_transfer(first.clone(), Payload::new()))
                    .await
                    .unwrap();
                first.delivery_id = None;
                first.delivery_tag = None;
                first.message_format = None;
                first.more = false;
                first.resume = false;
                first.settled = Some(settled);
                incoming
                    .send(queued_transfer(first, Payload::new()))
                    .await
                    .unwrap();
                incoming
                    .send(queued_transfer(
                        make_incoming_transfer(10, Some(vec![10]), false, false),
                        Payload::from_static(&[0, 0x53, 0x75, 0xa0, 2, 7, 8]),
                    ))
                    .await
                    .unwrap();
                if mode == 2 {
                    receiver
                        .inner
                        .link
                        .session_stop_reason
                        .set(SessionStopReason::Stopped)
                        .unwrap();
                }
                drop(incoming);
                let mut progress = Progress::default();
                let delivery = observed_delivery(mode, &mut receiver, &mut progress).await;
                assert_eq!(*delivery.delivery_id(), 10);

                let ReceiverTransferEffect::StateOnly {
                    info: Some(info),
                    retained: true,
                    more: true,
                } = progress.steps[0].effect()
                else {
                    panic!("first state-only identity")
                };
                assert_eq!(info.delivery_id(), 9);
                assert!(!info.same_delivery(&old));
                assert!(
                    matches!(progress.steps[1].effect(),ReceiverTransferEffect::StateOnly {info:None,retained,more:false} if *retained==!settled)
                );
                if !settled {
                    let guard = receiver.inner.link.unsettled.read();
                    let native = guard
                        .as_ref()
                        .unwrap()
                        .get(&DeliveryTag::from(vec![9]))
                        .unwrap()
                        .info
                        .as_ref()
                        .unwrap();
                    assert!(info.same_delivery(native));
                }

                assert!(receiver.inner.incomplete_transfer.is_none());
            }
        }
    }

    #[tokio::test]
    async fn stopped_collection_drains_normal_transfers_without_wire_work() {
        let (inner, _control, mut outgoing, incoming) = make_receiver_inner_with_channels(4096);
        let mut receiver = Receiver { inner };
        receiver.inner.auto_accept = true;
        let body = Payload::from_static(&[0, 0x53, 0x75, 0xa0, 2, 7, 8]);
        incoming
            .send(queued_transfer(
                make_incoming_transfer(1, Some(vec![1]), false, false),
                body.clone(),
            ))
            .await
            .unwrap();
        incoming
            .send(queued_transfer(
                make_incoming_transfer(2, Some(vec![2]), true, false),
                body.slice(..6),
            ))
            .await
            .unwrap();
        let mut last = make_incoming_transfer(2, None, false, false);
        last.message_format = None;
        incoming
            .send(queued_transfer(last, body.slice(6..)))
            .await
            .unwrap();
        incoming
            .send(queued_transfer(
                make_incoming_transfer(3, Some(vec![3]), true, false),
                body.slice(..6),
            ))
            .await
            .unwrap();
        let mut progress = Progress::default();
        assert!(matches!(
            receiver
                .recv_collecting_stopped::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>, _>(
                    &mut progress
                )
                .await,
            Err(RecvError::LinkStateError(LinkStateError::IllegalState))
        ));
        assert_eq!(receiver.inner.incoming.len(), 4);

        receiver
            .inner
            .link
            .session_stop_reason
            .set(SessionStopReason::Stopped)
            .unwrap();
        drop(incoming);
        assert_eq!(
            *observed_delivery(2, &mut receiver, &mut progress)
                .await
                .delivery_id(),
            1
        );
        assert_eq!(
            *observed_delivery(2, &mut receiver, &mut progress)
                .await
                .delivery_id(),
            2
        );
        assert!(matches!(
            receiver
                .recv_collecting_stopped::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>, _>(
                    &mut progress
                )
                .await
                .unwrap(),
            ReceiverCollectionStep::Drained {
                reason: SessionStopReason::Stopped
            }
        ));
        assert!(receiver.inner.incomplete_transfer.is_some());
        assert!(receiver.inner.receive_budget.retained_bytes() > 0);

        assert!(outgoing.try_recv().is_err());
    }

    #[tokio::test]
    async fn stopped_collection_keeps_a_queued_closing_detach_unrecoverable() {
        for closed in [false, true] {
            for with_error in [false, true] {
                let (inner, _control, mut outgoing, incoming) =
                    make_receiver_inner_with_channels(4096);
                inner
                    .link
                    .session_stop_reason
                    .set(SessionStopReason::Stopped)
                    .unwrap();
                let error =
                    with_error.then(|| definitions::Error::new(AmqpError::NotFound, None, None));
                incoming
                    .send(LinkFrame::Detach(
                        Detach {
                            handle: 0u32.into(),
                            closed,
                            error: error.clone(),
                        },
                        crate::endpoint::AttachmentId::allocate(),
                    ))
                    .await
                    .unwrap();
                drop(incoming);
                let mut receiver = Receiver { inner };
                let mut progress = Progress::default();
                let step=receiver.recv_collecting_stopped::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>,_>(&mut progress).await.unwrap();
                if closed {
                    assert!(
                        matches!(step,ReceiverCollectionStep::Closed {error:actual} if actual==error)
                    );
                } else {
                    assert!(matches!(
                        step,
                        ReceiverCollectionStep::Drained {
                            reason: SessionStopReason::Stopped
                        }
                    ));
                }

                assert!(progress.steps.is_empty());
                assert!(outgoing.try_recv().is_err());
            }
        }
    }

    #[tokio::test]
    async fn observed_protocol_close_returns_before_wire_and_uses_exact_finish() {
        fn expected(fault: u8, cause: &RecvError) -> bool {
            matches!(
                (fault, cause),
                (0, RecvError::MessageSizeExceeded(_))
                    | (1, RecvError::DeliveryTagIsNone)
                    | (2, RecvError::TransactionalAcquisitionIsNotImeplemented)
                    | (3, RecvError::InconsistentFieldInMultiFrameDelivery)
            )
        }
        for mode in 0..3 {
            for fault in 0..4 {
                #[cfg(not(feature = "transaction"))]
                if fault == 2 {
                    continue;
                }
                let (mut inner, control, mut outgoing, incoming) =
                    make_receiver_inner_with_channels(4096);
                if fault == 0 {
                    inner.link.max_message_size = 1;
                }
                let mut receiver = Receiver { inner };
                if mode == 1 {
                    receiver.request_detach().await.unwrap();
                    assert!(matches!(
                        outgoing.recv().await.unwrap(),
                        LinkFrame::Detach(Detach { closed: false, .. }, _)
                    ));
                }
                if mode == 2 {
                    receiver
                        .inner
                        .link
                        .session_stop_reason
                        .set(SessionStopReason::Stopped)
                        .unwrap();
                }
                if fault == 2 {
                    #[cfg(feature = "transaction")]
                    incoming
                        .send(LinkFrame::Acquisition(vec![1].into()))
                        .await
                        .unwrap();
                } else {
                    let body = Payload::from_static(&[0, 0x53, 0x75, 0xa0, 2, 7, 8]);
                    if fault == 3 {
                        incoming
                            .send(queued_transfer(
                                make_incoming_transfer(1, Some(vec![1]), true, false),
                                body.slice(..6),
                            ))
                            .await
                            .unwrap();
                        incoming
                            .send(queued_transfer(
                                make_incoming_transfer(1, Some(vec![2]), false, false),
                                body.slice(6..),
                            ))
                            .await
                            .unwrap();
                    } else {
                        incoming
                            .send(queued_transfer(
                                make_incoming_transfer(
                                    1,
                                    if fault == 1 { None } else { Some(vec![1]) },
                                    false,
                                    false,
                                ),
                                body,
                            ))
                            .await
                            .unwrap();
                    }
                }
                let mut progress = Progress::default();
                let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let mut before = || {
                    calls.fetch_add(1, Ordering::AcqRel);
                    Ok::<_, std::io::Error>(())
                };
                if mode == 2 {
                    drop(incoming);
                    let error=receiver.recv_collecting_stopped::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>,_>(&mut progress).await.unwrap_err();
                    assert!(expected(fault, &error));
                    assert!(receiver.self_close_cause().is_none());
                    assert!(outgoing.try_recv().is_err());
                } else {
                    if mode == 0 {
                        assert!(matches!(receiver.recv_observed::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>,_>(&mut progress).await.unwrap(),ReceiverReadStep::SelfClosing));
                    } else {
                        assert!(matches!(receiver.recv_detaching_exact::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>,_,_,_,_>(&mut progress,&mut before).await.unwrap(),ReceiverDetachStep::SelfClosing));
                    }
                    assert!(expected(fault, receiver.self_close_cause().unwrap()));
                    assert!(outgoing.try_recv().is_err());
                    assert_eq!(calls.load(Ordering::Acquire), 0);
                    if mode == 1 {
                        incoming
                            .send(LinkFrame::Detach(
                                Detach {
                                    handle: 0u32.into(),
                                    closed: false,
                                    error: None,
                                },
                                crate::endpoint::AttachmentId::allocate(),
                            ))
                            .await
                            .unwrap();
                    }
                    let (result, detaches) = tokio::join!(
                        receiver.finish_self_close_exact(&mut before),
                        crate::link::test_util::drive_exact_end(
                            control,
                            outgoing,
                            incoming,
                            peer_sender_attach(),
                            || {},
                            || None
                        )
                    );
                    result.unwrap();
                    assert_eq!(detaches, if mode == 0 { 2 } else { 1 });
                    assert_eq!(calls.load(Ordering::Acquire), 1);
                    assert!(receiver.self_close_cause().is_none());
                }
                if fault == 2 {
                    assert!(progress.steps.is_empty());
                } else {
                    assert!(matches!(
                        progress.steps.last().unwrap().effect(),
                        ReceiverTransferEffect::Failed
                    ));
                }
            }
        }
    }

    impl ReceiverInner<ReceiverLink<Target>> {
        async fn test_transfer<T>(
            &mut self,
            transfer: Transfer,
            payload: Payload,
        ) -> Result<Option<Delivery<T>>, RecvError>
        where
            for<'de> T: FromBody<'de> + Send,
        {
            match self
                .receive_transfer(
                    transfer,
                    payload,
                    None,
                    ReceiveContext::Live,
                    &mut NoopTransferObserver,
                )
                .await
            {
                Ok(delivery) => Ok(delivery),
                Err(ReceiveFault::Error(error)) => Err(error),
                Err(ReceiveFault::SelfClosing) => Err(self.finish_self_close().await),
            }
        }
    }

    use super::*;

    use crate::link::state::{LinkFlowState, LinkFlowStateInner, LinkState};
    use crate::util::Sealed;
    use fe2o3_amqp_types::{definitions::SenderSettleMode, messaging::Received};

    fn make_flow_state(link_credit: u32) -> ReceiverFlowState {
        Arc::new(LinkFlowState::receiver(LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit,
            available: 0,
            drain: false,
            properties: None,
        }))
    }

    fn make_disposer(
        tx: mpsc::Sender<LinkFrame>,
        unsettled: ArcReceiverUnsettledMap,
        credit_mode: CreditMode,
    ) -> ReceiverDisposer {
        if let Some(map) = unsettled.write().as_mut() {
            for (tag, entry) in map.as_inner_mut().iter_mut() {
                if entry.info.is_none() {
                    entry.info = Some(make_delivery_info(tag[0] as u32, tag.to_vec()));
                }
            }
        }
        ReceiverDisposer {
            outgoing: tx.into(),
            unsettled,
            rcv_settle_mode: ReceiverSettleMode::First,
            flow_state: make_flow_state(200),
            output_handle: Some(crate::endpoint::OutputAttachment::for_tests(1)),
            processed: Arc::new(AtomicU32::new(0)),
            credit_mode,
            session_stop_reason: Arc::new(OnceLock::new()),
        }
    }

    fn make_delivery_info(id: u32, tag: Vec<u8>) -> DeliveryInfo {
        DeliveryInfo {
            identity: std::sync::Arc::new(()),
            delivery_id: id,
            delivery_tag: DeliveryTag::from(tag),
            rcv_settle_mode: None,
            _sealed: Sealed {},
        }
    }

    fn current_delivery_info(disposer: &ReceiverDisposer, id: u32, tag: Vec<u8>) -> DeliveryInfo {
        let info = disposer
            .unsettled
            .read()
            .as_ref()
            .unwrap()
            .get(&DeliveryTag::from(tag))
            .unwrap()
            .info
            .as_ref()
            .unwrap()
            .clone();
        assert_eq!(info.delivery_id(), id);
        info
    }

    fn seed_unsettled(map: &ArcReceiverUnsettledMap, tags: &[Vec<u8>]) {
        let mut lock = map.write();
        let m = lock.get_or_insert_with(Default::default);
        for tag in tags {
            m.insert(
                DeliveryTag::from(tag.clone()),
                crate::link::receiver_delivery::ReceiverDelivery::new(None),
            );
        }
    }

    #[tokio::test]
    async fn explicit_detach_delivers_inflight_message_and_retains_partial_without_extra_detach() {
        use super::super::{state::LinkState, unsettled_store::Store};
        use fe2o3_amqp_types::messaging::Body;
        let map = Arc::new(Store::new(None));
        seed_unsettled(&map, &[vec![0x42], vec![0x43]]);
        let mut link = Receiver::builder()
            .name("detach")
            .source("source")
            .target("target")
            .create_link(
                map.clone(),
                crate::endpoint::OutputAttachment::for_tests(0),
                make_flow_state(2),
                Arc::new(OnceLock::new()),
                65532,
            );
        link.local_state = LinkState::Attached;
        let (control, _controls) = mpsc::channel(8);
        let (outgoing, mut sent) = mpsc::channel(8);
        let (incoming, frames) = mpsc::channel(8);
        let mut receiver = Receiver {
            inner: ReceiverInner {
                link,
                buffer_size: 8,
                credit_mode: CreditMode::Manual,
                processed: Arc::new(AtomicU32::new(0)),
                auto_accept: false,
                session: control,
                outgoing: outgoing.into(),
                incoming: frames,
                incomplete_transfer: None,
                self_close: None,
                incoming_recovery: Default::default(),
                receive_budget: super::super::receive_budget::ReceiveBudget::new(65536),
            },
        };
        receiver.request_detach().await.unwrap();
        assert!(matches!(
            sent.recv().await.unwrap(),
            LinkFrame::Detach(Detach { closed: false, .. }, _)
        ));
        let mut transfer: Transfer =
            serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x42, 0x43])
                .unwrap();
        let full = vec![0, 0x53, 0x75, 0xa0, 2, 0, 255];
        incoming
            .send(LinkFrame::Transfer {
                input_handle: crate::endpoint::InputHandle(0),
                attachment: crate::endpoint::AttachmentId::allocate(),
                performative: transfer.clone(),
                payload: full.into(),
                window_slot: None,
                queue_slot: None,
            })
            .await
            .unwrap();
        transfer.delivery_id = Some(1);
        transfer.delivery_tag = Some(vec![0x43].into());
        transfer.more = true;
        incoming
            .send(LinkFrame::Transfer {
                input_handle: crate::endpoint::InputHandle(0),
                attachment: crate::endpoint::AttachmentId::allocate(),
                performative: transfer,
                payload: vec![0, 0x53, 0x75, 0xa0, 2, 0].into(),
                window_slot: None,
                queue_slot: None,
            })
            .await
            .unwrap();
        incoming
            .send(LinkFrame::Detach(
                Detach {
                    handle: 0.into(),
                    closed: false,
                    error: None,
                },
                crate::endpoint::AttachmentId::allocate(),
            ))
            .await
            .unwrap();
        let delivery = receiver.recv::<Body<serde_amqp::Value>>().await.unwrap();
        assert_eq!(delivery.delivery_tag().as_ref(), &[0x42]);
        assert!(matches!(
            receiver.recv::<Body<serde_amqp::Value>>().await,
            Err(RecvError::LinkStateError(LinkStateError::RemoteDetached))
        ));
        let detached = receiver.into_detached().unwrap();
        assert_eq!(
            detached
                .inner
                .incomplete_transfer
                .as_ref()
                .unwrap()
                .buffer
                .as_slice(),
            &[0, 0x53, 0x75, 0xa0, 2, 0]
        );
        assert!(
            map.read()
                .as_ref()
                .unwrap()
                .get(&DeliveryTag::from(vec![0x42]))
                .unwrap()
                .received
        );
        assert!(
            !map.read()
                .as_ref()
                .unwrap()
                .get(&DeliveryTag::from(vec![0x43]))
                .unwrap()
                .received
        );
        let (control, _controls) = mpsc::channel(8);
        let (outgoing, mut next_sent) = mpsc::channel(8);
        let (_outcome, outcome) = crate::util::engine_outcome::EngineOutcome::channel();
        let mut next_session = SessionHandle {
            is_ended: true,
            engine_joined: false,
            control,
            outgoing: outgoing.into(),
            engine_handle: tokio::spawn(std::future::pending()),
            outcome,
            session_stop_reason: Arc::new(OnceLock::new()),
            max_frame_size: 65532,
            link_listener: (),
        };
        let interrupted = detached
            .resume_on_session_until(&next_session, std::future::ready(()))
            .await
            .unwrap_err();
        assert!(matches!(
            interrupted.kind,
            ReceiverResumeErrorKind::Interrupted
        ));
        assert_eq!(
            interrupted
                .detached_recver
                .inner
                .incomplete_transfer
                .as_ref()
                .unwrap()
                .buffer
                .as_slice(),
            &[0, 0x53, 0x75, 0xa0, 2, 0]
        );
        assert!(matches!(
            next_sent.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(interrupted);
        next_session.stop_and_join().await.unwrap();
        assert!(
            sent.try_recv().is_err(),
            "no second Detach or automatic disposition"
        );
    }

    #[tokio::test]
    async fn settled_partial_is_released_without_discarding_a_retained_delivery() {
        use super::super::{state::LinkState, unsettled_store::Store};
        let map = Arc::new(Store::new(None));
        seed_unsettled(&map, &[vec![0x42]]);
        let tag: DeliveryTag = vec![0x42].into();
        let mut link = Receiver::builder()
            .name("partial")
            .source("source")
            .target("target")
            .create_link(
                map.clone(),
                crate::endpoint::OutputAttachment::for_tests(0),
                make_flow_state(1),
                Arc::new(OnceLock::new()),
                65532,
            );
        link.local_state = LinkState::Attached;
        let (control, _controls) = mpsc::channel(8);
        let (outgoing, _outgoing) = mpsc::channel(8);
        let (_incoming, incoming) = mpsc::channel(8);
        let budget = super::super::receive_budget::ReceiveBudget::new(65536);
        let mut receiver = ReceiverInner {
            link,
            buffer_size: 8,
            credit_mode: CreditMode::Manual,
            processed: Arc::new(AtomicU32::new(0)),
            auto_accept: false,
            session: control,
            outgoing: outgoing.into(),
            incoming,
            incomplete_transfer: None,
            self_close: None,
            incoming_recovery: Default::default(),
            receive_budget: budget.clone(),
        };
        let mut transfer: Transfer =
            serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x42, 0x43])
                .unwrap();
        transfer.more = true;
        let prefix = vec![0, 0x53, 0x75, 0xa0, 2, 0];
        receiver
            .receive_transfer::<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>, _>(
                transfer,
                prefix.clone().into(),
                None,
                ReceiveContext::Live,
                &mut NoopTransferObserver,
            )
            .await
            .unwrap();
        receiver.discard_settled_partial();
        assert_eq!(
            receiver.incomplete_transfer.as_ref().unwrap().buffer,
            prefix
        );
        assert_eq!(budget.retained_bytes(), prefix.len());
        // A complete peer unsettled map without this tag removes its entry.
        map.write().as_mut().unwrap().swap_remove(&tag);
        receiver.discard_settled_partial();
        assert!(receiver.incomplete_transfer.is_none());
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[tokio::test]
    async fn state_only_resume_rebinds_delivery_identity_without_redelivery() {
        use super::super::{
            receiver_delivery::ReceiverDelivery, state::LinkState, unsettled_store::Store,
        };
        use fe2o3_amqp_types::{
            messaging::{Body, Received},
            primitives::OrderedMap,
        };
        let tag: DeliveryTag = vec![0x42].into();
        let mut retained = ReceiverDelivery::new(None);
        retained.received(
            Received {
                section_number: 1,
                section_offset: 0,
            }
            .into(),
            true,
        );
        // No identity is carried across Attach. Only the resumed Transfer can
        // assign this delivery a new session delivery-id.
        let map = Arc::new(Store::new(Some(
            [(tag.clone(), retained)]
                .into_iter()
                .collect::<OrderedMap<_, _>>(),
        )));
        let mut link = Receiver::builder()
            .name("resume")
            .source("source")
            .target("target")
            .create_link(
                map.clone(),
                crate::endpoint::OutputAttachment::for_tests(0),
                make_flow_state(0),
                Arc::new(OnceLock::new()),
                65532,
            );
        link.local_state = LinkState::Attached;
        let (control, _controls) = mpsc::channel(8);
        let (outgoing, _outgoing) = mpsc::channel(8);
        let (_incoming, incoming) = mpsc::channel(8);
        let mut receiver = Receiver {
            inner: ReceiverInner {
                link,
                buffer_size: 8,
                credit_mode: CreditMode::Manual,
                processed: Arc::new(AtomicU32::new(0)),
                auto_accept: false,
                session: control,
                outgoing: outgoing.into(),
                incoming,
                incomplete_transfer: None,
                self_close: None,
                incoming_recovery: Default::default(),
                receive_budget: super::super::receive_budget::ReceiveBudget::new(65536),
            },
        };
        let view = receiver.settlement_view();
        assert!(view.contains(&tag));
        assert!(view.delivery_info(&tag).is_none());
        let mut transfer: Transfer =
            serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x42, 0x43])
                .unwrap();
        transfer.delivery_id = Some(99);
        transfer.resume = true;
        let result = receiver
            .inner
            .test_transfer::<Body<serde_amqp::Value>>(transfer.clone(), Vec::new().into())
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "a completed message must not be emitted again"
        );
        assert_eq!(view.delivery_info(&tag).unwrap().delivery_id(), 99);
        // Unknown tags and terminally settled transfers never become disposable.
        transfer.delivery_tag = Some(vec![0x43].into());
        assert!(receiver
            .inner
            .test_transfer::<Body<serde_amqp::Value>>(transfer.clone(), Vec::new().into())
            .await
            .unwrap()
            .is_none());
        assert!(view.delivery_info(&vec![0x43].into()).is_none());
        transfer.delivery_tag = Some(tag.clone());
        transfer.settled = Some(true);
        assert!(receiver
            .inner
            .test_transfer::<Body<serde_amqp::Value>>(transfer, Vec::new().into())
            .await
            .unwrap()
            .is_none());
        assert!(!view.contains(&tag));
        assert!(view.delivery_info(&tag).is_none());
    }

    #[tokio::test]
    async fn accept_sends_disposition_frame() {
        let (tx, mut rx) = mpsc::channel::<LinkFrame>(16);
        let unsettled: ArcReceiverUnsettledMap =
            Arc::new(crate::link::unsettled_store::Store::new(None));
        seed_unsettled(&unsettled, &[vec![0x01]]);

        let disposer = make_disposer(tx, unsettled, CreditMode::Manual);
        let info = current_delivery_info(&disposer, 1, vec![0x01]);

        disposer.accept(info).await.unwrap();

        let frame = rx.try_recv().expect("should receive a frame");
        match frame {
            LinkFrame::Disposition(d) => {
                assert_eq!(d.role, Role::Receiver);
                assert_eq!(d.first, 1);
                assert!(d.settled);
                assert!(matches!(d.state, Some(DeliveryState::Accepted(_))));
            }
            other => panic!("expected Disposition, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn release_sends_released_disposition() {
        let (tx, mut rx) = mpsc::channel::<LinkFrame>(16);
        let unsettled: ArcReceiverUnsettledMap =
            Arc::new(crate::link::unsettled_store::Store::new(None));
        seed_unsettled(&unsettled, &[vec![0x02]]);

        let disposer = make_disposer(tx, unsettled, CreditMode::Manual);
        let info = current_delivery_info(&disposer, 2, vec![0x02]);

        disposer.release(info).await.unwrap();

        let frame = rx.try_recv().expect("should receive a frame");
        match frame {
            LinkFrame::Disposition(d) => {
                assert_eq!(d.first, 2);
                assert!(d.settled);
                assert!(matches!(d.state, Some(DeliveryState::Released(_))));
            }
            other => panic!("expected Disposition, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn accept_removes_from_unsettled_map() {
        let (tx, _rx) = mpsc::channel::<LinkFrame>(16);
        let unsettled: ArcReceiverUnsettledMap =
            Arc::new(crate::link::unsettled_store::Store::new(None));
        seed_unsettled(&unsettled, &[vec![0x0A], vec![0x0B]]);

        let disposer = make_disposer(tx, unsettled.clone(), CreditMode::Manual);
        let info = current_delivery_info(&disposer, 10, vec![0x0A]);

        disposer.accept(info).await.unwrap();

        let lock = unsettled.read();
        let map = lock.as_ref().unwrap();
        assert!(!map.contains_key(&DeliveryTag::from(vec![0x0A])));
        assert!(map.contains_key(&DeliveryTag::from(vec![0x0B])));
    }

    #[tokio::test]
    async fn processed_counter_increments() {
        let (tx, _rx) = mpsc::channel::<LinkFrame>(16);
        let unsettled: ArcReceiverUnsettledMap =
            Arc::new(crate::link::unsettled_store::Store::new(None));
        seed_unsettled(&unsettled, &[vec![1], vec![2], vec![3]]);

        let disposer = make_disposer(tx, unsettled, CreditMode::Manual);

        disposer
            .accept(current_delivery_info(&disposer, 1, vec![1]))
            .await
            .unwrap();
        disposer
            .accept(current_delivery_info(&disposer, 2, vec![2]))
            .await
            .unwrap();
        disposer
            .accept(current_delivery_info(&disposer, 3, vec![3]))
            .await
            .unwrap();

        assert_eq!(disposer.processed.load(Ordering::Acquire), 3);
    }

    #[tokio::test]
    async fn auto_credit_refresh_sends_flow_frame() {
        // Auto(10) → triggers refresh after 5 dispositions
        let max_credit = 10u32;
        let (tx, mut rx) = mpsc::channel::<LinkFrame>(32);
        let unsettled: ArcReceiverUnsettledMap =
            Arc::new(crate::link::unsettled_store::Store::new(None));
        let tags: Vec<Vec<u8>> = (1..=5).map(|i| vec![i]).collect();
        seed_unsettled(&unsettled, &tags);

        let disposer = make_disposer(tx, unsettled, CreditMode::Auto(max_credit));

        for i in 1..=5u32 {
            disposer
                .accept(current_delivery_info(&disposer, i, vec![i as u8]))
                .await
                .unwrap();
        }

        // Drain all frames: 5 Dispositions + 1 Flow
        let mut dispositions = 0;
        let mut flows = 0;
        while let Ok(frame) = rx.try_recv() {
            match frame {
                LinkFrame::Disposition(_) => dispositions += 1,
                LinkFrame::Flow(f, _) => {
                    flows += 1;
                    assert_eq!(f.link_credit, Some(max_credit));
                }
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        assert_eq!(dispositions, 5);
        assert_eq!(flows, 1, "should send exactly one Flow to refresh credit");

        // processed should have been reset to 0 after the refresh
        assert_eq!(disposer.processed.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn second_mode_unknown_receipt_does_not_create_phantom_unsettled_state() {
        let (tx, mut rx) = mpsc::channel::<LinkFrame>(8);
        let unsettled = Arc::new(crate::link::unsettled_store::Store::new(None));
        let mut disposer = make_disposer(tx, unsettled.clone(), CreditMode::Manual);
        disposer.rcv_settle_mode = ReceiverSettleMode::Second;
        disposer
            .accept(make_delivery_info(9, vec![9]))
            .await
            .unwrap();
        assert!(unsettled.read().is_none());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn no_disposition_for_unknown_delivery_tag() {
        let (tx, mut rx) = mpsc::channel::<LinkFrame>(16);
        let unsettled: ArcReceiverUnsettledMap =
            Arc::new(crate::link::unsettled_store::Store::new(None));
        seed_unsettled(&unsettled, &[vec![0x01]]);

        let disposer = make_disposer(tx, unsettled, CreditMode::Manual);

        // Accept a tag that is NOT in the unsettled map
        let info = make_delivery_info(99, vec![0xFF]);
        disposer.accept(info).await.unwrap();

        // No Disposition frame should be sent
        assert!(rx.try_recv().is_err());
        // An absent delivery cannot consume credit or change settlement state.
        assert_eq!(disposer.processed.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn stale_identity_cannot_settle_reused_tag_and_id() {
        let (tx, mut rx) = mpsc::channel::<LinkFrame>(8);
        let unsettled = Arc::new(crate::link::unsettled_store::Store::new(None));
        seed_unsettled(&unsettled, &[vec![1]]);
        let disposer = make_disposer(tx, unsettled.clone(), CreditMode::Manual);
        let old = current_delivery_info(&disposer, 1, vec![1]);
        let fresh = make_delivery_info(1, vec![1]);
        unsettled
            .write()
            .as_mut()
            .unwrap()
            .get_mut(&fresh.delivery_tag)
            .unwrap()
            .info = Some(fresh.clone());
        assert!(disposer.accept(old).await.is_err());
        assert!(rx.try_recv().is_err());
        assert!(unsettled
            .read()
            .as_ref()
            .unwrap()
            .contains_key(&fresh.delivery_tag));
        disposer.accept(fresh).await.unwrap();
        assert!(matches!(rx.try_recv().unwrap(), LinkFrame::Disposition(_)));
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let (tx, _rx) = mpsc::channel::<LinkFrame>(16);
        let unsettled: ArcReceiverUnsettledMap =
            Arc::new(crate::link::unsettled_store::Store::new(None));
        seed_unsettled(&unsettled, &[vec![1], vec![2]]);

        let disposer = make_disposer(tx, unsettled, CreditMode::Manual);
        let clone = disposer.clone();

        // Accept via the clone
        clone
            .accept(current_delivery_info(&disposer, 1, vec![1]))
            .await
            .unwrap();

        // Original sees the incremented counter
        assert_eq!(disposer.processed.load(Ordering::Acquire), 1);

        // Unsettled map updated via clone is visible from original
        let lock = disposer.unsettled.read();
        let map = lock.as_ref().unwrap();
        assert!(!map.contains_key(&DeliveryTag::from(vec![1u8])));
        assert!(map.contains_key(&DeliveryTag::from(vec![2u8])));
    }

    #[tokio::test]
    async fn concurrent_accepts_are_safe() {
        let (tx, mut rx) = mpsc::channel::<LinkFrame>(1024);
        let unsettled: ArcReceiverUnsettledMap =
            Arc::new(crate::link::unsettled_store::Store::new(None));
        let tags: Vec<Vec<u8>> = (0..100u8).map(|i| vec![i]).collect();
        seed_unsettled(&unsettled, &tags);

        let disposer = make_disposer(tx, unsettled, CreditMode::Manual);

        // Spawn 100 concurrent accepts
        let handles: Vec<_> = (0..100u8)
            .map(|i| {
                let d = disposer.clone();
                tokio::spawn(async move {
                    d.accept(current_delivery_info(&d, i as u32, vec![i]))
                        .await
                        .unwrap();
                })
            })
            .collect();

        futures_util::future::join_all(handles).await;

        assert_eq!(disposer.processed.load(Ordering::Acquire), 100);

        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 100);
    }

    fn make_receiver_inner_with_channels(
        max_frame_size: usize,
    ) -> (
        ReceiverInner<ReceiverLink<Target>>,
        mpsc::Receiver<SessionControl>,
        mpsc::Receiver<LinkFrame>,
        mpsc::Sender<LinkFrame>,
    ) {
        let (session_tx, session_rx) = mpsc::channel::<SessionControl>(16);
        let (outgoing_tx, outgoing_rx) = mpsc::channel::<LinkFrame>(16);
        let (incoming_tx, incoming_rx) = mpsc::channel::<LinkFrame>(16);
        let unsettled: ArcReceiverUnsettledMap =
            Arc::new(super::super::unsettled_store::Store::new(None));
        let link = ReceiverLink::<Target> {
            operation_dispatch: None,
            role: std::marker::PhantomData,
            local_state: LinkState::Attached,
            name: String::from("test-receiver"),
            output_handle: Some(crate::endpoint::OutputAttachment::for_tests(0)),
            input_handle: None,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: None,
            max_message_size: 0,
            offered_capabilities: None,
            desired_capabilities: None,
            flow_state: make_flow_state(200),
            unsettled,
            session_stop_reason: Arc::new(OnceLock::new()),
            max_frame_size,
            verify_incoming_source: true,
            verify_incoming_target: true,
        };
        let inner = ReceiverInner {
            link,
            buffer_size: 16,
            credit_mode: CreditMode::Auto(200),
            processed: Arc::new(AtomicU32::new(0)),
            auto_accept: false,
            session: session_tx,
            outgoing: outgoing_tx.into(),
            incoming: incoming_rx,
            incomplete_transfer: None,
            self_close: None,
            incoming_recovery: Default::default(),
            receive_budget: super::super::receive_budget::ReceiveBudget::new(65536),
        };
        (inner, session_rx, outgoing_rx, incoming_tx)
    }

    fn make_receiver_inner(max_frame_size: usize) -> ReceiverInner<ReceiverLink<Target>> {
        make_receiver_inner_with_channels(max_frame_size).0
    }

    fn make_incoming_transfer(
        delivery_id: u32,
        delivery_tag: Option<Vec<u8>>,
        more: bool,
        settled: bool,
    ) -> Transfer {
        Transfer {
            handle: Handle(0),
            delivery_id: Some(delivery_id),
            delivery_tag: delivery_tag.map(DeliveryTag::from),
            message_format: Some(0),
            settled: Some(settled),
            more,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        }
    }

    fn make_link_frame(transfer: Transfer, payload: Payload) -> LinkFrame {
        LinkFrame::Transfer {
            input_handle: crate::endpoint::InputHandle(0),
            attachment: crate::endpoint::AttachmentId::allocate(),
            performative: transfer,
            payload,
            window_slot: None,
            queue_slot: None,
        }
    }

    fn encoded_message_payload(body: &str) -> Payload {
        use fe2o3_amqp_types::messaging::{message::__private::Serializable, Message};
        let message = Message::from(body.to_string());
        Payload::from(serde_amqp::to_vec(&Serializable(message)).unwrap())
    }

    /// An encoded message with a header and a body section, so that section
    /// numbering can locate the body (`section_number == 1`).
    fn encoded_sectioned_payload() -> Payload {
        use fe2o3_amqp_types::{
            messaging::{message::__private::Serializable, AmqpValue, Body, Header, Message},
            primitives::Value,
        };
        let message = Message {
            header: Some(Header {
                durable: true,
                ..Default::default()
            }),
            delivery_annotations: None,
            message_annotations: None,
            properties: None,
            application_properties: None,
            body: Body::Value(AmqpValue(Value::Bool(true))),
            footer: None,
        };
        Payload::from(serde_amqp::to_vec(&Serializable(message)).unwrap())
    }

    fn assert_message_size_exceeded(error: RecvError, expected_size: u64, expected_max: u64) {
        match error {
            RecvError::MessageSizeExceeded(e) => {
                assert_eq!(e.size, expected_size);
                assert_eq!(e.max_size, expected_max);
            }
            other => panic!("expected MessageSizeExceeded, got {other:?}"),
        }
    }

    /// Drive the closing-detach handshake while `recv()` processes a malformed
    /// or oversized transfer: assert that the detach closes the link with the
    /// expected condition, answer it, and return the error `recv()` produced.
    async fn recv_expecting_fatal_close(
        inner: &mut ReceiverInner<ReceiverLink<Target>>,
        outgoing_rx: &mut mpsc::Receiver<LinkFrame>,
        incoming_tx: &mpsc::Sender<LinkFrame>,
        expected_condition: definitions::ErrorCondition,
    ) -> RecvError {
        let (result, ()) = tokio::join!(inner.recv::<String>(), async {
            match outgoing_rx.recv().await.expect("expected a closing detach") {
                LinkFrame::Detach(detach, _) => {
                    assert!(detach.closed, "the error detach must close the link");
                    let error = detach
                        .error
                        .as_ref()
                        .expect("the detach must carry the error");
                    assert_eq!(error.condition, expected_condition);
                }
                other => panic!("expected Detach, got {other:?}"),
            }

            // Answer the closing detach to complete the handshake.
            incoming_tx
                .send(LinkFrame::Detach(
                    Detach {
                        handle: Handle(0),
                        closed: true,
                        error: None,
                    },
                    crate::endpoint::AttachmentId::allocate(),
                ))
                .await
                .unwrap();
        });

        result.expect_err("a malformed delivery must fail")
    }

    fn encoded_binary_prefix(size: usize) -> Payload {
        let mut bytes = vec![0; size];
        if size >= 5 {
            bytes[..5].copy_from_slice(&[0, 0x53, 0x75, 0xa0, 200]);
        }
        Payload::from(bytes)
    }

    #[tokio::test]
    async fn accumulated_size_counts_tagless_continuation_but_not_other_delivery() {
        let mut inner = make_receiver_inner(4096);
        inner
            .test_transfer::<String>(
                make_incoming_transfer(1, Some(vec![0x01]), true, false),
                encoded_binary_prefix(60),
            )
            .await
            .unwrap();

        let continuation = make_incoming_transfer(1, None, true, false);
        assert_eq!(inner.accumulated_message_size(&continuation), 60);

        let other_delivery = make_incoming_transfer(2, Some(vec![0x02]), true, false);
        assert_eq!(inner.accumulated_message_size(&other_delivery), 0);
    }

    /// A delivery spanning exactly two frames whose total size exceeds the
    /// link's max-message-size must detach the link on the tagless final frame
    /// (AMQP 1.0 §2.6.5, `amqp:link:message-size-exceeded`).
    #[tokio::test]
    async fn two_frame_delivery_exceeding_max_message_size_is_rejected_on_final_frame() {
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        inner.link.max_message_size = 100;

        // The first frame carries the delivery tag, the final continuation
        // omits it.
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, Some(vec![0x01]), true, false),
                encoded_binary_prefix(60),
            ))
            .await
            .unwrap();
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, None, false, false),
                encoded_binary_prefix(60),
            ))
            .await
            .unwrap();

        // An oversized message is a link error: the link is detached with
        // `amqp:link:message-size-exceeded` and destroyed.
        let error = recv_expecting_fatal_close(
            &mut inner,
            &mut outgoing_rx,
            &incoming_tx,
            definitions::ErrorCondition::from(LinkError::MessageSizeExceeded),
        )
        .await;
        assert_message_size_exceeded(error, 120, 100);
        assert!(inner.incomplete_transfer.is_none());
        assert!(matches!(inner.link.local_state, LinkState::Closed));
    }

    /// Drop `recv()` once it has sent its own closing Detach, as a
    /// `tokio::select!` that another branch wins does, and check what it sent.
    async fn drop_recv_after_closing_detach(
        inner: &mut ReceiverInner<ReceiverLink<Target>>,
        outgoing_rx: &mut mpsc::Receiver<LinkFrame>,
        expected_condition: definitions::ErrorCondition,
    ) {
        tokio::select! {
            biased;
            result = inner.recv::<String>() => panic!("recv ended before the answer: {result:?}"),
            frame = outgoing_rx.recv() => match frame {
                Some(LinkFrame::Detach(detach, _)) => {
                    assert!(detach.closed);
                    assert_eq!(detach.error.unwrap().condition, expected_condition);
                }
                other => panic!("expected the closing Detach, got {other:?}"),
            },
        }
        assert!(matches!(inner.link.local_state, LinkState::CloseSent));
        assert!(inner.self_close.is_some());
    }

    /// The peer's answer to a closing Detach.
    async fn answer_close(incoming_tx: &mpsc::Sender<LinkFrame>) {
        incoming_tx
            .send(LinkFrame::Detach(
                Detach {
                    handle: Handle(0),
                    closed: true,
                    error: None,
                },
                crate::endpoint::AttachmentId::allocate(),
            ))
            .await
            .unwrap();
    }

    /// `recv()` is cancel-safe through the close it starts itself: dropped
    /// once its closing Detach is out, the next `recv()` takes the peer's
    /// answer as the answer, completes the close and returns the cause, not
    /// a Detach the peer sent on its own.
    #[tokio::test]
    async fn oversized_delivery_close_resumes_after_recv_is_dropped_mid_close() {
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        inner.link.max_message_size = 100;
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, Some(vec![0x01]), false, false),
                Payload::from(vec![0u8; 120]),
            ))
            .await
            .unwrap();
        drop_recv_after_closing_detach(
            &mut inner,
            &mut outgoing_rx,
            definitions::ErrorCondition::from(LinkError::MessageSizeExceeded),
        )
        .await;
        answer_close(&incoming_tx).await;
        let error = inner.recv::<String>().await.unwrap_err();
        assert_message_size_exceeded(error, 120, 100);
        assert!(matches!(inner.link.local_state, LinkState::Closed));
        assert!(inner.self_close.is_none());
        assert!(outgoing_rx.try_recv().is_err(), "the close is not repeated");
    }

    /// Dropped before its closing Detach could be queued, `recv()` resumes
    /// by sending it, then completes the close with the recorded cause.
    #[tokio::test]
    async fn oversized_delivery_close_resumes_after_recv_is_dropped_before_its_detach_is_sent() {
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        inner.link.max_message_size = 100;
        // Fill the outgoing queue so the closing Detach cannot be queued yet.
        let mut queued = 0;
        while inner
            .outgoing
            .try_send(LinkFrame::Detach(
                Detach {
                    handle: Handle(9),
                    closed: false,
                    error: None,
                },
                crate::endpoint::AttachmentId::allocate(),
            ))
            .is_ok()
        {
            queued += 1;
        }
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, Some(vec![0x01]), false, false),
                Payload::from(vec![0u8; 120]),
            ))
            .await
            .unwrap();
        tokio::select! {
            biased;
            result = inner.recv::<String>() => panic!("recv ended before its Detach was queued: {result:?}"),
            () = std::future::ready(()) => {}
        }
        assert!(matches!(inner.link.local_state, LinkState::Attached));
        assert!(inner.self_close.is_some());
        for _ in 0..queued {
            outgoing_rx.recv().await.unwrap();
        }
        drop_recv_after_closing_detach(
            &mut inner,
            &mut outgoing_rx,
            definitions::ErrorCondition::from(LinkError::MessageSizeExceeded),
        )
        .await;
        answer_close(&incoming_tx).await;
        assert_message_size_exceeded(inner.recv::<String>().await.unwrap_err(), 120, 100);
        assert!(matches!(inner.link.local_state, LinkState::Closed));
    }

    /// The close a malformed delivery starts resumes the same way and
    /// returns the malformation.
    #[tokio::test]
    async fn malformed_delivery_close_resumes_after_recv_is_dropped_mid_close() {
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        let mut missing_id = make_incoming_transfer(1, Some(vec![0x01]), true, false);
        missing_id.delivery_id = None;
        incoming_tx
            .send(make_link_frame(missing_id, encoded_binary_prefix(8)))
            .await
            .unwrap();
        drop_recv_after_closing_detach(
            &mut inner,
            &mut outgoing_rx,
            definitions::ErrorCondition::from(definitions::AmqpError::NotAllowed),
        )
        .await;
        answer_close(&incoming_tx).await;
        let error = inner.recv::<String>().await.unwrap_err();
        assert!(matches!(error, RecvError::DeliveryIdIsNone), "{error:?}");
        assert!(matches!(inner.link.local_state, LinkState::Closed));
    }

    /// A Detach the peer sends on its own while no close of the link's own
    /// is out stays the peer's.
    #[tokio::test]
    async fn a_detach_the_peer_sends_on_its_own_is_reported_as_the_peers() {
        let (mut inner, _session_rx, _outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        answer_close(&incoming_tx).await;
        let error = inner.recv::<String>().await.unwrap_err();
        assert!(
            matches!(
                error,
                RecvError::LinkStateError(LinkStateError::RemoteClosed)
            ),
            "{error:?}"
        );
    }

    /// A delivery spanning three or more frames must detach the link as soon
    /// as an intermediate `more=true` frame pushes the accumulated size over
    /// the limit, before the final frame arrives.
    #[tokio::test]
    async fn multi_frame_delivery_exceeding_max_message_size_is_rejected_on_intermediate_frame() {
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        inner.link.max_message_size = 100;

        // 50 + 50 reaches the limit but does not exceed it.
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, Some(vec![0x01]), true, false),
                encoded_binary_prefix(50),
            ))
            .await
            .unwrap();
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, None, true, false),
                encoded_binary_prefix(50),
            ))
            .await
            .unwrap();
        // The next intermediate frame exceeds the limit.
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, None, true, false),
                encoded_binary_prefix(1),
            ))
            .await
            .unwrap();

        let error = recv_expecting_fatal_close(
            &mut inner,
            &mut outgoing_rx,
            &incoming_tx,
            definitions::ErrorCondition::from(LinkError::MessageSizeExceeded),
        )
        .await;
        assert_message_size_exceeded(error, 101, 100);
        assert!(inner.incomplete_transfer.is_none());
        assert!(matches!(inner.link.local_state, LinkState::Closed));
    }

    /// A tagless-continuation delivery whose total size is exactly the limit
    /// is accepted and decoded.
    #[tokio::test]
    async fn in_limit_multi_frame_delivery_is_delivered() {
        let (mut inner, _session_rx, _outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);

        let body = "in-limit multi-frame message";
        let payload = encoded_message_payload(body);
        let total = payload.len() as u64;
        // Exactly at the limit: the enforcement uses `>` and must not reject.
        inner.link.max_message_size = total;

        let split = payload.len() / 2;
        let first = payload.slice(..split);
        let second = payload.slice(split..);
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, Some(vec![0x01]), true, false),
                first,
            ))
            .await
            .unwrap();
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, None, false, false),
                second,
            ))
            .await
            .unwrap();

        let delivery = inner
            .recv::<String>()
            .await
            .expect("in-limit delivery must be accepted");
        assert_eq!(delivery.body(), body);
    }

    /// The negotiated max-message-size and the receive budget bound a message;
    /// there is no further fixed ceiling, so one larger than 16 MiB arrives.
    #[tokio::test]
    async fn a_delivery_within_its_negotiated_size_and_budget_has_no_fixed_ceiling() {
        let (mut inner, _session_rx, _outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        let body = "x".repeat(17 * 1024 * 1024);
        let payload = encoded_message_payload(&body);
        inner.link.max_message_size = payload.len() as u64;
        inner.receive_budget = super::super::receive_budget::ReceiveBudget::new(payload.len());
        let third = payload.len() / 3;
        let parts = [
            payload.slice(..third),
            payload.slice(third..2 * third),
            payload.slice(2 * third..),
        ];
        for (index, part) in parts.into_iter().enumerate() {
            let first = index == 0;
            incoming_tx
                .send(make_link_frame(
                    make_incoming_transfer(1, first.then(|| vec![0x01]), index < 2, false),
                    part,
                ))
                .await
                .unwrap();
        }
        let delivery = inner
            .recv::<String>()
            .await
            .expect("a delivery within its negotiated size is accepted");
        assert_eq!(delivery.body().len(), body.len());
    }

    /// The first transfer of a multi-transfer delivery must carry the
    /// delivery-id and delivery-tag (AMQP 1.0 §2.7.5); a violation closes the
    /// link with `amqp:not-allowed` (AMQP 1.0 §2.6.5).
    #[tokio::test]
    async fn first_multi_frame_transfer_missing_mandatory_fields_closes_link() {
        // Missing delivery-tag
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, None, true, false),
                encoded_binary_prefix(8),
            ))
            .await
            .unwrap();
        let error = recv_expecting_fatal_close(
            &mut inner,
            &mut outgoing_rx,
            &incoming_tx,
            definitions::ErrorCondition::from(definitions::AmqpError::NotAllowed),
        )
        .await;
        assert!(matches!(error, RecvError::DeliveryTagIsNone));
        assert!(matches!(inner.link.local_state, LinkState::Closed));

        // Missing delivery-id
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        let mut missing_id = make_incoming_transfer(1, Some(vec![0x01]), true, false);
        missing_id.delivery_id = None;
        incoming_tx
            .send(make_link_frame(missing_id, encoded_binary_prefix(8)))
            .await
            .unwrap();
        let error = recv_expecting_fatal_close(
            &mut inner,
            &mut outgoing_rx,
            &incoming_tx,
            definitions::ErrorCondition::from(definitions::AmqpError::NotAllowed),
        )
        .await;
        assert!(matches!(error, RecvError::DeliveryIdIsNone));
        assert!(matches!(inner.link.local_state, LinkState::Closed));
    }

    /// The mandatory delivery-id and delivery-tag are required on the first
    /// transfer of any delivery, including a single-frame one (stricter than
    /// the transfer field text, matching go-amqp and Qpid Proton).
    #[tokio::test]
    async fn single_frame_transfer_missing_mandatory_fields_closes_link() {
        // Missing delivery-tag
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, None, false, false),
                encoded_binary_prefix(8),
            ))
            .await
            .unwrap();
        let error = recv_expecting_fatal_close(
            &mut inner,
            &mut outgoing_rx,
            &incoming_tx,
            definitions::ErrorCondition::from(definitions::AmqpError::NotAllowed),
        )
        .await;
        assert!(matches!(error, RecvError::DeliveryTagIsNone));
        assert!(matches!(inner.link.local_state, LinkState::Closed));

        // Missing delivery-id
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        let mut missing_id = make_incoming_transfer(1, Some(vec![0x01]), false, false);
        missing_id.delivery_id = None;
        incoming_tx
            .send(make_link_frame(missing_id, encoded_binary_prefix(8)))
            .await
            .unwrap();
        let error = recv_expecting_fatal_close(
            &mut inner,
            &mut outgoing_rx,
            &incoming_tx,
            definitions::ErrorCondition::from(definitions::AmqpError::NotAllowed),
        )
        .await;
        assert!(matches!(error, RecvError::DeliveryIdIsNone));
        assert!(matches!(inner.link.local_state, LinkState::Closed));
    }

    /// A state-carrying transfer with no buffered delivery and no delivery-tag
    /// cannot be attributed to a delivery and closes the link.
    #[tokio::test]
    async fn tagless_state_transfer_without_buffer_closes_link() {
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);

        let mut state_transfer = make_incoming_transfer(1, None, false, false);
        state_transfer.state = Some(DeliveryState::Received(Received {
            section_number: 0,
            section_offset: 0,
        }));
        incoming_tx
            .send(make_link_frame(state_transfer, Payload::new()))
            .await
            .unwrap();

        let error = recv_expecting_fatal_close(
            &mut inner,
            &mut outgoing_rx,
            &incoming_tx,
            definitions::ErrorCondition::from(definitions::AmqpError::NotAllowed),
        )
        .await;
        assert!(matches!(error, RecvError::DeliveryTagIsNone));
        assert!(matches!(inner.link.local_state, LinkState::Closed));
    }

    /// A continuation that explicitly names a different delivery while one is
    /// being assembled violates AMQP 1.0 §2.6.14 and closes the link.
    #[tokio::test]
    async fn continuation_with_different_delivery_tag_closes_link() {
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);

        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, Some(vec![0x01]), true, false),
                encoded_binary_prefix(8),
            ))
            .await
            .unwrap();
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, Some(vec![0x02]), false, false),
                encoded_binary_prefix(8),
            ))
            .await
            .unwrap();

        let error = recv_expecting_fatal_close(
            &mut inner,
            &mut outgoing_rx,
            &incoming_tx,
            definitions::ErrorCondition::from(definitions::AmqpError::NotAllowed),
        )
        .await;
        assert!(matches!(
            error,
            RecvError::InconsistentFieldInMultiFrameDelivery
        ));
        assert!(matches!(inner.link.local_state, LinkState::Closed));
    }

    /// A continuation whose present message-format differs from the first
    /// transfer is rejected (AMQP 1.0 §2.7.5) and closes the link.
    #[tokio::test]
    async fn continuation_with_different_message_format_closes_link() {
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);

        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, Some(vec![0x01]), true, false),
                encoded_binary_prefix(8),
            ))
            .await
            .unwrap();

        let mut different_format = make_incoming_transfer(1, Some(vec![0x01]), false, false);
        different_format.message_format = Some(1);
        incoming_tx
            .send(make_link_frame(different_format, encoded_binary_prefix(8)))
            .await
            .unwrap();

        let error = recv_expecting_fatal_close(
            &mut inner,
            &mut outgoing_rx,
            &incoming_tx,
            definitions::ErrorCondition::from(definitions::AmqpError::NotAllowed),
        )
        .await;
        assert!(matches!(
            error,
            RecvError::InconsistentFieldInMultiFrameDelivery
        ));
        assert!(matches!(inner.link.local_state, LinkState::Closed));
    }

    /// An aborted transfer matching the buffered delivery discards its chunks
    /// and its entry in the unsettled map; the link stays usable.
    #[tokio::test]
    async fn aborted_transfer_discards_the_buffered_delivery() {
        let (mut inner, _session_rx, _outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        let aborted_tag = DeliveryTag::from(vec![0x01]);

        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, Some(vec![0x01]), true, false),
                encoded_binary_prefix(8),
            ))
            .await
            .unwrap();

        // The sender aborts the delivery (`resume` mirrors `Sender::abort`).
        let mut abort = make_incoming_transfer(1, Some(vec![0x01]), false, false);
        abort.resume = true;
        abort.aborted = true;
        incoming_tx
            .send(make_link_frame(abort, Payload::new()))
            .await
            .unwrap();

        // The link remains usable: a following ordinary message is delivered.
        let body = "after abort";
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(2, Some(vec![0x02]), false, false),
                encoded_message_payload(body),
            ))
            .await
            .unwrap();

        let delivery = inner.recv::<String>().await.expect("ordinary delivery");
        assert_eq!(delivery.body(), body);
        assert!(inner.incomplete_transfer.is_none());

        let guard = inner.link.unsettled().read();
        assert!(guard
            .as_ref()
            .is_none_or(|map| !map.contains_key(&aborted_tag)));
    }

    /// The same as above when the aborting transfer omits the delivery tag:
    /// the buffered delivery's tag identifies the entry to discard.
    #[tokio::test]
    async fn aborted_transfer_with_omitted_tag_discards_the_buffered_delivery() {
        let (mut inner, _session_rx, _outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);
        let aborted_tag = DeliveryTag::from(vec![0x01]);

        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, Some(vec![0x01]), true, false),
                encoded_binary_prefix(8),
            ))
            .await
            .unwrap();

        let mut abort = make_incoming_transfer(1, None, false, false);
        abort.resume = true;
        abort.aborted = true;
        incoming_tx
            .send(make_link_frame(abort, Payload::new()))
            .await
            .unwrap();

        let body = "after abort";
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(2, Some(vec![0x02]), false, false),
                encoded_message_payload(body),
            ))
            .await
            .unwrap();

        let delivery = inner.recv::<String>().await.expect("ordinary delivery");
        assert_eq!(delivery.body(), body);
        assert!(inner.incomplete_transfer.is_none());

        let guard = inner.link.unsettled().read();
        assert!(guard
            .as_ref()
            .is_none_or(|map| !map.contains_key(&aborted_tag)));
    }

    /// An aborted transfer with no buffered delivery is ignored.
    #[tokio::test]
    async fn aborted_transfer_without_buffer_is_ignored() {
        let (mut inner, _session_rx, _outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);

        let mut abort = make_incoming_transfer(1, Some(vec![0x09]), false, false);
        abort.resume = true;
        abort.aborted = true;
        incoming_tx
            .send(make_link_frame(abort, encoded_binary_prefix(4)))
            .await
            .unwrap();

        let body = "after ignored abort";
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(2, Some(vec![0x0A]), false, false),
                encoded_message_payload(body),
            ))
            .await
            .unwrap();

        let delivery = inner.recv::<String>().await.expect("ordinary delivery");
        assert_eq!(delivery.body(), body);
        assert!(inner.incomplete_transfer.is_none());
    }

    /// An aborted transfer that explicitly names another delivery while one
    /// is being assembled violates AMQP 1.0 §2.6.14 and closes the link.
    #[tokio::test]
    async fn aborted_transfer_with_different_delivery_tag_closes_link() {
        let (mut inner, _session_rx, mut outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);

        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(1, Some(vec![0x01]), true, false),
                encoded_binary_prefix(8),
            ))
            .await
            .unwrap();

        let mut abort = make_incoming_transfer(1, Some(vec![0x02]), false, false);
        abort.resume = true;
        abort.aborted = true;
        incoming_tx
            .send(make_link_frame(abort, Payload::new()))
            .await
            .unwrap();

        let error = recv_expecting_fatal_close(
            &mut inner,
            &mut outgoing_rx,
            &incoming_tx,
            definitions::ErrorCondition::from(definitions::AmqpError::NotAllowed),
        )
        .await;
        assert!(matches!(
            error,
            RecvError::InconsistentFieldInMultiFrameDelivery
        ));
        assert!(matches!(inner.link.local_state, LinkState::Closed));
    }

    /// AMQP 1.0 §2.6.13: a resumed delivery that is not in the local
    /// unsettled map is ignored; the link stays usable.
    #[tokio::test]
    async fn resumed_delivery_unknown_to_unsettled_map_is_ignored() {
        let (mut inner, _session_rx, _outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);

        let ignored_tag = DeliveryTag::from(vec![0x0A]);
        let mut resumed = make_incoming_transfer(1, Some(vec![0x0A]), false, false);
        resumed.resume = true;
        incoming_tx
            .send(make_link_frame(resumed, encoded_message_payload("ignored")))
            .await
            .unwrap();

        // A following ordinary message is delivered; the ignored one is not.
        let body = "ordinary";
        incoming_tx
            .send(make_link_frame(
                make_incoming_transfer(2, Some(vec![0x0B]), false, false),
                encoded_message_payload(body),
            ))
            .await
            .unwrap();

        let delivery = inner.recv::<String>().await.expect("ordinary delivery");
        assert_eq!(delivery.body(), body);

        let guard = inner.link.unsettled().read();
        assert!(guard
            .as_ref()
            .is_none_or(|map| !map.contains_key(&ignored_tag)));
    }

    /// A resumed delivery present in the local unsettled map is delivered.
    #[tokio::test]
    async fn resumed_delivery_known_to_unsettled_map_is_delivered() {
        let (mut inner, _session_rx, _outgoing_rx, incoming_tx) =
            make_receiver_inner_with_channels(4096);

        seed_unsettled(&inner.link.unsettled, &[vec![0x0A]]);

        let body = "resumed";
        let mut resumed = make_incoming_transfer(1, Some(vec![0x0A]), false, false);
        resumed.resume = true;
        incoming_tx
            .send(make_link_frame(resumed, encoded_message_payload(body)))
            .await
            .unwrap();

        let delivery = inner.recv::<String>().await.expect("resumed delivery");
        assert_eq!(delivery.body(), body);
    }

    /// A tagless continuation carrying a `Received` state is attributed to the
    /// buffered delivery instead of failing with `DeliveryTagIsNone`, and it
    /// trims the buffer to the reported section and offset.
    #[tokio::test]
    async fn tagless_received_state_transfer_trims_the_buffer() {
        let mut inner = make_receiver_inner(4096);
        let tag = DeliveryTag::from(vec![0x01]);

        seed_unsettled(inner.link.unsettled(), &[tag.to_vec()]);

        let payload = encoded_sectioned_payload();
        let full_len = payload.len() as u64;
        inner
            .test_transfer::<String>(
                make_incoming_transfer(1, Some(vec![0x01]), true, false),
                payload,
            )
            .await
            .unwrap();

        let mut state_transfer = make_incoming_transfer(1, None, true, false);
        state_transfer.state = Some(DeliveryState::Received(Received {
            section_number: 1,
            section_offset: 0,
        }));
        inner
            .test_transfer::<String>(state_transfer, Payload::new())
            .await
            .expect("a tagless state transfer must be attributed to the buffer");

        // The buffer was trimmed to the reported section.
        let incomplete = inner
            .incomplete_transfer
            .as_ref()
            .expect("the delivery is still being assembled");
        assert!(
            (incomplete.buffer.len() as u64) < full_len,
            "the Received state must trim the buffer"
        );

        let guard = inner.link.unsettled().read();
        let state = guard
            .as_ref()
            .and_then(|map| map.get(&tag))
            .and_then(|entry| entry.remote.clone());
        assert!(matches!(
            state,
            Some(DeliveryState::Received(Received {
                section_number: 1,
                ..
            }))
        ));
    }

    /// The minimal `Attach` a sender peer sends for this receiver link: it
    /// must carry a source and an initial delivery count so
    /// `on_incoming_attach` accepts it.
    fn peer_sender_attach() -> Attach {
        Attach {
            name: String::from("test-receiver"),
            handle: Handle(0),
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(Box::new(Source::default())),
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: Some(0),
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
        let mut inner = make_receiver_inner(4092);
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
            make_receiver_inner_with_channels(4096);

        let (result, (saw_attach, detaches)) = tokio::join!(
            inner.close_with_error(None),
            crate::link::test_util::drive_simultaneous_detach_race(
                session_rx,
                outgoing_rx,
                incoming_tx,
                peer_sender_attach(),
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
            make_receiver_inner_with_channels(4096);

        let (result, (saw_attach, detaches)) = tokio::join!(
            inner.detach_with_error(None),
            crate::link::test_util::drive_simultaneous_detach_race(
                session_rx,
                outgoing_rx,
                incoming_tx,
                peer_sender_attach(),
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
    #[tokio::test]
    async fn received_prefix_is_trimmed_before_admitting_new_payload_against_both_bounds() {
        let mut inner = make_receiver_inner(4096);
        inner.link.max_message_size = 100;
        inner.receive_budget = crate::link::receive_budget::ReceiveBudget::new(100);
        let budget = inner.receive_budget.clone();
        inner
            .test_transfer::<String>(
                make_incoming_transfer(1, Some(vec![1]), true, false),
                encoded_binary_prefix(100),
            )
            .await
            .unwrap();
        assert_eq!(budget.retained_bytes(), 100);
        let mut continuation = make_incoming_transfer(1, None, true, false);
        continuation.state = Some(DeliveryState::Received(Received {
            section_number: 0,
            section_offset: 10,
        }));
        tokio::time::timeout(
            std::time::Duration::from_millis(300),
            inner.test_transfer::<String>(continuation, Payload::from(vec![0; 10])),
        )
        .await
        .expect("a valid reduced prefix must not enter a closing exchange")
        .unwrap();
        assert_eq!(inner.incomplete_transfer.as_ref().unwrap().buffer.len(), 20);
        assert_eq!(budget.retained_bytes(), 20);
        drop(inner);
        assert_eq!(budget.retained_bytes(), 0);
    }
}
