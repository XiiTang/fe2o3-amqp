//! Implements AMQP1.0 Link

use std::{marker::PhantomData, sync::Arc, sync::OnceLock};

use bytes::{BufMut, BytesMut};
use fe2o3_amqp_types::{
    definitions::{
        self, DeliveryNumber, DeliveryTag, MessageFormat, ReceiverSettleMode, Role,
        SenderSettleMode, SequenceNo, SessionError,
    },
    messaging::{DeliveryState, Received, Source, TargetArchetype},
    performatives::{Attach, Detach, Disposition, Transfer},
    primitives::{OrderedMap, Symbol},
};

pub use error::*;

pub use receiver::{CreditMode, Receiver, ReceiverDisposer};
pub use sender::Sender;
use serde::Serialize;
use serde_amqp::ser::Serializer;
use tokio::sync::{mpsc, oneshot};

use crate::{
    control::SessionControl,
    endpoint::{self, InputHandle, LinkAttach, LinkDetach, LinkFlow, OutputHandle, Settlement},
    link::delivery::UnsettledMessage,
    util::{AsDeliveryState, Consumer, Produce, Producer},
    Payload,
};

use self::{
    delivery::Delivery,
    resumption::ResumingDelivery,
    state::{LinkFlowState, LinkState},
    target_archetype::VerifyTargetArchetype,
};

cfg_transaction! {
    use crate::transaction::TXN_ID_KEY;
}

mod frame;
pub(crate) use frame::*;
pub mod builder;
pub mod delivery;
mod error;
mod incoming_recovery;
mod incomplete_transfer;
pub mod receive_budget;
pub mod receiver;
mod receiver_delivery;
mod receiver_link;
pub(crate) mod resumption;
pub mod sender;
mod sender_link;
pub(crate) mod shared_inner;
mod source;
pub(crate) mod state;
pub mod target_archetype;
pub(crate) mod unsettled_store;

/// Default amount of link credit
pub const DEFAULT_CREDIT: SequenceNo = 200;

/// An OrderedMap is used because Link may exchange their unsettled map
/// and `Map` should be considered ordered
pub(crate) type UnsettledMap<M> = OrderedMap<DeliveryTag, M>;

pub(crate) type SenderFlowState = Consumer<Arc<LinkFlowState<role::SenderMarker>>>;
pub(crate) type ReceiverFlowState = Arc<LinkFlowState<role::ReceiverMarker>>;

pub(crate) type SenderRelayFlowState = Producer<Arc<LinkFlowState<role::SenderMarker>>>;
pub(crate) type ReceiverRelayFlowState = ReceiverFlowState;

/// Type alias for sender link that ONLY represents the inner state of a Sender
pub(crate) type SenderLink<T> = Link<role::SenderMarker, T, SenderFlowState, UnsettledMessage>;

/// Type alias for receiver link that ONLY represents the inner state of receiver
pub(crate) type ReceiverLink<T> =
    Link<role::ReceiverMarker, T, ReceiverFlowState, receiver_delivery::ReceiverDelivery>;

pub(crate) type ArcUnsettledMap<S> = Arc<unsettled_store::Store<S>>;
pub(crate) type ArcSenderUnsettledMap = ArcUnsettledMap<UnsettledMessage>;
pub(crate) type ArcReceiverUnsettledMap = ArcUnsettledMap<receiver_delivery::ReceiverDelivery>;

pub mod role {
    //! Type state definition of link role

    use fe2o3_amqp_types::definitions::Role;

    /// Type state for link::builder::Builder
    #[derive(Debug)]
    pub struct SenderMarker {
        _private: (),
    }

    /// Type state for link::builder::Builder
    #[derive(Debug)]
    pub struct ReceiverMarker {
        _private: (),
    }

    /// Marker trait for role
    pub trait IntoRole {
        /// Get the role
        fn into_role() -> Role;
    }

    impl IntoRole for SenderMarker {
        fn into_role() -> Role {
            Role::Sender
        }
    }

    impl IntoRole for ReceiverMarker {
        fn into_role() -> Role {
            Role::Receiver
        }
    }
}

pub(crate) enum SenderAttachExchange {
    Complete,
    IncompleteUnsettled(Vec<(DeliveryTag, ResumingDelivery)>),
    Resume(Vec<(DeliveryTag, ResumingDelivery)>),
}

impl std::fmt::Debug for SenderAttachExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Complete => write!(f, "Complete"),
            Self::IncompleteUnsettled(_) => f.debug_tuple("IncompleteUnsettled(_)").finish(),
            Self::Resume(_) => f.debug_tuple("Resume(_)").finish(),
        }
    }
}

impl SenderAttachExchange {
    pub fn complete_or<E>(self, err: E) -> Result<(), E> {
        match self {
            Self::Complete => Ok(()),
            _ => Err(err),
        }
    }
}

/// Outcome of exchange of Attach frame on the receiver side
///
/// This is useful for exposing the outcome of resuming a local receiver
#[derive(Debug)]
#[must_use]
pub enum ReceiverAttachExchange {
    /// The attach exchange is completed without any unsettled deliveries
    Complete,

    /// At least one party indicated an incomplete unsettled map during the attach exchange
    IncompleteUnsettled,

    /// The link will be resuming
    Resume,
}

impl ReceiverAttachExchange {
    /// Returns `Ok(())` if the value is `Complete` otherwise returns `Err
    pub fn complete_or<E>(self, err: E) -> Result<(), E> {
        match self {
            Self::Complete => Ok(()),
            _ => Err(err),
        }
    }
}

/// Manages the link state
///
/// # Type Parameters
///
/// R: role
///
/// T: target
///
/// F: link flow state
///
/// M: unsettledMessage type
#[derive(Debug)]
pub(crate) struct Link<R, T, F, M> {
    pub(crate) role: PhantomData<R>,

    pub(crate) local_state: LinkState,
    // pub(crate) state_code: Arc<AtomicU8>,
    pub(crate) name: String,

    pub(crate) output_handle: Option<OutputHandle>, // local handle
    pub(crate) input_handle: Option<InputHandle>,   // remote handle

    /// The `Sender` will manage whether to wait for incoming disposition
    pub(crate) snd_settle_mode: SenderSettleMode,
    pub(crate) rcv_settle_mode: ReceiverSettleMode,

    pub(crate) source: Option<Source>,
    pub(crate) target: Option<T>,

    /// If zero, the max size is not set.
    /// If zero, the attach frame should treated is None
    pub(crate) max_message_size: u64,

    // capabilities
    pub(crate) offered_capabilities: Option<Vec<Symbol>>, // TODO: Add accessor fns
    pub(crate) desired_capabilities: Option<Vec<Symbol>>, // TODO: Add accessor fns

    /// See Section 2.6.7 Flow Control
    pub(crate) flow_state: F,
    pub(crate) unsettled: ArcUnsettledMap<M>,

    /// Why the session (or its connection) stopped, shared from the session
    pub(crate) session_stop_reason: Arc<OnceLock<SessionStopReason>>,

    pub(crate) verify_incoming_source: bool,
    pub(crate) verify_incoming_target: bool,
}

impl<R, T, F, M> Link<R, T, F, M>
where
    R: role::IntoRole + Send + Sync,
    T: Into<TargetArchetype> + TryFrom<TargetArchetype> + VerifyTargetArchetype + Clone + Send,
    F: AsRef<LinkFlowState<R>> + Send + Sync,
    M: AsDeliveryState + Send + Sync,
{
    fn get_unsettled_map(
        &self,
        is_reattaching: bool,
        partial_unsettled: usize,
    ) -> Option<OrderedMap<DeliveryTag, Option<DeliveryState>>> {
        // When reattaching (as opposed to resuming), the unsettled map MUST be null.
        if is_reattaching {
            return None;
        }

        let guard = self.unsettled.read();
        let map = guard.as_ref()?;
        match (map.len(), partial_unsettled) {
            (0, _) => None,
            (_, 0..=1) => {
                let v = map
                    .iter()
                    .map(|(key, val)| (key.clone(), val.as_delivery_state().clone()))
                    .collect();
                Some(v)
            }
            (total, denom) => {
                let len = total / denom;
                let v = (0..len)
                    .zip(map.iter())
                    .map(|(_, (key, val))| (key.clone(), val.as_delivery_state().clone()))
                    .collect();
                Some(v)
            }
        }
    }

    fn as_complete_attach(&self, handle: OutputHandle, is_reattaching: bool) -> Attach {
        self.as_attach_inner(handle, is_reattaching, 1)
    }

    fn as_attach_inner(
        &self,
        handle: OutputHandle,
        is_reattaching: bool,
        partial_unsettled: usize,
    ) -> Attach {
        let unsettled = self.get_unsettled_map(is_reattaching, partial_unsettled);

        let max_message_size = match self.max_message_size {
            0 => None,
            val => Some(val),
        };
        let initial_delivery_count = Some(self.flow_state.as_ref().initial_delivery_count());
        let properties = self.flow_state.as_ref().properties();
        let incomplete_unsettled = !matches!(partial_unsettled, 0..=1);

        Attach {
            name: self.name.clone(),
            handle: handle.into(),
            role: R::into_role(),
            snd_settle_mode: self.snd_settle_mode.clone(),
            rcv_settle_mode: self.rcv_settle_mode.clone(),
            source: self.source.clone().map(Box::new),
            target: self.target.clone().map(Into::into).map(Box::new),
            unsettled,
            incomplete_unsettled,

            // This MUST NOT be null if role is sender,
            // and it is ignored if the role is receiver.
            // See subsection 2.6.7.
            initial_delivery_count,

            max_message_size,
            offered_capabilities: self.offered_capabilities.clone().map(Into::into),
            desired_capabilities: self.desired_capabilities.clone().map(Into::into),
            properties,
        }
    }

    fn as_maybe_incomplete_attach(
        &self,
        max_frame_size: usize,
        handle: OutputHandle,
        is_reattaching: bool,
    ) -> Result<Attach, SendAttachErrorKind> {
        let mut denominator = 1usize; // This is going to be the denominator
        let mut buf = BytesMut::new();

        let mut attach = self.as_attach_inner(handle.clone(), is_reattaching, denominator);
        let mut serializer = Serializer::from((&mut buf).writer());
        attach
            .serialize(&mut serializer)
            .map_err(|_| SendAttachErrorKind::IllegalState)?; // This should not happen

        while buf.len() > max_frame_size {
            buf.clear();
            denominator *= 2;

            attach = self.as_attach_inner(handle.clone(), is_reattaching, denominator);
            let mut serializer = Serializer::from((&mut buf).writer());
            attach
                .serialize(&mut serializer)
                .map_err(|_| SendAttachErrorKind::IllegalState)?; // This should not happen
        }

        Ok(attach)
    }

    /// # Cancel safety
    ///
    /// This is cancel safe if oneshot channel is cancel safe
    pub(crate) async fn send_attach_inner(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        session: &mpsc::Sender<SessionControl>,
        is_reattaching: bool,
    ) -> Result<(), SendAttachErrorKind> {
        // Create Attach frame
        let handle = match &self.output_handle {
            Some(h) => h.clone(),
            None => return Err(SendAttachErrorKind::IllegalState),
        };

        let unsettled_map_len = if is_reattaching {
            // If reattaching, the unsettled map MUST be null
            //
            // It is ok to clear the map here because link will always try to send attach
            // before it handles the remote attach.
            let mut guard = self.unsettled.write();
            *guard = None;
            None
        } else {
            let guard = self.unsettled.read();
            guard.as_ref().map(|m| m.len())
        };

        let attach = match unsettled_map_len {
            Some(0) | None => self.as_complete_attach(handle, is_reattaching),
            Some(_) => {
                let max_frame_size = get_max_frame_size(session, &self.session_stop_reason).await?; // FIXME: cancel safe?
                self.as_maybe_incomplete_attach(max_frame_size, handle, is_reattaching)?
            }
        };
        let incomplete_unsettled = attach.incomplete_unsettled;
        let frame = LinkFrame::Attach(attach);

        match self.local_state {
            LinkState::Unattached
            | LinkState::Detached // May attempt to resume
            | LinkState::DetachSent => {
                writer.send(frame).await // cancel safe
                    .map_err(|_| match self.session_stop_reason.get() {
                        Some(reason) => SendAttachErrorKind::SessionStopped(reason.clone()),
                        None => SendAttachErrorKind::IllegalState, // defensive: no stop reason recorded; failure is link-local
                    })?;
                if incomplete_unsettled {
                    self.local_state = LinkState::IncompleteAttachSent
                } else {
                    self.local_state = LinkState::AttachSent
                }
            }
            LinkState::AttachReceived => {
                writer.send(frame).await // cancel safe
                    .map_err(|_| match self.session_stop_reason.get() {
                        Some(reason) => SendAttachErrorKind::SessionStopped(reason.clone()),
                        None => SendAttachErrorKind::IllegalState, // defensive: no stop reason recorded; failure is link-local
                    })?;
                if incomplete_unsettled {
                    self.local_state = LinkState::IncompleteAttachExchanged
                } else {
                    self.local_state = LinkState::Attached
                }
            }
            _ => return Err(SendAttachErrorKind::IllegalState),
        }

        Ok(())
    }
}

/// # Cancel safety
///
/// This should cancel safe if oneshot channel is cancel safe
pub(crate) async fn get_max_frame_size(
    control: &mpsc::Sender<SessionControl>,
    session_stop_reason: &OnceLock<SessionStopReason>,
) -> Result<usize, SendAttachErrorKind> {
    let (tx, rx) = oneshot::channel();
    control
        .send(SessionControl::GetMaxFrameSize(tx))
        .await // cancel safe
        .map_err(|_| match session_stop_reason.get() {
            Some(reason) => SendAttachErrorKind::SessionStopped(reason.clone()),
            None => SendAttachErrorKind::IllegalState, // defensive: no stop reason recorded; failure is link-local
        })?;
    rx.await // FIXME: is oneshot channel cancel safe?
        .map_err(|_| match session_stop_reason.get() {
            Some(reason) => SendAttachErrorKind::SessionStopped(reason.clone()),
            None => SendAttachErrorKind::IllegalState, // defensive: no stop reason recorded; failure is link-local
        })
}

impl<R, T, F, M> endpoint::LinkDetach for Link<R, T, F, M>
where
    R: role::IntoRole + Send + Sync,
    T: Send,
    F: AsRef<LinkFlowState<R>> + Send + Sync,
    M: AsDeliveryState + Send + Sync,
{
    type DetachError = DetachError;

    /// Closing or not isn't taken care of here but outside
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    fn on_incoming_detach(&mut self, detach: Detach) -> Result<(), Self::DetachError> {
        #[cfg(feature = "tracing")]
        tracing::trace!(detach = ?detach);
        #[cfg(feature = "log")]
        log::trace!("RECV detach = {:?}", detach);

        match detach.closed {
            true => match self.local_state {
                LinkState::Attached
                | LinkState::AttachSent
                | LinkState::AttachReceived
                | LinkState::IncompleteAttachExchanged
                | LinkState::IncompleteAttachSent
                | LinkState::IncompleteAttachReceived => {
                    self.local_state = LinkState::CloseReceived;
                    match detach.error {
                        Some(error) => Err(DetachError::RemoteClosedWithError(error)),
                        None => Ok(()),
                    }
                }
                LinkState::DetachSent => {
                    self.local_state = LinkState::CloseReceived;
                    match detach.error {
                        Some(error) => Err(DetachError::RemoteClosedWithError(error)),
                        None => Err(DetachError::ClosedByRemote),
                    }
                }
                LinkState::CloseSent => {
                    self.local_state = LinkState::Closed;
                    let _ = self.output_handle.take();
                    match detach.error {
                        Some(error) => Err(DetachError::RemoteClosedWithError(error)),
                        None => Ok(()),
                    }
                }
                _ => Err(DetachError::IllegalState),
            },
            false => {
                match self.local_state {
                    LinkState::Attached => self.local_state = LinkState::DetachReceived,
                    LinkState::DetachSent => {
                        self.local_state = LinkState::Detached;
                        // Dropping output handle as it is already detached
                        let _ = self.output_handle.take();
                    }
                    _ => return Err(DetachError::IllegalState),
                }

                match detach.error {
                    Some(error) => Err(DetachError::RemoteDetachedWithError(error)),
                    None => Ok(()),
                }
            }
        }
    }

    /// # Cancel safety
    ///
    /// This is cancel safe because it only .await on sending over `tokio::mpsc::Sender`
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn send_detach(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        closed: bool,
        error: Option<definitions::Error>,
    ) -> Result<(), Self::DetachError> {
        // Change the state whether sending the detach frame succeeds or not
        match (&self.local_state, closed) {
            (LinkState::Attached, false) => self.local_state = LinkState::DetachSent,
            (LinkState::DetachReceived, false) => self.local_state = LinkState::Detached,
            (LinkState::CloseReceived, false) => return Err(DetachError::ClosedByRemote),
            (LinkState::Attached, true) => self.local_state = LinkState::CloseSent,
            (LinkState::DetachReceived, true) => return Err(DetachError::DetachedByRemote),
            (LinkState::CloseReceived, true) => self.local_state = LinkState::Closed,
            _ => return Err(DetachError::IllegalState),
        };

        match self.output_handle.clone() {
            Some(handle) => {
                let detach = Detach {
                    handle: handle.into(),
                    closed,
                    error,
                };

                #[cfg(feature = "tracing")]
                tracing::debug!("Sending detach: {:?}", detach);
                #[cfg(feature = "log")]
                log::debug!("Sending detach: {:?}", detach);

                // An error here means the session is already closed, so we can't send a detach
                let result = writer
                    .send(LinkFrame::Detach(detach))
                    .await // cancel safe
                    .map_err(|_| match self.session_stop_reason.get() {
                        Some(reason) => DetachError::SessionStopped(reason.clone()),
                        None => DetachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                    });

                // Retain the identity while a non-closing Detach is in flight:
                // preceding Transfer frames still belong to this endpoint.
                if !matches!(self.local_state, LinkState::DetachSent) {
                    self.output_handle.take();
                }
                result
            }
            None => Err(DetachError::IllegalState),
        }
    }
}

#[derive(Debug)]
pub(crate) enum LinkRelay<O> {
    Sender {
        tx: mpsc::Sender<LinkIncomingItem>,
        output_handle: O,
        // This should be wrapped inside a Producer because the SenderLink
        // needs to consume link credit from LinkFlowState
        flow_state: SenderRelayFlowState,
        unsettled: ArcSenderUnsettledMap,
        receiver_settle_mode: ReceiverSettleMode,
    },
    Receiver {
        tx: mpsc::Sender<LinkIncomingItem>,
        output_handle: O,
        flow_state: ReceiverRelayFlowState,
        unsettled: ArcReceiverUnsettledMap,
        receiver_settle_mode: ReceiverSettleMode,
        more: bool,
        current_tag: Option<DeliveryTag>,
    },
}

impl LinkRelay<()> {
    pub fn new_sender(
        tx: mpsc::Sender<LinkIncomingItem>,
        flow_state: SenderRelayFlowState,
        unsettled: ArcSenderUnsettledMap,
    ) -> Self {
        Self::Sender {
            tx,
            output_handle: (),
            flow_state,
            unsettled,
            receiver_settle_mode: Default::default(),
        }
    }

    pub fn new_receiver(
        tx: mpsc::Sender<LinkIncomingItem>,
        flow_state: ReceiverRelayFlowState,
        unsettled: ArcReceiverUnsettledMap,
        receiver_settle_mode: ReceiverSettleMode,
    ) -> Self {
        Self::Receiver {
            tx,
            output_handle: (),
            flow_state,
            unsettled,
            receiver_settle_mode,
            more: false,
            current_tag: None,
        }
    }

    pub fn with_output_handle(self, output_handle: OutputHandle) -> LinkRelay<OutputHandle> {
        match self {
            LinkRelay::Sender {
                tx,
                flow_state,
                unsettled,
                receiver_settle_mode,
                ..
            } => LinkRelay::Sender {
                tx,
                output_handle,
                flow_state,
                unsettled,
                receiver_settle_mode,
            },
            LinkRelay::Receiver {
                tx,
                flow_state,
                unsettled,
                receiver_settle_mode,
                more,
                current_tag,
                ..
            } => LinkRelay::Receiver {
                tx,
                output_handle,
                flow_state,
                unsettled,
                receiver_settle_mode,
                more,
                current_tag,
            },
        }
    }
}

impl LinkRelay<OutputHandle> {
    pub(crate) async fn send(
        &mut self,
        frame: LinkFrame,
    ) -> Result<(), mpsc::error::SendError<LinkFrame>> {
        match self {
            LinkRelay::Sender { tx, .. } => tx.send(frame).await,
            LinkRelay::Receiver { tx, .. } => tx.send(frame).await,
        }
    }

    #[allow(unused_variables)]
    pub(crate) async fn on_incoming_flow(
        &mut self,
        flow: LinkFlow,
    ) -> Result<Option<LinkFlow>, LinkRelayError> {
        match self {
            LinkRelay::Sender {
                flow_state,
                output_handle,
                tx,
                ..
            } => {
                #[cfg(feature = "transaction")]
                {
                    use serde_amqp::Value;
                    if let Some(Value::Binary(txn_id)) =
                        flow.properties.as_ref().and_then(|m| m.get(TXN_ID_KEY))
                    {
                        let frame = LinkFrame::Acquisition(txn_id.clone());
                        tx.send(frame)
                            .await
                            .map_err(|_| LinkRelayError::UnattachedHandle)?;
                    }
                }

                let ret = flow_state.produce((flow, output_handle.clone())).await;
                Ok(ret)
            }
            LinkRelay::Receiver {
                flow_state,
                output_handle,
                ..
            } => {
                let ret = flow_state.on_incoming_flow(flow, output_handle.clone());
                Ok(ret)
            }
        }
    }

    /// Returns whether an echo is needed
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(crate) fn on_incoming_disposition(
        &mut self,
        _role: Role, // Is a role check necessary?
        settled: bool,
        state: Option<DeliveryState>,
        // Disposition only contains the delivery ids, which are assigned by the
        // sessions
        delivery_tag: DeliveryTag,
    ) -> bool {
        match self {
            LinkRelay::Sender {
                unsettled,
                receiver_settle_mode,
                ..
            } => {
                let echo = if settled {
                    // Upon receiving the updated delivery state from the receiver, the sender will, if it has not already spontaneously
                    // attained a terminal state (e.g., through the expiry of the TTL at the sender), update its view of the state and
                    // communicate this back to the sending application.

                    // Since we are settling (ie. forgetting) this message, we don't care whether the
                    // receiving end is alive or not
                    {
                        let mut guard = unsettled.write();
                        guard
                            .as_mut()
                            .and_then(|m| m.swap_remove(&delivery_tag))
                            .map(|msg| msg.settle_with_state(state));
                    }
                    false
                } else {
                    let is_terminal = match &state {
                        Some(s) => s.is_terminal(),
                        None => false, // Probably should not assume the state is not specified
                    };
                    {
                        let mut guard = unsettled.write();
                        // Once the receiving application has finished processing the message,
                        // it indicates to the link endpoint a **terminal delivery state** that
                        // reflects the outcome of the application processing
                        if is_terminal {
                            let _result = guard
                                .as_mut()
                                .and_then(|m| m.swap_remove(&delivery_tag))
                                .map(|msg| msg.settle_with_state(state));
                        } else if let Some(msg) =
                            guard.as_mut().and_then(|m| m.get_mut(&delivery_tag))
                        {
                            msg.state = state;
                        }
                    }

                    // If the receiver is in mode Second, it will send a non-settled terminal state
                    // to indicate end of processing
                    match receiver_settle_mode {
                        ReceiverSettleMode::First => {
                            // The receiver will spontaneously settle all incoming transfers.
                            false
                        }
                        ReceiverSettleMode::Second => {
                            // The receiver will only settle after sending the disposition to
                            // the sender and receiving a disposition indicating settlement of the
                            // delivery from the sender.

                            is_terminal
                        }
                    }
                };

                echo
            }
            LinkRelay::Receiver { unsettled, .. } => {
                if settled {
                    let mut guard = unsettled.write();
                    // let _state = remove_from_unsettled(unsettled, &delivery_tag).await;
                    let _state = guard.as_mut().and_then(|m| m.swap_remove(&delivery_tag));
                } else {
                    let mut guard = unsettled.write();
                    if let Some(msg_state) = guard.as_mut().and_then(|m| m.get_mut(&delivery_tag)) {
                        msg_state.remote_state(state);
                    }
                }

                // Only the sender needs to auto-reply to receiver's disposition, thus
                // `echo = false`
                false
            }
        }
    }

    /// LinkRelay operates in session's event loop
    ///
    /// The session needs a map of delivery_id and delivery_tag
    pub(crate) async fn on_incoming_transfer(
        &mut self,
        transfer: Transfer,
        payload: Payload,
        window_slot: Option<crate::session::receive_window::Slot>,
    ) -> Result<Option<(DeliveryNumber, DeliveryTag)>, LinkRelayError> {
        match self {
            LinkRelay::Sender { .. } => Err(LinkRelayError::TransferFrameToSender),
            LinkRelay::Receiver {
                tx,
                unsettled,
                more,
                current_tag,
                ..
            } => {
                let first = !*more;
                if first
                    && (transfer.delivery_id.is_none()
                        || transfer.delivery_tag.is_none()
                        || transfer.message_format.is_none())
                {
                    return Err(LinkRelayError::InvalidTransfer);
                }
                if transfer.delivery_tag.as_ref().is_some_and(|tag| {
                    tag.len() > 32
                        || (!first && current_tag.as_ref().is_some_and(|current| current != tag))
                }) {
                    return Err(LinkRelayError::InvalidTransfer);
                }
                let settled = transfer.settled.unwrap_or(false);
                let delivery_id = transfer.delivery_id;
                let tag = transfer
                    .delivery_tag
                    .clone()
                    .or_else(|| current_tag.clone());
                // Publish initial unsettled identity before handing bytes to the
                // receiver. A following disposition can otherwise race decoding.
                let known = if let Some(tag) = &tag {
                    let mut guard = unsettled.write();
                    if settled || transfer.aborted {
                        if let Some(map) = guard.as_mut() {
                            map.swap_remove(tag);
                        }
                        false
                    } else {
                        if first
                            && !transfer.resume
                            && transfer.delivery_id.is_some()
                            && transfer.message_format.is_some()
                            && tag.len() <= 32
                        {
                            let map = guard.get_or_insert_with(UnsettledMap::new);
                            if map.contains_key(tag) {
                                return Err(LinkRelayError::InvalidTransfer);
                            }
                            map.insert(
                                tag.clone(),
                                receiver_delivery::ReceiverDelivery::new(transfer.state.clone()),
                            );
                        }
                        guard.as_ref().is_some_and(|map| map.contains_key(tag))
                    }
                } else {
                    false
                };
                *more = transfer.more && !transfer.aborted;
                *current_tag = if *more { tag.clone() } else { None };
                tx.send(LinkFrame::Transfer {
                    input_handle: InputHandle::from(transfer.handle.clone()),
                    performative: transfer,
                    payload,
                    window_slot,
                    queue_slot: None,
                })
                .await
                .map_err(|_| LinkRelayError::UnattachedHandle)?;
                if first && known {
                    return Ok(delivery_id.zip(tag));
                }
                Ok(None)
            }
        }
    }

    /// This is cancel safe because it only .await on sending over `tokio::mpsc::Sender`
    pub async fn on_incoming_detach(
        &mut self,
        detach: Detach,
    ) -> Result<(), mpsc::error::SendError<LinkFrame>> {
        match self {
            LinkRelay::Sender { tx, .. } => {
                tx.send(LinkFrame::Detach(detach)).await?;
            }
            LinkRelay::Receiver { tx, .. } => {
                tx.send(LinkFrame::Detach(detach)).await?;
            }
        }
        Ok(())
    }
}

pub(crate) fn get_max_message_size(local: u64, remote: Option<u64>) -> u64 {
    let remote_max_msg_size = remote.unwrap_or(0);
    match local {
        0 => remote_max_msg_size,
        val => {
            if remote_max_msg_size == 0 {
                val
            } else {
                u64::min(val, remote_max_msg_size)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::link::state::LinkFlowStateInner;

    #[tokio::test]
    async fn test_producer_notify() {
        use std::sync::Arc;
        use tokio::sync::Notify;

        use super::*;
        use crate::endpoint::OutputHandle;
        use crate::util::{Produce, Producer};

        let notifier = Arc::new(Notify::new());
        let state = LinkFlowState::sender(LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 0,
            available: 0,
            drain: false,
            properties: None,
        });
        let mut producer = Producer::new(notifier.clone(), Arc::new(state));
        let notified = notifier.notified();

        let handle = tokio::spawn(async move {
            let item = (LinkFlow::default(), OutputHandle(0));
            producer.produce(item).await;
        });

        notified.await;
        handle.await.unwrap();
    }
}

#[cfg(test)]
mod incoming_identity_tests {
    use super::*;
    fn transfer() -> Transfer {
        serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x55, 0x43])
            .unwrap()
    }
    #[tokio::test]
    async fn second_settlement_waits_for_a_terminal_peer_outcome() {
        let (tx, _rx) = mpsc::channel(8);
        let (reply, mut outcome) = oneshot::channel();
        let tag: DeliveryTag = vec![1].into();
        let mut messages = UnsettledMap::new();
        messages.insert(
            tag.clone(),
            UnsettledMessage::new(Payload::new(), None, 0, reply),
        );
        let unsettled = Arc::new(unsettled_store::Store::new(Some(messages)));
        let flow = Arc::new(LinkFlowState::sender(state::LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 1,
            available: 0,
            drain: false,
            properties: None,
        }));
        let mut relay = LinkRelay::new_sender(
            tx,
            Producer::new(Arc::new(tokio::sync::Notify::new()), flow),
            unsettled.clone(),
        )
        .with_output_handle(OutputHandle(0));
        if let LinkRelay::Sender {
            receiver_settle_mode,
            ..
        } = &mut relay
        {
            *receiver_settle_mode = ReceiverSettleMode::Second;
        }
        let received = fe2o3_amqp_types::messaging::Received {
            section_number: 1,
            section_offset: 0,
        };
        assert!(!relay.on_incoming_disposition(
            Role::Receiver,
            false,
            Some(received.into()),
            tag.clone()
        ));
        assert!(matches!(
            outcome.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(unsettled.read().as_ref().unwrap().contains_key(&tag));
        assert!(relay.on_incoming_disposition(
            Role::Receiver,
            false,
            Some(fe2o3_amqp_types::messaging::Accepted {}.into()),
            tag
        ));
        assert!(matches!(
            outcome.await.unwrap(),
            Some(DeliveryState::Accepted(_))
        ));
    }

    #[tokio::test]
    async fn settlement_before_application_decode_and_fragment_continuations_use_one_identity() {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let (tx, mut input) = mpsc::channel(8);
            let unsettled = Arc::new(unsettled_store::Store::new(None));
            let flow = Arc::new(LinkFlowState::receiver(state::LinkFlowStateInner {
                initial_delivery_count: 0,
                delivery_count: 0,
                link_credit: 4,
                available: 0,
                drain: false,
                properties: None,
            }));
            let mut relay = LinkRelay::new_receiver(tx, flow, unsettled.clone(), mode)
                .with_output_handle(OutputHandle(0));
            let mut first = transfer();
            first.more = true;
            let tag = first.delivery_tag.clone().unwrap();
            assert_eq!(
                relay
                    .on_incoming_transfer(first.clone(), Payload::new(), None)
                    .await
                    .unwrap(),
                Some((0, tag.clone()))
            );
            assert!(unsettled.read().as_ref().unwrap().contains_key(&tag));
            assert!(!relay.on_incoming_disposition(Role::Sender, true, None, tag.clone()));
            assert!(!unsettled.read().as_ref().unwrap().contains_key(&tag));
            let mut next = first.clone();
            next.delivery_id = None;
            next.delivery_tag = None;
            next.message_format = None;
            assert_eq!(
                relay
                    .on_incoming_transfer(next.clone(), Payload::new(), None)
                    .await
                    .unwrap(),
                None
            );
            next.more = false;
            assert_eq!(
                relay
                    .on_incoming_transfer(next, Payload::new(), None)
                    .await
                    .unwrap(),
                None
            );
            assert!(!unsettled.read().as_ref().unwrap().contains_key(&tag));
            first.delivery_id = Some(1);
            first.delivery_tag = Some(vec![0x56].into());
            first.more = false;
            assert_eq!(
                relay
                    .on_incoming_transfer(first, Payload::new(), None)
                    .await
                    .unwrap(),
                Some((1, vec![0x56].into()))
            );
            assert!(matches!(
                input.recv().await.unwrap(),
                LinkFrame::Transfer { .. }
            ));
        }
    }
    #[test]
    fn completed_decode_never_recreates_a_delivery_settled_before_application_read() {
        use crate::endpoint::ReceiverLink as _;
        for settled in [false, true] {
            let map = Arc::new(unsettled_store::Store::new(None));
            let tag = transfer().delivery_tag.unwrap();
            if !settled {
                map.write()
                    .get_or_insert_with(UnsettledMap::new)
                    .insert(tag.clone(), receiver_delivery::ReceiverDelivery::new(None));
            }
            let flow = Arc::new(LinkFlowState::receiver(state::LinkFlowStateInner {
                initial_delivery_count: 0,
                delivery_count: 0,
                link_credit: 4,
                available: 0,
                drain: false,
                properties: None,
            }));
            let mut link = Receiver::builder()
                .name("race")
                .source("source")
                .target("target")
                .receiver_settle_mode(ReceiverSettleMode::Second)
                .create_link(
                    map.clone(),
                    OutputHandle(0),
                    flow,
                    Arc::new(OnceLock::new()),
                );
            link.local_state = LinkState::Attached;
            let delivery: Delivery<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>> = link
                .on_complete_transfer(
                    transfer(),
                    &Payload::from_static(&[0, 0x53, 0x77, 0x40]),
                    1,
                    0,
                )
                .unwrap();
            assert_eq!(delivery.delivery_tag(), &tag);
            assert_eq!(
                map.read().as_ref().is_some_and(|m| m.contains_key(&tag)),
                !settled
            );
        }
    }
}
