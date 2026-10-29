//! Implements AMQP1.0 Link

use std::{marker::PhantomData, sync::Arc, sync::OnceLock};

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

pub use receiver::{
    CreditMode, Receiver, ReceiverCollectionStep, ReceiverDetachError, ReceiverDetachStep,
    ReceiverDisposer, ReceiverReadStep, ReceiverTransferEffect, ReceiverTransferObserver,
    ReceiverTransferProgress,
};
pub use sender::Sender;
use serde_amqp::serialized_size;
use tokio::sync::mpsc;
#[cfg(test)]
use tokio::sync::oneshot;

use crate::{
    control::SessionControl,
    endpoint::{
        self, AttachmentId, InputHandle, LinkDetach, LinkFlow, OutputAttachment, Settlement,
    },
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
pub use unsettled_store::{UnsettledCapacityExceeded, UnsettledLimits};

/// Internal purpose of Attach validation; close-only never reconciles deliveries.
pub(crate) enum AttachPurpose {
    Ordinary,
    CloseOnly,
}

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
    pub(crate) operation_dispatch: Option<Arc<std::sync::atomic::AtomicBool>>,
    // pub(crate) state_code: Arc<AtomicU8>,
    pub(crate) name: String,

    /// The local handle and the attachment it was allocated for
    pub(crate) output_handle: Option<OutputAttachment>,
    pub(crate) input_handle: Option<InputHandle>, // remote handle

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

    /// The negotiated max frame size (encoder max frame length), shared from
    /// the session and the connection; used to split transfers and attach
    /// frames that must fit within the frame size
    pub(crate) max_frame_size: usize,

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

    fn as_complete_attach(&self, handle: endpoint::OutputHandle, is_reattaching: bool) -> Attach {
        self.as_attach_inner(handle, is_reattaching, 1)
    }

    fn as_attach_inner(
        &self,
        handle: endpoint::OutputHandle,
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
        handle: endpoint::OutputHandle,
        is_reattaching: bool,
    ) -> Result<Attach, SendAttachErrorKind> {
        let mut denominator = 1usize; // This is going to be the denominator

        let mut attach = self.as_attach_inner(handle.clone(), is_reattaching, denominator);
        // `SizeSerializer` computes the serialized size without allocating a
        // buffer; only the size is needed here.
        while serialized_size(&attach).map_err(|_| SendAttachErrorKind::IllegalState)? // This should not happen
            > max_frame_size
        {
            denominator *= 2;

            attach = self.as_attach_inner(handle.clone(), is_reattaching, denominator);
        }

        Ok(attach)
    }

    /// # Cancel safety
    ///
    /// This is cancel safe: the only `.await` is on sending over
    /// `tokio::mpsc::Sender`, which is cancel safe.
    pub(crate) async fn send_attach_inner(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        is_reattaching: bool,
        _purpose: &AttachPurpose,
    ) -> Result<(), SendAttachErrorKind> {
        // Create Attach frame
        let handle = match &self.output_handle {
            Some(attachment) => attachment.handle.clone(),
            None => return Err(SendAttachErrorKind::IllegalState),
        };

        let unsettled_map_len = if is_reattaching {
            // If reattaching, the unsettled map MUST be null
            //
            // It is ok to clear the map here because link will always try to send attach
            // before it handles the remote attach.
            self.unsettled.clear_for_reattach();
            None
        } else {
            let guard = self.unsettled.read();
            guard.as_ref().map(|m| m.len())
        };

        let attach = match unsettled_map_len {
            Some(0) | None => self.as_complete_attach(handle, is_reattaching),
            Some(_) => {
                // The connection engine publishes the negotiated encoder max
                // frame size before the connection handle is created; links
                // are only attached after the connection is opened, so the
                // value is always populated.
                self.as_maybe_incomplete_attach(self.max_frame_size, handle, is_reattaching)?
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
                if let Some(flag) = &self.operation_dispatch {
                    flag.store(true, std::sync::atomic::Ordering::Release);
                }
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

impl<R, T, F, M> endpoint::LinkDetach for Link<R, T, F, M>
where
    R: role::IntoRole + Send + Sync,
    T: Send,
    F: AsRef<LinkFlowState<R>> + Send + Sync,
    M: AsDeliveryState + Send + Sync,
{
    type DetachError = DetachError;

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    fn on_detach_reply(&mut self, detach: Detach) -> Result<(), Self::DetachError> {
        #[cfg(feature = "tracing")]
        tracing::trace!(detach = ?detach);
        #[cfg(feature = "log")]
        log::trace!("RECV detach = {:?}", detach);

        // The engine only receives a detach here as the answer to a detach or
        // close it sent itself: a peer-initiated detach is answered by the
        // session relay at arrival and recorded via
        // `apply_remote_detach_outcome`.
        //
        // A crossing detach (closed while DetachSent, or open while
        // CloseSent) must not be applied here: the caller reattaches first
        // (§2.6.6). Without this check `apply_remote_detach_outcome` would
        // terminalize the link and break that handshake.
        match self.local_state {
            LinkState::DetachSent if !detach.closed => {}
            LinkState::CloseSent if detach.closed => {}
            _ => return Err(DetachError::IllegalState),
        }
        self.apply_remote_detach_outcome(detach)
            .map_err(DetachError::from)
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
        // Preserve the attached state if queuing is cancelled or refused before dispatch.
        let next_state = match (&self.local_state, closed) {
            (
                LinkState::Attached | LinkState::AttachSent | LinkState::IncompleteAttachSent,
                false,
            ) => LinkState::DetachSent,
            (LinkState::Attached, true) => LinkState::CloseSent,
            _ => return Err(DetachError::IllegalState),
        };

        match self.output_handle.clone() {
            Some(handle) => {
                let detach = Detach {
                    handle: handle.handle.clone().into(),
                    closed,
                    error,
                };

                #[cfg(feature = "tracing")]
                tracing::debug!("Sending detach: {:?}", detach);
                #[cfg(feature = "log")]
                log::debug!("Sending detach: {:?}", detach);

                // An error here means the session is already closed, so we can't send a detach
                writer
                    .send(LinkFrame::Detach(detach, handle.id))
                    .await // cancel safe
                    .map_err(|_| match self.session_stop_reason.get() {
                        Some(reason) => DetachError::SessionStopped(reason.clone()),
                        None => DetachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                    })?;

                self.local_state = next_state;
                if let Some(flag) = &self.operation_dispatch {
                    flag.store(true, std::sync::atomic::Ordering::Release);
                }
                // The receiver must parse the original attachment's admitted
                // prefix until its first native Detach, including after Close.
                // Sender/controller close retain their existing release rule.
                if !matches!(self.local_state, LinkState::DetachSent)
                    && R::into_role() != Role::Receiver
                {
                    self.output_handle.take();
                }
                Ok(())
            }
            None => Err(DetachError::IllegalState),
        }
    }

    /// See [`endpoint::LinkDetach::apply_remote_detach_outcome`] for the
    /// documented contract of this method.
    fn apply_remote_detach_outcome(
        &mut self,
        detach: Detach,
    ) -> Result<(), ApplyRemoteDetachError> {
        match self.local_state {
            LinkState::Attached
            | LinkState::AttachSent
            | LinkState::AttachReceived
            | LinkState::IncompleteAttachExchanged
            | LinkState::IncompleteAttachSent
            | LinkState::IncompleteAttachReceived
            | LinkState::DetachSent
            | LinkState::CloseSent => {
                self.local_state = if detach.closed {
                    LinkState::Closed
                } else {
                    LinkState::Detached
                };
                let _ = self.output_handle.take();
                match (detach.closed, detach.error) {
                    (true, Some(error)) => {
                        Err(ApplyRemoteDetachError::RemoteClosedWithError(error))
                    }
                    (true, None) => Ok(()),
                    (false, Some(error)) => {
                        Err(ApplyRemoteDetachError::RemoteDetachedWithError(error))
                    }
                    (false, None) => Ok(()),
                }
            }
            _ => Err(ApplyRemoteDetachError::IllegalState),
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

    pub fn with_output_handle(
        self,
        output_handle: OutputAttachment,
    ) -> LinkRelay<OutputAttachment> {
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

impl LinkRelay<OutputAttachment> {
    pub(crate) async fn send(&mut self, frame: LinkFrame) -> Result<(), LinkRelayError> {
        match self {
            LinkRelay::Sender { tx, .. } => tx.send(frame).await,
            LinkRelay::Receiver { tx, .. } => tx.send(frame).await,
        }
        .map_err(|_| LinkRelayError::UnattachedHandle)
    }

    #[allow(unused_variables)]
    /// Hands a flow to the link; a flow it answers with goes out for the
    /// relay's attachment.
    pub(crate) async fn on_incoming_flow(
        &mut self,
        flow: LinkFlow,
    ) -> Result<Option<(LinkFlow, AttachmentId)>, LinkRelayError> {
        match self {
            LinkRelay::Sender {
                flow_state,
                output_handle,
                tx,
                unsettled,
                ..
            } => {
                unsettled.check()?;
                #[cfg(feature = "transaction")]
                {
                    use serde_amqp::Value;
                    if let Some(Value::Binary(txn_id)) =
                        flow.properties.as_ref().and_then(|m| m.get(TXN_ID_KEY))
                    {
                        let frame = LinkFrame::Acquisition(txn_id.clone());
                        unsettled
                            .until_terminated(tx.send(frame))
                            .await?
                            .map_err(|_| LinkRelayError::UnattachedHandle)?;
                    }
                }

                let ret = flow_state
                    .produce((flow, output_handle.handle.clone()))
                    .await;
                Ok(ret.map(|flow| (flow, output_handle.id)))
            }
            LinkRelay::Receiver {
                flow_state,
                output_handle,
                unsettled,
                ..
            } => {
                unsettled.check()?;
                let ret = flow_state.on_incoming_flow(flow, output_handle.handle.clone());
                Ok(ret.map(|flow| (flow, output_handle.id)))
            }
        }
    }

    /// Returns whether an echo is needed
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub(crate) fn on_incoming_disposition(
        &mut self,
        _role: Role,
        settled: bool,
        state: Option<DeliveryState>,
        delivery_tag: DeliveryTag,
    ) -> Result<bool, UnsettledCapacityExceeded> {
        match self {
            LinkRelay::Sender {
                unsettled,
                receiver_settle_mode,
                ..
            } => {
                unsettled.check()?;
                let terminal = state.as_ref().is_some_and(DeliveryState::is_terminal);
                if settled || terminal {
                    if let Some(message) = unsettled.withdraw(&delivery_tag) {
                        let _ = message.settle_with_state(state);
                    }
                } else {
                    unsettled.set_state(&delivery_tag, state)?;
                }
                Ok(!settled
                    && terminal
                    && matches!(receiver_settle_mode, ReceiverSettleMode::Second))
            }
            LinkRelay::Receiver { unsettled, .. } => {
                unsettled.check()?;
                if settled {
                    unsettled.remove(&delivery_tag);
                } else {
                    unsettled.update(&delivery_tag, |entry| entry.remote_state(state))?;
                }
                Ok(false)
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
                output_handle,
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
                    unsettled.check()?;
                    if settled || transfer.aborted {
                        unsettled.remove(tag);
                        false
                    } else {
                        if first
                            && !transfer.resume
                            && transfer.delivery_id.is_some()
                            && transfer.message_format.is_some()
                            && tag.len() <= 32
                        {
                            if unsettled
                                .read()
                                .as_ref()
                                .is_some_and(|map| map.contains_key(tag))
                            {
                                return Err(LinkRelayError::InvalidTransfer);
                            }
                            unsettled.insert(
                                tag.clone(),
                                receiver_delivery::ReceiverDelivery::new(transfer.state.clone()),
                            )?;
                        }
                        unsettled
                            .read()
                            .as_ref()
                            .is_some_and(|map| map.contains_key(tag))
                    }
                } else {
                    false
                };
                *more = transfer.more && !transfer.aborted;
                *current_tag = if *more { tag.clone() } else { None };
                unsettled
                    .until_terminated(tx.send(LinkFrame::Transfer {
                        input_handle: InputHandle::from(transfer.handle.clone()),
                        attachment: output_handle.id,
                        performative: transfer,
                        payload,
                        window_slot,
                        queue_slot: None,
                    }))
                    .await?
                    .map_err(|_| LinkRelayError::UnattachedHandle)?;
                if first && known {
                    return Ok(delivery_id.zip(tag));
                }
                Ok(None)
            }
        }
    }

    /// Forward an incoming detach into the link engine's channel. A dropped
    /// or exhausted reader cannot delay the required peer reply.
    ///
    /// If the peer detached the link on its own, also returns the reply
    /// detach for the session to send back, and fails the still-pending
    /// deliveries when the detach closes the link. If the detach instead
    /// answers one the link sent itself, returns `None`: the engine's own
    /// close/detach procedure is waiting for it.
    ///
    /// Cancel safe: only `.await`s on sending over `tokio::mpsc::Sender`.
    pub(crate) async fn on_incoming_detach(
        &mut self,
        detach: Detach,
        remote_initiated: bool,
    ) -> Option<Detach> {
        let attachment = match self {
            LinkRelay::Sender { output_handle, .. } | LinkRelay::Receiver { output_handle, .. } => {
                output_handle.id
            }
        };
        let frame = LinkFrame::Detach(detach.clone(), attachment);
        match self {
            LinkRelay::Sender {
                tx,
                output_handle,
                unsettled,
                ..
            } => {
                // The peer closed the link: the store closes before the
                // forward, which can wait for room in the link's channel, so
                // no delivery registered meanwhile waits for an outcome.
                if remote_initiated && detach.closed {
                    unsettled.close(remote_close_error(&detach));
                }
                // Admitted teardown owns its actual reply even after Store exhaustion.
                let forwarded = tx.send(frame).await.is_ok();

                if !remote_initiated {
                    // The detach is the response to our own detach/close. The
                    // link endpoint's local handshake consumes the forwarded
                    // frame — unless the endpoint was dropped, in which case
                    // the forward fails and nothing else would fail the
                    // deliveries that are still pending on the closed link.
                    if !forwarded && detach.closed {
                        unsettled.close(remote_close_error(&detach));
                    }
                    return None;
                }

                Some(Detach {
                    handle: output_handle.clone().into(),
                    closed: detach.closed,
                    error: detach.error.clone(),
                })
            }
            LinkRelay::Receiver {
                tx, output_handle, ..
            } => {
                let _ = tx.send(frame).await;
                if !remote_initiated {
                    return None;
                }

                Some(Detach {
                    handle: output_handle.clone().into(),
                    closed: detach.closed,
                    error: detach.error.clone(),
                })
            }
        }
    }
}

/// The error a peer's closing detach fails a sender's deliveries with.
fn remote_close_error(detach: &Detach) -> LinkStateError {
    match detach.error.clone() {
        Some(error) => LinkStateError::RemoteClosedWithError(error),
        None => LinkStateError::RemoteClosed,
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
pub(crate) mod test_util {
    //! Shared test helpers for the link unit tests.
    //!
    //! `drive_simultaneous_detach_race` is a deterministic peer/session
    //! harness for the AMQP 1.0 §2.6.6 simultaneous-detach race: the peer's
    //! frames are played in scripted order off the frames the link under test
    //! emits, so no timing is involved. The mock peer is passive: it replies
    //! to the link's `Attach`/`Detach` rather than initiating its own
    //! reattach. It drives either side of the race (the closing side via
    //! `close_with_error` or the suspending side via `detach_with_error`).

    use fe2o3_amqp_types::{
        definitions::Handle,
        performatives::{Attach, Detach},
    };
    use tokio::sync::mpsc;

    use crate::{
        control::SessionControl,
        endpoint::{AttachmentId, OutputAttachment},
        link::LinkFrame,
        link::LinkRelay,
    };

    /// Drive the peer side of a §2.6.6 simultaneous-detach race, for either
    /// direction: the closing side (`close_with_error`) or the suspending
    /// side (`detach_with_error`).
    ///
    /// 1. answer the link's `AllocateLink` (capturing the new incoming
    ///    sender from the relay) so the reattach can proceed;
    /// 2. on the link's first detach, reply with a detach whose `closed` flag
    ///    is the opposite (the peer does the mirror action);
    /// 3. on the link's `Attach`, reply with the peer's `Attach`;
    /// 4. on the link's second detach, reply with a closing detach.
    ///
    /// Returns `(saw_attach, detaches)`.
    pub(crate) async fn drive_simultaneous_detach_race(
        mut session_rx: mpsc::Receiver<SessionControl>,
        mut outgoing_rx: mpsc::Receiver<LinkFrame>,
        initial_incoming_tx: mpsc::Sender<LinkFrame>,
        peer_attach: Attach,
    ) -> (bool, usize) {
        let mut incoming_tx = initial_incoming_tx;
        let mut saw_attach = false;
        let mut detaches = 0usize;

        loop {
            tokio::select! {
                ctrl = session_rx.recv() => match ctrl {
                    Some(SessionControl::GetIncomingWindow(responder)) => {
                        let _ = responder.send(16);
                    }
                    Some(SessionControl::AllocateLink {
                        link_relay,
                        responder,
                        ..
                    }) => {
                        incoming_tx = match link_relay {
                            LinkRelay::Sender { tx, .. } => tx,
                            LinkRelay::Receiver { tx, .. } => tx,
                        };
                        let _ = responder.send(Ok(OutputAttachment::for_tests(1)));
                    }
                    Some(_) => {}
                    None => break,
                },
                frame = outgoing_rx.recv() => match frame {
                    Some(LinkFrame::Detach(detach, _)) => {
                        detaches += 1;
                        // First detach: the peer does the mirror action, so
                        // reply with the opposite `closed`. Second: the reply
                        // to the closing detach we sent after reattaching.
                        let closed = if detaches == 1 { !detach.closed } else { true };
                        let _ = incoming_tx
                            .send(LinkFrame::Detach(
                                Detach {
                                    handle: Handle(0),
                                    closed,
                                    error: None,
                                },
                                AttachmentId::allocate(),
                            ))
                            .await;
                        if detaches >= 2 {
                            break;
                        }
                    }
                    Some(LinkFrame::Attach(_)) => {
                        saw_attach = true;
                        let _ = incoming_tx.send(LinkFrame::Attach(peer_attach.clone())).await;
                    }
                    Some(_) => {}
                    None => break,
                },
            }
        }

        (saw_attach, detaches)
    }
    /// Drive exact end handshakes, including an already-detached endpoint.
    pub(crate) async fn drive_exact_end(
        mut control: mpsc::Receiver<SessionControl>,
        mut outgoing: mpsc::Receiver<LinkFrame>,
        mut incoming: mpsc::Sender<LinkFrame>,
        peer_attach: Attach,
        mut at_attach: impl FnMut(),
        mut at_close: impl FnMut() -> Option<fe2o3_amqp_types::definitions::Error>,
    ) -> usize {
        let mut attached = false;
        let mut detaches = 0;
        loop {
            tokio::select! {
                next=control.recv()=>match next {
                    Some(SessionControl::GetIncomingWindow(reply))=>{reply.send(16).unwrap();},
                    Some(SessionControl::AllocateLink { link_relay,responder,.. })=>{
                        incoming=match link_relay { LinkRelay::Sender{tx,..}|LinkRelay::Receiver{tx,..}=>tx };
                        responder.send(Ok(OutputAttachment::for_tests(7))).unwrap();
                    }
                    Some(_) => {},
                    None=>panic!("native session disappeared before end completed"),
                },
                frame=outgoing.recv()=>match frame.expect("owned outgoing link queue") {
                    LinkFrame::Attach(attach)=>{
                        assert!(attach.unsettled.is_none());
                        assert!(!attach.incomplete_unsettled);
                        at_attach();attached=true;
                        incoming.send(LinkFrame::Attach(peer_attach.clone())).await.unwrap();
                        if peer_attach.incomplete_unsettled {return detaches;}
                    }
                    LinkFrame::Detach(detach,_)=>{
                        detaches+=1;
                        let error=if attached {
                            assert!(detach.closed);assert_eq!(detach.handle,7u32.into());
                            at_close()
                        } else {None};
                        incoming.send(LinkFrame::Detach(Detach {handle:peer_attach.handle.clone(),
                            closed:attached || !detach.closed,error},AttachmentId::allocate())).await.unwrap();
                        if attached {return detaches;}
                    }
                    frame=>panic!("unexpected work in close-only exchange: {frame:?}"),
                },
            }
        }
    }

    pub(crate) struct CorrelationGuard(pub std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for CorrelationGuard {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
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
    use crate::link::state::LinkFlowStateInner;
    fn transfer() -> Transfer {
        serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x55, 0x43])
            .unwrap()
    }
    #[tokio::test]
    async fn unsettled_receiver_relay_caps_count_and_bytes_before_forwarding() {
        for limits in [UnsettledLimits::new(1, 128), UnsettledLimits::new(10, 2)] {
            let (tx, mut frames) = mpsc::channel(8);
            let store = Arc::new(unsettled_store::Store::new(None, Some(limits)).unwrap());
            let flow = Arc::new(LinkFlowState::receiver(LinkFlowStateInner {
                initial_delivery_count: 0,
                delivery_count: 0,
                link_credit: 10,
                available: 0,
                drain: false,
                properties: None,
            }));
            let mut relay =
                LinkRelay::new_receiver(tx, flow, store.clone(), ReceiverSettleMode::Second)
                    .with_output_handle(crate::endpoint::OutputAttachment::for_tests(0));
            let mut frame = transfer();
            frame.more = false;
            let first = relay
                .on_incoming_transfer(frame.clone(), Payload::new(), None)
                .await;
            if limits.max_bytes == 2 {
                assert!(matches!(
                    first,
                    Err(LinkRelayError::UnsettledCapacityExceeded(_))
                ));
            } else {
                first.unwrap();
                frames.try_recv().unwrap();
                frame.delivery_id = Some(1);
                frame.delivery_tag = Some(vec![2].into());
                let result = relay
                    .on_incoming_transfer(frame, Payload::new(), None)
                    .await;
                assert!(matches!(
                    result,
                    Err(LinkRelayError::UnsettledCapacityExceeded(_))
                ));
            }
            assert!(frames.try_recv().is_err());
            assert_eq!(store.usage(), (0, 0));
            assert!(store.check().is_err());
        }
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
        let unsettled = Arc::new(unsettled_store::Store::new(Some(messages), None).unwrap());
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
        .with_output_handle(crate::endpoint::OutputAttachment::for_tests(0));
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
        assert!(!relay
            .on_incoming_disposition(Role::Receiver, false, Some(received.into()), tag.clone())
            .unwrap());
        assert!(matches!(
            outcome.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(unsettled.read().as_ref().unwrap().contains_key(&tag));
        assert!(relay
            .on_incoming_disposition(
                Role::Receiver,
                false,
                Some(fe2o3_amqp_types::messaging::Accepted {}.into()),
                tag
            )
            .unwrap());
        assert!(matches!(
            outcome.await.unwrap(),
            Ok(Some(DeliveryState::Accepted(_)))
        ));
    }

    #[tokio::test]
    async fn settlement_before_application_decode_and_fragment_continuations_use_one_identity() {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let (tx, mut input) = mpsc::channel(8);
            let unsettled = Arc::new(unsettled_store::Store::new(None, None).unwrap());
            let flow = Arc::new(LinkFlowState::receiver(state::LinkFlowStateInner {
                initial_delivery_count: 0,
                delivery_count: 0,
                link_credit: 4,
                available: 0,
                drain: false,
                properties: None,
            }));
            let mut relay = LinkRelay::new_receiver(tx, flow, unsettled.clone(), mode)
                .with_output_handle(crate::endpoint::OutputAttachment::for_tests(0));
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
            assert!(!relay
                .on_incoming_disposition(Role::Sender, true, None, tag.clone())
                .unwrap());
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
            let map = Arc::new(unsettled_store::Store::new(None, None).unwrap());
            let tag = transfer().delivery_tag.unwrap();
            if !settled {
                map.insert(tag.clone(), receiver_delivery::ReceiverDelivery::new(None))
                    .unwrap();
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
                    crate::endpoint::OutputAttachment::for_tests(0),
                    flow,
                    Arc::new(OnceLock::new()),
                    65532,
                );
            link.local_state = LinkState::Attached;
            let delivery: Delivery<fe2o3_amqp_types::messaging::Body<serde_amqp::Value>> = link
                .on_complete_transfer(
                    transfer(),
                    Payload::from_static(&[0, 0x53, 0x77, 0x40]),
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

#[cfg(test)]
mod detach_admission_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn attached_link() -> ReceiverLink<fe2o3_amqp_types::messaging::Target> {
        let flow = Arc::new(LinkFlowState::receiver(state::LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 1,
            available: 0,
            drain: false,
            properties: None,
        }));
        let mut link = Receiver::builder()
            .name("detach-admission")
            .source("source")
            .target("target")
            .create_link(
                Arc::new(unsettled_store::Store::new(None, None).unwrap()),
                crate::endpoint::OutputAttachment::for_tests(7),
                flow,
                Arc::new(OnceLock::new()),
                65532,
            );
        link.local_state = LinkState::Attached;
        link.operation_dispatch = Some(Arc::new(AtomicBool::new(false)));
        link
    }

    #[tokio::test]
    async fn rejected_detach_preserves_state_handle_and_session_stop_reason() {
        for closed in [false, true] {
            for stopped in [false, true] {
                let mut link = attached_link();
                if stopped {
                    link.session_stop_reason
                        .set(SessionStopReason::RemoteEnded)
                        .unwrap();
                }
                let (writer, receiver) = crate::session::transfer_queue::channel(1);
                drop(receiver);
                let error = link.send_detach(&writer, closed, None).await.unwrap_err();
                if stopped {
                    assert!(matches!(
                        error,
                        DetachError::SessionStopped(SessionStopReason::RemoteEnded)
                    ));
                } else {
                    assert!(matches!(error, DetachError::IllegalState));
                }
                assert!(matches!(link.local_state, LinkState::Attached));
                assert_eq!(link.output_handle.as_ref().unwrap().handle.0, 7);
                assert!(!link
                    .operation_dispatch
                    .as_ref()
                    .unwrap()
                    .load(Ordering::Acquire));
            }
        }
    }

    #[tokio::test]
    async fn accepted_detach_commits_one_frame_and_one_state_transition() {
        for closed in [false, true] {
            let mut link = attached_link();
            let (writer, mut receiver) = crate::session::transfer_queue::channel(2);
            link.send_detach(&writer, closed, None).await.unwrap();
            if closed {
                assert!(matches!(link.local_state, LinkState::CloseSent));
                assert_eq!(link.output_handle.as_ref().unwrap().handle.0, 7);
            } else {
                assert!(matches!(link.local_state, LinkState::DetachSent));
                assert_eq!(link.output_handle.as_ref().unwrap().handle.0, 7);
            }
            assert!(link
                .operation_dispatch
                .as_ref()
                .unwrap()
                .load(Ordering::Acquire));
            let LinkFrame::Detach(detach, _) = receiver.try_recv().unwrap() else {
                panic!("expected Detach");
            };
            assert_eq!(detach.handle, 7u32.into());
            assert_eq!(detach.closed, closed);
            assert!(matches!(
                link.send_detach(&writer, closed, None).await,
                Err(DetachError::IllegalState)
            ));
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert_eq!(link.output_handle.as_ref().unwrap().handle.0, 7);
        }
    }
    fn cap_control_relay_store() -> ArcReceiverUnsettledMap {
        Arc::new(unsettled_store::Store::new(None, Some(UnsettledLimits::new(1, 4096))).unwrap())
    }
    fn cap_control_exhaust_relay(
        store: &unsettled_store::Store<receiver_delivery::ReceiverDelivery>,
    ) -> UnsettledCapacityExceeded {
        let tag = store
            .read()
            .as_ref()
            .unwrap()
            .keys()
            .next()
            .unwrap()
            .clone();
        let state = fe2o3_amqp_types::messaging::Rejected {
            error: Some(definitions::Error::new(
                definitions::AmqpError::InternalError,
                Some("x".repeat(8192)),
                None,
            )),
        };
        store
            .update(&tag, |entry| entry.remote_state(Some(state.into())))
            .unwrap_err()
    }
    #[tokio::test]
    async fn cap_control_relay_transfer_wait_keeps_capacity_cause() {
        let store = cap_control_relay_store();
        let link = attached_link();
        let attachment = crate::endpoint::OutputAttachment::for_tests(0);
        let (tx, _frames) = mpsc::channel(1);
        tx.try_send(LinkFrame::Flow(LinkFlow::default(), attachment.id))
            .unwrap();
        let mut relay = LinkRelay::new_receiver(
            tx,
            link.flow_state.clone(),
            store.clone(),
            ReceiverSettleMode::Second,
        )
        .with_output_handle(attachment);
        let performative = Transfer {
            handle: definitions::Handle(0),
            delivery_id: Some(0),
            delivery_tag: Some(vec![1].into()),
            message_format: Some(0),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        };
        assert!(
            cap_control_observe_relay(
                &store,
                relay.on_incoming_transfer(performative, Payload::new(), None)
            )
            .await
        );
    }
    async fn cap_control_observe_relay<T, E: std::error::Error + 'static>(
        store: &unsettled_store::Store<receiver_delivery::ReceiverDelivery>,
        operation: impl std::future::Future<Output = Result<T, E>>,
    ) -> bool {
        let mut waiting = Box::pin(operation);
        assert!(futures_util::poll!(&mut waiting).is_pending());
        let cause = cap_control_exhaust_relay(store);
        match futures_util::poll!(&mut waiting) {
            std::task::Poll::Ready(Err(error)) => {
                matches!((&error as &dyn std::error::Error).downcast_ref::<LinkRelayError>(), Some(LinkRelayError::UnsettledCapacityExceeded(error)) if *error == cause)
            }
            _ => false,
        }
    }
}

#[cfg(test)]
#[path = "../../tests/support/detached_peer.rs"]
mod detached_peer;
#[cfg(test)]
#[path = "../../tests/support/interrupted_resume.rs"]
mod interrupted_resume;
#[cfg(test)]
mod cleanup_capacity_tests {
    use super::{
        delivery::UnsettledMessage,
        interrupted_resume::{CapConstructors, CapState},
        receiver_delivery::ReceiverDelivery,
    };
    use crate::{Payload, Receiver, Sender};

    // Invoke the real Store admission entry at the peer gate. Keeping this
    // trigger inside the crate avoids a test hook in the native public API.
    fn sender_cap(sender: &Sender) -> CapState {
        let store = sender.inner.link.unsettled.clone();
        let retained = store.clone();
        CapState {
            exhaust: Box::new(move || {
                let (tx, _outcome) = crate::completion::channel();
                assert!(store
                    .register(
                        vec![2].into(),
                        UnsettledMessage::new(Payload::from(vec![0; 8192]), None, 0, tx)
                    )
                    .is_err());
            }),
            check: Box::new(move || {
                assert!(retained.check().is_err());
            }),
        }
    }
    fn receiver_cap(receiver: &Receiver) -> CapState {
        let store = receiver.inner.link.unsettled.clone();
        let retained = store.clone();
        CapState {
            exhaust: Box::new(move || {
                let state = fe2o3_amqp_types::messaging::Rejected {
                    error: Some(fe2o3_amqp_types::definitions::Error::new(
                        fe2o3_amqp_types::definitions::AmqpError::InternalError,
                        Some("x".repeat(8192)),
                        None,
                    )),
                };
                assert!(store
                    .insert(vec![2].into(), ReceiverDelivery::new(Some(state.into())))
                    .is_err());
            }),
            check: Box::new(move || {
                assert!(retained.check().is_err());
            }),
        }
    }
    #[tokio::test(start_paused = true)]
    async fn admitted_resume_cleanup_outlives_interrupt_and_cap() {
        super::interrupted_resume::run(Some(CapConstructors {
            sender: sender_cap,
            receiver: receiver_cap,
        }))
        .await;
    }
}
