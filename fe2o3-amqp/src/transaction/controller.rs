use fe2o3_amqp_types::{
    definitions::{self, SenderSettleMode},
    messaging::{Accepted, DeliveryState, Message, SerializableBody},
    transaction::{Coordinator, Declare, Declared, Discharge, TransactionId},
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{oneshot, Mutex};

use crate::{
    endpoint::Settlement,
    link::{
        self,
        builder::{WithSource, WithoutName, WithoutTarget},
        role,
        sender::SenderInner,
        shared_inner::LinkEndpointInnerDetach,
        LinkStateError, SendError, SenderAttachError, SenderLink,
    },
    session::SessionHandle,
    Sendable,
};

use super::ControllerSendError;
#[cfg(docsrs)]
use super::{OwnedTransaction, Transaction};

pub(crate) type ControlLink = SenderLink<Coordinator>;

/// Transaction controller
///
/// This represents the controller side of a control link. The usage is similar to that of [`crate::Sender`]
/// but doesn't allow user to send any custom messages as the control link is purely used for declaring
/// and discharging transactions. Please also see [`Transaction`] and [`OwnedTransaction`]
///
/// # Example
///
/// ```rust,ignore
/// let controller = Controller::attach(&mut session, "controller").await.unwrap();
/// let mut txn = Transaction::declare(&controller, None).await.unwrap();
/// txn.commit().await.unwrap();
/// controller.close().await.unwrap();
/// ```
#[derive(Debug)]
pub struct Controller {
    pub(crate) inner: Mutex<Option<SenderInner<ControlLink>>>,
}

#[inline]
async fn send_on_control_link<T>(
    sender: &mut SenderInner<ControlLink>,
    sendable: Sendable<T>,
) -> Result<oneshot::Receiver<Result<Option<DeliveryState>, LinkStateError>>, link::SendError>
where
    T: SerializableBody,
{
    match sender
        .send_with_state::<T, link::SendError>(sendable, None, false)
        .await?
    {
        Settlement::Settled(_) => Err(SendError::IllegalDeliveryState),
        Settlement::Unsettled {
            delivery_tag: _,
            outcome,
        } => Ok(outcome),
    }
}

/// Send a declare message on the control link to obtain a transaction identifier.
pub(crate) async fn declare_on_link(
    inner: &mut SenderInner<ControlLink>,
    global_id: Option<TransactionId>,
) -> Result<Declared, ControllerSendError> {
    // To begin transactional work, the transaction controller needs to obtain a transaction
    // identifier from the resource. It does this by sending a message to the coordinator whose
    // body consists of the declare type in a single amqp-value section. Other standard message
    // sections such as the header section SHOULD be ignored.
    let declare = Declare { global_id };
    let message = Message::builder().value(declare).build();
    // This message MUST NOT be sent settled as the sender is REQUIRED to receive and interpret
    // the outcome of the declare from the receiver
    let sendable = Sendable::builder().message(message).settled(false).build();

    let outcome = send_on_control_link(inner, sendable)
        .await?
        .await
        .map_err(|_| match inner.link.session_stop_reason.get() {
            Some(reason) => LinkStateError::SessionStopped(reason.clone()),
            None => LinkStateError::IllegalState, // defensive: no stop reason recorded; failure is link-local
        })?;
    let outcome = outcome?;
    outcome
        .ok_or(ControllerSendError::NonTerminalDeliveryState)?
        .declared_or_else(|state| {
            if let DeliveryState::Rejected(rejected) = state {
                ControllerSendError::Rejected(rejected)
            } else {
                ControllerSendError::IllegalDeliveryState
            }
        })
}

/// Send a discharge message on the control link.
pub(crate) async fn discharge_on_link(
    inner: &mut SenderInner<ControlLink>,
    txn_id: TransactionId,
    fail: impl Into<Option<bool>>,
) -> Result<Accepted, ControllerSendError> {
    let discharge = Discharge {
        txn_id,
        fail: fail.into(),
    };
    // As with the declare message, it is an error if the sender sends the transfer pre-settled.
    let message = Message::builder().value(discharge).build();
    let sendable = Sendable::builder().message(message).settled(false).build();

    let outcome = send_on_control_link(inner, sendable)
        .await?
        .await
        .map_err(|_| match inner.link.session_stop_reason.get() {
            Some(reason) => LinkStateError::SessionStopped(reason.clone()),
            None => LinkStateError::IllegalState, // defensive: no stop reason recorded; failure is link-local
        })?;
    let outcome = outcome?;
    outcome
        .ok_or(ControllerSendError::NonTerminalDeliveryState)?
        .accepted_or_else(|state| {
            if let DeliveryState::Rejected(rejected) = state {
                ControllerSendError::Rejected(rejected)
            } else {
                ControllerSendError::IllegalDeliveryState
            }
        })
}

impl Controller {
    /// Creates a new builder for controller
    pub fn builder() -> link::builder::Builder<
        role::SenderMarker,
        Coordinator,
        WithoutName,
        WithSource,
        WithoutTarget,
    > {
        link::builder::Builder::<
            role::SenderMarker,
            Coordinator,
            WithoutName,
            WithSource,
            WithoutTarget,
        >::new()
    }

    /// Close the control link with error
    pub async fn close_with_error(
        mut self,
        error: definitions::Error,
    ) -> Result<(), link::DetachError> {
        self.inner
            .get_mut()
            .as_mut()
            .expect("owned controller")
            .close_with_error(Some(error))
            .await
    }

    /// Close the link
    pub async fn close(self) -> Result<(), link::DetachError> {
        self.close_retained().await
    }

    /// Close without consuming ownership. An interrupted waiter retains the
    /// native endpoint so its owner can inspect and stop its parent safely.
    pub async fn close_retained(&self) -> Result<(), link::DetachError> {
        self.close_retained_inner(None).await
    }
    /// Retained close with dispatch metadata that survives cancellation.
    pub async fn close_retained_tracked(
        &self,
        dispatch: Arc<AtomicBool>,
    ) -> Result<(), link::DetachError> {
        self.close_retained_inner(Some(dispatch)).await
    }
    async fn close_retained_inner(
        &self,
        dispatch: Option<Arc<AtomicBool>>,
    ) -> Result<(), link::DetachError> {
        let mut guard = self.inner.lock().await;
        let inner = guard.as_mut().expect("retained controller");
        inner.link.operation_dispatch = dispatch;
        inner.close_with_error(None).await
    }
    /// Attach a coordinator with dispatch metadata at the native queue boundary.
    pub async fn attach_with_coordinator_tracked<R>(
        session: &mut SessionHandle<R>,
        name: impl Into<String>,
        coordinator: Coordinator,
        dispatch: Arc<AtomicBool>,
    ) -> Result<Self, SenderAttachError> {
        Self::builder()
            .name(name)
            .coordinator(coordinator)
            .sender_settle_mode(SenderSettleMode::Unsettled)
            .attach_tracked(session, dispatch)
            .await
    }

    /// The coordinator capabilities from the peer's Attach.
    pub async fn capabilities(&self) -> Vec<fe2o3_amqp_types::transaction::TxnCapability> {
        self.inner
            .lock()
            .await
            .as_ref()
            .and_then(|inner| inner.link.target.as_ref())
            .and_then(|target| target.capabilities.clone())
            .map(|array| array.into_inner())
            .unwrap_or_default()
    }

    /// Attach the controller with the default [`Coordinator`]
    pub async fn attach<R>(
        session: &mut SessionHandle<R>,
        name: impl Into<String>,
    ) -> Result<Self, SenderAttachError> {
        Self::attach_with_coordinator(session, name, Coordinator::default()).await
    }

    /// Attach the controller with a customized [`Coordinator`]
    pub async fn attach_with_coordinator<R>(
        session: &mut SessionHandle<R>,
        name: impl Into<String>,
        coordinator: Coordinator,
    ) -> Result<Self, SenderAttachError> {
        Self::builder()
            .name(name)
            .coordinator(coordinator)
            .sender_settle_mode(SenderSettleMode::Unsettled)
            .attach(session)
            .await
    }

    /// Consume the controller and return the underlying control link.
    ///
    /// The controller must not be shared with any borrowed [`Transaction`] at this point.
    pub(crate) fn into_inner(mut self) -> SenderInner<ControlLink> {
        self.inner.get_mut().take().expect("owned controller")
    }
}

impl Drop for Controller {
    fn drop(&mut self) {
        use crate::endpoint::LinkExt;
        if let Some(inner) = self.inner.get_mut().as_mut() {
            // Local ownership release must not silently roll back remote work.
            inner.link.output_handle_mut().take();
        }
    }
}

/// A transaction with a shared controller, suitable for a resource registry.
/// Dropping it sends neither Discharge nor Detach.
#[derive(Debug)]
pub struct SharedTransaction {
    controller: Arc<Controller>,
    declared: Declared,
    dispatched: Arc<AtomicBool>,
    completed: bool,
}
impl SharedTransaction {
    /// Declare on an existing controller. Dispatch metadata survives cancellation.
    pub async fn declare(
        controller: Arc<Controller>,
        global_id: Option<TransactionId>,
        dispatched: Arc<AtomicBool>,
    ) -> Result<Self, ControllerSendError> {
        let declared = {
            let mut guard = controller.inner.lock().await;
            let inner = guard.as_mut().expect("shared controller");
            let message = Message::builder().value(Declare { global_id }).build();
            let outcome = inner.send_control_tracked(message, &dispatched).await?;
            outcome.declared_or_else(|state| match state {
                DeliveryState::Rejected(rejected) => ControllerSendError::Rejected(rejected),
                _ => ControllerSendError::IllegalDeliveryState,
            })?
        };
        Ok(Self {
            controller,
            declared,
            dispatched: Arc::new(AtomicBool::new(false)),
            completed: false,
        })
    }

    /// True once the first Discharge Transfer entered the native session queue.
    pub fn discharge_dispatched(&self) -> bool {
        self.dispatched.load(Ordering::Acquire)
    }

    /// Metadata that remains available when a caller's future is cancelled.
    pub fn dispatch_tracker(&self) -> Arc<AtomicBool> {
        self.dispatched.clone()
    }
    /// Bind an operation's metadata before any Discharge was dispatched.
    pub fn set_dispatch_tracker(&mut self, tracker: Arc<AtomicBool>) {
        if !self.discharge_dispatched() {
            self.dispatched = tracker;
        }
    }
}
impl super::TransactionBase for SharedTransaction {
    fn txn_id(&self) -> &TransactionId {
        &self.declared.txn_id
    }
}
impl super::TransactionDischarge for SharedTransaction {
    type Error = ControllerSendError;
    fn is_discharged(&self) -> bool {
        self.completed
    }
    async fn discharge(&mut self, fail: bool) -> Result<(), Self::Error> {
        if self.completed {
            return Ok(());
        }
        if self.discharge_dispatched() {
            return Err(ControllerSendError::DischargeOutcomeUnknown);
        }
        let mut guard = self.controller.inner.lock().await;
        let inner = guard.as_mut().expect("shared controller");
        let message = Message::builder()
            .value(Discharge {
                txn_id: self.declared.txn_id.clone(),
                fail: Some(fail),
            })
            .build();
        inner
            .send_control_tracked(message, &self.dispatched)
            .await?
            .accepted_or_else(|state| match state {
                DeliveryState::Rejected(rejected) => ControllerSendError::Rejected(rejected),
                _ => ControllerSendError::IllegalDeliveryState,
            })?;
        self.completed = true;
        Ok(())
    }
}
impl super::TransactionPosting for SharedTransaction {}
impl super::TransactionRetirement for SharedTransaction {
    type RetireError = link::DispositionError;
}

/// Return locally reserved credit when no Transfer reached the session queue.
struct ControlCredit<'a> {
    flow: Arc<crate::link::state::LinkFlowState<role::SenderMarker>>,
    tag: u32,
    dispatched: &'a AtomicBool,
}
impl Drop for ControlCredit<'_> {
    fn drop(&mut self) {
        if self.dispatched.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.flow.lock.write();
        // A peer drain can advance the sequence independently; preserve that
        // newer flow epoch rather than undoing its discarded credit.
        if state.delivery_count == self.tag.wrapping_add(1) {
            state.delivery_count = self.tag;
            state.link_credit = state.link_credit.saturating_add(1);
        }
    }
}

impl SenderInner<ControlLink> {
    async fn send_control_tracked<T: SerializableBody>(
        &mut self,
        message: Message<T>,
        dispatched: &AtomicBool,
    ) -> Result<DeliveryState, ControllerSendError> {
        use crate::endpoint::LinkExt;
        use fe2o3_amqp_types::messaging::message::__private::Serializable;
        let payload = bytes::Bytes::from(
            serde_amqp::to_vec(&Serializable(message))
                .map_err(|_| ControllerSendError::MessageEncodeError)?,
        );
        if let Some(max_size) = self.link.max_message_size() {
            if payload.len() as u64 > max_size {
                return Err(ControllerSendError::MessageSizeExceeded(
                    link::MessageSizeExceeded {
                        size: payload.len() as u64,
                        max_size,
                    },
                ));
            }
        }
        let tag = self
            .link
            .get_delivery_tag_or_detached(self.incoming.recv())
            .await?;
        let _credit = ControlCredit {
            flow: self.link.flow_state.state().clone(),
            tag: u32::from_be_bytes(tag),
            dispatched,
        };
        let transfer = self.link.generate_non_resuming_transfer_performative(
            tag.to_vec().into(),
            0,
            Some(false),
            None,
            false,
        )?;
        let settlement = self
            .link
            .send_payload_with_transfer_tracked(
                &self.outgoing,
                0,
                transfer,
                payload,
                Some(dispatched),
            )
            .await?;
        let Settlement::Unsettled { outcome, .. } = settlement else {
            return Err(ControllerSendError::IllegalDeliveryState);
        };
        outcome
            .await
            .map_err(|_| match self.link.session_stop_reason.get() {
                Some(reason) => LinkStateError::SessionStopped(reason.clone()),
                None => LinkStateError::IllegalState,
            })??
            .ok_or(ControllerSendError::NonTerminalDeliveryState)
    }
}

#[cfg(test)]
mod tracked_credit_tests {
    use super::*;
    use crate::{
        link::{
            state::{LinkFlowState, LinkFlowStateInner},
            LinkFrame,
        },
        util::{Consume, Consumer},
    };
    use fe2o3_amqp_types::performatives::Transfer;
    fn credit(count: u32) -> crate::link::SenderFlowState {
        Consumer::new(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(LinkFlowState::sender(LinkFlowStateInner {
                initial_delivery_count: count,
                delivery_count: count,
                link_credit: 1,
                available: 0,
                drain: false,
                properties: None,
            })),
        )
    }
    fn frame(tag: [u8; 4]) -> LinkFrame {
        LinkFrame::Transfer {
            input_handle: crate::endpoint::InputHandle(0),
            window_slot: None,
            performative: Transfer {
                handle: 0.into(),
                delivery_id: None,
                delivery_tag: Some(tag.to_vec().into()),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            },
            payload: bytes::Bytes::from_static(b"control"),
            queue_slot: None,
        }
    }
    #[tokio::test]
    async fn cancelled_control_before_queue_dispatch_restores_credit_and_sequence() {
        for count in [0, u32::MAX] {
            let flow = credit(count);
            let dispatched = AtomicBool::new(false);
            let (writer, mut queued) = crate::session::transfer_queue::channel(1);
            writer.send(frame([0; 4])).await.unwrap();
            let mut operation = Box::pin(async {
                let tag = flow.consume(1).await;
                let _credit = ControlCredit {
                    flow: flow.state().clone(),
                    tag: u32::from_be_bytes(tag),
                    dispatched: &dispatched,
                };
                writer.send(frame(tag)).await.unwrap();
                dispatched.store(true, Ordering::Release);
            });
            assert!(futures_util::poll!(&mut operation).is_pending());
            drop(operation);
            assert!(!dispatched.load(Ordering::Acquire));
            assert_eq!(flow.state().lock.read().delivery_count, count);
            assert_eq!(flow.state().lock.read().link_credit, 1);
            queued.recv().await.unwrap();
            assert!(queued.try_recv().is_err());
            assert_eq!(u32::from_be_bytes(flow.consume(1).await), count);
        }
    }
    #[tokio::test]
    async fn dispatched_control_keeps_consumed_credit() {
        let flow = credit(0);
        let dispatched = AtomicBool::new(false);
        let tag = flow.consume(1).await;
        let (writer, mut queued) = crate::session::transfer_queue::channel(1);
        let reservation = ControlCredit {
            flow: flow.state().clone(),
            tag: u32::from_be_bytes(tag),
            dispatched: &dispatched,
        };
        writer.send(frame(tag)).await.unwrap();
        dispatched.store(true, Ordering::Release);
        drop(reservation);
        assert_eq!(flow.state().lock.read().delivery_count, 1);
        assert_eq!(flow.state().lock.read().link_credit, 0);
        assert!(queued.recv().await.is_some());
    }
}
