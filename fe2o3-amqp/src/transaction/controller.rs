use fe2o3_amqp_types::{
    definitions::{self, SenderSettleMode},
    messaging::{Accepted, DeliveryState, Message, SerializableBody},
    transaction::{Coordinator, Declare, Declared, Discharge, TransactionId},
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Mutex;

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

/// Opaque native attachment identity used only for command correlation.
#[derive(Clone, PartialEq, Eq)]
pub struct ControlAttachment(crate::endpoint::AttachmentId);
impl std::fmt::Debug for ControlAttachment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ControlAttachment")
    }
}

/// Incoming-frame fences from the fixed owner of this command.
pub trait ControlProbe: Send + Sync {
    /// Capture a fence after all command fragments were admitted.
    fn dispatch_fence(&self) -> u64;
    /// The matching observed Detach sequence, if available.
    fn detach_sequence(&self, attachment: &ControlAttachment) -> Option<u64>;
}

/// Read-only native dispatch and answer provenance for one control request.
#[derive(Clone)]
pub struct ControlReceipt {
    facts: Box<ControlReceiptFacts>,
}
#[derive(Clone)]
struct ControlReceiptFacts {
    attachment: Option<ControlAttachment>,
    complete_fence: Option<u64>,
    detach_sequence: Option<u64>,
    answered: Option<DeliveryState>,
}
impl std::fmt::Debug for ControlReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlReceipt")
            .field("complete", &self.complete())
            .finish_non_exhaustive()
    }
}
impl ControlReceipt {
    /// The native attachment fixed at first admission.
    pub fn attachment(&self) -> Option<&ControlAttachment> {
        self.facts.attachment.as_ref()
    }
    /// Incoming fence after final admission.
    pub fn complete_fence(&self) -> Option<u64> {
        self.facts.complete_fence
    }
    /// Whether the final command fragment was admitted.
    pub fn complete(&self) -> bool {
        self.facts.complete_fence.is_some()
    }
    /// The fixed attachment's observed Detach sequence.
    pub fn detach_sequence(&self) -> Option<u64> {
        self.facts.detach_sequence
    }
    /// The actual terminal disposition accepted by the native unsettled owner.
    pub fn answered(&self) -> Option<&DeliveryState> {
        self.facts.answered.as_ref()
    }
}

/// Shared provenance owner of exactly one native control command.
#[derive(Clone)]
pub struct ControlTracker(Arc<ControlTrackingState>);
struct ControlTrackingState {
    probe: Arc<dyn ControlProbe>,
    snapshot: std::sync::Mutex<ControlReceipt>,
    #[cfg(test)]
    after_answer: std::sync::Mutex<Option<AfterAnswer>>,
}
#[cfg(test)]
type AfterAnswer = Arc<dyn Fn() + Send + Sync>;
impl std::fmt::Debug for ControlTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlTracker").finish_non_exhaustive()
    }
}
impl ControlTracker {
    /// Fix the original command's probe before dispatch; it cannot be replaced.
    pub fn new(probe: Arc<dyn ControlProbe>) -> Self {
        Self(Arc::new(ControlTrackingState {
            probe,
            #[cfg(test)]
            after_answer: std::sync::Mutex::new(None),
            snapshot: std::sync::Mutex::new(ControlReceipt {
                facts: Box::new(ControlReceiptFacts {
                            attachment: None,
                    complete_fence: None,
                    detach_sequence: None,
                    answered: None,
                }),
            }),
        }))
    }
    /// Snapshot provenance without holding the snapshot lock while calling the probe.
    pub fn receipt(&self) -> ControlReceipt {
        let mut receipt = self.0.snapshot.lock().unwrap().clone();
        receipt.facts.detach_sequence = receipt
            .facts
            .attachment
            .as_ref()
            .and_then(|a| self.0.probe.detach_sequence(a));
        receipt
    }
    pub(crate) fn first_admitted(&self, attachment: crate::endpoint::AttachmentId) {
        let mut snapshot = self.0.snapshot.lock().unwrap();
        assert!(snapshot.facts.attachment.is_none(), "one attachment per control command");
        snapshot.facts.attachment = Some(ControlAttachment(attachment));
    }
    pub(crate) fn complete_admitted(&self) {
        let fence = self.0.probe.dispatch_fence();
        let mut snapshot = self.0.snapshot.lock().unwrap();
        assert!(
            snapshot.facts.attachment.is_some() && snapshot.facts.complete_fence.is_none(),
            "one complete admission per request"
        );
        snapshot.facts.complete_fence = Some(fence);
    }
    pub(crate) fn answered_state(&self, state: &DeliveryState) {
        if !state.is_terminal() {
            return;
        }
        let mut snapshot = self.0.snapshot.lock().unwrap();
        match &snapshot.facts.answered {
            None => snapshot.facts.answered = Some(state.clone()),
            Some(prior) => assert_eq!(prior, state, "one terminal native answer per request"),
        }
        drop(snapshot);
        #[cfg(test)]
        {
            let callback = self.0.after_answer.lock().unwrap().clone();
            if let Some(callback) = callback {
                callback();
            }
        }
    }
}

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
) -> Result<
    crate::completion::Receiver<Result<Option<DeliveryState>, LinkStateError>>,
    link::SendError,
>
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
    /// Retain the native controller and scoped correlation through exact close.
    pub async fn close_retained_tracked_exact<F, G, E>(
        &self,
        dispatch: Arc<AtomicBool>,
        mut before: F,
    ) -> Result<(), link::LinkExchangeError<SenderAttachError, E>>
    where
        F: FnMut() -> Result<G, E> + Send,
        G: Send,
        E: Send,
    {
        let mut guard = self.inner.lock().await;
        let inner = guard.as_mut().expect("retained controller");
        inner.link.operation_dispatch = Some(dispatch);
        link::shared_inner::close_exact(inner, false, &mut before).await
    }

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
    /// Discharge with immutable provenance; interrupted dispatch cannot be replayed.
    pub async fn discharge_tracked(
        &mut self,
        fail: bool,
        tracking: ControlTracker,
    ) -> Result<(), ControllerSendError> {
        self.discharge_inner(fail, Some(tracking)).await
    }
    async fn discharge_inner(
        &mut self,
        fail: bool,
        tracking: Option<ControlTracker>,
    ) -> Result<(), ControllerSendError> {
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
            .send_control_tracked(message, &self.dispatched, tracking.as_ref())
            .await?
            .accepted_or_else(|state| match state {
                DeliveryState::Rejected(rejected) => ControllerSendError::Rejected(rejected),
                _ => ControllerSendError::IllegalDeliveryState,
            })?;
        self.completed = true;
        Ok(())
    }

    /// Declare on an existing controller. Dispatch metadata survives cancellation.
    pub async fn declare(
        controller: Arc<Controller>,
        global_id: Option<TransactionId>,
        dispatched: Arc<AtomicBool>,
    ) -> Result<Self, ControllerSendError> {
        Self::declare_inner(controller, global_id, dispatched, None).await
    }

    /// Declare with immutable native dispatch and answer provenance.
    pub async fn declare_tracked(
        controller: Arc<Controller>,
        global_id: Option<TransactionId>,
        dispatched: Arc<AtomicBool>,
        tracking: ControlTracker,
    ) -> Result<Self, ControllerSendError> {
        Self::declare_inner(controller, global_id, dispatched, Some(tracking)).await
    }

    async fn declare_inner(
        controller: Arc<Controller>,
        global_id: Option<TransactionId>,
        dispatched: Arc<AtomicBool>,
        tracking: Option<ControlTracker>,
    ) -> Result<Self, ControllerSendError> {
        let declared = {
            let mut guard = controller.inner.lock().await;
            let inner = guard.as_mut().expect("shared controller");
            let message = Message::builder().value(Declare { global_id }).build();
            let outcome = inner
                .send_control_tracked(message, &dispatched, tracking.as_ref())
                .await?;
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
        self.discharge_inner(fail, None).await
    }
}
impl super::TransactionPosting for SharedTransaction {}
impl super::TransactionRetirement for SharedTransaction {
    type RetireError = link::DispositionError;
}

impl SenderInner<ControlLink> {
    async fn send_control_tracked<T: SerializableBody>(
        &mut self,
        message: Message<T>,
        dispatched: &AtomicBool,
        tracking: Option<&ControlTracker>,
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
        let transfer = self.link.generate_non_resuming_transfer_performative(
            fe2o3_amqp_types::definitions::DeliveryTag::from([0; 4]),
            0,
            Some(false),
            None,
            false,
        )?;
        let settlement = self
            .link
            .send_new_payload(
                &self.outgoing,
                self.incoming.recv(),
                0,
                transfer,
                payload,
                Some(dispatched),
                tracking,
            )
            .await
            .map_err(|error| control_link_error(error, tracking))?;
        let Settlement::Unsettled { outcome, .. } = settlement else {
            return Err(ControllerSendError::IllegalDeliveryState);
        };
        tokio::pin!(outcome);
        loop {
            tokio::select! { biased;
                result = outcome.as_mut() => {
                    let result = result.map_err(|_| match self.link.session_stop_reason.get() {
                        Some(reason) => LinkStateError::SessionStopped(reason.clone()),
                        None => LinkStateError::IllegalState,
                    }).and_then(|value| value)
                        .map_err(|error| control_link_error(error,tracking))?;
                    return result.ok_or(ControllerSendError::NonTerminalDeliveryState);
                }
                frame = self.incoming.recv() => match frame {
                    Some(link::LinkFrame::Detach(detach,_)) => {
                        let closed=detach.closed;let error=detach.error.clone();
                        let _=crate::endpoint::LinkDetach::apply_remote_detach_outcome(&mut self.link,detach);
                        return Err(ControllerSendError::ControlDetached {
                            closed,error,receipt:tracking.map(ControlTracker::receipt),
                        });
                    }
                    Some(link::LinkFrame::Flow(..)) => {},
                    Some(link::LinkFrame::Attach(_)|link::LinkFrame::Disposition(_)) =>
                        return Err(ControllerSendError::NonTerminalDeliveryState),
                    Some(link::LinkFrame::Transfer { .. }|link::LinkFrame::Acquisition(_)) =>
                        return Err(ControllerSendError::IllegalDeliveryState),
                    None => return Err(control_link_error(match self.link.session_stop_reason.get() {
                        Some(reason)=>LinkStateError::SessionStopped(reason.clone()),
                        None=>LinkStateError::IllegalState,
                    },tracking)),
                }
            }
        }
    }
}

fn control_link_error(
    error: LinkStateError,
    tracking: Option<&ControlTracker>,
) -> ControllerSendError {
    let (closed, error) = match error {
        LinkStateError::RemoteClosed => (true, None),
        LinkStateError::RemoteClosedWithError(error) => (true, Some(error)),
        LinkStateError::RemoteDetached => (false, None),
        LinkStateError::RemoteDetachedWithError(error) => (false, Some(error)),
        other => return ControllerSendError::LinkStateError(other),
    };
    ControllerSendError::ControlDetached {
        closed,
        error,
        receipt: tracking.map(ControlTracker::receipt),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn tracked_terminal_state_precedes_oneshot_and_survives_control_end() {
        use crate::{
            endpoint::{OutgoingChannel, Session as _},
            session::Builder,
            util::Producer,
        };
        use fe2o3_amqp_types::{
            messaging::{Accepted, Rejected},
            performatives::Disposition,
            states::SessionState,
        };
        for direct_settle in [false, true] {
            for settled in [false, true] {
                for state in [
                    DeliveryState::Declared(Declared {
                        txn_id: vec![7].into(),
                    }),
                    DeliveryState::Accepted(Accepted {}),
                    DeliveryState::Rejected(Rejected { error: None }),
                ] {
                    let tracker = tracking();
                    let (native_tx, mut result) = oneshot::channel();
                    let (arrived, at) = std::sync::mpsc::sync_channel(0);
                    let (release, wait) = std::sync::mpsc::sync_channel(0);
                    let wait = std::sync::Mutex::new(wait);
                    *tracker.0.after_answer.lock().unwrap() = Some(Arc::new(move || {
                        arrived.send(()).unwrap();
                        wait.lock()
                            .unwrap()
                            .recv_timeout(std::time::Duration::from_secs(10))
                            .unwrap();
                    }));
                    let store = Arc::new(Store::new(None));
                    let tag: fe2o3_amqp_types::definitions::DeliveryTag = vec![7].into();
                    store
                        .register(
                            tag.clone(),
                            link::delivery::UnsettledMessage::new_tracked(
                                bytes::Bytes::new(),
                                Some(state.clone()),
                                0,
                                native_tx,
                                tracker.clone(),
                            ),
                        )
                        .unwrap();
                    let mut session = Builder::new().into_session(
                        OutgoingChannel(0),
                        SessionState::Mapped,
                        Arc::new(OnceLock::new()),
                    );
                    let (frames, _inbox) = mpsc::channel(2);
                    let flow = Arc::new(LinkFlowState::sender(LinkFlowStateInner {
                        initial_delivery_count: 0,
                        delivery_count: 0,
                        link_credit: 1,
                        available: 0,
                        drain: false,
                        properties: None,
                    }));
                    let relay = link::LinkRelay::new_sender(
                        frames,
                        Producer::new(Arc::new(Notify::new()), flow),
                        store.clone(),
                    )
                    .with_output_handle(OutputAttachment::for_tests(0));
                    session.link_by_input_handle.insert(InputHandle(0), relay);
                    session
                        .delivery_tag_by_id
                        .insert((Role::Receiver, 7), (InputHandle(0), tag.clone()));
                    let thread_state = state.clone();
                    let owned_store = store.clone();
                    let thread = std::thread::spawn(move || {
                        if direct_settle {
                            owned_store.withdraw(&tag).unwrap().settle().unwrap();
                        } else {
                            session
                                .on_incoming_disposition(Disposition {
                                    role: Role::Receiver,
                                    first: 7,
                                    last: None,
                                    settled,
                                    state: Some(thread_state),
                                    batchable: false,
                                })
                                .unwrap();
                        }
                    });
                    at.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
                    assert_eq!(tracker.receipt().answered(), Some(&state));
                    assert!(matches!(
                        result.try_recv(),
                        Err(oneshot::error::TryRecvError::Empty)
                    ));
                    // A later control end cannot overwrite an answer accepted by the unsettled owner.
                    store.close(LinkStateError::RemoteClosed);
                    assert_eq!(tracker.receipt().answered(), Some(&state));
                    release.send(()).unwrap();
                    thread.join().unwrap();
                    assert_eq!(result.try_recv().unwrap().unwrap(), Some(state));
                }
            }
        }
        let tracker = tracking();
        let (tx, mut rx) = oneshot::channel();
        link::delivery::UnsettledMessage::new_tracked(
            bytes::Bytes::new(),
            None,
            0,
            tx,
            tracker.clone(),
        )
        .fail(LinkStateError::IllegalState)
        .unwrap();
        assert!(tracker.receipt().answered().is_none());
        assert!(matches!(
            rx.try_recv().unwrap(),
            Err(LinkStateError::IllegalState)
        ));
    }

    use super::*;
    use crate::{
        control::SessionControl,
        endpoint::{InputHandle, OutputAttachment},
        link::{
            state::{LinkFlowState, LinkFlowStateInner, LinkState},
            unsettled_store::Store,
            LinkFrame,
        },
        util::Consumer,
    };
    use fe2o3_amqp_types::{
        definitions::{ReceiverSettleMode, Role},
        messaging::TargetArchetype,
        performatives::{Attach, Detach},
    };
    use std::sync::{
        atomic::{AtomicU64, AtomicUsize},
        OnceLock,
    };
    use tokio::sync::{mpsc, Notify};

    struct Probe(AtomicU64);
    impl ControlProbe for Probe {
        fn dispatch_fence(&self) -> u64 {
            self.0.fetch_add(1, Ordering::AcqRel)
        }
        fn detach_sequence(&self, _: &ControlAttachment) -> Option<u64> {
            Some(2)
        }
    }
    fn tracking() -> ControlTracker {
        ControlTracker::new(Arc::new(Probe(AtomicU64::new(0))))
    }
    fn fixture(
        max_frame: usize,
        capacity: usize,
    ) -> (
        Arc<Controller>,
        mpsc::Receiver<SessionControl>,
        mpsc::Receiver<LinkFrame>,
        mpsc::Sender<LinkFrame>,
        link::ArcSenderUnsettledMap,
    ) {
        let (control, controls) = mpsc::channel(16);
        let (outgoing, sent) = mpsc::channel(capacity);
        let (incoming, frames) = mpsc::channel(16);
        let flow = Arc::new(LinkFlowState::sender(LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 1,
            available: 0,
            drain: false,
            properties: None,
        }));
        let store = Arc::new(Store::new(None));
        let coordinator = Coordinator {
            capabilities: Some(
                vec![fe2o3_amqp_types::transaction::TxnCapability::DistributedTransactions].into(),
            ),
        };
        let mut link = Controller::builder()
            .name("control")
            .coordinator(coordinator)
            .sender_settle_mode(SenderSettleMode::Unsettled)
            .create_link(
                store.clone(),
                OutputAttachment::for_tests(0),
                Consumer::new(Arc::new(Notify::new()), flow),
                Arc::new(OnceLock::new()),
                max_frame,
            );
        link.local_state = LinkState::Attached;
        link.input_handle = Some(InputHandle(0));
        let inner = SenderInner {
            link,
            buffer_size: 16,
            session: control,
            outgoing: outgoing.into(),
            incoming: frames,
        };
        (
            Arc::new(Controller {
                inner: Mutex::new(Some(inner)),
            }),
            controls,
            sent,
            incoming,
            store,
        )
    }
    fn peer_attach() -> Attach {
        Attach {
            name: "control".into(),
            handle: 17u32.into(),
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: Some(Box::new(TargetArchetype::Coordinator(Coordinator {
                capabilities: Some(
                    vec![fe2o3_amqp_types::transaction::TxnCapability::DistributedTransactions]
                        .into(),
                ),
            }))),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    #[tokio::test]
    async fn controller_crossed_close_uses_the_exact_hook_and_keeps_phase_error() {
        use crate::link::test_util::{drive_exact_end, CorrelationGuard};
        for incomplete in [false, true] {
            let (controller, controls, outgoing, incoming, _) = fixture(4096, 16);
            let dispatch = Arc::new(AtomicBool::new(false));
            let calls = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicBool::new(false));
            let mut attach = peer_attach();
            attach.incomplete_unsettled = incomplete;
            let at = || {
                assert_eq!(calls.load(Ordering::Acquire), 1);
                assert!(!dropped.load(Ordering::Acquire));
            };
            let (result, detaches) = tokio::join!(
                controller.close_retained_tracked_exact(dispatch.clone(), || {
                    calls.fetch_add(1, Ordering::AcqRel);
                    Ok::<_, std::io::Error>(CorrelationGuard(dropped.clone()))
                }),
                drive_exact_end(controls, outgoing, incoming, attach, at, || {
                    at();
                    None
                })
            );
            if incomplete {
                assert!(matches!(
                    result,
                    Err(link::LinkExchangeError::Attach(
                        SenderAttachError::IllegalState
                    ))
                ));
            } else {
                result.unwrap();
                assert_eq!(detaches, 2);
            }
            assert!(dispatch.load(Ordering::Acquire));
            assert!(dropped.load(Ordering::Acquire));
            assert!(controller.inner.try_lock().is_ok());
        }
    }

    async fn control_detach(
        discharge: bool,
        fail: bool,
        closed: bool,
        error: Option<definitions::Error>,
    ) {
        let (controller, _controls, mut sent, incoming, store) = fixture(4096, 16);
        let tracker = tracking();
        let dispatched = Arc::new(AtomicBool::new(false));
        let mut transaction = SharedTransaction {
            controller: controller.clone(),
            declared: Declared {
                txn_id: vec![1].into(),
            },
            dispatched: dispatched.clone(),
            completed: false,
        };
        let work = async {
            if discharge {
                transaction.discharge_tracked(fail, tracker.clone()).await
            } else {
                SharedTransaction::declare_tracked(
                    controller.clone(),
                    None,
                    dispatched.clone(),
                    tracker.clone(),
                )
                .await
                .map(drop)
            }
        };
        tokio::pin!(work);
        tokio_test::assert_pending!(futures_util::poll!(work.as_mut()));
        let LinkFrame::Transfer { .. } = sent.recv().await.unwrap() else {
            panic!("control Transfer")
        };
        assert!(dispatched.load(Ordering::Acquire));
        assert!(tracker.receipt().complete());
        if closed {
            store.close(match &error {
                Some(error) => LinkStateError::RemoteClosedWithError(error.clone()),
                None => LinkStateError::RemoteClosed,
            });
        }
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
        let result = work.await;
        assert!(
            matches!(result,Err(ControllerSendError::ControlDetached {closed:actual,error:actual_error,receipt:Some(receipt)}) if actual==closed && actual_error==error && receipt.complete() && receipt.attachment().is_some())
        );
        assert!(controller.inner.try_lock().is_ok());
        if !closed {
            assert_eq!(store.read().as_ref().unwrap().len(), 1);
        }
        assert!(sent.try_recv().is_err());
    }
    #[tokio::test]
    async fn a_nonclosing_coordinator_detach_completes_the_tracked_control_wait() {
        for discharge in [false, true] {
            for fail in [false, true] {
                control_detach(
                    discharge,
                    fail,
                    false,
                    Some(definitions::Error::new(
                        fe2o3_amqp_types::transaction::TransactionError::Rollback,
                        None,
                        None,
                    )),
                )
                .await;
            }
        }
    }
    #[tokio::test]
    async fn control_detach_and_confirmed_outcome_keep_their_provenance() {
        for discharge in [false, true] {
            for closed in [false, true] {
                control_detach(discharge, false, closed, None).await;
            }
        }
        for discharge in [false, true] {
            for closed in [false, true] {
                let (controller, _controls, mut sent, incoming, _store) = fixture(4096, 16);
                let tracker = tracking();
                let dispatched = Arc::new(AtomicBool::new(false));
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
                let mut transaction = SharedTransaction {
                    controller: controller.clone(),
                    declared: Declared {
                        txn_id: vec![1].into(),
                    },
                    dispatched: dispatched.clone(),
                    completed: false,
                };
                let result = if discharge {
                    transaction.discharge_tracked(false, tracker.clone()).await
                } else {
                    SharedTransaction::declare_tracked(
                        controller.clone(),
                        None,
                        dispatched.clone(),
                        tracker.clone(),
                    )
                    .await
                    .map(drop)
                };
                assert!(
                    matches!(result,Err(ControllerSendError::ControlDetached {closed:actual,error:None,..}) if actual==closed)
                );
                assert!(!dispatched.load(Ordering::Acquire));
                assert!(tracker.receipt().attachment().is_none());
                assert!(tracker.receipt().complete_fence().is_none());
                assert!(tracker.receipt().answered().is_none());
                assert!(sent.try_recv().is_err());
                assert!(controller.inner.try_lock().is_ok());
            }
        }
        for closed in [false, true] {
            let (controller, _controls, mut sent, incoming, store) = fixture(4096, 16);
            let tracker = tracking();
            let dispatched = Arc::new(AtomicBool::new(false));
            let work = SharedTransaction::declare_tracked(
                controller.clone(),
                None,
                dispatched,
                tracker.clone(),
            );
            tokio::pin!(work);
            tokio_test::assert_pending!(futures_util::poll!(work.as_mut()));
            let LinkFrame::Transfer { performative, .. } = sent.recv().await.unwrap() else {
                panic!("control Transfer")
            };
            let state = DeliveryState::Declared(Declared {
                txn_id: vec![7].into(),
            });
            let message = store
                .withdraw(performative.delivery_tag.as_ref().unwrap())
                .unwrap();
            message.settle_with_state(Some(state.clone())).unwrap();
            assert_eq!(tracker.receipt().answered(), Some(&state));
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
            let transaction = work.await.unwrap();
            assert_eq!(
                super::super::TransactionBase::txn_id(&transaction).as_ref(),
                &[7u8]
            );
            assert!(controller.inner.try_lock().is_ok());
        }
    }
    #[tokio::test]
    async fn control_receipt_requires_final_admission_and_original_attachment() {
        for discharge in [false, true] {
            for closed in [false, true] {
                for with_error in [false, true] {
                    let (controller, _controls, mut sent, incoming, store) = fixture(512, 1);
                    let tracker = tracking();
                    let dispatched = Arc::new(AtomicBool::new(false));
                    let mut transaction = SharedTransaction {
                        controller: controller.clone(),
                        declared: Declared {
                            txn_id: vec![1; 1024].into(),
                        },
                        dispatched: dispatched.clone(),
                        completed: false,
                    };
                    let work = async {
                        if discharge {
                            transaction.discharge_tracked(false, tracker.clone()).await
                        } else {
                            SharedTransaction::declare_tracked(
                                controller.clone(),
                                Some(vec![2; 1024].into()),
                                dispatched.clone(),
                                tracker.clone(),
                            )
                            .await
                            .map(drop)
                        }
                    };
                    tokio::pin!(work);
                    tokio_test::assert_pending!(futures_util::poll!(work.as_mut()));
                    assert_eq!(sent.len(), 1);
                    assert!(dispatched.load(Ordering::Acquire));
                    let receipt = tracker.receipt();
                    assert!(receipt.attachment().is_some());
                    assert!(!receipt.complete());
                    assert!(receipt.complete_fence().is_none());
                    let error = with_error.then(|| {
                        definitions::Error::new(
                            fe2o3_amqp_types::transaction::TransactionError::Rollback,
                            None,
                            None,
                        )
                    });
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
                    let result = work.await;
                    assert!(
                        matches!(result,Err(ControllerSendError::ControlDetached {closed:actual,error:actual_error,receipt:Some(receipt)}) if actual==closed && actual_error==error && receipt.attachment().is_some() && !receipt.complete())
                    );
                    assert!(controller.inner.try_lock().is_ok());
                    let LinkFrame::Transfer { performative, .. } = sent.try_recv().unwrap() else {
                        panic!("first fragment")
                    };
                    assert!(performative.more);
                    if !closed {
                        assert_eq!(store.read().as_ref().unwrap().len(), 1);
                    }
                    assert!(sent.try_recv().is_err());
                }
            }
        }
    }
}
