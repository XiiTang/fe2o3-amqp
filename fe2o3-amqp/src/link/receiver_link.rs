use std::sync::{Arc, OnceLock};

use fe2o3_amqp_types::{
    definitions::{Fields, Handle},
    messaging::{message::DecodeIntoMessage, FromBody},
};

use crate::{
    endpoint::LinkExt,
    util::{is_consecutive, AsByteIterator, IntoReader, Sealed},
};

use super::{delivery::DeliveryInfo, *};

impl<Tar> endpoint::ReceiverLink for ReceiverLink<Tar>
where
    Tar: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync,
{
    type FlowError = FlowError;
    type TransferError = ReceiverTransferError;
    type DispositionError = DispositionError;

    /// Set and send flow state
    ///
    /// This is cancel safe because it only `.await` on sending over a `tokio::mpsc::Sender`
    async fn send_flow(
        &self,
        writer: &crate::session::transfer_queue::Sender,
        link_credit: Option<u32>,
        drain: Option<bool>,
        echo: bool,
        include_properties: bool,
    ) -> Result<(), Self::FlowError> {
        let handle = self
            .output_handle
            .clone()
            .ok_or(Self::FlowError::IllegalState)?
            .into();

        let flow = self.get_link_flow(handle, link_credit, drain, echo, include_properties);
        writer
            .send(LinkFrame::Flow(flow))
            .await // cancel safe
            .map_err(|_| match self.session_stop_reason.get() {
                Some(reason) => Self::FlowError::SessionStopped(reason.clone()),
                None => Self::FlowError::IllegalState, // defensive: no stop reason recorded; failure is link-local
            })
    }

    fn on_transfer_state(
        &mut self,
        delivery_tag: &Option<DeliveryTag>,
        settled: Option<bool>,
        state: DeliveryState,
    ) -> Result<(), Self::TransferError> {
        let delivery_tag = delivery_tag
            .as_ref()
            .ok_or(Self::TransferError::DeliveryTagIsNone)?;
        let mut guard = self.unsettled.write();
        let map = guard.get_or_insert(OrderedMap::new());

        if matches!(settled, Some(true)) {
            // FIXME: Simply remove from the unsettled map?
            let _ = map.swap_remove(delivery_tag);
        } else {
            if let Some(value) = map.get_mut(delivery_tag) {
                value.remote_state(Some(state));
            }
        }
        Ok(())
    }

    fn on_incomplete_transfer(
        &mut self,
        delivery_tag: DeliveryTag,
        section_number: u32,
        section_offset: u64,
    ) {
        // link-credit is defined as
        // "The current maximum number of messages that can be handled
        // at the receiver endpoint of the link"
        // So there is no need to decrement the link-credit on incomplete delivery

        let state = DeliveryState::Received(Received {
            section_number,
            section_offset,
        });

        {
            let mut guard = self.unsettled.write();
            if let Some(current) = guard.as_mut().and_then(|map| map.get_mut(&delivery_tag)) {
                current.received(state, false);
            }
        }
    }

    fn on_complete_transfer<'a, T, P>(
        &mut self,
        transfer: Transfer,
        payload: P,
        section_number: u32,
        section_offset: u64,
    ) -> Result<Delivery<T>, Self::TransferError>
    where
        for<'de> T: FromBody<'de> + Send,
        P: IntoReader<'a> + AsByteIterator + Send + 'a,
    {
        match self.local_state {
            LinkState::Attached | LinkState::IncompleteAttachExchanged | LinkState::DetachSent => {}
            _ => return Err(ReceiverTransferError::IllegalState),
        }

        // ReceiverFlowState will not wait until link credit is available.
        // Will return with an error if there is not enough link credit.
        self.flow_state.consume(1)?;

        // This only takes care of whether the message is considered
        // sett
        let settled_by_sender = transfer.settled.unwrap_or(false)
            || !transfer.delivery_tag.as_ref().is_some_and(|tag| {
                self.unsettled
                    .read()
                    .as_ref()
                    .is_some_and(|map| map.contains_key(tag))
            });
        let delivery_id = transfer
            .delivery_id
            .ok_or(Self::TransferError::DeliveryIdIsNone)?;
        let delivery_tag = transfer
            .delivery_tag
            .ok_or(Self::TransferError::DeliveryTagIsNone)?;
        let message_format = transfer.message_format;

        let encoded: Vec<u8> = payload.as_byte_iterator().copied().collect();
        let admission = fe2o3_amqp_types::messaging::message::admission::validate(&encoded);
        let (result, mode) = if settled_by_sender {
            // If the message is pre-settled, there is no need to
            // add to the unsettled map and no need to reply to the Sender
            let result = admission.and_then(|_| {
                T::decode_message_from_reader(serde_amqp::read::SliceReader::new(&encoded))
            });
            (result, None)
        } else {
            // If the message is being sent settled by the sender, the value of this
            // field is ignored.
            let mode = match transfer.rcv_settle_mode {
                Some(mode) => {
                    // If the negotiated link value is first, then it is illegal to set this
                    // field to second.
                    if matches!(&self.rcv_settle_mode, ReceiverSettleMode::First)
                        && matches!(mode, ReceiverSettleMode::Second)
                    {
                        return Err(Self::TransferError::IllegalRcvSettleModeInTransfer);
                    }
                    Some(mode)
                }
                None => None,
            };

            let result = admission.and_then(|_| {
                T::decode_message_from_reader(serde_amqp::read::SliceReader::new(&encoded))
            });

            let state = DeliveryState::Received(Received {
                section_number, // What is section number?
                section_offset,
            });

            // Upon receiving the transfer, the receiving link endpoint (receiver)
            // will create an entry in its own unsettled map and make the transferred
            // message data available to the application to process.
            //
            // Add to unsettled map
            // Insert into local unsettled map with Received state
            // Mode Second doesn't automatically send back a disposition
            // (ie. thus doesn't call `link.dispose()`) and thus need to manually
            // set the delivery state
            {
                let mut lock = self.unsettled.write();
                if let Some(current) = lock.as_mut().and_then(|map| map.get_mut(&delivery_tag)) {
                    current.received(state, result.is_ok());
                    current.info = Some(DeliveryInfo {
                        delivery_id,
                        delivery_tag: delivery_tag.clone(),
                        rcv_settle_mode: mode.clone(),
                        _sealed: Sealed {},
                    });
                }
            }
            (result, mode)
        };

        let message = match result {
            Ok(message) => message,
            Err(source) => {
                let info = DeliveryInfo {
                    delivery_id,
                    delivery_tag,
                    rcv_settle_mode: mode,
                    _sealed: Sealed {},
                };
                return Err(MessageDecodeError { source, info }.into());
            }
        };

        let link_output_handle = self
            .output_handle
            .clone()
            .ok_or(ReceiverTransferError::IllegalState)?
            .into();

        let delivery = Delivery {
            link_output_handle,
            delivery_id,
            delivery_tag,
            message_format,
            rcv_settle_mode: mode,
            message,
        };

        Ok(delivery)
    }

    /// This is cancel safe because it only `.await` on sending over `tokio::mpsc::Sender`
    async fn dispose(
        &self,
        writer: &crate::session::transfer_queue::Sender,
        delivery_info: DeliveryInfo,
        settled: Option<bool>,
        state: DeliveryState,
        batchable: bool,
    ) -> Result<(), Self::DispositionError> {
        let settled = settled.unwrap_or({
            match delivery_info
                .rcv_settle_mode
                .as_ref()
                .unwrap_or(&self.rcv_settle_mode)
            {
                ReceiverSettleMode::First => {
                    // If first, this indicates that the receiver MUST settle
                    // the delivery once it has arrived without waiting
                    // for the sender to settle first.

                    // The delivery is not inserted into unsettled map if in First mode
                    true
                }
                ReceiverSettleMode::Second => {
                    // If second, this indicates that the receiver MUST NOT settle until sending
                    // its disposition to the sender and receiving a settled disposition from
                    // the sender.
                    false
                }
            }
        });

        let unsettled_state = if settled {
            let mut lock = self.unsettled.write();
            lock.as_mut()
                .and_then(|map| map.swap_remove(&delivery_info.delivery_tag))
        } else {
            let mut lock = self.unsettled.write();
            // If the key is present in the map, the old value will be returned, which
            // we don't really need
            lock.as_mut()
                .and_then(|map| map.get_mut(&delivery_info.delivery_tag))
                .map(|entry| {
                    entry.local_state(state.clone());
                    entry.clone()
                })
        };

        // Only dispose if message is found in unsettled map
        if unsettled_state.is_some() {
            let disposition = Disposition {
                role: Role::Receiver,
                first: delivery_info.delivery_id,
                last: None,
                settled,
                state: Some(state),
                batchable,
            };
            let frame = LinkFrame::Disposition(disposition);
            writer
                .send(frame)
                .await // cancel safe
                .map_err(|_| match self.session_stop_reason.get() {
                    Some(reason) => Self::DispositionError::SessionStopped(reason.clone()),
                    None => Self::DispositionError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                })?;
        }

        Ok(())
    }

    /// This is cancel safe because all internal `.await` points are cancel safe
    async fn dispose_all(
        &self,
        writer: &crate::session::transfer_queue::Sender,
        mut delivery_infos: Vec<DeliveryInfo>,
        settled: Option<bool>,
        state: DeliveryState,
        batchable: bool,
    ) -> Result<(), Self::DispositionError> {
        // sorting before filtering may be more cache/branch-prediction friendly?
        delivery_infos.sort_by_key(|left| left.delivery_id);
        {
            let reader = self.unsettled.read();
            delivery_infos.retain(|info| {
                reader
                    .as_ref()
                    .map(|m| m.contains_key(&info.delivery_tag))
                    .unwrap_or(false)
            });
        }
        let chunk_inds = consecutive_chunk_indices(&delivery_infos);

        let mut prev_ind = 0;
        for ind in chunk_inds {
            let slice = &delivery_infos[prev_ind..ind];
            self.dispose_consecutive(writer, slice, settled, state.clone(), batchable)
                .await?; // cancel safe
            prev_ind = ind;
        }
        let final_slice = &delivery_infos[prev_ind..];
        self.dispose_consecutive(writer, final_slice, settled, state, batchable)
            .await // cancel safe
    }
}

fn consecutive_chunk_indices(delivery_infos: &[DeliveryInfo]) -> Vec<usize> {
    delivery_infos
        .windows(2)
        .enumerate()
        .filter_map(|(i, infos)| {
            if is_consecutive(&infos[0].delivery_id, &infos[1].delivery_id)
                && infos[0].rcv_settle_mode == infos[1].rcv_settle_mode
            {
                None
            } else {
                // window size is 2, so the iter is 1 less than the total len
                Some(i + 1)
            }
        })
        .collect()
}

/// Derive recovery progress from complete encoded section boundaries.
pub(crate) fn count_number_of_sections_and_offset(
    bytes: &[u8],
) -> Result<(u32, u64), ReceiverTransferError> {
    fe2o3_amqp_types::messaging::message::admission::prefix_position(bytes)
        .map_err(ReceiverTransferError::InvalidMessageEncoding)
}

impl<T> ReceiverLink<T> {
    fn handle_unsettled_in_attach(
        &mut self,
        remote_unsettled: Option<OrderedMap<DeliveryTag, Option<DeliveryState>>>,
        remote_incomplete: bool,
    ) -> ReceiverAttachExchange {
        let remote_is_empty = remote_unsettled.as_ref().is_none_or(|map| map.is_empty());
        {
            let mut guard = self.unsettled.write();
            if let Some(local) = guard.as_mut() {
                local.as_inner_mut().retain(|tag, entry| {
                    if let Some(remote) = remote_unsettled.as_ref().and_then(|map| map.get(tag)) {
                        entry.remote_state(remote.clone());
                        true
                    } else {
                        remote_incomplete
                    }
                });
            }
        }
        if remote_incomplete
            || matches!(
                self.local_state,
                LinkState::IncompleteAttachReceived
                    | LinkState::IncompleteAttachSent
                    | LinkState::IncompleteAttachExchanged
            )
        {
            ReceiverAttachExchange::IncompleteUnsettled
        } else if remote_is_empty {
            ReceiverAttachExchange::Complete
        } else {
            ReceiverAttachExchange::Resume
        }
    }

    /// This is cancel safe because it only `.await` on sending over a `tokio::mpsc::Sender`
    async fn dispose_consecutive(
        &self,
        writer: &crate::session::transfer_queue::Sender,
        consecutive_infos: &[DeliveryInfo],
        settled: Option<bool>,
        state: DeliveryState,
        batchable: bool,
    ) -> Result<(), DispositionError> {
        // This shouldn't happen but just being cautious
        if consecutive_infos.is_empty() {
            return Ok(());
        }

        let settled = settled.unwrap_or({
            match consecutive_infos[0]
                .rcv_settle_mode
                .as_ref()
                .unwrap_or(&self.rcv_settle_mode)
            {
                ReceiverSettleMode::First => true,
                ReceiverSettleMode::Second => false,
            }
        });

        // TODO: Individually checking whether a delivery is already dropped is probably too heavy?
        if settled {
            let mut lock = self.unsettled.write();
            for info in consecutive_infos {
                lock.as_mut()
                    .and_then(|map| map.swap_remove(&info.delivery_tag));
            }
        } else {
            let mut lock = self.unsettled.write();
            for info in consecutive_infos {
                if let Some(entry) = lock
                    .as_mut()
                    .and_then(|map| map.get_mut(&info.delivery_tag))
                {
                    entry.local_state(state.clone());
                }
            }
        }

        let disposition = Disposition {
            role: Role::Receiver,
            first: consecutive_infos[0].delivery_id,
            last: consecutive_infos.last().map(|el| el.delivery_id),
            settled,
            state: Some(state),
            batchable,
        };
        let frame = LinkFrame::Disposition(disposition);
        writer
            .send(frame)
            .await // cancel safe
            .map_err(|_| match self.session_stop_reason.get() {
                Some(reason) => DispositionError::SessionStopped(reason.clone()),
                None => DispositionError::IllegalState, // defensive: no stop reason recorded; failure is link-local
            })
    }

    fn get_link_flow(
        &self,
        handle: Handle,
        link_credit: Option<u32>,
        drain: Option<bool>,
        echo: bool,
        include_properties: bool,
    ) -> LinkFlow {
        match (link_credit, drain) {
            (Some(link_credit), Some(drain)) => {
                let mut guard = self.flow_state.lock.write();
                guard.link_credit = link_credit;
                guard.drain = drain;

                let properties = if include_properties {
                    guard.properties.clone()
                } else {
                    None
                };

                LinkFlow {
                    handle,
                    // When the flow state is being sent from the receiver endpoint to the sender
                    // endpoint this field MUST be set to the last known value of the corresponding
                    // sending endpoint.
                    delivery_count: Some(guard.delivery_count),
                    link_credit: Some(link_credit),
                    // The receiver sets this to the last known value seen from the sender
                    // available: Some(writer.available),
                    available: None,
                    drain,
                    echo,
                    properties,
                }
            }
            (Some(link_credit), None) => {
                let mut guard = self.flow_state.lock.write();
                guard.link_credit = link_credit;

                let properties = if include_properties {
                    guard.properties.clone()
                } else {
                    None
                };

                LinkFlow {
                    handle,
                    // When the flow state is being sent from the receiver endpoint to the sender
                    // endpoint this field MUST be set to the last known value of the corresponding
                    // sending endpoint.
                    delivery_count: Some(guard.delivery_count),
                    link_credit: Some(link_credit),
                    // The receiver sets this to the last known value seen from the sender
                    // available: Some(writer.available),
                    available: None,
                    drain: guard.drain,
                    echo,
                    properties,
                }
            }
            (None, Some(drain)) => {
                let mut guard = self.flow_state.lock.write();
                guard.drain = drain;

                let properties = if include_properties {
                    guard.properties.clone()
                } else {
                    None
                };

                LinkFlow {
                    handle,
                    // When the flow state is being sent from the receiver endpoint to the sender
                    // endpoint this field MUST be set to the last known value of the corresponding
                    // sending endpoint.
                    delivery_count: Some(guard.delivery_count),
                    link_credit: Some(guard.link_credit),
                    // The receiver sets this to the last known value seen from the sender
                    // available: Some(writer.available),
                    available: None,
                    drain,
                    echo,
                    properties,
                }
            }
            (None, None) => {
                let guard = self.flow_state.lock.read();

                let properties = if include_properties {
                    guard.properties.clone()
                } else {
                    None
                };

                LinkFlow {
                    handle,
                    // When the flow state is being sent from the receiver endpoint to the sender
                    // endpoint this field MUST be set to the last known value of the corresponding
                    // sending endpoint.
                    delivery_count: Some(guard.delivery_count),
                    link_credit: Some(guard.link_credit),
                    // The receiver sets this to the last known value seen from the sender
                    // available: Some(writer.available),
                    available: None,
                    drain: guard.drain,
                    echo,
                    properties,
                }
            }
        }
    }
}

impl<T> endpoint::LinkAttach for ReceiverLink<T>
where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync,
{
    type AttachExchange = ReceiverAttachExchange;
    type AttachError = ReceiverAttachError;

    fn on_incoming_attach(
        &mut self,
        remote_attach: Attach,
    ) -> Result<Self::AttachExchange, Self::AttachError> {
        use self::source::VerifySource;

        match (&self.local_state, remote_attach.incomplete_unsettled) {
            (LinkState::AttachSent, false) => {
                self.local_state = LinkState::Attached;
            }
            (LinkState::IncompleteAttachSent, false) => {
                self.local_state = LinkState::IncompleteAttachExchanged;
            }
            (LinkState::Unattached, false) | (LinkState::Detached, false) => {
                self.local_state = LinkState::AttachReceived; // re-attaching
            }
            (LinkState::AttachSent, true) | (LinkState::IncompleteAttachSent, true) => {
                self.local_state = LinkState::IncompleteAttachExchanged;
            }
            (LinkState::Unattached, true) | (LinkState::Detached, true) => {
                self.local_state = LinkState::IncompleteAttachReceived; // re-attaching
            }
            _ => return Err(ReceiverAttachError::IllegalState),
        };

        self.input_handle = Some(InputHandle::from(remote_attach.handle));

        // In this case, the sender is considered to hold the authoritative version of the
        // version of the source properties
        let remote_source = remote_attach
            .source
            // Only need to check the source
            //
            // If there is no pre-existing terminus, and the peer does not wish to create a new one,
            // this is indicated by setting the local terminus (source or target as appropriate) to null.
            .ok_or(ReceiverAttachError::IncomingSourceIsNone)?;
        if self.verify_incoming_source {
            if let Some(local_source) = &self.source {
                local_source.verify_as_receiver(&remote_source)?;
            }
        }
        self.source = Some(*remote_source);

        // When set at the sender this indicates the actual settlement mode in use
        //
        // The receiver doesn't really care what snd_settle_mode is in use. It uses
        // the `settled` field of the Transfer and rcv_settle_mode to decide whether a
        // delivery is settled
        self.snd_settle_mode = remote_attach.snd_settle_mode;

        // The `rcv-settle-mode` field in the attach response from the sender only
        // expresses the sender's *desired* settlement mode for the receiver ("when set
        // at the sender this indicates the desired value for the settlement mode at the
        // receiver"). Since the receiver initiated the attach, its own choice governs,
        // so no validation is performed here; the local `rcv_settle_mode` is used at
        // delivery time (falling back to the per-transfer value set by the sender).

        // The delivery-count is initialized by the sender when a link endpoint is
        // created, and is incremented whenever a message is sent
        let initial_delivery_count = remote_attach
            .initial_delivery_count
            .ok_or(ReceiverAttachError::InitialDeliveryCountIsNone)?;

        let target = remote_attach
            .target
            .map(|t| T::try_from(*t))
            .transpose()
            .map_err(|_| ReceiverAttachError::CoordinatorIsNotImplemented)?;

        // **the receiver is considered to hold the authoritative version of the target properties**,
        // Is this verification necessary?
        //
        // If there is no pre-existing terminus, and the peer does not wish to create a new one,
        // this is indicated by setting the local terminus (source or target as appropriate) to null.
        if self.verify_incoming_target {
            if let (Some(local_target), Some(remote_target)) = (&self.target, &target) {
                local_target.verify_as_receiver(remote_target)?
            }
        }

        self.max_message_size =
            get_max_message_size(self.max_message_size, remote_attach.max_message_size);

        self.flow_state
            .as_ref()
            .initial_delivery_count_mut(|_| initial_delivery_count);
        self.flow_state
            .as_ref()
            .delivery_count_mut(|_| initial_delivery_count);

        if let Some(remote_properties) = remote_attach.properties {
            self.properties_mut(|local_properties| {
                local_properties
                    .get_or_insert(OrderedMap::new())
                    .as_inner_mut()
                    .extend(remote_properties.into_inner());
            });
        }

        // Ok(Self::AttachExchange::Complete)
        Ok(self.handle_unsettled_in_attach(
            remote_attach.unsettled,
            remote_attach.incomplete_unsettled,
        ))
    }

    /// # Cancel safety
    ///
    /// This is cancel safe: it only awaits on sending over `tokio::mpsc::Sender`
    /// (see `send_attach_inner`) and on `tokio::sync::mpsc::Receiver::recv`,
    /// both of which are cancel safe.
    async fn send_attach(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        is_reattaching: bool,
    ) -> Result<(), Self::AttachError> {
        self.send_attach_inner(writer, is_reattaching).await?;
        Ok(())
    }
}

impl<T> endpoint::Link for ReceiverLink<T> where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync
{
}

impl<T> endpoint::LinkExt for ReceiverLink<T>
where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync,
{
    type FlowState = ReceiverFlowState;
    type Unsettled = ArcReceiverUnsettledMap;
    type Target = T;

    fn local_state(&self) -> &LinkState {
        &self.local_state
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn output_handle_mut(&mut self) -> &mut Option<OutputHandle> {
        &mut self.output_handle
    }

    fn session_stop_reason(&self) -> &Arc<OnceLock<SessionStopReason>> {
        &self.session_stop_reason
    }

    fn flow_state(&self) -> &Self::FlowState {
        &self.flow_state
    }

    fn unsettled(&self) -> &Self::Unsettled {
        &self.unsettled
    }

    fn rcv_settle_mode(&self) -> &ReceiverSettleMode {
        &self.rcv_settle_mode
    }

    fn max_message_size(&self) -> Option<u64> {
        match self.max_message_size {
            0 => None,
            _ => Some(self.max_message_size),
        }
    }

    fn properties<F, O>(&self, op: F) -> O
    where
        F: FnOnce(&Option<Fields>) -> O,
    {
        let guard = self.flow_state.lock.read();
        op(&guard.properties)
    }

    fn properties_mut<F, O>(&self, op: F) -> O
    where
        F: FnOnce(&mut Option<Fields>) -> O,
    {
        let mut guard = self.flow_state.lock.write();
        op(&mut guard.properties)
    }

    /// # Cancel safety
    ///
    /// This is cancel safe: it only awaits on sending over `tokio::mpsc::Sender`
    /// and on `tokio::sync::mpsc::Receiver::recv`, both of which are cancel safe.
    async fn exchange_attach(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        reader: &mut mpsc::Receiver<LinkFrame>,
        is_reattaching: bool,
    ) -> Result<Self::AttachExchange, ReceiverAttachError> {
        // Send out local attach
        self.send_attach(writer, is_reattaching).await?;

        // Wait for remote attach
        let remote_attach = match reader
            .recv()
            .await // cancel safe
            .ok_or_else(|| match self.session_stop_reason.get() {
                Some(reason) => ReceiverAttachError::SessionStopped(reason.clone()),
                None => ReceiverAttachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
            })? {
            LinkFrame::Attach(attach) => attach,
            _ => return Err(ReceiverAttachError::NonAttachFrameReceived),
        };

        self.on_incoming_attach(remote_attach)
    }

    async fn handle_attach_error(
        &mut self,
        attach_error: ReceiverAttachError,
        writer: &crate::session::transfer_queue::Sender,
        reader: &mut mpsc::Receiver<LinkFrame>,
        session: &mpsc::Sender<SessionControl>,
    ) -> ReceiverAttachError {
        match attach_error {
            // Errors that indicate failed attachment
            ReceiverAttachError::SessionStopped(_)
            | ReceiverAttachError::IllegalState
            | ReceiverAttachError::NonAttachFrameReceived
            | ReceiverAttachError::ExpectImmediateDetach
            | ReceiverAttachError::RemoteClosedWithError(_) => attach_error,

            ReceiverAttachError::DuplicatedLinkName => {
                let error = definitions::Error::new(
                    SessionError::HandleInUse,
                    "Link name is in use".to_string(),
                    None,
                );
                session
                    .send(SessionControl::End(Some(error)))
                    .await
                    .map(|_| attach_error)
                    .unwrap_or(match self.session_stop_reason.get() {
                        Some(reason) => ReceiverAttachError::SessionStopped(reason.clone()),
                        None => ReceiverAttachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                    })
            }

            // ReceiverAttachError::SndSettleModeNotSupported
            ReceiverAttachError::IncomingSourceIsNone => {
                // Just send detach immediately
                let err = self
                    .send_detach(writer, true, None)
                    .await
                    .map(|_| attach_error)
                    .unwrap_or(match self.session_stop_reason.get() {
                        Some(reason) => ReceiverAttachError::SessionStopped(reason.clone()),
                        None => ReceiverAttachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                    });
                recv_detach(self, reader, err).await
            }

            ReceiverAttachError::CoordinatorIsNotImplemented
            | ReceiverAttachError::InitialDeliveryCountIsNone
            | ReceiverAttachError::SourceAddressIsNoneWhenDynamicIsTrue
            | ReceiverAttachError::TargetAddressIsSomeWhenDynamicIsTrue
            | ReceiverAttachError::DynamicNodePropertiesIsSomeWhenDynamicIsFalse => {
                match (&attach_error).try_into() {
                    Ok(error) => match self.send_detach(writer, true, Some(error)).await {
                        Ok(_) => recv_detach(self, reader, attach_error).await,
                        Err(_) => match self.session_stop_reason.get() {
                            Some(reason) => ReceiverAttachError::SessionStopped(reason.clone()),
                            None => ReceiverAttachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                        },
                    },
                    Err(_) => attach_error,
                }
            }
            _ => attach_error,
        }
    }
}

async fn recv_detach<T>(
    link: &mut ReceiverLink<T>,
    reader: &mut mpsc::Receiver<LinkFrame>,
    err: ReceiverAttachError,
) -> ReceiverAttachError
where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync,
{
    match reader.recv().await {
        Some(LinkFrame::Detach(remote_detach)) => match link.on_detach_reply(remote_detach) {
            Ok(_) => err,
            Err(detach_error) => detach_error.try_into().unwrap_or(err),
        },
        Some(_) => ReceiverAttachError::NonAttachFrameReceived,
        None => match link.session_stop_reason.get() {
            Some(reason) => ReceiverAttachError::SessionStopped(reason.clone()),
            None => ReceiverAttachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
        },
    }
}

#[cfg(test)]
mod tests {
    use fe2o3_amqp_types::{
        messaging::{
            message::{__private::Serializable, Body},
            AmqpValue, DeliveryAnnotations, Header, Message, MessageAnnotations,
        },
        primitives::{OrderedMap, Value},
    };
    use serde_amqp::to_vec;

    use crate::link::receiver_link::count_number_of_sections_and_offset;

    use super::is_consecutive;

    #[test]
    fn recovery_reconciles_only_complete_peer_maps_and_preserves_local_progress() {
        use super::*;
        for incomplete in [false, true] {
            let known: DeliveryTag = vec![1].into();
            let absent: DeliveryTag = vec![2].into();
            let unknown: DeliveryTag = vec![3].into();
            let mut local = OrderedMap::new();
            for tag in [&known, &absent] {
                let mut entry = receiver_delivery::ReceiverDelivery::new(None);
                entry.received(
                    fe2o3_amqp_types::messaging::Received {
                        section_number: 1,
                        section_offset: 0,
                    }
                    .into(),
                    true,
                );
                local.insert(tag.clone(), entry);
            }
            let map = Arc::new(unsettled_store::Store::new(Some(local)));
            let flow = Arc::new(LinkFlowState::receiver(state::LinkFlowStateInner {
                initial_delivery_count: 0,
                delivery_count: 0,
                link_credit: 1,
                available: 0,
                drain: false,
                properties: None,
            }));
            let mut link = Receiver::builder()
                .name("resume")
                .source("source")
                .target("target")
                .create_link(
                    map.clone(),
                    OutputHandle(0),
                    flow,
                    Arc::new(OnceLock::new()),
                    65532,
                );
            link.local_state = LinkState::Attached;
            let mut peer = OrderedMap::new();
            peer.insert(
                known.clone(),
                Some(fe2o3_amqp_types::messaging::Accepted {}.into()),
            );
            peer.insert(unknown.clone(), None);
            let result = link.handle_unsettled_in_attach(Some(peer), incomplete);
            assert_eq!(
                matches!(result, ReceiverAttachExchange::IncompleteUnsettled),
                incomplete
            );
            let guard = map.read();
            let local = guard.as_ref().unwrap();
            assert_eq!(local.contains_key(&absent), incomplete);
            assert!(!local.contains_key(&unknown));
            assert!(local.get(&known).unwrap().received);
            assert!(matches!(
                local.get(&known).unwrap().local,
                Some(DeliveryState::Received(_))
            ));
            assert!(matches!(
                local.get(&known).unwrap().remote,
                Some(DeliveryState::Accepted(_))
            ));
        }
    }

    #[test]
    fn test_section_numbers() {
        let message = Message {
            header: Some(Header {
                durable: true,
                ..Default::default()
            }),
            // header: None,
            delivery_annotations: Some(DeliveryAnnotations(OrderedMap::new())),
            // delivery_annotations: None,
            message_annotations: Some(MessageAnnotations(OrderedMap::new())),
            // message_annotations: None,
            properties: None,
            application_properties: None,
            body: Body::Value(AmqpValue(Value::Bool(true))),
            footer: None,
        };
        // let mut buf = Vec::new();
        // let mut serializer = serde_amqp::ser::Serializer::new(&mut buf);
        // message.serialize(&mut serializer).unwrap();
        let buf = to_vec(&Serializable(message)).unwrap();
        let (_nums, _offset) = count_number_of_sections_and_offset(&buf).unwrap();
    }

    #[test]
    fn test_consecutive_chunks() {
        let expected = [vec![0u32, 1, 2, 3], vec![5, 6], vec![8, 9], vec![11]];
        let vals: Vec<u32> = expected.iter().flatten().copied().collect();
        assert_eq!(vals.len() - 1, vals.windows(2).len());

        let inds: Vec<usize> = vals
            .windows(2)
            .enumerate()
            .filter_map(|(i, vals)| {
                if is_consecutive(&vals[0], &vals[1]) {
                    None
                } else {
                    Some(i + 1)
                }
            })
            .collect();

        let mut prev_ind = 0;
        for i in 0..inds.len() {
            let ind = inds[i];
            let slice = &vals[prev_ind..ind];
            assert_eq!(slice, expected[i]);
            prev_ind = ind;
        }
        let final_slice = &vals[prev_ind..];
        assert_eq!(final_slice, expected.last().unwrap())
    }
}
