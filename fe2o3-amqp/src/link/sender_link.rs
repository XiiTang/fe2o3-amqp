use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, OnceLock,
};

use fe2o3_amqp_types::{definitions::Fields, messaging::MESSAGE_FORMAT};
use futures_util::Future;
use serde_amqp::serialized_size;

use crate::endpoint::LinkExt;

use super::{
    resumption::{resume_delivery, Resumption},
    *,
};

impl<T> SenderLink<T>
where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync,
{
    /// Whether a transfer goes out settled, which leaves no outcome to wait for.
    ///
    /// If not set on the first (or only) transfer for a (multi-transfer) delivery, then the
    /// settled flag MUST be interpreted as being false.
    pub(crate) fn is_settled_on_send(&self, transfer: &Transfer) -> bool {
        transfer.settled.unwrap_or(match self.snd_settle_mode {
            SenderSettleMode::Settled => true,
            SenderSettleMode::Unsettled => false,
            SenderSettleMode::Mixed => false,
        })
    }

    /// Hands the frames of a transfer that resumes, restates or aborts a
    /// delivery of an earlier attachment to the session, without taking link
    /// credit: the delivery keeps its tag (AMQP 1.0 §2.6.13), and its entry
    /// in the store, which the resume left there.
    pub(crate) async fn send_resuming_transfer<Fut>(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        detached: Fut,
        transfer: Transfer,
        payload: Payload,
    ) -> Result<(), LinkStateError>
    where
        Fut: Future<Output = Option<LinkFrame>> + Send,
    {
        let attachment = self
            .output_handle
            .clone()
            .ok_or(LinkStateError::IllegalState)?;
        let input_handle = self
            .input_handle
            .clone()
            .ok_or(LinkStateError::IllegalState)?;
        tokio::pin!(detached);
        for (transfer, payload) in self.split_transfer(transfer, payload)? {
            let frame = LinkFrame::Transfer {
                window_slot: None,
                queue_slot: None,
                input_handle: input_handle.clone(),
                attachment: attachment.id,
                performative: transfer,
                payload,
            };
            if let Err(cause) = self.hand_over(writer, &mut detached, frame).await {
                return Err(self.interrupted(cause));
            }
        }
        Ok(())
    }

    /// The frames a delivery's transfer and payload go out in, in order. Every
    /// transfer a link hands to the session is split here: new deliveries
    /// (`send_delivery`), and resumed, restated and aborted ones
    /// (`send_resuming_transfer`).
    fn split_transfer(
        &self,
        mut transfer: Transfer,
        mut payload: Payload,
    ) -> Result<Vec<(Transfer, Payload)>, LinkStateError> {
        let mut frames = Vec::new();

        // The connection engine publishes the negotiated encoder max frame
        // length before the connection handle is created; links are only
        // attached after the connection is opened, so the value is always
        // populated. Per AMQP 1.0 §2.7.1 `max-frame-size` is directional: a
        // peer MUST NOT send frames larger than its partner can accept, so
        // the outbound limit is the remote's advertised value as enforced by
        // the transport encoder (see `ConnectionEngine::max_frame_size`).
        //
        // Maximum payload bytes per transfer frame. The frame body (serialized
        // transfer performative + payload) must fit within the encoder's
        // `max_frame_body_size` (i.e. `self.max_frame_size - 4`): the 4-byte
        // frame header (doff, type, channel) is written by the encoder and the
        // 4-byte size prefix is added by the length-delimited codec, so a
        // frame that respects this bound never exceeds the negotiated max
        // frame size. The delivery-id is only assigned by the session *after*
        // the link splits, so its worst-case size (a uint, 0x70 + 4 bytes) is
        // reserved to guarantee that the first frame still fits. The transfer
        // is measured in place with the worst-case delivery-id set (and
        // restored afterwards) so the real serializer decides the list
        // encoding; `serialized_size` computes the size without allocating a
        // buffer. The first frame of a split delivery is sent with `more=true`
        // (the single-frame branch below explicitly resets it to `false`).
        transfer.more = true;
        let orig_delivery_id = transfer.delivery_id; // None on all send paths
        transfer.delivery_id = Some(u32::MAX);
        let performative_size =
            serialized_size(&transfer).map_err(|_| LinkStateError::IllegalState)?; // This should not happen
        transfer.delivery_id = orig_delivery_id;
        // A frame is also bounded by what the session's transfer queue
        // retains per frame: a peer that omits max-frame-size accepts frames
        // of any size, which the queue could never take.
        let max_payload = (self.max_frame_size - 4 - performative_size)
            .min(crate::session::transfer_queue::MAX_FRAME_PAYLOAD);

        // Split the payload so that every transfer frame fits within the
        // negotiated max frame size and the queue, keeping the session's
        // transfer-id and window accounting consistent with the frames
        // actually sent.
        let more = payload.len() > max_payload;
        if !more {
            transfer.more = false;
            frames.push((transfer, payload));
        } else {
            // The first frame
            let partial = payload.split_to(max_payload);
            transfer.more = true;
            frames.push((transfer.clone(), partial));

            // AMQP 1.0 §2.7.5: on continuation transfers the delivery-id may be
            // omitted and the delivery-tag / message-format can only be omitted.
            // Clear them *before* the loop so the last transfer is also cleared
            // even when there is no middle frame (i.e. the delivery spans exactly
            // two frames).
            transfer.delivery_tag = None;
            transfer.message_format = None;
            transfer.settled = None;
            transfer.rcv_settle_mode = None;

            // The transfers in the middle
            while payload.len() > max_payload {
                let partial = payload.split_to(max_payload);
                frames.push((transfer.clone(), partial));
            }

            // The last transfer
            // For messages that are too large to fit within the maximum frame size, additional
            // data MAY be trans- ferred in additional transfer frames by setting the more flag on
            // all but the last transfer frame
            transfer.more = false;
            frames.push((transfer, payload));
        }

        Ok(frames)
    }

    /// Sends a new delivery. This is the one place a delivery takes link
    /// credit and, with an outcome to wait for, is registered: it waits for
    /// credit, then for the session queue's room for its first frame,
    /// holding neither while it waits for the other and either wait ending
    /// at the peer's detach. Then, in one synchronous step under the
    /// unsettled store's lock, it takes the credit, gives the first frame the
    /// delivery tag the credit gives, registers `unsettled` under it and
    /// hands the frame over. A send cut short, or refused, before that step
    /// keeps the link's credit and delivery-count, which the receiver's next
    /// flow would never give back; the relay closing the store for the
    /// peer's closing detach is ordered with it by the same lock, so a
    /// delivery is either refused with the close's error or failed by it.
    /// qpid-proton (`pni_advance_sender`) and azure-amqp (`TrySendDelivery`)
    /// likewise take credit as a delivery enters the session.
    ///
    /// The later frames of a delivery spanning several are handed over as the
    /// queue takes them, each wait ending at the peer's detach. A closing
    /// detach then fails the send; a suspending one ends the hand-over to
    /// that attachment: a registered delivery keeps its whole payload and
    /// registration, for an explicit resume to send again, and the send
    /// returns its pending settlement; a settled one is lost, which its
    /// settlement allows, and the send fails.
    ///
    /// `transfer`'s own delivery tag only sizes the frames: every tag the
    /// credit gives is four bytes.
    ///
    /// # Cancel safety
    ///
    /// Dropped before the first frame is handed over, the send takes nothing;
    /// dropped later, it withdraws the registration, so the map keeps only
    /// deliveries whose transfer was handed over or that a suspension left to
    /// resume.
    #[allow(
        clippy::too_many_arguments,
        reason = "keep native admission and optional control provenance in the same checked send entry"
    )]
    pub(crate) async fn send_delivery<Fut>(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        detached: Fut,
        transfer: Transfer,
        payload: Payload,
        unsettled: Option<UnsettledMessage>,
        dispatched: Option<&AtomicBool>,
        #[cfg(feature = "transaction")] tracking: Option<&crate::transaction::ControlTracker>,
    ) -> Result<DeliveryTag, LinkStateError>
    where
        Fut: Future<Output = Option<LinkFrame>> + Send,
    {
        let registration = match unsettled {
            Some(unsettled) => Registration::New(unsettled),
            None => Registration::None,
        };
        self.deliver(
            writer,
            detached,
            transfer,
            payload,
            registration,
            dispatched,
            #[cfg(feature = "transaction")]
            tracking,
        )
        .await
        .map(|delivery_tag| delivery_tag.expect("only a moved delivery can be gone"))
    }

    /// [`Self::send_delivery`], registering the delivery as `registration`
    /// says. `None` when the delivery a `Registration::Move` would send was
    /// settled or withdrawn before it could be: nothing is sent then.
    #[allow(
        clippy::too_many_arguments,
        reason = "keep native admission and optional control provenance in the same checked send entry"
    )]
    async fn deliver<Fut>(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        detached: Fut,
        transfer: Transfer,
        payload: Payload,
        mut registration: Registration,
        dispatched: Option<&AtomicBool>,
        #[cfg(feature = "transaction")] tracking: Option<&crate::transaction::ControlTracker>,
    ) -> Result<Option<DeliveryTag>, LinkStateError>
    where
        Fut: Future<Output = Option<LinkFrame>> + Send,
    {
        let attachment = self
            .output_handle
            .clone()
            .ok_or(LinkStateError::IllegalState)?;
        let input_handle = self
            .input_handle
            .clone()
            .ok_or(LinkStateError::IllegalState)?;
        let mut frames = self.split_transfer(transfer, payload)?.into_iter();
        let (mut first, first_payload) = frames.next().ok_or(LinkStateError::IllegalState)?;
        tokio::pin!(detached);
        let registered = !matches!(registration, Registration::None);
        let moved = matches!(registration, Registration::Move(_));
        let delivery_tag = loop {
            self.credit_or_detached(&mut detached).await?;
            // The detach branch is polled first, as for credit.
            let room = tokio::select! {
                biased;
                frame = detached.as_mut() => { // cancel safe
                    return Err(self.interrupted(Stopped::Detached(frame)));
                }
                room = writer.reserve(first_payload.len()) => { // cancel safe
                    room.map_err(|_| stopped(&self.session_stop_reason))?
                }
            };
            let mut open = match registered {
                // The relay closed the store for the peer's closing detach,
                // which the link has yet to read, when this fails.
                true => Some(self.unsettled.open()?),
                false => None,
            };
            if let (Registration::Move(old), Some(open)) = (&registration, open.as_ref()) {
                if !open.contains(old) {
                    // Settled or withdrawn meanwhile: nothing to send.
                    return Ok(None);
                }
            }
            let Some(tag) = self.flow_state.try_consume(1) else {
                // A drain took the credit while the frame waited for room.
                continue;
            };
            let delivery_tag = DeliveryTag::from(tag);
            first.delivery_tag = Some(delivery_tag.clone());
            let unsettled = match std::mem::replace(&mut registration, Registration::None) {
                Registration::New(unsettled) => Some(unsettled),
                Registration::Move(old) => open.as_mut().and_then(|open| open.withdraw(&old)),
                Registration::None => None,
            };
            if let (Some(open), Some(unsettled)) = (open.as_mut(), unsettled) {
                // Registered before the hand-over: the session may settle the
                // delivery before this task resumes.
                open.register(delivery_tag.clone(), unsettled);
            }
            room.send(LinkFrame::Transfer {
                window_slot: None,
                queue_slot: None,
                input_handle: input_handle.clone(),
                attachment: attachment.id,
                performative: first,
                payload: first_payload,
            });
            #[cfg(feature = "transaction")]
            if let Some(tracker) = tracking {
                tracker.first_admitted(attachment.id);
            }
            drop(open);
            break delivery_tag;
        };
        if let Some(dispatched) = dispatched {
            dispatched.store(true, Ordering::Release);
        }
        // A new delivery cut short is withdrawn, its outcome with it; a moved
        // one was the store's before this send and stays, under its new tag,
        // for the next resume or the close.
        let registration = registered.then(|| UnsettledRegistration {
            unsettled: self.unsettled.clone(),
            delivery_tag: (!moved).then(|| delivery_tag.clone()),
        });
        for (transfer, payload) in frames {
            let frame = LinkFrame::Transfer {
                window_slot: None,
                queue_slot: None,
                input_handle: input_handle.clone(),
                attachment: attachment.id,
                performative: transfer,
                payload,
            };
            if let Err(cause) = self.hand_over(writer, &mut detached, frame).await {
                let error = self.interrupted(cause);
                return match (registration, &error) {
                    (
                        Some(registration),
                        LinkStateError::RemoteDetached | LinkStateError::RemoteDetachedWithError(_),
                    ) => {
                        // Suspended: the delivery waits for an explicit resume.
                        registration.keep();
                        #[cfg(feature = "transaction")]
                        if tracking.is_some() {
                            return Err(error);
                        }
                        Ok(Some(delivery_tag))
                    }
                    _ => Err(error),
                };
            }
            if let Some(dispatched) = dispatched {
                dispatched.store(true, Ordering::Release);
            }
        }
        if let Some(registration) = registration {
            registration.keep();
        }
        #[cfg(feature = "transaction")]
        if let Some(tracker) = tracking {
            tracker.complete_admitted();
        }
        Ok(Some(delivery_tag))
    }

    /// Sends a new delivery of `payload`, settled or with an outcome to wait
    /// for, as the link's settle mode and `transfer` say.
    #[allow(
        clippy::too_many_arguments,
        reason = "keep native admission and optional control provenance in the same checked send entry"
    )]
    pub(crate) async fn send_new_payload<Fut>(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        detached: Fut,
        message_format: MessageFormat,
        transfer: Transfer,
        payload: Payload,
        dispatched: Option<&AtomicBool>,
        #[cfg(feature = "transaction")] tracking: Option<&crate::transaction::ControlTracker>,
    ) -> Result<Settlement, LinkStateError>
    where
        Fut: Future<Output = Option<LinkFrame>> + Send,
    {
        if self.is_settled_on_send(&transfer) {
            let delivery_tag = self
                .send_delivery(
                    writer,
                    detached,
                    transfer,
                    payload,
                    None,
                    dispatched,
                    #[cfg(feature = "transaction")]
                    tracking,
                )
                .await?;
            return Ok(Settlement::Settled(delivery_tag));
        }
        let (tx, rx) = crate::completion::channel();
        #[cfg(feature = "transaction")]
        let unsettled = match tracking {
            Some(tracking) => UnsettledMessage::new_tracked(
                payload.clone(),
                None,
                message_format,
                tx,
                tracking.clone(),
            ),
            None => UnsettledMessage::new(payload.clone(), None, message_format, tx),
        };
        #[cfg(not(feature = "transaction"))]
        let unsettled = UnsettledMessage::new(payload.clone(), None, message_format, tx);
        let delivery_tag = self
            .send_delivery(
                writer,
                detached,
                transfer,
                payload,
                Some(unsettled),
                dispatched,
                #[cfg(feature = "transaction")]
                tracking,
            )
            .await?;
        Ok(Settlement::Unsettled {
            delivery_tag,
            outcome: rx,
        })
    }

    /// Sends the delivery under `delivery_tag` again as a new delivery, after
    /// a resume found the peer has no record of it. It stays in the store
    /// under its old tag until the step that registers it under the new one,
    /// so a resend cut short leaves it for the next resume, or the close.
    pub(crate) async fn resend<Fut>(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        detached: Fut,
        delivery_tag: DeliveryTag,
    ) -> Result<(), LinkStateError>
    where
        Fut: Future<Output = Option<LinkFrame>> + Send,
    {
        let Some((payload, message_format)) = self.unsettled.read().as_ref().and_then(|map| {
            let local = map.get(&delivery_tag)?;
            Some((local.payload.clone(), local.message_format))
        }) else {
            return Ok(());
        };
        let transfer = self.generate_non_resuming_transfer_performative(
            DeliveryTag::from([0; 4]),
            message_format,
            None,
            None,
            false,
        )?;
        if self.is_settled_on_send(&transfer) {
            self.send_delivery(
                writer,
                detached,
                transfer,
                payload,
                None,
                None,
                #[cfg(feature = "transaction")]
                None,
            )
            .await?;
            if let Some(local) = self.unsettled.withdraw(&delivery_tag) {
                let _ = local.settle();
            }
            return Ok(());
        }
        self.deliver(
            writer,
            detached,
            transfer,
            payload,
            Registration::Move(delivery_tag),
            None,
            #[cfg(feature = "transaction")]
            None,
        )
        .await
        .map(|_| ())
    }

    /// Hands `frame` to the session queue, unless the peer's detach, or the
    /// session's stop, comes first.
    async fn hand_over<Fut>(
        &self,
        writer: &crate::session::transfer_queue::Sender,
        detached: &mut std::pin::Pin<&mut Fut>,
        frame: LinkFrame,
    ) -> Result<(), Stopped>
    where
        Fut: Future<Output = Option<LinkFrame>> + Send,
    {
        tokio::select! {
            biased;
            frame = detached.as_mut() => Err(Stopped::Detached(frame)), // cancel safe
            sent = writer.send(frame) => sent.map_err(|_| Stopped::Session), // cancel safe
        }
    }

    /// The error a hand-over stopped by `stopped` reports. A peer's detach,
    /// which the relay already answered, is recorded on the link here.
    fn interrupted(&mut self, cause: Stopped) -> LinkStateError {
        match cause {
            // Reported as-is even when the session is stopping: a stop
            // without a detach shows up as the channel closing (`None`).
            Stopped::Detached(Some(LinkFrame::Detach(detach, _))) => {
                let closed = detach.closed;
                match (self.apply_remote_detach_outcome(detach), closed) {
                    (Ok(_), true) => LinkStateError::RemoteClosed,
                    (Ok(_), false) => LinkStateError::RemoteDetached,
                    (Err(err), _) => LinkStateError::from(err),
                }
            }
            Stopped::Detached(Some(_frame)) => {
                // Other frames should not forwarded to the sender by the session
                #[cfg(feature = "tracing")]
                tracing::error!("Unexpected frame: {:?}", _frame);
                #[cfg(feature = "log")]
                log::error!("Unexpected frame: {:?}", _frame);

                LinkStateError::ExpectImmediateDetach
            }
            // The channel closed without a frame: the session (or its
            // connection) stopped and the engine dropped the relay.
            Stopped::Detached(None) => match self.session_stop_reason.get() {
                Some(reason) => LinkStateError::SessionStopped(reason.clone()),
                None => LinkStateError::ExpectImmediateDetach, // defensive: no stop reason recorded; failure is link-local
            },
            Stopped::Session => stopped(&self.session_stop_reason),
        }
    }

    /// Waits for link credit, or for the peer's detach.
    async fn credit_or_detached<Fut>(
        &mut self,
        detached: &mut std::pin::Pin<&mut Fut>,
    ) -> Result<(), LinkStateError>
    where
        Fut: Future<Output = Option<LinkFrame>> + Send,
    {
        // The detach branch is polled first so that a remote detach that has
        // already arrived is observed before a concurrently granted link-credit
        // is used. Otherwise a send could race the remote closing the link,
        // consuming credit and writing a transfer the relay can no longer
        // forward.
        tokio::select! {
            biased;
            frame = detached.as_mut() => Err(self.interrupted(Stopped::Detached(frame))), // cancel safe
            () = self.flow_state.credit_ready(1) => {
                // link-credit is defined as
                // "The current maximum number of messages that can be handled
                // at the receiver endpoint of the link"

                // Draining should already set the link credit to 0, causing
                // sender to wait for new link credit
                Ok(())
            }
        }
    }

    pub(crate) fn generate_non_resuming_transfer_performative(
        &self,
        delivery_tag: DeliveryTag,
        message_format: MessageFormat,
        settled: Option<bool>,
        state: Option<DeliveryState>,
        batchable: bool,
    ) -> Result<Transfer, LinkStateError> {
        let handle = self
            .output_handle
            .clone()
            .ok_or(LinkStateError::IllegalState)?
            .into();

        let settled = match self.snd_settle_mode {
            SenderSettleMode::Settled => true,
            SenderSettleMode::Unsettled => false,
            // If not set on the first (or only) transfer for a (multi-transfer)
            // delivery, then the settled flag MUST be interpreted as being false.
            SenderSettleMode::Mixed => settled.unwrap_or(false),
        };

        // If true, the resume flag indicates that the transfer is being used to reassociate an
        // unsettled delivery from a dissociated link endpoint
        let resume = false;

        let transfer = Transfer {
            handle,
            delivery_id: None, // This will be set by the session
            delivery_tag: Some(delivery_tag),
            message_format: Some(message_format),
            settled: Some(settled),
            more: false, // This will be changed later

            // If not set, this value is defaulted to the value negotiated
            // on link attach.
            rcv_settle_mode: None,
            state,
            resume,
            aborted: false,
            batchable,
        };
        Ok(transfer)
    }
}

impl<T> endpoint::SenderLink for SenderLink<T>
where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync,
{
    type FlowError = FlowError;
    type TransferError = LinkStateError;
    type DispositionError = DispositionError;

    async fn send_payload<Fut>(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        detached: Fut,
        payload: Payload,
        message_format: MessageFormat,
        settled: Option<bool>,
        state: Option<DeliveryState>,
        batchable: bool,
    ) -> Result<Settlement, Self::TransferError>
    where
        Fut: Future<Output = Option<LinkFrame>> + Send,
    {
        let transfer = self.generate_non_resuming_transfer_performative(
            DeliveryTag::from([0; 4]),
            message_format,
            settled,
            state,
            batchable,
        )?;

        self.send_new_payload(
            writer,
            detached,
            message_format,
            transfer,
            payload,
            None,
            #[cfg(feature = "transaction")]
            None,
        )
        .await
    }

    async fn dispose(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        delivery_id: DeliveryNumber,
        delivery_tag: DeliveryTag,
        settled: bool,
        state: DeliveryState,
        batchable: bool,
    ) -> Result<(), Self::DispositionError> {
        if let SenderSettleMode::Settled = self.snd_settle_mode {
            return Ok(());
        }

        if settled {
            if let Some(msg) = self.unsettled.withdraw(&delivery_tag) {
                let _ = msg.settle();
            }
        } else {
            self.unsettled.set_state(&delivery_tag, Some(state.clone()));
        }

        send_disposition(
            writer,
            delivery_id,
            None,
            settled,
            Some(state),
            batchable,
            &self.session_stop_reason,
        )
        .await
    }

    async fn batch_dispose(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        mut ids_and_tags: Vec<(DeliveryNumber, DeliveryTag)>,
        settled: bool,
        state: DeliveryState,
        batchable: bool,
    ) -> Result<(), Self::DispositionError> {
        if let SenderSettleMode::Settled = self.snd_settle_mode {
            return Ok(());
        }

        let mut first = None;
        let mut last = None;

        ids_and_tags.sort_by_key(|left| left.0);

        // Find continuous ranges
        for (delivery_id, delivery_tag) in ids_and_tags {
            if settled {
                if let Some(msg) = self.unsettled.withdraw(&delivery_tag) {
                    let _ = msg.settle();
                }
            } else {
                self.unsettled.set_state(&delivery_tag, Some(state.clone()));
            }

            match (first, last) {
                // First pair
                (None, _) => first = Some(delivery_id),
                // Second pair
                (Some(first_id), None) => {
                    // Find discontinuity
                    if delivery_id - first_id > 1 {
                        send_disposition(
                            writer,
                            first_id,
                            None,
                            settled,
                            Some(state.clone()),
                            batchable,
                            &self.session_stop_reason,
                        )
                        .await?;
                    }
                    last = Some(delivery_id);
                }
                // Third and more
                (Some(first_id), Some(last_id)) => {
                    // Find discontinuity
                    if delivery_id - last_id > 1 {
                        send_disposition(
                            writer,
                            first_id,
                            Some(last_id),
                            settled,
                            Some(state.clone()),
                            batchable,
                            &self.session_stop_reason,
                        )
                        .await?;
                    }
                    last = Some(delivery_id);
                }
            }
        }

        // if there is only one message to dispose
        if let (Some(first_id), None) = (first, last) {
            send_disposition(
                writer,
                first_id,
                None,
                settled,
                Some(state),
                batchable,
                &self.session_stop_reason,
            )
            .await?;
        }
        Ok(())
    }
}

/// The error a refused hand-over to the session reports.
fn stopped(session_stop_reason: &OnceLock<SessionStopReason>) -> LinkStateError {
    match session_stop_reason.get() {
        Some(reason) => LinkStateError::SessionStopped(reason.clone()),
        None => LinkStateError::IllegalState, // defensive: no stop reason recorded; failure is link-local
    }
}

#[inline]
async fn send_disposition(
    writer: &crate::session::transfer_queue::Sender,
    first: DeliveryNumber,
    last: Option<DeliveryNumber>,
    settled: bool,
    state: Option<DeliveryState>,
    batchable: bool,
    session_stop_reason: &OnceLock<SessionStopReason>,
) -> Result<(), IllegalLinkStateError> {
    let disposition = Disposition {
        role: Role::Sender,
        first,
        last,
        settled,
        state,
        batchable,
    };
    let frame = LinkFrame::Disposition(disposition);
    writer
        .send(frame)
        .await
        .map_err(|_| match session_stop_reason.get() {
            Some(reason) => IllegalLinkStateError::SessionStopped(reason.clone()),
            None => IllegalLinkStateError::IllegalState, // defensive: no stop reason recorded; failure is link-local
        })
}

impl<T> SenderLink<T> {
    /// Reconciles the local unsettled deliveries with the peer's map. The
    /// deliveries stay in the store: one that settles here is settled now,
    /// the rest wait there for their resume step.
    fn handle_unsettled_in_attach(
        &mut self,
        remote_unsettled: Option<OrderedMap<DeliveryTag, Option<DeliveryState>>>,
    ) -> Result<SenderAttachExchange, SenderAttachError> {
        let mut remote_map = remote_unsettled.unwrap_or_default();
        let local_empty = self
            .unsettled
            .read()
            .as_ref()
            .is_none_or(|map| map.is_empty());
        if local_empty && remote_map.is_empty() {
            return Ok(SenderAttachExchange::Complete);
        }
        let decided: Vec<(DeliveryTag, Resumption)> = self
            .unsettled
            .read()
            .iter()
            .flatten()
            .map(|(tag, local)| {
                let remote = remote_map.swap_remove(tag);
                (tag.clone(), resume_delivery(local, remote))
            })
            .collect();
        let mut v = Vec::with_capacity(decided.len() + remote_map.len());
        for (tag, resumption) in decided {
            match resumption {
                Resumption::Resuming(resuming) => v.push((tag, resuming)),
                Resumption::Settle(state) => {
                    // This will fail if the oneshot receiver is already
                    // dropped, which means the application probably doesn't
                    // care about the delivery state anyway
                    if let Some(local) = self.unsettled.withdraw(&tag) {
                        let _ = local.settle_with_state(state);
                    }
                }
            }
        }
        // These are unsettled messages not found in the local map, assume
        // the message format is 0
        v.extend(remote_map.into_keys().map(|tag| {
            (
                tag,
                ResumingDelivery::Abort {
                    message_format: MESSAGE_FORMAT,
                },
            )
        }));

        match self.local_state {
            LinkState::IncompleteAttachReceived
            | LinkState::IncompleteAttachSent
            | LinkState::IncompleteAttachExchanged => {
                Ok(SenderAttachExchange::IncompleteUnsettled(v))
            }
            _ => Ok(SenderAttachExchange::Resume(v)),
        }
    }
}

impl<T> endpoint::LinkAttach for SenderLink<T>
where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync,
{
    type AttachExchange = SenderAttachExchange;
    type AttachError = SenderAttachError;

    fn on_incoming_attach(
        &mut self,
        attach: Attach,
    ) -> Result<Self::AttachExchange, Self::AttachError> {
        self.accept_attach(attach, AttachPurpose::Ordinary)
    }

    fn on_incoming_attach_for_close(&mut self, attach: Attach) -> Result<(), Self::AttachError> {
        match self.accept_attach(attach, AttachPurpose::CloseOnly)? {
            SenderAttachExchange::Complete => Ok(()),
            _ => Err(SenderAttachError::IllegalState),
        }
    }

    async fn send_attach(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        is_reattaching: bool,
    ) -> Result<(), Self::AttachError> {
        self.send_attach_inner(writer, is_reattaching, &AttachPurpose::Ordinary)
            .await?;
        Ok(())
    }
}

impl<T> endpoint::Link for SenderLink<T> where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync
{
}

impl<T> endpoint::LinkExt for SenderLink<T>
where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync,
{
    type FlowState = SenderFlowState;
    type Unsettled = ArcSenderUnsettledMap;
    type Target = T;

    fn local_state(&self) -> &LinkState {
        &self.local_state
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn output_handle_mut(&mut self) -> &mut Option<crate::endpoint::OutputAttachment> {
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
        let guard = self.flow_state.state().lock.read();
        op(&guard.properties)
    }

    fn properties_mut<F, O>(&self, op: F) -> O
    where
        F: FnOnce(&mut Option<Fields>) -> O,
    {
        let mut guard = self.flow_state.state().lock.write();
        op(&mut guard.properties)
    }

    async fn exchange_attach(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        reader: &mut mpsc::Receiver<LinkFrame>,
        is_reattaching: bool,
    ) -> Result<Self::AttachExchange, Self::AttachError> {
        self.exchange_attach_inner(writer, reader, is_reattaching, AttachPurpose::Ordinary)
            .await
    }

    async fn exchange_attach_for_close(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        reader: &mut mpsc::Receiver<LinkFrame>,
    ) -> Result<(), Self::AttachError> {
        match self
            .exchange_attach_inner(writer, reader, true, AttachPurpose::CloseOnly)
            .await?
        {
            SenderAttachExchange::Complete => Ok(()),
            _ => Err(SenderAttachError::IllegalState),
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn handle_attach_error(
        &mut self,
        attach_error: SenderAttachError,
        writer: &crate::session::transfer_queue::Sender,
        reader: &mut mpsc::Receiver<LinkFrame>,
        session: &mpsc::Sender<SessionControl>,
    ) -> SenderAttachError {
        match attach_error {
            SenderAttachError::NativeBufferTooLarge
            | SenderAttachError::SessionStopped(_)
            | SenderAttachError::SessionNotMapped
            | SenderAttachError::IllegalState
            | SenderAttachError::NonAttachFrameReceived
            | SenderAttachError::ExpectImmediateDetach
            | SenderAttachError::RemoteClosedWithError(_) => attach_error,

            SenderAttachError::DuplicatedLinkName => {
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
                        Some(reason) => SenderAttachError::SessionStopped(reason.clone()),
                        None => SenderAttachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                    })
            }

            SenderAttachError::SndSettleModeNotSupported
            | SenderAttachError::IncomingTargetIsNone => {
                // Just send detach immediately
                let err = self
                    .send_detach(writer, true, None)
                    .await
                    .map(|_| attach_error)
                    .unwrap_or(match self.session_stop_reason.get() {
                        Some(reason) => SenderAttachError::SessionStopped(reason.clone()),
                        None => SenderAttachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                    });
                recv_detach(self, reader, err).await
            }

            SenderAttachError::CoordinatorIsNotImplemented
            | SenderAttachError::SourceAddressIsSomeWhenDynamicIsTrue
            | SenderAttachError::TargetAddressIsNoneWhenDynamicIsTrue
            | SenderAttachError::DynamicNodePropertiesIsSomeWhenDynamicIsFalse => {
                try_detach_with_error(self, attach_error, writer, reader).await
            }
            #[cfg(feature = "transaction")]
            SenderAttachError::DesireTxnCapabilitiesNotSupported => {
                try_detach_with_error(self, attach_error, writer, reader).await
            }
        }
    }
}

async fn try_detach_with_error<T>(
    link: &mut SenderLink<T>,
    attach_error: SenderAttachError,
    writer: &crate::session::transfer_queue::Sender,
    reader: &mut mpsc::Receiver<LinkFrame>,
) -> SenderAttachError
where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync,
{
    match (&attach_error).try_into() {
        Ok(err) => {
            match link.send_detach(writer, true, Some(err)).await {
                Ok(_) => match reader.recv().await {
                    Some(LinkFrame::Detach(remote_detach, _)) => {
                        let _ = link.on_detach_reply(remote_detach); // FIXME: hadnle detach errors?
                        attach_error
                    }
                    Some(_) => SenderAttachError::NonAttachFrameReceived,
                    None => match link.session_stop_reason().get() {
                        Some(reason) => SenderAttachError::SessionStopped(reason.clone()),
                        None => SenderAttachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                    },
                },
                Err(_) => match link.session_stop_reason().get() {
                    Some(reason) => SenderAttachError::SessionStopped(reason.clone()),
                    None => SenderAttachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                },
            }
        }
        Err(_) => attach_error,
    }
}

async fn recv_detach<T>(
    link: &mut SenderLink<T>,
    reader: &mut mpsc::Receiver<LinkFrame>,
    err: SenderAttachError,
) -> SenderAttachError
where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync,
{
    match reader.recv().await {
        Some(LinkFrame::Detach(remote_detach, _)) => match link.on_detach_reply(remote_detach) {
            Ok(_) => err,
            Err(detach_error) => detach_error.try_into().unwrap_or(err),
        },
        Some(_) => SenderAttachError::NonAttachFrameReceived,
        None => match link.session_stop_reason.get() {
            Some(reason) => SenderAttachError::SessionStopped(reason.clone()),
            None => SenderAttachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
        },
    }
}

/// Withdraws a new delivery registered in the unsettled map unless its whole transfer was handed
/// to the session or a suspension left it to resume: on an error, and when the sending future is
/// dropped before the hand-over. Without a tag it withdraws nothing, for a delivery that was the
/// store's before the send.
struct UnsettledRegistration {
    unsettled: ArcSenderUnsettledMap,
    delivery_tag: Option<DeliveryTag>,
}

impl UnsettledRegistration {
    fn keep(mut self) {
        self.delivery_tag = None;
    }
}

impl Drop for UnsettledRegistration {
    fn drop(&mut self) {
        if let Some(delivery_tag) = self.delivery_tag.take() {
            let _ = self.unsettled.withdraw(&delivery_tag);
        }
    }
}

/// How a new delivery is registered at the step that takes its credit.
enum Registration {
    /// Settled on send: nothing to register.
    None,
    /// A delivery new to the store.
    New(UnsettledMessage),
    /// The delivery the store holds under this tag, moved to the new one.
    Move(DeliveryTag),
}

/// What stopped a hand-over to the session before the queue took the frame.
#[allow(
    clippy::large_enum_variant,
    reason = "keep the peer's detach inline on the error path, as LinkFrame is kept elsewhere"
)]
enum Stopped {
    /// The peer's detach, or the link's channel closing without one.
    Detached(Option<LinkFrame>),
    /// The session's queue closed.
    Session,
}

impl<T> SenderLink<T>
where
    T: Into<TargetArchetype>
        + TryFrom<TargetArchetype>
        + VerifyTargetArchetype
        + Clone
        + Send
        + Sync,
{
    fn accept_attach(
        &mut self,
        remote_attach: Attach,
        purpose: AttachPurpose,
    ) -> Result<SenderAttachExchange, SenderAttachError> {
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
            _ => return Err(SenderAttachError::IllegalState),
        };

        self.input_handle = Some(InputHandle::from(remote_attach.handle));

        // In this case, the sender is considered to hold the authoritative version of the
        // version of the source properties
        //
        // Only need to check the target
        //
        // If there is no pre-existing terminus, and the peer does not wish to create a new one,
        // this is indicated by setting the local terminus (source or target as appropriate) to null.
        if self.verify_incoming_source {
            if let (Some(local_source), Some(remote_source)) = (&self.source, &remote_attach.source)
            {
                local_source.verify_as_sender(remote_source)?;
            }
        }

        let target = remote_attach
            .target
            .map(|t| T::try_from(*t))
            .transpose()
            .map_err(|_| SenderAttachError::CoordinatorIsNotImplemented)?;

        // Note that it is the responsibility of the transaction controller to
        // verify that the capabilities of the controller meet its requirements.
        //
        // the receiver is considered to hold the authoritative version of the target properties
        match (&self.target, &target) {
            (Some(local_target), Some(remote_target)) => {
                if self.verify_incoming_target {
                    local_target.verify_as_sender(remote_target)?
                }
            }
            // Only need to check the target
            //
            // If there is no pre-existing terminus, and the peer does not wish to create a new one,
            // this is indicated by setting the local terminus (source or target as appropriate) to null.
            (_, None) => return Err(SenderAttachError::IncomingTargetIsNone),
            _ => {}
        }
        self.target = target;

        // The `rcv-settle-mode` field in the attach response from the receiver is the
        // receiver's *actual* settlement mode in use ("when set at the receiver this
        // indicates the actual settlement mode in use"). The sender must adapt its
        // settlement handshake accordingly: with `second`, the receiver settles only
        // after receiving the sender's confirming disposition. The session already
        // propagates the actual mode to the link relay (`SessionInner::on_incoming_attach`),
        // which drives the confirming echo at delivery time, so no strict validation
        // is needed here. The local value is still updated with the receiver's actual
        // mode so that it reflects the negotiated state (e.g. on reattach and through
        // the `rcv_settle_mode` accessor), mirroring how the receiver side records the
        // sender's actual `snd-settle-mode`.
        self.rcv_settle_mode = remote_attach.rcv_settle_mode;

        // The `snd-settle-mode` field in the attach response from the receiver only
        // expresses the receiver's *desired* settlement mode for the sender. When the
        // sender initiates the attach, the sender's own choice is the settlement mode in
        // use and the receiver SHOULD respect it, failing the attach if it cannot.
        //
        // A definite conflict (neither side is `mixed`) is still treated as an error
        // because it signals a receiver that expects a settlement behavior the sender
        // will not provide; a `mixed` response is tolerated either way.
        if let (SenderSettleMode::Settled, SenderSettleMode::Unsettled)
        | (SenderSettleMode::Unsettled, SenderSettleMode::Settled) =
            (&self.snd_settle_mode, &remote_attach.snd_settle_mode)
        {
            return Err(SenderAttachError::SndSettleModeNotSupported);
        }

        self.max_message_size =
            get_max_message_size(self.max_message_size, remote_attach.max_message_size);

        if let Some(remote_properties) = remote_attach.properties {
            self.properties_mut(|local_properties| {
                local_properties
                    .get_or_insert_with(Default::default)
                    .as_inner_mut()
                    .extend(remote_properties.into_inner());
            })
        }

        if matches!(purpose, AttachPurpose::CloseOnly) {
            if remote_attach.incomplete_unsettled
                || matches!(
                    self.local_state,
                    LinkState::IncompleteAttachSent
                        | LinkState::IncompleteAttachReceived
                        | LinkState::IncompleteAttachExchanged
                )
            {
                return Err(SenderAttachError::IllegalState);
            }
            return Ok(SenderAttachExchange::Complete);
        }

        self.handle_unsettled_in_attach(remote_attach.unsettled)
    }

    async fn exchange_attach_inner(
        &mut self,
        writer: &crate::session::transfer_queue::Sender,
        reader: &mut mpsc::Receiver<LinkFrame>,
        is_reattaching: bool,
        purpose: AttachPurpose,
    ) -> Result<SenderAttachExchange, SenderAttachError> {
        // Send out local attach
        self.send_attach_inner(writer, is_reattaching, &purpose)
            .await?;

        // Wait for remote attach
        let remote_attach =
            match reader
                .recv()
                .await
                .ok_or_else(|| match self.session_stop_reason.get() {
                    Some(reason) => SenderAttachError::SessionStopped(reason.clone()),
                    None => SenderAttachError::IllegalState, // defensive: no stop reason recorded; failure is link-local
                })? {
                LinkFrame::Attach(attach) => attach,
                _ => return Err(SenderAttachError::NonAttachFrameReceived),
            };

        match purpose {
            AttachPurpose::Ordinary => {
                endpoint::LinkAttach::on_incoming_attach(self, remote_attach)
            }
            AttachPurpose::CloseOnly => {
                endpoint::LinkAttach::on_incoming_attach_for_close(self, remote_attach)?;
                Ok(SenderAttachExchange::Complete)
            }
        }
    }
}
