use crate::util::engine_outcome::{EngineOutcome, EngineOutcomeSender};
use fe2o3_amqp_types::{
    definitions::{self, AmqpError, SessionError},
    performatives::End,
};
use std::sync::{Arc, OnceLock};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    connection::{self, ConnectionStopReason},
    control::{ConnectionControl, SessionControl},
    endpoint::{self, IncomingChannel, Session},
    link::{LinkFrame, SessionStopReason},
    util::Running,
    SendBound,
};

use super::{
    error::{
        connection_stop_reason_or_closed, AllocLinkError, BeginError, Error, SessionInnerError,
    },
    frame::{SessionIncomingItem, SessionOutgoingItem},
    SessionFrame, SessionFrameBody, SessionState,
};

async fn send_outgoing_item(
    outgoing: &mpsc::Sender<SessionFrame>,
    outgoing_item: SessionOutgoingItem,
    conn_stop: &Arc<OnceLock<ConnectionStopReason>>,
) -> Result<(), SessionInnerError> {
    match outgoing_item {
        SessionOutgoingItem::SingleFrame(frame) => {
            outgoing
                .send(frame)
                .await
                // The receiving half must have dropped, and thus the `Connection`
                // event loop has stopped.
                .map_err(|_| {
                    SessionInnerError::ConnectionStopped(connection_stop_reason_or_closed(
                        conn_stop,
                    ))
                })?;
        }
        SessionOutgoingItem::MultipleFrames(frames) => {
            for frame in frames {
                outgoing
                    .send(frame)
                    .await
                    // The receiving half must have dropped, and thus the `Connection`
                    // event loop has stopped.
                    .map_err(|_| {
                        SessionInnerError::ConnectionStopped(connection_stop_reason_or_closed(
                            conn_stop,
                        ))
                    })?;
            }
        }
    }
    Ok(())
}

pub(crate) struct SessionEngine<S: Session> {
    pub conn_control: mpsc::Sender<ConnectionControl>,
    pub session: S,
    pub control: mpsc::Receiver<SessionControl>,
    pub incoming: mpsc::Receiver<SessionIncomingItem>,
    pub outgoing: mpsc::Sender<SessionFrame>,

    pub(crate) retired_allocations: crate::completion::Retired<crate::endpoint::OutputAttachment>,
    pub outgoing_link_frames: mpsc::Receiver<LinkFrame>,
}

impl<S> SessionEngine<S>
where
    S: endpoint::Session,
    BeginError: From<S::BeginError>,
{
    pub(crate) async fn begin_client_session(
        conn_control: mpsc::Sender<ConnectionControl>,
        session: S,
        control: mpsc::Receiver<SessionControl>,
        incoming: mpsc::Receiver<SessionIncomingItem>,
        outgoing: mpsc::Sender<SessionFrame>,
        outgoing_link_frames: mpsc::Receiver<LinkFrame>,
    ) -> Result<Self, BeginError> {
        let mut engine = Self {
            conn_control,
            session,
            control,
            incoming,
            outgoing,
            retired_allocations: Default::default(),
            outgoing_link_frames,
        };

        // send a begin
        engine.session.send_begin(&engine.outgoing).await?;
        // wait for an incoming begin
        let frame = match engine.incoming.recv().await {
            Some(frame) => frame,
            None => {
                // Connection sender must have dropped
                return Err(BeginError::ConnectionStopped(
                    connection_stop_reason_or_closed(engine.session.connection_stop_reason()),
                ));
            }
        };
        let SessionFrame { channel, body, .. } = frame;
        let channel = IncomingChannel(channel);
        let remote_begin = match body {
            SessionFrameBody::Begin(begin) => begin,
            SessionFrameBody::End(end) => match end.error {
                Some(err) => return Err(BeginError::RemoteEndedWithError(err)),
                None => return Err(BeginError::RemoteEnded),
            },
            _ => return Err(BeginError::IllegalState),
        };
        engine.session.on_incoming_begin(channel, remote_begin)?;
        Ok(engine)
    }
}

cfg_not_wasm32! {
    impl<S> SessionEngine<S>
    where
        S: endpoint::SessionEndpoint<State = SessionState> + Send + Sync + 'static,
        AllocLinkError: From<S::AllocError>,
        SessionInnerError: From<S::Error> + From<S::BeginError> + From<S::EndError>,
    {
        pub fn spawn(self) -> (JoinHandle<()>, EngineOutcome<Error>) {
            let (tx, rx) = EngineOutcome::channel();
            let handle = tokio::spawn(self.event_loop(tx));
            (handle, rx)
        }
    }
}

cfg_wasm32! {
    impl<S> SessionEngine<S>
    where
        S: endpoint::SessionEndpoint<State = SessionState> + SendBound + Sync + 'static,
        AllocLinkError: From<S::AllocError>,
        SessionInnerError: From<S::Error> + From<S::BeginError> + From<S::EndError>,
    {
        pub fn spawn_local(self) -> (JoinHandle<()>, EngineOutcome<Error>) {
            let (tx, rx) = EngineOutcome::channel();
            let handle = tokio::task::spawn_local(self.event_loop(tx));
            (handle, rx)
        }

        pub fn spawn_on_local_set(self, local_set: &tokio::task::LocalSet) -> (JoinHandle<()>, EngineOutcome<Error>) {
            let (tx, rx) = EngineOutcome::channel();
            let handle = local_set.spawn_local(self.event_loop(tx));
            (handle, rx)
        }
    }
}

impl<S> SessionEngine<S>
where
    S: endpoint::SessionEndpoint<State = SessionState> + SendBound + Sync + 'static,
    AllocLinkError: From<S::AllocError>,
    SessionInnerError: From<S::Error> + From<S::BeginError> + From<S::EndError>,
{
    #[inline]
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_incoming(
        &mut self,
        incoming: SessionIncomingItem,
    ) -> Result<Running, SessionInnerError> {
        let SessionFrame { channel, body, .. } = incoming;
        let channel = IncomingChannel(channel);
        match body {
            SessionFrameBody::TransferRefused => {
                if matches!(self.session.local_state(), SessionState::Mapped) {
                    self.session.on_refused_incoming_transfer();
                }
            }
            SessionFrameBody::Begin(begin) => {
                self.session.on_incoming_begin(channel, begin)?;
            }
            SessionFrameBody::Attach(attach) => {
                self.session.on_incoming_attach(attach).await?;
            }
            SessionFrameBody::Flow(flow) => {
                if let Some(outgoing_item) = self.session.on_incoming_flow(flow).await? {
                    send_outgoing_item(
                        &self.outgoing,
                        outgoing_item,
                        self.session.connection_stop_reason(),
                    )
                    .await?;
                }
            }
            SessionFrameBody::Transfer {
                performative,
                payload,
            } => {
                // A received transfer can require an immediate disposition back — only the
                // transactional session produces one (the presumptive-outcome reply to a
                // posted transfer, AMQP §4.4.1); non-transactional sessions return `None`.
                // The transaction discharge path routes its own reply via
                // `SessionControl::Disposition` instead.
                if let Some(disposition) = self
                    .session
                    .on_incoming_transfer(performative, payload)
                    .await?
                {
                    let frame = self.session.on_outgoing_disposition(disposition)?;
                    self.outgoing.send(frame).await.map_err(|_| {
                        SessionInnerError::ConnectionStopped(connection_stop_reason_or_closed(
                            self.session.connection_stop_reason(),
                        ))
                    })?;
                }

                // A receiver may already have consumed a queued frame. Only
                // consumed slots permit the session window to grow again.
                if let Some(outgoing_item) = self.session.maybe_outgoing_session_flow() {
                    send_outgoing_item(
                        &self.outgoing,
                        outgoing_item,
                        self.session.connection_stop_reason(),
                    )
                    .await?;
                }
            }
            SessionFrameBody::Disposition(disposition) => {
                if let Some(dispositions) = self.session.on_incoming_disposition(disposition)? {
                    for disposition in dispositions {
                        let disposition = self.session.on_outgoing_disposition(disposition)?;
                        self.outgoing
                            .send(disposition)
                            .await
                            // The receiving half must have dropped, and thus the `Connection`
                            // event loop has stopped. It should be treated as an io error
                            .map_err(|_| {
                                SessionInnerError::ConnectionStopped(
                                    connection_stop_reason_or_closed(
                                        self.session.connection_stop_reason(),
                                    ),
                                )
                            })?;
                    }
                }
            }
            SessionFrameBody::Detach(detach) => {
                // If the peer detached the link on its own, the relay has a
                // reply to send back. The reply goes out through the same
                // path as any other detach, and no answer is expected for it.
                if let Some((detach, attachment)) = self.session.on_incoming_detach(detach).await? {
                    if let Some(frame) = self.session.on_outgoing_detach(detach, attachment, false)
                    {
                        self.outgoing.send(frame).await.map_err(|_| {
                            SessionInnerError::ConnectionStopped(connection_stop_reason_or_closed(
                                self.session.connection_stop_reason(),
                            ))
                        })?;
                    }
                }
            }
            SessionFrameBody::End(end) => {
                let end_error = end.error.clone();
                let result = self.session.on_incoming_end(channel, end);
                if matches!(self.session.local_state(), SessionState::EndReceived) {
                    // Record the stop reason before the link channel is closed,
                    // so links that fail on the closure observe the reason. The
                    // `EndReceived` state only results from a remote-initiated
                    // end, so the error (if any) is the remote's.
                    self.session.set_session_stop_reason(match end_error {
                        Some(error) => SessionStopReason::RemoteEndedWithError(error),
                        None => match self.session.connection_stop_reason().get() {
                            Some(reason) => SessionStopReason::from(reason.clone()),
                            None => SessionStopReason::RemoteEnded,
                        },
                    });
                    // if control is closing, finish sending all buffered messages before closing
                    self.outgoing_link_frames.close();
                    while let Some(frame) = self.outgoing_link_frames.recv().await {
                        self.on_outgoing_link_frames(frame).await?;
                    }

                    self.session.send_end(&self.outgoing, None).await?;
                }
                result?;
            }
        }

        match self.session.local_state() {
            SessionState::Unmapped => Ok(Running::Stop),
            _ => Ok(Running::Continue),
        }
    }

    /// The session stop reason derived from the connection's recorded stop;
    /// `Ended` when the connection has not stopped.
    fn session_stop_reason_from_connection(&self) -> SessionStopReason {
        match self.session.connection_stop_reason().get() {
            Some(reason) => SessionStopReason::from(reason.clone()),
            None => SessionStopReason::Ended,
        }
    }

    #[inline]
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_control(&mut self, control: SessionControl) -> Result<Running, SessionInnerError> {
        #[cfg(feature = "tracing")]
        tracing::trace!("control: {}", control);
        #[cfg(feature = "log")]
        log::trace!("control: {}", control);
        for attachment in self.retired_allocations.take() {
            self.session.retire_link_allocation(&attachment);
        }
        match control {
            SessionControl::End(error) => {
                // Record the stop reason before the link channel is closed, so
                // links that fail on the closure observe the reason.
                self.session.set_session_stop_reason(match &error {
                    Some(error) => SessionStopReason::EndedWithError(error.clone()),
                    None => self.session_stop_reason_from_connection(),
                });
                // if control is closing, finish sending all buffered messages before closing
                self.outgoing_link_frames.close();
                while let Some(frame) = self.outgoing_link_frames.recv().await {
                    self.on_outgoing_link_frames(frame).await?;
                }

                self.session.send_end(&self.outgoing, error).await?;
            }
            SessionControl::AllocateLink {
                link_name,
                link_relay,
                responder,
            } => {
                let result = self.session.allocate_link(link_name, Some(link_relay));
                let retired = self.retired_allocations.clone();
                responder.on_untaken(move |result| {
                    if let Ok(attachment) = result {
                        retired.push(attachment.clone());
                    }
                });
                let _ = responder.send(result.map_err(Into::into));
                for attachment in self.retired_allocations.take() {
                    self.session.retire_link_allocation(&attachment);
                }
            }
            SessionControl::AllocateIncomingLink {
                link_name,
                link_relay,
                input_handle,
                responder,
            } => {
                let result =
                    self.session
                        .allocate_incoming_link(link_name, link_relay, input_handle);
                let retired = self.retired_allocations.clone();
                responder.on_untaken(move |result| {
                    if let Ok(attachment) = result {
                        retired.push(attachment.clone());
                    }
                });
                let _ = responder.send(result.map_err(Into::into));
                for attachment in self.retired_allocations.take() {
                    self.session.retire_link_allocation(&attachment);
                }
            }
            SessionControl::DeallocateLink(attachment) => {
                self.session.deallocate_link(&attachment);
            }
            SessionControl::Disposition(disposition) => {
                let disposition = self.session.on_outgoing_disposition(disposition)?;
                self.outgoing
                    .send(disposition)
                    .await
                    // The receiving half must have dropped, and thus the `Connection`
                    // event loop has stopped. It should be treated as an io error
                    .map_err(|_| {
                        SessionInnerError::ConnectionStopped(connection_stop_reason_or_closed(
                            self.session.connection_stop_reason(),
                        ))
                    })?;
            }
            SessionControl::CloseConnectionWithError((condition, description)) => {
                let error = definitions::Error::new(condition, description, None);
                let control = ConnectionControl::Close(Some(error));
                self.conn_control.send(control).await.map_err(|_| {
                    SessionInnerError::ConnectionStopped(connection_stop_reason_or_closed(
                        self.session.connection_stop_reason(),
                    ))
                })?;
            }
            SessionControl::GetIncomingWindow(resp) => {
                let _ = resp.send(self.session.receive_window().maximum());
            }

            #[cfg(feature = "transaction")]
            SessionControl::AllocateTransactionId { resp } => {
                let result = self.session.allocate_transaction_id();
                let _ = resp.send(result);
            }
            #[cfg(feature = "transaction")]
            SessionControl::CommitTransaction { txn_id, resp } => {
                let result = self.session.commit_transaction(txn_id).await?;
                let _ = resp.send(result);
            }
            #[cfg(feature = "transaction")]
            SessionControl::RollbackTransaction { txn_id, resp } => {
                let result = self.session.rollback_transaction(txn_id)?;
                let _ = resp.send(result);
            }
            #[cfg(feature = "transaction")]
            SessionControl::AbortTransaction(txn_id) => {
                let _ = self.session.rollback_transaction(txn_id);
            }
        }

        match self.session.local_state() {
            SessionState::Unmapped => Ok(Running::Stop),
            _ => Ok(Running::Continue),
        }
    }

    #[inline]
    async fn on_outgoing_link_frames(
        &mut self,
        frame: LinkFrame,
    ) -> Result<Running, SessionInnerError> {
        match self.session.local_state() {
            SessionState::Mapped => {}
            _ => return Err(SessionInnerError::IllegalState), // End session with illegal state
        }

        let outgoing_item = match frame {
            LinkFrame::Attach(attach) => self
                .session
                .on_outgoing_attach(attach)
                .map(SessionOutgoingItem::SingleFrame)
                .map(Some)?,
            LinkFrame::Flow(flow, attachment) => self
                .session
                .on_outgoing_flow(flow, attachment)?
                .map(SessionOutgoingItem::SingleFrame),
            LinkFrame::Transfer {
                window_slot: _,
                queue_slot,
                input_handle,
                attachment,
                performative,
                payload,
            } => self.session.on_outgoing_transfer(
                input_handle,
                attachment,
                performative,
                payload,
                queue_slot,
            )?,
            LinkFrame::Disposition(disposition) => self
                .session
                .on_outgoing_disposition(disposition)
                .map(SessionOutgoingItem::SingleFrame)
                .map(Some)?,
            LinkFrame::Detach(detach, attachment) => self
                .session
                .on_outgoing_detach(detach, attachment, true)
                .map(SessionOutgoingItem::SingleFrame),

            #[cfg(feature = "transaction")]
            LinkFrame::Acquisition(_) => {
                // This is purely used to notify sender about TxnAcquisition, which is not implemented
                unreachable!("LinkFrame::Acquisition should not appear in outgoing link frames")
            }
        };

        if let Some(outgoing_item) = outgoing_item {
            send_outgoing_item(
                &self.outgoing,
                outgoing_item,
                self.session.connection_stop_reason(),
            )
            .await?;
        }

        match self.session.local_state() {
            SessionState::Unmapped => Ok(Running::Stop),
            _ => Ok(Running::Continue),
        }
    }

    #[inline]
    async fn on_error(&mut self, kind: &SessionInnerError) -> Result<Running, SessionInnerError> {
        use definitions::Error;

        #[cfg(not(target_arch = "wasm32"))]
        #[cfg(all(feature = "transaction", feature = "acceptor"))]
        use fe2o3_amqp_types::transaction::TransactionError;

        match kind {
            SessionInnerError::UnsettledCapacityExceeded(cause) => {
                self.session.report_unsettled_capacity(cause);
                self.end_session(Some(Error::new(
                    AmqpError::ResourceLimitExceeded,
                    None,
                    None,
                )))
                .await
            }
            SessionInnerError::WindowViolation => {
                let error = Error::new(SessionError::WindowViolation, None, None);
                self.end_session(Some(error)).await
            }
            SessionInnerError::InvalidTransfer => {
                let error = Error::new(AmqpError::InvalidField, None, None);
                self.end_session(Some(error)).await
            }
            SessionInnerError::UnattachedHandle => {
                let error = Error::new(SessionError::UnattachedHandle, None, None);
                self.end_session(Some(error)).await
            }
            SessionInnerError::RemoteAttachingLinkNameNotFound => {
                let error = Error::new(
                    AmqpError::InternalError,
                    Some(String::from("Link name is not found")),
                    None,
                );
                self.end_session(Some(error)).await
            }
            SessionInnerError::HandleInUse => {
                let error = Error::new(SessionError::HandleInUse, None, None);
                self.end_session(Some(error)).await
            }
            SessionInnerError::IllegalState => {
                let error = Error::new(AmqpError::IllegalState, None, None);
                self.end_session(Some(error)).await
            }
            SessionInnerError::ConnectionStopped(reason) => {
                // The session cannot handle a connection stop; it ends with the
                // connection. The outcome is sanitized before it reaches the
                // handle, so connection-level errors are reported through the
                // `ConnectionHandle` instead.
                #[cfg(feature = "tracing")]
                tracing::warn!("Connection stopped before the session ended; ending the session");
                #[cfg(feature = "log")]
                log::warn!("Connection stopped before the session ended; ending the session");
                Err(SessionInnerError::ConnectionStopped(reason.clone()))
            }
            SessionInnerError::TransferFrameToSender => {
                let error = Error::new(
                    AmqpError::NotAllowed,
                    Some(String::from("Found Transfer frame sent Sender link")),
                    None,
                );
                self.end_session(Some(error)).await
            }
            SessionInnerError::RemoteEnded | SessionInnerError::RemoteEndedWithError(_) => {
                self.end_session(None).await
            }

            #[cfg(not(target_arch = "wasm32"))]
            #[cfg(all(feature = "transaction", feature = "acceptor"))]
            SessionInnerError::UnknownTxnId => {
                let error = Error::new(TransactionError::UnknownId, None, None);
                self.end_session(Some(error)).await
            }
        }
    }

    async fn end_session(
        &mut self,
        error: Option<definitions::Error>,
    ) -> Result<Running, SessionInnerError> {
        match self.session.local_state() {
            SessionState::Unmapped => {}
            SessionState::BeginSent | SessionState::BeginReceived | SessionState::Mapped => {
                self.session
                    .send_end(&self.outgoing, error)
                    .await
                    .map_err(|_| {
                        SessionInnerError::ConnectionStopped(connection_stop_reason_or_closed(
                            self.session.connection_stop_reason(),
                        ))
                    })?;
                let (channel, end) = self.wait_for_remote_end(false).await?;
                self.session.on_incoming_end(channel, end)?;
            }
            SessionState::EndSent => {
                self.wait_for_remote_end(false).await?;
            }
            SessionState::EndReceived => {
                self.session
                    .send_end(&self.outgoing, error)
                    .await
                    .map_err(|_| {
                        SessionInnerError::ConnectionStopped(connection_stop_reason_or_closed(
                            self.session.connection_stop_reason(),
                        ))
                    })?;
            }
            SessionState::Discarding => {
                // The DISCARDING state is a variant of the CLOSE SENT state where the close is triggered
                // by an error. In this case any incoming frames on the connection MUST be silently discarded
                // until the peer’s close frame is received.
                self.wait_for_remote_end(true).await?;
            }
        }

        Ok(Running::Stop)
    }

    async fn wait_for_remote_end(
        &mut self,
        discard_other_frame: bool,
    ) -> Result<(IncomingChannel, End), SessionInnerError> {
        loop {
            let frame = self
                .incoming
                .recv()
                .await
                .ok_or(SessionInnerError::ConnectionStopped(
                    connection_stop_reason_or_closed(self.session.connection_stop_reason()),
                ))?;
            match frame.body {
                SessionFrameBody::End(end) => return Ok((IncomingChannel(frame.channel), end)),
                _ => {
                    if !discard_other_frame {
                        let _ = self.on_incoming(frame).await?;
                    }
                }
            }
        }
    }

    #[inline]
    fn continue_or_stop_by_state(&self) -> Running {
        match self.session.local_state() {
            SessionState::BeginSent
            | SessionState::BeginReceived
            | SessionState::Mapped
            | SessionState::EndSent
            | SessionState::EndReceived => Running::Continue,
            SessionState::Unmapped | SessionState::Discarding => Running::Stop,
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(name = "Session::event_loop", skip(self), fields(outgoing_channel = %self.session.outgoing_channel().0)))]
    async fn event_loop(mut self, tx: EngineOutcomeSender<Error>) {
        let mut outcome = Ok(());
        let receive_window = self.session.receive_window().clone();
        loop {
            let result = tokio::select! {
                _ = self.retired_allocations.ready() => {
                    for attachment in self.retired_allocations.take() {
                        self.session.retire_link_allocation(&attachment);
                    }
                    Ok(Running::Continue)
                },
                _ = receive_window.changed.notified() => {
                    if let Some(item) = self.session.maybe_outgoing_session_flow() {
                        send_outgoing_item(&self.outgoing, item, self.session.connection_stop_reason()).await.map(|_| Running::Continue)
                    } else { Ok(Running::Continue) }
                },
                incoming = self.incoming.recv() => {
                    match incoming {
                        Some(incoming) => self.on_incoming(incoming).await,
                        None => {
                            // Check local state
                            match self.continue_or_stop_by_state() {
                                Running::Continue => {
                                    // The connection stopped before the session ended;
                                    // yield the stop as an error so it flows through the
                                    // generic error path. Connection-level errors are
                                    // reported through the `ConnectionHandle`.
                                    Err(SessionInnerError::ConnectionStopped(
                                        connection_stop_reason_or_closed(
                                            self.session.connection_stop_reason(),
                                        ),
                                    ))
                                },
                                Running::Stop => Ok(Running::Stop),
                            }
                        }
                    }
                },
                control = self.control.recv() => {
                    match control {
                        Some(control) => {
                            self.on_control(control).await
                        },
                        None => {
                            // all Links and SessionHandle are dropped
                            // Complete ending session
                            'inner: loop {
                                match self.session.local_state() {
                                    SessionState::Mapped
                                    | SessionState::EndReceived => {
                                        if let Err(_err) = self.on_control(SessionControl::End(None)).await {
                                            #[cfg(feature = "tracing")]
                                            tracing::error!("err {:?}", _err);
                                            #[cfg(feature = "log")]
                                            log::error!("err {:?}", _err);
                                        }
                                    },
                                    // Wait for remote end
                                    SessionState::Discarding | SessionState::EndSent => {
                                        match self.incoming.recv().await {
                                            Some(incoming) => {
                                                let _ = self.on_incoming(incoming).await;
                                            },
                                            None => {
                                                // The connection must have already stopped before session negotiated ending
                                                break 'inner
                                            }
                                        }
                                    },
                                    SessionState::Unmapped => break 'inner,
                                    _ => break 'inner,
                                }
                            }

                            Ok(Running::Stop)
                        }
                    }
                },
                frame = self.outgoing_link_frames.recv() => {
                    match frame {
                        Some(frame) => self.on_outgoing_link_frames(frame).await,
                        None => {
                            // All Links and SessionHandle are dropped
                            //
                            // Upon ending, all link-to-session channels will be closed
                            // first while the session is still waitint for remote end frame.
                            Ok(Running::Continue)
                        }
                    }
                }
            };

            let running = match result {
                Ok(running) => running,
                Err(error) => {
                    // A connection stop is logged as a warning (the session ends
                    // with the connection) rather than as an unexpected error.
                    if !matches!(error, SessionInnerError::ConnectionStopped(_)) {
                        #[cfg(feature = "tracing")]
                        tracing::error!("{:?}", error);
                        #[cfg(feature = "log")]
                        log::error!("{:?}", error);
                    }
                    match self.on_error(&error).await {
                        Ok(running) => {
                            if !matches!(outcome, Err(SessionInnerError::UnsettledCapacityExceeded(_))) {
                                outcome = Err(error);
                            }
                            running
                        }
                        Err(cleanup_error) => {
                            // Exhaustion remains the terminal cause even if the peer
                            // disappears during the End exchange.
                            if !matches!(outcome, Err(SessionInnerError::UnsettledCapacityExceeded(_))) {
                            outcome = Err(
                                if matches!(error, SessionInnerError::UnsettledCapacityExceeded(_))
                                {
                                    error
                                } else {
                                    cleanup_error
                                },
                            );
                            }
                            Running::Stop
                        }
                    }
                }
            };

            match running {
                Running::Continue => {}
                Running::Stop => break,
            }
        }

        #[cfg(feature = "tracing")]
        tracing::debug!("Stopped");
        #[cfg(feature = "log")]
        log::debug!("Stopped");
        // Publish the stop reason before the link relays and unsettled maps are dropped,
        // so every link that wakes on the channel closures sees it.
        let session_stop_reason = match &outcome {
            Err(SessionInnerError::ConnectionStopped(reason)) => {
                SessionStopReason::from(reason.clone())
            }
            Err(SessionInnerError::RemoteEndedWithError(error)) => {
                SessionStopReason::RemoteEndedWithError(error.clone())
            }
            Err(SessionInnerError::RemoteEnded) => SessionStopReason::RemoteEnded,
            Err(SessionInnerError::UnsettledCapacityExceeded(cause)) => {
                SessionStopReason::UnsettledCapacityExceeded(cause.clone())
            }
            _ => SessionStopReason::Ended,
        };
        let _ =
            connection::deallocate_session(&mut self.conn_control, self.session.outgoing_channel())
                .await;
        // The session ends with the connection; sanitize the connection stop
        // so the handle observes a clean end. Connection-level errors are
        // reported through the `ConnectionHandle`.
        let result = match outcome {
            Err(SessionInnerError::ConnectionStopped(_)) => Ok(()),
            other => other.map_err(Into::into),
        };
        tx.complete(result, || {
            self.session.set_session_stop_reason(session_stop_reason)
        });
    }
}

#[cfg(test)]
mod allocation_cancel_tests {
    use super::*;
    use crate::{
        endpoint::OutgoingChannel,
        link::{
            state::{LinkFlowState, LinkFlowStateInner},
            unsettled_store::Store,
            LinkRelay,
        },
        session::{Builder, SessionState},
    };
    use fe2o3_amqp_types::definitions::ReceiverSettleMode;
    fn engine() -> SessionEngine<super::super::Session> {
        let (conn_control, _) = mpsc::channel(1);
        let (_, control) = mpsc::channel(1);
        let (_, incoming) = mpsc::channel(1);
        let (outgoing, _) = mpsc::channel(1);
        let (_, outgoing_link_frames) = mpsc::channel(1);
        SessionEngine {
            conn_control,
            control,
            incoming,
            outgoing,
            retired_allocations: Default::default(),
            outgoing_link_frames,
            session: Builder::new().into_session(
                OutgoingChannel(0),
                SessionState::Mapped,
                Arc::new(OnceLock::new()),
            ),
        }
    }
    fn relay() -> (LinkRelay<()>, mpsc::Receiver<crate::link::LinkIncomingItem>) {
        let (tx, rx) = mpsc::channel(1);
        let flow = Arc::new(LinkFlowState::receiver(LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 0,
            available: 0,
            drain: false,
            properties: None,
        }));
        (
            LinkRelay::new_receiver(
                tx,
                flow,
                Arc::new(Store::new(None, None).unwrap()),
                ReceiverSettleMode::First,
            ),
            rx,
        )
    }
    #[tokio::test]
    async fn cancelled_allocator_releases_handle_without_ending_session() {
        let mut engine = engine();
        let (link_relay, _inbox) = relay();
        let (responder, waiter) = crate::completion::channel();
        drop(waiter);
        engine
            .on_control(SessionControl::AllocateLink {
                link_name: "cancelled".into(),
                link_relay,
                responder,
            })
            .await
            .unwrap();
        assert!(engine.session.link_by_name.is_empty());
        assert!(engine.session.link_name_by_output_handle.is_empty());
        assert!(matches!(engine.session.local_state, SessionState::Mapped));
    }
    #[tokio::test]
    async fn abandoned_allocation_answers_reach_sink_and_release_both_roles() {
        for incoming in [false, true] {
            for sent_first in [false, true] {
                let mut engine = engine();
                let (link_relay, _inbox) = relay();
                let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
                let output = seen.clone();
                let sink = crate::completion::CompletionSink::new(
                    move |result: Result<crate::endpoint::OutputAttachment, AllocLinkError>| {
                        output.lock().push(result.unwrap().handle.0);
                    },
                );
                let (responder, waiter) = crate::completion::channel_with(Some(sink));
                let waiter = if sent_first {
                    Some(waiter)
                } else {
                    drop(waiter);
                    None
                };
                let control = if incoming {
                    SessionControl::AllocateIncomingLink {
                        link_name: "late".into(),
                        link_relay,
                        input_handle: crate::endpoint::InputHandle(0),
                        responder,
                    }
                } else {
                    SessionControl::AllocateLink {
                        link_name: "late".into(),
                        link_relay,
                        responder,
                    }
                };
                engine.on_control(control).await.unwrap();
                drop(waiter);
                assert_eq!(*seen.lock(), [0]);
                // Any next control drains retirement before allocating a sibling.
                let (link_relay, _sibling) = relay();
                let (responder, reply) = crate::completion::channel();
                engine
                    .on_control(SessionControl::AllocateLink {
                        link_name: "sibling".into(),
                        link_relay,
                        responder,
                    })
                    .await
                    .unwrap();
                assert_eq!(reply.await.unwrap().unwrap().handle.0, 0);
                assert_eq!(engine.session.link_name_by_output_handle.len(), 1);
                assert!(engine.session.link_by_input_handle.is_empty());
                assert!(matches!(engine.session.local_state, SessionState::Mapped));
            }
        }
    }

    #[cfg(all(feature = "transaction", feature = "acceptor"))]
    #[tokio::test]
    async fn late_transaction_control_replies_do_not_end_the_engine() {
        use crate::{
            endpoint::HandleDeclare,
            transaction::{
                coordinator::ControlLinkAcceptor, manager::TransactionManager, session::TxnSession,
                AllocTxnIdError,
            },
        };
        use fe2o3_amqp_types::{
            messaging::Accepted,
            transaction::{TransactionError, TransactionId},
        };
        for sent_first in [false, true] {
            let base = engine();
            let (control, _) = mpsc::channel(1);
            let (writer, _frames) = mpsc::channel(8);
            let session = TxnSession {
                control,
                session: base.session,
                txn_manager: TransactionManager::new(writer.into(), ControlLinkAcceptor::default()),
                max_frame_size: 4096,
            };
            let mut engine = SessionEngine {
                session,
                conn_control: base.conn_control,
                control: base.control,
                incoming: base.incoming,
                outgoing: base.outgoing,
                outgoing_link_frames: base.outgoing_link_frames,
                retired_allocations: base.retired_allocations,
            };
            let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
            let output = seen.clone();
            let (resp, waiter) =
                crate::completion::channel_with(Some(crate::completion::CompletionSink::new(
                    move |result: Result<TransactionId, AllocTxnIdError>| {
                        output.lock().push(result.unwrap())
                    },
                )));
            let waiter = if sent_first {
                Some(waiter)
            } else {
                drop(waiter);
                None
            };
            engine
                .on_control(SessionControl::AllocateTransactionId { resp })
                .await
                .unwrap();
            drop(waiter);
            assert_eq!(seen.lock().len(), 1);
            for commit in [false, true] {
                let id = engine.session.allocate_transaction_id().unwrap();
                let completed = Arc::new(parking_lot::Mutex::new(Vec::new()));
                let output = completed.clone();
                let (resp, waiter) =
                    crate::completion::channel_with(Some(crate::completion::CompletionSink::new(
                        move |result: Result<Accepted, TransactionError>| {
                            output.lock().push(result)
                        },
                    )));
                let waiter = if sent_first {
                    Some(waiter)
                } else {
                    drop(waiter);
                    None
                };
                let control = if commit {
                    SessionControl::CommitTransaction { txn_id: id, resp }
                } else {
                    SessionControl::RollbackTransaction { txn_id: id, resp }
                };
                engine.on_control(control).await.unwrap();
                drop(waiter);
                assert_eq!(completed.lock().len(), 1);
                assert!(completed.lock()[0].is_ok());
            }
            assert!(matches!(
                engine.session.session.local_state,
                SessionState::Mapped
            ));
        }
    }

    #[tokio::test]
    async fn cancelled_unattached_endpoint_allows_same_name_reuse() {
        let mut engine = engine();
        let (link_relay, inbox) = relay();
        let (responder, waiter) = crate::completion::channel();
        engine
            .on_control(SessionControl::AllocateLink {
                link_name: "reuse".into(),
                link_relay,
                responder,
            })
            .await
            .unwrap();
        waiter.await.unwrap().unwrap();
        drop(inbox);
        let (link_relay, _inbox) = relay();
        let (responder, waiter) = crate::completion::channel();
        engine
            .on_control(SessionControl::AllocateLink {
                link_name: "reuse".into(),
                link_relay,
                responder,
            })
            .await
            .unwrap();
        waiter.await.unwrap().unwrap();
        assert_eq!(engine.session.link_name_by_output_handle.len(), 1);
        assert!(matches!(engine.session.local_state, SessionState::Mapped));
    }
}

/// Once the session replies to a peer's detach, the attachment it ended has
/// no handle: a transfer, link flow or local detach the link made for it,
/// queued before the reply went out, is not written after the reply.
#[cfg(test)]
mod detached_attachment_tests {
    use super::*;
    use crate::{
        endpoint::{AttachmentId, InputHandle, LinkFlow, OutgoingChannel, OutputAttachment},
        link::{
            state::{LinkFlowState, LinkFlowStateInner},
            unsettled_store::Store,
            LinkRelay,
        },
        session::{Builder, SessionState},
        util::Producer,
        Payload,
    };
    use fe2o3_amqp_types::performatives::{Detach, Flow, Transfer};
    use tokio::sync::Semaphore;

    #[derive(Clone, Copy, Debug)]
    enum Stale {
        Transfer,
        Flow,
        Detach,
    }
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Path {
        /// The frame reaches the session with the peer's window open.
        Direct,
        /// A transfer waits for the peer's window, which opens after the
        /// reply.
        ZeroWindowReplay,
        /// The frame is still queued when the session ends, which consumes
        /// it before its End.
        EndDrain,
        /// A later attachment holds the same handle when the frame arrives.
        HandleReused,
    }
    struct Harness {
        engine: SessionEngine<super::super::Session>,
        link_frames: mpsc::Sender<LinkFrame>,
        written: mpsc::Receiver<SessionFrame>,
        _link_inboxes: Vec<mpsc::Receiver<LinkFrame>>,
    }
    fn harness() -> Harness {
        let (conn_control, _) = mpsc::channel(1);
        let (_, control) = mpsc::channel(1);
        let (_, incoming) = mpsc::channel(1);
        let (outgoing, written) = mpsc::channel(16);
        let (link_frames, outgoing_link_frames) = mpsc::channel(16);
        let mut session = Builder::new().into_session(
            OutgoingChannel(0),
            SessionState::Mapped,
            Arc::new(OnceLock::new()),
        );
        session.remote_incoming_window = 8;
        Harness {
            engine: SessionEngine {
                conn_control,
                control,
                incoming,
                outgoing,
                retired_allocations: Default::default(),
                outgoing_link_frames,
                session,
            },
            link_frames,
            written,
            _link_inboxes: Vec::new(),
        }
    }
    impl Harness {
        /// Allocates a sender link named `name` and attaches it as the peer's
        /// input handle 0.
        fn attach(&mut self, name: &str) -> OutputAttachment {
            let (tx, inbox) = mpsc::channel(4);
            self._link_inboxes.push(inbox);
            let flow = Arc::new(LinkFlowState::sender(LinkFlowStateInner {
                initial_delivery_count: 0,
                delivery_count: 0,
                link_credit: 8,
                available: 0,
                drain: false,
                properties: None,
            }));
            let relay = LinkRelay::new_sender(
                tx,
                Producer::new(Arc::new(tokio::sync::Notify::new()), flow),
                Arc::new(Store::new(None, None).unwrap()),
            );
            let attachment = self
                .engine
                .session
                .allocate_link(name.into(), Some(relay))
                .unwrap();
            let relay = self
                .engine
                .session
                .link_by_name
                .get_mut(name)
                .unwrap()
                .take()
                .unwrap();
            self.engine
                .session
                .link_by_input_handle
                .insert(InputHandle(0), relay);
            attachment
        }
        async fn peer_detaches(&mut self) {
            let detach = Detach {
                handle: 0u32.into(),
                closed: true,
                error: None,
            };
            self.engine
                .on_incoming(SessionFrame::new(0u16, SessionFrameBody::Detach(detach)))
                .await
                .unwrap();
        }
        async fn handle(&mut self, frame: LinkFrame) {
            self.engine.on_outgoing_link_frames(frame).await.unwrap();
        }
        async fn peer_opens_window(&mut self) {
            let flow = Flow {
                next_incoming_id: Some(self.engine.session.next_outgoing_id),
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
            self.engine
                .on_incoming(SessionFrame::new(0u16, SessionFrameBody::Flow(flow)))
                .await
                .unwrap();
        }
    }
    fn transfer(attachment: AttachmentId, tag: u8, permit: &Arc<Semaphore>) -> LinkFrame {
        let mut performative: Transfer =
            serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x55, 0x43])
                .unwrap();
        performative.delivery_tag = Some(vec![tag].into());
        LinkFrame::Transfer {
            input_handle: InputHandle(0),
            attachment,
            performative,
            payload: Payload::from_static(b"data"),
            window_slot: None,
            queue_slot: Some(permit.clone().try_acquire_owned().unwrap()),
        }
    }
    fn stale(kind: Stale, attachment: AttachmentId, permit: &Arc<Semaphore>) -> LinkFrame {
        match kind {
            Stale::Transfer => transfer(attachment, 1, permit),
            Stale::Flow => LinkFrame::Flow(
                LinkFlow {
                    handle: 0u32.into(),
                    link_credit: Some(8),
                    ..Default::default()
                },
                attachment,
            ),
            Stale::Detach => LinkFrame::Detach(
                Detach {
                    handle: 0u32.into(),
                    closed: true,
                    error: None,
                },
                attachment,
            ),
        }
    }
    #[tokio::test]
    async fn a_frame_of_an_attachment_the_peer_detached_is_not_written_after_the_reply() {
        for kind in [Stale::Transfer, Stale::Flow, Stale::Detach] {
            for path in [
                Path::Direct,
                Path::ZeroWindowReplay,
                Path::EndDrain,
                Path::HandleReused,
            ] {
                if path == Path::ZeroWindowReplay && !matches!(kind, Stale::Transfer) {
                    // Only transfers wait for the peer's window.
                    continue;
                }
                let case = format!("{kind:?} {path:?}");
                let mut h = harness();
                let permits = Arc::new(Semaphore::new(1));
                let a = h.attach("A");
                if path == Path::ZeroWindowReplay {
                    h.engine.session.remote_incoming_window = 0;
                    h.handle(stale(kind, a.id, &permits)).await;
                    assert_eq!(
                        h.engine
                            .session
                            .remote_incoming_window_exhausted_buffer
                            .len(),
                        1,
                        "{case}"
                    );
                }
                h.peer_detaches().await;
                assert!(
                    matches!(
                        h.written.try_recv().unwrap().body,
                        SessionFrameBody::Detach(_)
                    ),
                    "{case}: the reply"
                );
                let next_outgoing_id = h.engine.session.next_outgoing_id;
                let b = (path == Path::HandleReused).then(|| h.attach("B"));
                if let Some(b) = &b {
                    assert_eq!(b.handle, a.handle, "{case}: B reuses A's handle");
                }
                match path {
                    Path::Direct | Path::HandleReused => {
                        h.handle(stale(kind, a.id, &permits)).await
                    }
                    Path::ZeroWindowReplay => h.peer_opens_window().await,
                    Path::EndDrain => {
                        h.link_frames
                            .send(stale(kind, a.id, &permits))
                            .await
                            .unwrap();
                        h.engine
                            .on_control(SessionControl::End(None))
                            .await
                            .unwrap();
                        assert!(
                            matches!(h.written.try_recv().unwrap().body, SessionFrameBody::End(_)),
                            "{case}: End follows the reply"
                        );
                    }
                }
                let window = h.engine.session.remote_incoming_window;
                match h.written.try_recv() {
                    Err(mpsc::error::TryRecvError::Empty) => {}
                    Ok(frame) => panic!("{case}: wrote {:?}", frame.body),
                    Err(error) => panic!("{case}: {error:?}"),
                }
                assert_eq!(
                    h.engine.session.next_outgoing_id, next_outgoing_id,
                    "{case}: no transfer-id taken"
                );
                assert_eq!(window, 8, "{case}: the peer's window is unchanged");
                assert!(h.engine.session.delivery_tag_by_id.is_empty(), "{case}");
                assert!(
                    h.engine
                        .session
                        .remote_incoming_window_exhausted_buffer
                        .is_empty(),
                    "{case}"
                );
                assert!(h.engine.session.close_pending.is_empty(), "{case}");
                assert_eq!(
                    permits.available_permits(),
                    1,
                    "{case}: queue room released"
                );
                if let Some(b) = b {
                    // B still holds the handle and its own frames go out.
                    assert!(h
                        .engine
                        .session
                        .is_current_output_attachment(&b.handle, b.id));
                    h.handle(transfer(b.id, 2, &permits)).await;
                    let SessionFrameBody::Transfer { performative, .. } =
                        h.written.try_recv().unwrap().body
                    else {
                        panic!("{case}: B's transfer")
                    };
                    assert_eq!(performative.delivery_id, Some(next_outgoing_id), "{case}");
                    assert_eq!(h.engine.session.delivery_tag_by_id.len(), 1, "{case}");
                    let flow = LinkFlow {
                        handle: b.handle.clone().into(),
                        ..Default::default()
                    };
                    h.handle(LinkFrame::Flow(flow, b.id)).await;
                    assert!(
                        matches!(
                            h.written.try_recv().unwrap().body,
                            SessionFrameBody::Flow(_)
                        ),
                        "{case}: B's flow"
                    );
                }
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod refused_transfer_tests {
    use super::*;
    use crate::{
        endpoint::{InputHandle, OutgoingChannel, OutputAttachment},
        link::{
            state::{LinkFlowState, LinkFlowStateInner},
            unsettled_store::Store,
            LinkRelay,
        },
        session::Builder,
        Payload, Session as NativeSession,
    };
    use fe2o3_amqp_types::{
        definitions::ReceiverSettleMode,
        performatives::{End, Transfer},
    };

    async fn exercise<S>(wrap: impl FnOnce(NativeSession) -> S, mut check: impl FnMut(&S))
    where
        S: endpoint::SessionEndpoint<State = SessionState> + Send + Sync + 'static,
        AllocLinkError: From<S::AllocError>,
        SessionInnerError: From<S::Error> + From<S::BeginError> + From<S::EndError>,
    {
        let mut native = Builder::new().incoming_window(1).into_session(
            OutgoingChannel(0),
            SessionState::Mapped,
            Arc::new(OnceLock::new()),
        );
        native.remote_outgoing_window = 1;
        let (link_tx, mut link_rx) = mpsc::channel(2);
        let store = Arc::new(Store::new(None, None).unwrap());
        let flow = Arc::new(LinkFlowState::receiver(LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 2,
            available: 0,
            drain: false,
            properties: None,
        }));
        let relay =
            LinkRelay::new_receiver(link_tx, flow, store.clone(), ReceiverSettleMode::First)
                .with_output_handle(OutputAttachment::for_tests(0));
        native.link_by_input_handle.insert(InputHandle(0), relay);
        let (conn_control, _conn_rx) = mpsc::channel(4);
        let (control_tx, control) = mpsc::channel(4);
        let (incoming_tx, incoming) = mpsc::channel(4);
        let (outgoing, mut written) = mpsc::channel(4);
        let (_link_tx, outgoing_link_frames) = mpsc::channel(4);
        let mut engine = SessionEngine {
            conn_control,
            control,
            incoming,
            outgoing,
            outgoing_link_frames,
            session: wrap(native),
            retired_allocations: Default::default(),
        };
        engine
            .on_incoming(SessionFrame::new(0u16, SessionFrameBody::TransferRefused))
            .await
            .unwrap();
        check(&engine.session);
        assert_eq!(engine.session.receive_window().available(), 1);
        assert!(store.read().is_none());
        assert!(link_rx.try_recv().is_err());
        let (join, mut outcome) = engine.spawn();
        let SessionFrameBody::Flow(flow) = written.recv().await.unwrap().body else {
            panic!("automatic session Flow")
        };
        assert_eq!(flow.next_incoming_id, Some(1));
        assert_eq!(flow.incoming_window, 1);
        assert!(flow.handle.is_none());
        let transfer: Transfer =
            serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x55, 0x43])
                .unwrap();
        incoming_tx
            .send(SessionFrame::new(
                0u16,
                SessionFrameBody::Transfer {
                    performative: transfer,
                    payload: Payload::from_static(&[0, 0x53, 0x75, 0xa0, 1, 7]),
                },
            ))
            .await
            .unwrap();
        let frame = link_rx.recv().await.unwrap();
        assert!(matches!(frame, LinkFrame::Transfer { .. }));
        drop(frame);
        let SessionFrameBody::Flow(flow) = written.recv().await.unwrap().body else {
            panic!("following consumed window")
        };
        assert_eq!(flow.next_incoming_id, Some(2));
        assert_eq!(flow.incoming_window, 1);
        assert!(flow.handle.is_none());
        control_tx.send(SessionControl::End(None)).await.unwrap();
        assert!(matches!(
            written.recv().await.unwrap().body,
            SessionFrameBody::End(_)
        ));
        incoming_tx
            .send(SessionFrame::new(
                0u16,
                SessionFrameBody::End(End { error: None }),
            ))
            .await
            .unwrap();
        join.await.unwrap();
        outcome.recv().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn refused_transfer_keeps_only_physical_session_progress() {
        exercise(
            |session| session,
            |session| {
                assert_eq!(session.next_incoming_id, 1);
                assert_eq!(session.remote_outgoing_window, 0);
                assert!(session.delivery_tag_by_id.is_empty());
            },
        )
        .await;
        #[cfg(feature = "acceptor")]
        {
            let (listener, mut notices) = mpsc::channel(2);
            exercise(
                |session| crate::acceptor::session::ListenerSession {
                    session,
                    link_listener: listener,
                    pending_link_flows: Default::default(),
                },
                |session| {
                    assert!(session.pending_link_flows.is_empty());
                    assert!(notices.try_recv().is_err());
                    assert!(session.session.delivery_tag_by_id.is_empty());
                },
            )
            .await;
        }
        #[cfg(all(feature = "transaction", feature = "acceptor"))]
        {
            use crate::transaction::{
                coordinator::ControlLinkAcceptor, manager::TransactionManager, session::TxnSession,
            };
            let (control, _rx) = mpsc::channel(2);
            let (outgoing, _rx2) = crate::session::transfer_queue::channel(2);
            exercise(
                |session| TxnSession {
                    session,
                    control,
                    txn_manager: TransactionManager::new(outgoing, ControlLinkAcceptor::default()),
                    max_frame_size: 4096,
                },
                |session| {
                    assert!(session.txn_manager.txns.is_empty());
                    assert!(session.session.delivery_tag_by_id.is_empty());
                },
            )
            .await;
            let (listener, mut notices) = mpsc::channel(2);
            let (control, _rx) = mpsc::channel(2);
            let (outgoing, _rx2) = crate::session::transfer_queue::channel(2);
            exercise(
                |session| TxnSession {
                    session: crate::acceptor::session::ListenerSession {
                        session,
                        link_listener: listener,
                        pending_link_flows: Default::default(),
                    },
                    control,
                    txn_manager: TransactionManager::new(outgoing, ControlLinkAcceptor::default()),
                    max_frame_size: 4096,
                },
                |session| {
                    assert!(session.txn_manager.txns.is_empty());
                    assert!(session.session.pending_link_flows.is_empty());
                    assert!(notices.try_recv().is_err());
                },
            )
            .await;
        }
    }
}
