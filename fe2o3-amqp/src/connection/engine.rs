//! The engine handles incoming and outgoing frames and messages to reduce
//! transferring frames/messages over channels

use std::io;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use fe2o3_amqp_types::definitions::{self, AmqpError};
use fe2o3_amqp_types::performatives::Close;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use crate::control::ConnectionControl;
use crate::endpoint::{IncomingChannel, OutgoingChannel};
use crate::frames::amqp::{self, Frame, FrameBody};
use crate::session::frame::{SessionFrame, SessionFrameBody};
use crate::transport::Transport;
use crate::util::{
    engine_outcome::{EngineOutcome, EngineOutcomeSender},
    Running,
};
use crate::{endpoint, transport, SendBound};

use super::ConnectionStopReason;
use super::{heartbeat::HeartBeat, ConnectionState};
use super::{AllocSessionError, ConnectionInnerError, ConnectionStateError, Error, OpenError};

#[derive(Debug)]
pub(crate) struct ConnectionEngine<Io, C> {
    transport: Transport<Io, amqp::Frame>,
    connection: C,
    control: Receiver<ConnectionControl>,
    outgoing_session_frames: Receiver<SessionFrame>,
    heartbeat: HeartBeat,
    write_pending: bool,
    retired_allocations: crate::completion::Retired<OutgoingChannel>,
}

impl<Io, C> ConnectionEngine<Io, C>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: endpoint::Connection,
{
    /// The shared cell holding why the connection stopped, on the connection
    /// object and shared with the sessions and the handle
    pub(crate) fn connection_stop_reason(&self) -> &Arc<OnceLock<ConnectionStopReason>> {
        self.connection.connection_stop_reason()
    }

    /// The negotiated max frame size (encoder max frame length) of this
    /// connection, i.e. the maximum length of the frame data (excluding the
    /// 4-byte size prefix) that the transport encoder will emit.
    ///
    /// Per AMQP 1.0 §2.7.1 the `max-frame-size` field of the Open performative
    /// is directional: it is the largest frame the advertising peer can
    /// accept, and a peer MUST NOT send frames larger than its partner can
    /// handle. The outbound limit is therefore derived from the *remote*
    /// value (clamped to at least `MIN_MAX_FRAME_SIZE`), not from the minimum
    /// of the two advertised values. This is the same value the transport
    /// encoder enforces, so link-level splitting that respects it guarantees
    /// every frame fits.
    ///
    /// The value is final once the Open exchange completes in `open_inner`,
    /// i.e. before any `ConnectionHandle` is created.
    pub(crate) fn max_frame_size(&self) -> usize {
        self.transport.encoder_max_frame_size()
    }
}

cfg_not_wasm32! {
    impl<Io, C> ConnectionEngine<Io, C>
    where
        Io: AsyncRead + AsyncWrite + std::fmt::Debug + Send + Unpin + 'static,
        C: endpoint::Connection<State = ConnectionState> + std::fmt::Debug + Send + Sync + 'static,
        C::AllocError: Into<AllocSessionError>,
        C::CloseError: From<transport::Error>,
        C::OpenError: From<transport::Error>,
        ConnectionInnerError: From<C::Error> + From<C::OpenError> + From<C::CloseError>,
        ConnectionStateError: From<C::OpenError> + From<C::CloseError>,
        OpenError: From<C::OpenError>,
    {
        pub fn spawn(self) -> (JoinHandle<()>, EngineOutcome<Error>) {
            let (tx, rx) = EngineOutcome::channel();
            let handle = tokio::spawn(self.event_loop(tx));
            (handle, rx)
        }
    }
}

cfg_wasm32! {
    impl<Io, C> ConnectionEngine<Io, C>
    where
        Io: AsyncRead + AsyncWrite + std::fmt::Debug + SendBound + Unpin + 'static,
        C: endpoint::Connection<State = ConnectionState> + std::fmt::Debug + Send + Sync + 'static,
        C::AllocError: Into<AllocSessionError>,
        C::CloseError: From<transport::Error>,
        C::OpenError: From<transport::Error>,
        ConnectionInnerError: From<C::Error> + From<C::OpenError> + From<C::CloseError>,
        ConnectionStateError: From<C::OpenError> + From<C::CloseError>,
        OpenError: From<C::OpenError>,
    {
        pub fn spawn_local(
            self
        ) -> (JoinHandle<()>, EngineOutcome<Error>) {
            let (tx, rx) = EngineOutcome::channel();
            let handle = tokio::task::spawn_local(self.event_loop(tx));
            (handle, rx)
        }

        pub fn spawn_on_local_set(
            self,
            local_set: &tokio::task::LocalSet,
        ) -> (JoinHandle<()>, EngineOutcome<Error>) {
            let (tx, rx) = EngineOutcome::channel();
            let handle = local_set.spawn_local(self.event_loop(tx));
            (handle, rx)
        }
    }
}

impl<Io, C> ConnectionEngine<Io, C>
where
    Io: AsyncRead + AsyncWrite + std::fmt::Debug + SendBound + Unpin + 'static,
    C: endpoint::Connection<State = ConnectionState> + std::fmt::Debug + Send + Sync + 'static,
    C::AllocError: Into<AllocSessionError>,
    C::CloseError: From<transport::Error>,
    C::OpenError: From<transport::Error>,
    ConnectionInnerError: From<C::Error> + From<C::OpenError> + From<C::CloseError>,
    ConnectionStateError: From<C::OpenError> + From<C::CloseError>,
    OpenError: From<C::OpenError>,
{
    async fn close_connection(
        &mut self,
        error: Option<definitions::Error>,
    ) -> Result<Running, ConnectionInnerError> {
        match self.connection.local_state() {
            ConnectionState::Start
            | ConnectionState::HeaderReceived
            | ConnectionState::HeaderSent
            | ConnectionState::HeaderExchange => Err(ConnectionInnerError::IllegalState),
            ConnectionState::OpenPipe
            | ConnectionState::OpenClosePipe
            | ConnectionState::OpenReceived
            | ConnectionState::OpenSent
            | ConnectionState::Opened => {
                self.connection
                    .send_close(&mut self.transport, error)
                    .await?;
                let (channel, close) = self.wait_for_remote_close(false).await?;
                self.connection.on_incoming_close(channel, close)?;
                Ok(Running::Stop)
            }
            ConnectionState::CloseReceived => {
                self.connection
                    .send_close(&mut self.transport, error)
                    .await?;
                Ok(Running::Stop)
            }
            ConnectionState::ClosePipe | ConnectionState::CloseSent => {
                let (channel, close) = self.wait_for_remote_close(false).await?;
                self.connection.on_incoming_close(channel, close)?;
                Ok(Running::Stop)
            }
            ConnectionState::Discarding => {
                let (channel, close) = self.wait_for_remote_close(true).await?;
                self.connection.on_incoming_close(channel, close)?;
                Ok(Running::Stop)
            }
            ConnectionState::End => Ok(Running::Stop),
        }
    }

    async fn wait_for_remote_close(
        &mut self,
        discard_other: bool,
    ) -> Result<(IncomingChannel, Close), ConnectionInnerError> {
        loop {
            let frame = self.transport.next().await.ok_or_else(|| {
                transport::Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Expecting remote close",
                ))
            })??;

            match frame.body {
                FrameBody::Close(close) => return Ok((IncomingChannel(frame.channel), close)),
                _ => {
                    if !discard_other {
                        self.on_incoming(frame).await?;
                    }
                }
            }
        }
    }

    async fn open_inner(&mut self) -> Result<(), OpenError> {
        self.connection.send_open(&mut self.transport).await?;

        // Wait for an Open
        let frame = match self.transport.next().await {
            Some(frame) => match frame {
                Ok(fr) => fr,
                Err(error) => return Err(error.into()),
            },
            None => {
                return Err(OpenError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Expecting an Open frame",
                )))
            }
        };
        let Frame { channel, body } = frame;
        let channel = endpoint::IncomingChannel(channel);
        let remote_open = match body {
            FrameBody::Open(open) => open,
            FrameBody::Close(close) => match close.error {
                Some(error) => return Err(OpenError::RemoteClosedWithError(error)),
                None => return Err(OpenError::RemoteClosed),
            },
            _ => return Err(OpenError::IllegalState),
        };

        // Handle incoming remote_open
        let remote_max_frame_size = remote_open.max_frame_size.0 as usize;
        let remote_idle_timeout = remote_open.idle_time_out;
        self.connection.on_incoming_open(channel, remote_open)?;

        // update transport setting
        let local_max_frame_size = self.connection.local_open().max_frame_size.0 as usize;
        self.transport
            .set_encoder_max_frame_size(remote_max_frame_size)
            .set_decoder_max_frame_size(local_max_frame_size);

        // Set heartbeat here because in pipelined-open, the Open frame
        // may be recved after mux loop is started
        match &remote_idle_timeout {
            Some(0) | None => self.heartbeat = HeartBeat::never(),
            Some(millis) => {
                let period = Duration::from_millis(*millis as u64);
                self.heartbeat = HeartBeat::new(period);
            }
        };

        Ok(())
    }

    /// Open Connection without starting the Engine::event_loop()
    pub(crate) async fn open(
        transport: Transport<Io, amqp::Frame>,
        connection: C,
        control: Receiver<ConnectionControl>,
        outgoing_session_frames: Receiver<SessionFrame>,
    ) -> Result<Self, OpenError> {
        let mut engine = Self {
            transport,
            connection,
            control,
            outgoing_session_frames,
            heartbeat: HeartBeat::never(),
            write_pending: false,
            retired_allocations: Default::default(),
        };

        match engine.open_inner().await {
            Ok(_) => Ok(engine),
            Err(error) => {
                // The opening's failure is how it ended: what closing the
                // connection after it meets does not replace it.
                let _ = engine.close_connection(None).await;
                Err(error)
            }
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn forward_to_session(
        &mut self,
        channel: IncomingChannel,
        frame: SessionFrame,
    ) -> Result<(), ConnectionInnerError> {
        match &self.connection.local_state() {
            // A peer's session frames can cross our Close (AMQP 1.0
            // section 2.4.6). Keep receiving until its Close; on_control has
            // already closed and drained the outgoing queue before our Close.
            ConnectionState::Opened | ConnectionState::CloseSent => {}
            _ => return Err(ConnectionInnerError::IllegalState),
        };

        match self.connection.session_tx_by_incoming_channel(channel) {
            Some(tx) => tx.send(frame).await?,
            None => return Err(ConnectionInnerError::NotFound(None)),
        };
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(name = "RECV", skip_all))]
    async fn on_incoming(&mut self, frame: Frame) -> Result<Running, ConnectionInnerError> {
        #[cfg(feature = "tracing")]
        tracing::trace!(?frame);
        #[cfg(feature = "log")]
        log::trace!("RECV frame={:?}", frame);

        // In the DISCARDING state, any incoming frames on the connection MUST
        // be silently discarded until the peer's close frame is received
        // (AMQP 1.0 section 2.4.6).
        if matches!(self.connection.local_state(), ConnectionState::Discarding)
            && !matches!(frame.body, FrameBody::Close(_))
        {
            return Ok(Running::Continue);
        }

        let Frame { channel, body } = frame;
        let channel = IncomingChannel(channel);
        match body {
            FrameBody::TransferRefused(_) => {
                if matches!(self.connection.local_state(), ConnectionState::Opened) {
                    if let Some(tx) = self.connection.session_tx_by_incoming_channel(channel) {
                        let _ = tx
                            .send(SessionFrame::new(
                                channel,
                                SessionFrameBody::TransferRefused,
                            ))
                            .await;
                    }
                }
            }
            FrameBody::Open(open) => {
                let remote_idle_timeout = open.idle_time_out;
                self.connection.on_incoming_open(channel, open)?;

                // Set heartbeat here because in pipelined-open, the Open frame
                // may be recved after mux loop is started
                match &remote_idle_timeout {
                    Some(millis) => {
                        let period = Duration::from_millis(*millis as u64);
                        self.heartbeat = HeartBeat::new(period);
                    }
                    None => self.heartbeat = HeartBeat::never(),
                };
            }
            FrameBody::Begin(begin) => {
                self.connection.on_incoming_begin(channel, begin).await?;
            }
            FrameBody::Attach(attach) => {
                let sframe = SessionFrame::new(channel, SessionFrameBody::Attach(attach));
                self.forward_to_session(channel, sframe).await?;
            }
            FrameBody::Flow(flow) => {
                let sframe = SessionFrame::new(channel, SessionFrameBody::Flow(flow));
                self.forward_to_session(channel, sframe).await?;
            }
            FrameBody::Transfer {
                performative,
                payload,
            } => {
                let sframe = SessionFrame::new(
                    channel,
                    SessionFrameBody::Transfer {
                        performative,
                        payload,
                    },
                );
                self.forward_to_session(channel, sframe).await?;
            }
            FrameBody::Disposition(disposition) => {
                let sframe = SessionFrame::new(channel, SessionFrameBody::Disposition(disposition));
                self.forward_to_session(channel, sframe).await?;
            }
            FrameBody::Detach(detach) => {
                let sframe = SessionFrame::new(channel, SessionFrameBody::Detach(detach));
                self.forward_to_session(channel, sframe).await?;
            }
            FrameBody::End(end) => {
                self.connection.on_incoming_end(channel, end).await?;
            }
            FrameBody::Close(close) => {
                let result = self.connection.on_incoming_close(channel, close);
                if matches!(
                    self.connection.local_state(),
                    ConnectionState::CloseReceived
                ) {
                    self.outgoing_session_frames.close();
                    while let Some(frame) = self.outgoing_session_frames.recv().await {
                        self.on_outgoing_session_frames(frame).await?;
                    }

                    self.connection
                        .send_close(&mut self.transport, None)
                        .await?;
                }
                result?;
            }
            FrameBody::Empty => {
                // do nothing, IdleTimeout is tracked by Transport
            }
        }

        match self.connection.local_state() {
            ConnectionState::End => Ok(Running::Stop),
            _ => Ok(Running::Continue),
        }
    }

    #[inline]
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn on_control(
        &mut self,
        control: ConnectionControl,
    ) -> Result<Running, ConnectionInnerError> {
        #[cfg(feature = "tracing")]
        tracing::debug!("{}", control);
        #[cfg(feature = "log")]
        log::debug!("{}", control);
        for channel in self.retired_allocations.take() {
            self.connection.deallocate_session(channel);
        }
        match control {
            ConnectionControl::Close(error) => {
                // Record a locally initiated close with an error before the
                // channels close, so sessions and links observe the local
                // error regardless of how the peer responds.
                if let Some(error) = &error {
                    self.connection.set_connection_stop_reason(
                        ConnectionStopReason::ClosedWithError(error.clone()),
                    );
                }
                self.outgoing_session_frames.close();
                while let Some(frame) = self.outgoing_session_frames.recv().await {
                    self.on_outgoing_session_frames(frame).await?;
                }

                self.connection
                    .send_close(&mut self.transport, error)
                    .await?;
            }
            ConnectionControl::AllocateSession { tx, responder } => {
                let result = self.connection.allocate_session(tx).map_err(Into::into);
                let retired = self.retired_allocations.clone();
                responder.on_untaken(move |result| {
                    if let Ok(channel) = result {
                        retired.push(*channel);
                    }
                });
                let _ = responder.send(result);
                for channel in self.retired_allocations.take() {
                    self.connection.deallocate_session(channel);
                }
            }
            ConnectionControl::DeallocateSession(session_id) => {
                self.connection.deallocate_session(session_id)
            }
        }

        match self.connection.local_state() {
            ConnectionState::End => Ok(Running::Stop),
            _ => Ok(Running::Continue),
        }
    }

    #[inline]
    #[cfg_attr(feature = "tracing", tracing::instrument(name = "SEND", skip_all))]
    async fn on_outgoing_session_frames(
        &mut self,
        frame: SessionFrame,
    ) -> Result<Running, ConnectionInnerError> {
        match self.connection.local_state() {
            // The drain during the close exchange runs while the state is
            // `CloseReceived`; the buffered frames are flushed as part of
            // closing the connection.
            ConnectionState::Opened | ConnectionState::CloseReceived => {}
            _ => return Err(ConnectionInnerError::IllegalState),
        }

        let SessionFrame {
            channel,
            body,
            queue_slot: _queue_slot,
        } = frame;
        let channel = OutgoingChannel(channel);
        let frame = match body {
            SessionFrameBody::TransferRefused => unreachable!("incoming-only refused Transfer"),
            SessionFrameBody::Begin(begin) => self.connection.on_outgoing_begin(channel, begin)?,
            SessionFrameBody::Attach(attach) => Frame::new(channel, FrameBody::Attach(attach)),
            SessionFrameBody::Flow(flow) => Frame::new(channel, FrameBody::Flow(flow)),
            SessionFrameBody::Transfer {
                performative,
                payload,
            } => Frame::new(
                channel,
                FrameBody::Transfer {
                    performative,
                    payload,
                },
            ),
            SessionFrameBody::Disposition(disposition) => {
                Frame::new(channel, FrameBody::Disposition(disposition))
            }
            SessionFrameBody::Detach(detach) => Frame::new(channel, FrameBody::Detach(detach)),
            SessionFrameBody::End(end) => self.connection.on_outgoing_end(channel, end)?,
        };

        #[cfg(feature = "tracing")]
        tracing::trace!(channel = frame.channel, frame = ?frame.body);
        #[cfg(feature = "log")]
        log::trace!("SEND channel = {}, frame = {:?}", frame.channel, frame.body);
        // The engine admits one frame, then flushes it concurrently with reads.
        // `feed` cannot wait on a previous write in the normal event loop because
        // its outbound branch is disabled until that write has flushed.
        self.transport.feed(frame).await?;
        self.write_pending = true;
        Ok(Running::Continue)
    }

    #[inline]
    async fn on_heartbeat(&mut self) -> Result<Running, ConnectionInnerError> {
        match &self.connection.local_state() {
            ConnectionState::Start | ConnectionState::CloseSent => return Ok(Running::Continue),
            ConnectionState::End => return Ok(Running::Stop),
            _ => {}
        }

        let frame = Frame::empty();
        // The engine admits one frame, then flushes it concurrently with reads.
        // `feed` cannot wait on a previous write in the normal event loop because
        // its outbound branch is disabled until that write has flushed.
        self.transport.feed(frame).await?;
        self.write_pending = true;
        Ok(Running::Continue)
    }

    #[inline]
    async fn on_error(
        &mut self,
        error: &ConnectionInnerError,
    ) -> Result<Running, ConnectionInnerError> {
        match error {
            ConnectionInnerError::TransportError(_) => Ok(Running::Stop),
            // The stream that ended while the connection was open gets the
            // Close an illegal state does.
            ConnectionInnerError::IllegalState | ConnectionInnerError::IncomingClosed => {
                let error = definitions::Error::new(AmqpError::IllegalState, None, None);
                self.close_connection(Some(error)).await?;
                Ok(Running::Stop)
            }
            ConnectionInnerError::NotImplemented(description) => {
                let error =
                    definitions::Error::new(AmqpError::NotImplemented, description.clone(), None);
                self.close_connection(Some(error)).await?;
                Ok(Running::Stop)
            }
            ConnectionInnerError::NotFound(description) => {
                let error = definitions::Error::new(AmqpError::NotFound, description.clone(), None);
                self.close_connection(Some(error)).await?;
                Ok(Running::Stop)
            }
            ConnectionInnerError::RemoteClosed | ConnectionInnerError::RemoteClosedWithError(_) => {
                self.close_connection(None).await
            }
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(name = "Connection::event_loop", skip(self), fields(container_id = %self.connection.local_open().container_id)))]
    async fn event_loop(mut self, tx: EngineOutcomeSender<Error>) {
        let mut outcome = Ok(());
        loop {
            let result = tokio::select! {
                _ = self.retired_allocations.ready() => {
                    for channel in self.retired_allocations.take() {
                        self.connection.deallocate_session(channel);
                    }
                    Ok(Running::Continue)
                },
                _ = self.heartbeat.next(), if !self.write_pending => self.on_heartbeat().await,
                progress = self.transport.next_event(self.write_pending) => {
                    let incoming = match progress {
                        transport::DuplexEvent::Flushed => {
                            self.write_pending = false;
                            continue;
                        }
                        transport::DuplexEvent::Incoming(incoming) => incoming,
                    };
                    let result = match incoming {
                        Some(incoming) => {
                            match incoming {
                                Ok(frame) => self.on_incoming(frame).await,
                                Err(err) => Err(err.into()),
                            }
                        },
                        None => {
                            // Incoming stream is closed

                            match self.connection.local_state() {
                                ConnectionState::Start
                                | ConnectionState::HeaderReceived
                                | ConnectionState::HeaderSent
                                | ConnectionState::HeaderExchange
                                | ConnectionState::OpenPipe
                                | ConnectionState::OpenClosePipe
                                | ConnectionState::OpenReceived
                                | ConnectionState::OpenSent
                                | ConnectionState::Opened
                                | ConnectionState::CloseReceived
                                | ConnectionState::CloseSent => Err(ConnectionInnerError::IncomingClosed),
                                ConnectionState::ClosePipe
                                | ConnectionState::Discarding
                                | ConnectionState::End => Ok(Running::Stop),
                            }
                        },
                    };

                    result
                },
                control = self.control.recv() => {
                    match control {
                        Some(control) => self.on_control(control).await,
                        None => {
                            // All control channel are dropped (which is impossible)
                            // Finish closing the connection
                            loop {
                                match self.connection.local_state() {
                                    ConnectionState::Opened
                                    | ConnectionState::CloseReceived
                                    | ConnectionState::OpenSent
                                    | ConnectionState::OpenPipe => {
                                        if let Err(_err) = self.on_control(ConnectionControl::Close(None)).await {
                                            #[cfg(feature = "tracing")]
                                            tracing::error!("err {:?}", _err);
                                            #[cfg(feature = "log")]
                                            log::error!("err {:?}", _err);
                                        }
                                    }
                                    ConnectionState::Discarding
                                    | ConnectionState::CloseSent
                                    | ConnectionState::OpenClosePipe
                                    | ConnectionState::ClosePipe => {
                                        match self.transport.next().await {
                                            Some(Ok(frame)) => {
                                                let _ = self.on_incoming(frame).await;
                                            },
                                            Some(Err(_err)) => {
                                                #[cfg(feature = "tracing")]
                                                tracing::error!("err {:?}", _err);
                                                #[cfg(feature = "log")]
                                                log::error!("err {:?}", _err);
                                            },
                                            None => break
                                        }
                                    },
                                    ConnectionState::End => break,
                                    _ => break, // This should not happen
                                }
                            }

                            Ok(Running::Stop)
                        }
                    }
                },
                frame = self.outgoing_session_frames.recv(), if !self.write_pending => {
                    match frame {
                        Some(frame) => self.on_outgoing_session_frames(frame).await,
                        None => {
                            // Upon closing, the outgoing_session_frames channel will be closed
                            // first while the connection may still be waiting for remote
                            // close frame.
                            Ok(Running::Continue)
                        }
                    }
                }
            };

            let running = match result {
                Ok(running) => running,
                Err(error) => {
                    #[cfg(feature = "tracing")]
                    tracing::error!("{:?}", error);
                    #[cfg(feature = "log")]
                    log::error!("{:?}", error);
                    // The first failure is how the engine ended: what its
                    // cleanup meets after it, a Close that cannot be sent or a
                    // stream that ends before the peer's Close, does not
                    // replace it.
                    let running = match self.on_error(&error).await {
                        Ok(running) => running,
                        Err(_cleanup) => {
                            // Stop the session if error cannot be handled
                            #[cfg(feature = "tracing")]
                            tracing::error!("Unable to handle error {:?}", _cleanup);
                            #[cfg(feature = "log")]
                            log::error!("Unable to handle error {:?}", _cleanup);
                            Running::Stop
                        }
                    };
                    outcome = Err(error);
                    running
                }
            };

            match running {
                Running::Continue => {}
                Running::Stop => break,
            }
        }

        // Clean Shutdown
        //
        // When the Receiver is dropped, it is possible for unprocessed messages to remain
        // in the channel. Instead, it is usually desirable to perform a “clean” shutdown.
        // To do this, the receiver first closes the channels, which will prevent any
        // further messages to be sent into them.
        let close = self.transport.close().await.map_err(Into::into);
        let result = outcome.and(close).map_err(Into::into);

        // Publish the stop reason before the channels are closed, so every
        // session that wakes on the channel closure sees it.
        let connection_stop_reason = match &result {
            Err(Error::RemoteClosedWithError(error)) => {
                ConnectionStopReason::RemoteClosedWithError(error.clone())
            }
            Err(Error::RemoteClosed) => ConnectionStopReason::RemoteClosed,
            _ => ConnectionStopReason::Closed,
        };
        tx.complete(result, || {
            self.connection
                .set_connection_stop_reason(connection_stop_reason);
            self.control.close();
            self.outgoing_session_frames.close();
        });

        #[cfg(feature = "tracing")]
        tracing::debug!("Stopped");
        #[cfg(feature = "log")]
        log::debug!("Stopped");
    }
}

#[cfg(test)]
mod incoming_tests {
    use super::*;
    use crate::endpoint::Connection as _;
    use crate::{frames::amqp::RefusedTransfer, Connection};
    use fe2o3_amqp_types::performatives::Open;
    use tokio::sync::mpsc;

    fn refused() -> Frame {
        Frame::new(3u16, FrameBody::TransferRefused(RefusedTransfer::new()))
    }

    fn test_open() -> Open {
        Open {
            container_id: "refusal".into(),
            hostname: None,
            max_frame_size: Default::default(),
            channel_max: Default::default(),
            idle_time_out: None,
            outgoing_locales: None,
            incoming_locales: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    #[tokio::test]
    async fn a_refused_transfer_on_a_retired_session_is_consumed_quietly() {
        let mut connection = Connection::new(ConnectionState::Opened, test_open());
        let (to_session, mut session) = mpsc::channel(1);
        connection
            .session_by_incoming_channel
            .insert(IncomingChannel(3), Arc::new(to_session));
        let (io, _peer) = tokio::io::duplex(4096);
        let (_control, controls) = mpsc::channel(1);
        let (_out, outgoing) = mpsc::channel(1);
        let mut engine = ConnectionEngine {
            transport: Transport::bind(io, 4096, None),
            connection,
            control: controls,
            outgoing_session_frames: outgoing,
            heartbeat: HeartBeat::never(),
            write_pending: false,
            retired_allocations: Default::default(),
        };
        assert!(matches!(
            engine.on_incoming(refused()).await.unwrap(),
            Running::Continue
        ));
        {
            let second = engine.on_incoming(refused());
            tokio::pin!(second);
            tokio_test::assert_pending!(futures_util::poll!(second.as_mut()));
            let first = session.recv().await.unwrap();
            assert_eq!(first.channel, 3);
            assert!(matches!(first.body, SessionFrameBody::TransferRefused));
            assert!(matches!(second.await.unwrap(), Running::Continue));
        }
        assert!(matches!(
            session.recv().await.unwrap().body,
            SessionFrameBody::TransferRefused
        ));
        drop(session);
        assert!(matches!(
            engine.on_incoming(refused()).await.unwrap(),
            Running::Continue
        ));
        engine.connection.session_by_incoming_channel.clear();
        assert!(matches!(
            engine.on_incoming(refused()).await.unwrap(),
            Running::Continue
        ));
        assert!(matches!(
            engine.connection.local_state,
            ConnectionState::Opened
        ));
        for state in [
            ConnectionState::CloseSent,
            ConnectionState::Discarding,
            ConnectionState::End,
        ] {
            engine.connection.local_state = state.clone();
            let result = engine.on_incoming(refused()).await.unwrap();
            assert!(matches!(
                (state, result),
                (ConnectionState::End, Running::Stop)
                    | (
                        ConnectionState::CloseSent | ConnectionState::Discarding,
                        Running::Continue
                    )
            ));
        }
    }

    /// The real Close control closes outbound admission before writing Close.
    /// Its receipt by the peer gates already-in-flight session input: it must
    /// still reach the original relay, without any frame following our Close.
    #[tokio::test]
    async fn a_local_close_keeps_crossing_session_frames_without_later_output() {
        use fe2o3_amqp_types::{
            definitions::Role,
            performatives::{Attach, Begin, End, Flow},
        };

        for peer_error in [
            None,
            Some(definitions::Error::new(
                AmqpError::InternalError,
                None,
                None,
            )),
        ] {
            let mut connection = Connection::new(ConnectionState::Opened, test_open());
            let (to_session, mut session) = mpsc::channel(8);
            let outgoing_channel = connection.allocate_session(to_session).unwrap();
            let (io, peer_io) = tokio::io::duplex(4096);
            let mut peer = Transport::<_, Frame>::bind(peer_io, 4096, None);
            let (_control, controls) = mpsc::channel(1);
            let (out, outgoing) = mpsc::channel(1);
            let mut engine = ConnectionEngine {
                transport: Transport::bind(io, 4096, None),
                connection,
                control: controls,
                outgoing_session_frames: outgoing,
                heartbeat: HeartBeat::never(),
                write_pending: false,
                retired_allocations: Default::default(),
            };
            assert!(matches!(
                engine
                    .on_control(ConnectionControl::Close(None))
                    .await
                    .unwrap(),
                Running::Continue
            ));
            assert!(matches!(
                engine.connection.local_state(),
                ConnectionState::CloseSent
            ));
            assert!(matches!(
                peer.next().await.unwrap().unwrap().body,
                FrameBody::Close(Close { error: None })
            ));
            assert!(out.is_closed());

            let frames = [
                FrameBody::Begin(Begin {
                    remote_channel: Some(outgoing_channel.0),
                    next_outgoing_id: 0,
                    incoming_window: 8,
                    outgoing_window: 8,
                    handle_max: Default::default(),
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                }),
                FrameBody::Attach(Attach {
                    name: "crossing-attach".into(),
                    handle: 0.into(),
                    role: Role::Receiver,
                    snd_settle_mode: Default::default(),
                    rcv_settle_mode: Default::default(),
                    source: None,
                    target: None,
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: None,
                    max_message_size: None,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                }),
                FrameBody::Flow(Flow {
                    next_incoming_id: Some(0),
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
                }),
                FrameBody::End(End { error: None }),
            ];
            for (index, frame) in frames.into_iter().enumerate() {
                peer.send(Frame::new(3u16, frame)).await.unwrap();
                let frame = engine.transport.next().await.unwrap().unwrap();
                assert!(matches!(
                    engine.on_incoming(frame).await.unwrap(),
                    Running::Continue
                ));
                let received = session.recv().await.unwrap();
                assert_eq!(received.channel, 3);
                assert!(matches!(
                    (index, received.body),
                    (0, SessionFrameBody::Begin(_))
                        | (1, SessionFrameBody::Attach(_))
                        | (2, SessionFrameBody::Flow(_))
                        | (3, SessionFrameBody::End(_))
                ));
                assert!(matches!(
                    engine.connection.local_state(),
                    ConnectionState::CloseSent
                ));
            }
            // Closed outbound admission also refuses new session output.
            assert!(out
                .send(SessionFrame::new(
                    0u16,
                    SessionFrameBody::End(End { error: None })
                ))
                .await
                .is_err());
            {
                let next = peer.next();
                tokio::pin!(next);
                tokio_test::assert_pending!(futures_util::poll!(next.as_mut()));
            }

            peer.send(Frame::new(
                0u16,
                FrameBody::Close(Close {
                    error: peer_error.clone(),
                }),
            ))
            .await
            .unwrap();
            let frame = engine.transport.next().await.unwrap().unwrap();
            let result = engine.on_incoming(frame).await;
            match peer_error {
                None => assert!(matches!(result.unwrap(), Running::Stop)),
                Some(expected) => assert!(
                    matches!(result, Err(ConnectionInnerError::RemoteClosedWithError(actual)) if actual == expected)
                ),
            }
        }
    }
}

#[cfg(test)]
mod allocation_completion_tests {
    use super::*;
    use crate::connection::Connection;
    use fe2o3_amqp_types::performatives::Open;
    use tokio::sync::mpsc;
    #[tokio::test]
    async fn late_session_allocation_retires_without_ending_connection() {
        for sent_first in [false, true] {
            let (io, _peer) = tokio::io::duplex(4096);
            let (_, control) = mpsc::channel(1);
            let (_, outgoing_session_frames) = mpsc::channel(1);
            let mut engine = ConnectionEngine {
                connection: Connection::new(
                    ConnectionState::Opened,
                    Open {
                        container_id: "test".into(),
                        hostname: None,
                        max_frame_size: Default::default(),
                        channel_max: Default::default(),
                        idle_time_out: None,
                        outgoing_locales: None,
                        incoming_locales: None,
                        offered_capabilities: None,
                        desired_capabilities: None,
                        properties: None,
                    },
                ),
                transport: Transport::bind(io, 512, None),
                control,
                outgoing_session_frames,
                heartbeat: HeartBeat::never(),
                write_pending: false,
                retired_allocations: Default::default(),
            };
            let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
            let output = seen.clone();
            let (responder, waiter) =
                crate::completion::channel_with(Some(crate::completion::CompletionSink::new(
                    move |result: Result<OutgoingChannel, AllocSessionError>| {
                        output.lock().push(result.unwrap().0)
                    },
                )));
            let waiter = if sent_first {
                Some(waiter)
            } else {
                drop(waiter);
                None
            };
            let (tx, _incoming) = mpsc::channel(1);
            engine
                .on_control(ConnectionControl::AllocateSession { tx, responder })
                .await
                .unwrap();
            drop(waiter);
            assert_eq!(*seen.lock(), [0]);
            let (tx, _incoming) = mpsc::channel(1);
            let (responder, reply) = crate::completion::channel();
            engine
                .on_control(ConnectionControl::AllocateSession { tx, responder })
                .await
                .unwrap();
            assert_eq!(reply.await.unwrap().unwrap().0, 0);
            assert_eq!(engine.connection.session_by_outgoing_channel.len(), 1);
            assert!(matches!(
                engine.connection.local_state,
                ConnectionState::Opened
            ));
        }
    }
}
