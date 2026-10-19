use fe2o3_amqp_types::performatives::{Attach, Detach, Disposition, Transfer};

use crate::{
    endpoint::{AttachmentId, InputHandle, LinkFlow},
    Payload,
};

#[cfg(feature = "transaction")]
use fe2o3_amqp_types::transaction::TransactionId;

pub(crate) type LinkIncomingItem = LinkFrame;

/// Link frames.
///
/// This is a subset of the AMPQ frames. A transfer, flow or detach names the
/// attachment it was made for: one a link sends goes out only while that
/// attachment still holds its output handle (`Session::is_current_output_attachment`),
/// and one the session relays to a link carries the relay's.
// #[derive(Debug)]
pub(crate) enum LinkFrame {
    Attach(Attach),
    Flow(LinkFlow, AttachmentId),
    Transfer {
        input_handle: InputHandle,
        attachment: AttachmentId,
        performative: Transfer,
        payload: Payload,
        window_slot: Option<crate::session::receive_window::Slot>,
        queue_slot: Option<tokio::sync::OwnedSemaphorePermit>,
    },
    Disposition(Disposition),
    Detach(Detach, AttachmentId),

    #[cfg(feature = "transaction")]
    /// Indicating to the receiver that Txn controller side is requesting for
    /// a transactional acquisition
    Acquisition(TransactionId),
}

impl std::fmt::Debug for LinkFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Attach(arg0) => f.debug_tuple("Attach").field(arg0).finish(),
            Self::Flow(arg0, arg1) => f.debug_tuple("Flow").field(arg0).field(arg1).finish(),
            Self::Transfer {
                input_handle,
                attachment,
                performative,
                payload,
                ..
            } => f
                .debug_struct("Transfer")
                .field("input_handle", input_handle)
                .field("attachment", attachment)
                .field("performative", performative)
                .field("payload.len", &payload.len())
                .finish(),
            Self::Disposition(arg0) => f.debug_tuple("Disposition").field(arg0).finish(),
            Self::Detach(arg0, arg1) => f.debug_tuple("Detach").field(arg0).field(arg1).finish(),
            #[cfg(feature = "transaction")]
            Self::Acquisition(arg0) => f.debug_tuple("Acquisition").field(arg0).finish(),
        }
    }
}
