//! Trait abstraction of Connection, Session and Link
//!
//! Frame          Connection  Session  Link
//! ========================================
//! open               H
//! begin              I          H
//! attach                        I       H
//! flow                          I       H
//! transfer                      I       H
//! disposition                   I       H
//! detach                        I       H
//! end                I          H
//! close              H
//! ----------------------------------------
//! Key:
//!     H: handled by the endpoint
//!     I: intercepted (endpoint examines
//!         the frame, but delegates
//!         further processing to another
//!         endpoint)

use fe2o3_amqp_types::{
    definitions::{DeliveryTag, Fields, Handle, SequenceNo},
    messaging::DeliveryState,
    performatives::Flow,
    primitives::{Boolean, Uint},
};

use crate::link::LinkStateError;

mod connection;
pub(crate) use self::connection::*;

mod session;
pub(crate) use self::session::*;

cfg_transaction! {
    mod txn_resource;
    pub(crate) use self::txn_resource::*;
}

mod link;
pub(crate) use self::link::*;

cfg_not_transaction! {
    pub(crate) trait SessionEndpoint: Session {}

    impl<T> SessionEndpoint for T where T: Session {}
}

cfg_transaction! {
    pub(crate) trait SessionEndpoint: Session + HandleDeclare + HandleDischarge {}

    impl<T> SessionEndpoint for T where T: Session + HandleDeclare + HandleDischarge {}
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub(crate) struct OutgoingChannel(pub u16);

impl From<OutgoingChannel> for u16 {
    fn from(channel: OutgoingChannel) -> Self {
        channel.0
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub(crate) struct IncomingChannel(pub u16);

impl From<IncomingChannel> for u16 {
    fn from(channel: IncomingChannel) -> Self {
        channel.0
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub(crate) struct OutputHandle(pub Uint);

impl From<Handle> for OutputHandle {
    fn from(handle: Handle) -> Self {
        Self(handle.0)
    }
}

impl From<OutputHandle> for Handle {
    fn from(handle: OutputHandle) -> Self {
        Self(handle.0)
    }
}

/// One attachment of a link to an output handle. Every allocation, and every
/// reallocation for a resume on the same or another session, takes an
/// identity no attachment in the process had before, so a frame made for one
/// attachment is never taken for a later one that reuses its handle number.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AttachmentId(u64);

impl AttachmentId {
    /// A new identity; only a session allocating an output handle takes one.
    pub(crate) fn allocate() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}

/// A link's output handle together with the attachment it was allocated for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct OutputAttachment {
    pub handle: OutputHandle,
    pub id: AttachmentId,
}

#[cfg(test)]
impl OutputAttachment {
    /// A new attachment of output handle `handle`.
    pub(crate) fn for_tests(handle: u32) -> Self {
        Self {
            handle: OutputHandle(handle),
            id: AttachmentId::allocate(),
        }
    }
}

impl From<OutputAttachment> for Handle {
    fn from(attachment: OutputAttachment) -> Self {
        attachment.handle.into()
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub(crate) struct InputHandle(pub Uint);

impl From<Handle> for InputHandle {
    fn from(handle: Handle) -> Self {
        Self(handle.0)
    }
}

impl From<InputHandle> for Handle {
    fn from(handle: InputHandle) -> Self {
        Self(handle.0)
    }
}

/// A subset of the fields in the Flow performative
#[derive(Debug, Default)]
pub(crate) struct LinkFlow {
    /// Link handle
    pub handle: Handle,

    /// The endpoint’s value for the delivery-count sequence number
    pub delivery_count: Option<SequenceNo>,

    /// The current maximum number of messages that can be received
    pub link_credit: Option<Uint>,

    /// The number of available messages
    pub available: Option<Uint>,

    /// Indicates drain mode
    pub drain: Boolean,

    /// Request state from partner
    pub echo: Boolean,

    /// Link state properties
    pub properties: Option<Fields>,
}

impl TryFrom<Flow> for LinkFlow {
    type Error = ();

    fn try_from(value: Flow) -> Result<Self, Self::Error> {
        let flow = LinkFlow {
            handle: value.handle.ok_or(())?,
            delivery_count: value.delivery_count,
            link_credit: value.link_credit,
            available: value.available,
            drain: value.drain,
            echo: value.echo,
            properties: value.properties,
        };
        Ok(flow)
    }
}

pub(crate) enum Settlement {
    Settled(DeliveryTag),
    Unsettled {
        delivery_tag: DeliveryTag,
        outcome: crate::completion::Receiver<Result<Option<DeliveryState>, LinkStateError>>,
    },
}
