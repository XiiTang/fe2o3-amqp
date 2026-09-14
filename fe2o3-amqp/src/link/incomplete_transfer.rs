use fe2o3_amqp_types::performatives::Transfer;

use crate::Payload;

use super::ReceiverTransferError;
use fe2o3_amqp_types::messaging::message::admission::{prefix_offset, prefix_position};

macro_rules! or_assign {
    ($self:ident, $other:ident, $field:ident) => {
        match &$self.performative.$field {
            Some(value) => {
                if let Some(other_value) = $other.$field {
                    if *value != other_value {
                        return Err(ReceiverTransferError::InconsistentFieldInMultiFrameDelivery)
                    }
                }
            },
            None => {
                $self.performative.$field = $other.$field;
            }
        }
    };

    ($self:ident, $other:ident, $($field:ident), *) => {
        $(or_assign!($self, $other, $field);)*
    }
}

#[derive(Debug)]
pub(crate) struct IncompleteTransfer {
    storage: super::receive_budget::Reservation,
    pub performative: Transfer,
    pub buffer: Vec<u8>,
    pub section_number: Option<u32>,
    pub section_offset: u64,
}

impl IncompleteTransfer {
    pub fn new(
        transfer: Transfer,
        partial_payload: Payload,
        budget: &std::sync::Arc<super::receive_budget::ReceiveBudget>,
    ) -> Result<Self, ReceiverTransferError> {
        let storage = budget.reserve(partial_payload.len())?;
        let (number, offset) = prefix_position(&partial_payload)
            .map_err(ReceiverTransferError::InvalidMessageEncoding)?;
        Ok(Self {
            storage,
            performative: transfer,
            buffer: partial_payload.to_vec(),
            section_number: Some(number),
            section_offset: offset,
        })
    }
    /// Like `|=` operator but works on the field level
    pub fn or_assign(&mut self, other: Transfer) -> Result<(), ReceiverTransferError> {
        or_assign! {
            self, other,
            delivery_id,
            delivery_tag,
            message_format
        };

        // If not set on the first (or only) transfer for a (multi-transfer)
        // delivery, then the settled flag MUST be interpreted as being false. For
        // subsequent transfers in a multi-transfer delivery if the settled flag
        // is left unset then it MUST be interpreted as true if and only if the
        // value of the settled flag on any of the preceding transfers was true;
        // if no preceding transfer was sent with settled being true then the
        // value when unset MUST be taken as false.
        match &self.performative.settled {
            Some(value) => {
                if let Some(other_value) = other.settled {
                    if !value {
                        self.performative.settled = Some(other_value);
                    }
                }
            }
            None => self.performative.settled = other.settled,
        }

        if let Some(other_state) = other.state {
            if let Some(state) = &self.performative.state {
                // Note that if the transfer performative (or an earlier disposition
                // performative referring to the delivery) indicates that the delivery has
                // attained a terminal state, then no future transfer or disposition sent
                // by the sender can alter that terminal state.
                if !state.is_terminal() {
                    self.performative.state = Some(other_state);
                }
            } else {
                self.performative.state = Some(other_state);
            }
        }

        Ok(())
    }

    /// Append payload after the receiver has checked its materialization bound.
    pub fn append(&mut self, other: Payload) -> Result<(), ReceiverTransferError> {
        self.storage.resize(
            self.buffer
                .len()
                .checked_add(other.len())
                .ok_or(ReceiverTransferError::MessageSizeExceeded)?,
        )?;
        self.buffer.reserve_exact(other.len());
        self.buffer.extend_from_slice(&other);
        let (number, offset) =
            prefix_position(&self.buffer).map_err(ReceiverTransferError::InvalidMessageEncoding)?;
        self.section_number = Some(number);
        self.section_offset = offset;
        Ok(())
    }
    pub fn keep_buffer_till_section_number_and_offset(
        &mut self,
        number: u32,
        offset: u64,
    ) -> Result<(), ReceiverTransferError> {
        let index = prefix_offset(&self.buffer, number, offset)
            .map_err(ReceiverTransferError::InvalidMessageEncoding)?;
        self.buffer.truncate(index);
        self.buffer.shrink_to_fit();
        self.storage.resize(index)?;
        let (number, offset) =
            prefix_position(&self.buffer).map_err(ReceiverTransferError::InvalidMessageEncoding)?;
        self.section_number = Some(number);
        self.section_offset = offset;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn transfer() -> Transfer {
        serde_amqp::from_slice(&[0, 0x53, 0x14, 0xc0, 7, 4, 0x43, 0x43, 0xa0, 1, 0x55, 0x43])
            .unwrap()
    }
    #[test]
    fn every_fragment_boundary_and_explicit_recovery_preserve_exact_encoded_sections() {
        let message = [
            0, 0x53, 0x75, 0xa0, 6, 0, 0x53, 0x75, 0, 255, 7, 0, 0x53, 0x75, 0xa0, 1, 9,
        ];
        for split in 0..=message.len() {
            let mut part = IncompleteTransfer::new(
                transfer(),
                Payload::copy_from_slice(&message[..split]),
                &super::super::receive_budget::ReceiveBudget::new(1024),
            )
            .unwrap();
            part.append(Payload::copy_from_slice(&message[split..]))
                .unwrap();
            assert_eq!(part.buffer, message);
            assert_eq!((part.section_number, part.section_offset), (Some(2), 0));
            assert!(part
                .keep_buffer_till_section_number_and_offset(0, 12)
                .is_err());
            assert_eq!(part.buffer, message);
            part.keep_buffer_till_section_number_and_offset(1, 3)
                .unwrap();
            assert_eq!(part.buffer, &message[..14]);
            part.append(Payload::copy_from_slice(&message[14..]))
                .unwrap();
            assert_eq!(part.buffer, message);
        }
    }
    #[test]
    fn continuation_identity_changes_fail_and_empty_fragments_do_not_grow_storage() {
        let mut part = IncompleteTransfer::new(
            transfer(),
            Payload::new(),
            &super::super::receive_budget::ReceiveBudget::new(1024),
        )
        .unwrap();
        for _ in 0..10000 {
            part.append(Payload::new()).unwrap();
        }
        assert_eq!(part.buffer.capacity(), 0);
        let mut changed = transfer();
        changed.delivery_tag = Some(vec![1].into());
        assert!(part.or_assign(changed).is_err());
    }
}
