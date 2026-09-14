//! Per-link resumed-transfer identity; never turn an unknown resume into a new delivery.
use super::RecvError;
use fe2o3_amqp_types::performatives::Transfer;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Ignore,
    StateOnly,
    Payload,
}
#[derive(Debug, Default)]
pub(crate) struct IncomingRecovery {
    active: Option<(Transfer, Kind)>,
}
impl IncomingRecovery {
    pub fn reset(&mut self) {
        self.active = None;
    }
    pub fn continuation(&mut self, transfer: &mut Transfer) -> Result<Option<Kind>, RecvError> {
        let Some((first, kind)) = &self.active else {
            return Ok(None);
        };
        macro_rules! identity {
            ($field:ident) => {
                if transfer
                    .$field
                    .as_ref()
                    .zip(first.$field.as_ref())
                    .is_some_and(|(a, b)| a != b)
                {
                    return Err(RecvError::InconsistentFieldInMultiFrameDelivery);
                }
                if transfer.$field.is_none() {
                    transfer.$field = first.$field.clone();
                }
            };
        }
        identity!(delivery_id);
        identity!(delivery_tag);
        identity!(message_format);
        let kind = *kind;
        if !transfer.more || transfer.aborted {
            self.active = None;
        }
        Ok(Some(kind))
    }
    pub fn start(&mut self, transfer: &Transfer, kind: Kind) {
        if transfer.more && !transfer.aborted {
            self.active = Some((transfer.clone(), kind));
        }
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
    fn ignored_resume_covers_omitted_and_repeated_resume_flags_until_final_frame() {
        for kind in [Kind::Ignore, Kind::StateOnly, Kind::Payload] {
            let mut state = IncomingRecovery::default();
            let mut first = transfer();
            first.resume = true;
            first.more = true;
            state.start(&first, kind);
            for resume in [false, true, false] {
                let mut next = first.clone();
                next.resume = resume;
                next.delivery_id = None;
                next.delivery_tag = None;
                next.message_format = None;
                assert_eq!(state.continuation(&mut next).unwrap(), Some(kind));
                assert_eq!(next.delivery_tag, first.delivery_tag);
            }
            let mut invalid = first.clone();
            invalid.delivery_id = Some(7);
            assert!(state.continuation(&mut invalid).is_err());
            first.more = false;
            assert_eq!(state.continuation(&mut first).unwrap(), Some(kind));
            assert_eq!(state.continuation(&mut first).unwrap(), None);
        }
    }
    #[test]
    fn abort_and_explicit_new_attach_clear_only_current_resume_identity() {
        let mut state = IncomingRecovery::default();
        let mut transfer = transfer();
        transfer.more = true;
        transfer.resume = true;
        state.start(&transfer, Kind::Ignore);
        transfer.aborted = true;
        assert_eq!(
            state.continuation(&mut transfer).unwrap(),
            Some(Kind::Ignore)
        );
        assert_eq!(state.continuation(&mut transfer).unwrap(), None);
        transfer.aborted = false;
        state.start(&transfer, Kind::Payload);
        state.reset();
        assert_eq!(state.continuation(&mut transfer).unwrap(), None);
    }
}
