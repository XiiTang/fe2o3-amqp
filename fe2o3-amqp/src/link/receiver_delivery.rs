//! Local receipt progress and the peer's delivery state are different facts.
use crate::util::AsDeliveryState;
use fe2o3_amqp_types::messaging::DeliveryState;
#[derive(Debug, Clone, Default)]
pub(crate) struct ReceiverDelivery {
    pub local: Option<DeliveryState>,
    pub remote: Option<DeliveryState>,
    pub received: bool,
    pub info: Option<super::delivery::DeliveryInfo>,
}
impl ReceiverDelivery {
    pub fn new(remote: Option<DeliveryState>) -> Self {
        Self {
            local: None,
            remote,
            received: false,
            info: None,
        }
    }
    pub fn remote_state(&mut self, state: Option<DeliveryState>) {
        if !self.remote.as_ref().is_some_and(DeliveryState::is_terminal) {
            self.remote = state;
        }
    }
    pub fn local_state(&mut self, state: DeliveryState) {
        if !self.local.as_ref().is_some_and(DeliveryState::is_terminal) {
            self.local = Some(state);
        }
    }
    pub fn received(&mut self, state: DeliveryState, complete: bool) {
        self.local_state(state);
        self.received |= complete;
    }
}
impl AsDeliveryState for ReceiverDelivery {
    fn as_delivery_state(&self) -> &Option<DeliveryState> {
        &self.local
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fe2o3_amqp_types::messaging::{Accepted, Received, Released};
    #[test]
    fn peer_state_cannot_masquerade_as_local_receipt_or_overwrite_local_outcome() {
        let mut delivery = ReceiverDelivery::new(Some(Accepted {}.into()));
        assert_eq!(delivery.as_delivery_state(), &None);
        assert!(!delivery.received);
        delivery.received(
            Received {
                section_number: 1,
                section_offset: 16,
            }
            .into(),
            false,
        );
        assert!(!delivery.received);
        assert!(matches!(delivery.local, Some(DeliveryState::Received(_))));
        assert!(matches!(delivery.remote, Some(DeliveryState::Accepted(_))));
        delivery.received(
            Received {
                section_number: 2,
                section_offset: 0,
            }
            .into(),
            true,
        );
        assert!(delivery.received);
        delivery.local_state(Released {}.into());
        delivery.remote_state(Some(
            Received {
                section_number: 0,
                section_offset: 0,
            }
            .into(),
        ));
        assert!(matches!(delivery.local, Some(DeliveryState::Released(_))));
        assert!(matches!(delivery.remote, Some(DeliveryState::Accepted(_))));
        delivery.received(
            Received {
                section_number: 3,
                section_offset: 0,
            }
            .into(),
            true,
        );
        assert!(matches!(delivery.local, Some(DeliveryState::Released(_))));
    }
}
