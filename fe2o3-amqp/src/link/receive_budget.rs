//! Shared native storage bound for incomplete messages across receiving links.
use super::ReceiverTransferError;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// A bound on simultaneously retained message fragments, not on total traffic.
/// Share one instance across all receivers belonging to the same connection.
#[derive(Debug)]
pub struct ReceiveBudget {
    maximum: usize,
    used: AtomicUsize,
}
impl ReceiveBudget {
    /// Create a native fragment-storage budget.
    pub fn new(maximum: usize) -> Arc<Self> {
        Arc::new(Self {
            maximum,
            used: AtomicUsize::new(0),
        })
    }
    /// Current retained encoded bytes.
    pub fn retained_bytes(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
    pub(crate) fn reserve(
        self: &Arc<Self>,
        size: usize,
    ) -> Result<Reservation, ReceiverTransferError> {
        let mut reservation = Reservation {
            budget: self.clone(),
            size: 0,
        };
        reservation.resize(size)?;
        Ok(reservation)
    }
}
#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Arc<ReceiveBudget>,
    size: usize,
}
impl Reservation {
    pub fn resize(&mut self, size: usize) -> Result<(), ReceiverTransferError> {
        if size > self.size {
            let extra = size - self.size;
            self.budget
                .used
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                    used.checked_add(extra)
                        .filter(|next| *next <= self.budget.maximum)
                })
                .map_err(|_| ReceiverTransferError::MessageSizeExceeded)?;
        } else {
            self.budget
                .used
                .fetch_sub(self.size - size, Ordering::AcqRel);
        }
        self.size = size;
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.size, Ordering::AcqRel);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partials_share_capacity_fail_before_growth_and_release_on_abort_or_drop() {
        let budget = ReceiveBudget::new(10);
        let mut a = budget.reserve(6).unwrap();
        let b = budget.reserve(4).unwrap();
        assert!(a.resize(7).is_err());
        assert_eq!(budget.retained_bytes(), 10);
        drop(b);
        a.resize(10).unwrap();
        assert_eq!(budget.retained_bytes(), 10);
        a.resize(3).unwrap();
        assert_eq!(budget.retained_bytes(), 3);
        assert!(budget.reserve(usize::MAX).is_err());
        assert_eq!(budget.retained_bytes(), 3);
        drop(a);
        assert_eq!(budget.retained_bytes(), 0);
    }
    #[test]
    fn concurrent_receivers_cannot_over_reserve_or_leak_capacity() {
        let budget = ReceiveBudget::new(1024);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let budget = budget.clone();
                scope.spawn(move || {
                    for _ in 0..1000 {
                        if let Ok(mut lease) = budget.reserve(128) {
                            let _ = lease.resize(512);
                            assert!(budget.retained_bytes() <= 1024);
                        }
                    }
                });
            }
        });
        assert_eq!(budget.retained_bytes(), 0);
    }
}
