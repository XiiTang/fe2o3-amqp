//! Session transfer slots are released by consumers, never by queue forwarding.
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tokio::sync::Notify;

#[derive(Debug)]
pub(crate) struct ReceiveWindow {
    maximum: u32,
    held: AtomicU32,
    released: AtomicU32,
    pub changed: Notify,
}
impl ReceiveWindow {
    pub fn new(maximum: u32) -> Arc<Self> {
        Arc::new(Self {
            maximum,
            held: AtomicU32::new(0),
            released: AtomicU32::new(0),
            changed: Notify::new(),
        })
    }
    pub fn maximum(&self) -> u32 {
        self.maximum
    }
    pub fn available(&self) -> u32 {
        self.maximum - self.held.load(Ordering::Acquire)
    }
    pub fn take_released(&self) -> u32 {
        self.released.swap(0, Ordering::AcqRel)
    }
    pub fn reserve(self: &Arc<Self>) -> Option<Slot> {
        self.held
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(1).filter(|n| *n <= self.maximum)
            })
            .ok()?;
        Some(Slot(self.clone()))
    }
}
#[derive(Debug)]
pub(crate) struct Slot(Arc<ReceiveWindow>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.held.fetch_sub(1, Ordering::AcqRel);
        self.0.released.fetch_add(1, Ordering::AcqRel);
        self.0.changed.notify_one();
    }
}
