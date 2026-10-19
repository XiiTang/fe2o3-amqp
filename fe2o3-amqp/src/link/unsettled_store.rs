//! A single engine-owned unsettled map with coalesced change notifications.
//!
//! A sender's store closes for good when its link closes: the relay records
//! why under the store's lock as it takes the pending deliveries, and fails
//! them; a delivery registered later is refused with that error instead of
//! waiting for an outcome that can no longer come. Sender code changes the map
//! only through the methods of `Store<UnsettledMessage>`, so no path recreates
//! a closed map.
use super::{
    delivery::UnsettledMessage, receiver_delivery::ReceiverDelivery, LinkStateError, UnsettledMap,
};
use fe2o3_amqp_types::{definitions::DeliveryTag, messaging::DeliveryState};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::{Deref, DerefMut};
use tokio::sync::watch;
#[derive(Debug)]
struct Entries<T> {
    map: Option<UnsettledMap<T>>,
    /// Why a sender's link closed; set once and never cleared.
    closed: Option<LinkStateError>,
}
#[derive(Debug)]
pub(crate) struct Store<T> {
    entries: RwLock<Entries<T>>,
    revision: watch::Sender<u64>,
}
impl<T> Store<T> {
    pub fn new(value: Option<UnsettledMap<T>>) -> Self {
        Self {
            entries: RwLock::new(Entries {
                map: value,
                closed: None,
            }),
            revision: watch::channel(0).0,
        }
    }
    pub fn read(&self) -> MappedRwLockReadGuard<'_, Option<UnsettledMap<T>>> {
        RwLockReadGuard::map(self.entries.read(), |entries| &entries.map)
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.revision.subscribe()
    }
    /// A reattach sends a null unsettled map and starts without one.
    pub fn clear_for_reattach(&self) {
        *self.write_entries() = None;
    }
    fn write_entries(&self) -> Write<'_, T> {
        Write {
            guard: Some(self.entries.write()),
            revision: &self.revision,
        }
    }
}
impl Store<ReceiverDelivery> {
    pub fn write(&self) -> Write<'_, ReceiverDelivery> {
        self.write_entries()
    }
}
impl Store<UnsettledMessage> {
    /// Holds the store open for one registration, or returns why the link
    /// closed. While the guard lives the relay cannot close the store, so a
    /// delivery registered under it is one the close will fail.
    pub fn open(&self) -> Result<Open<'_>, LinkStateError> {
        let write = self.write_entries();
        if let Some(error) = write.closed().clone() {
            return Err(error);
        }
        Ok(Open { write })
    }
    /// Registers `message` under `tag`, or gives it back with why the link
    /// closed.
    #[cfg(test)]
    #[allow(
        clippy::result_large_err,
        reason = "give the test its unregistered delivery back without allocating"
    )]
    pub fn register(
        &self,
        tag: DeliveryTag,
        message: UnsettledMessage,
    ) -> Result<(), (LinkStateError, UnsettledMessage)> {
        match self.open() {
            Ok(mut open) => {
                open.register(tag, message);
                Ok(())
            }
            Err(error) => Err((error, message)),
        }
    }
    /// Closes the store for good with `error`, the first one if it closed
    /// before, and fails every pending delivery with it outside the lock.
    pub fn close(&self, error: LinkStateError) {
        let (entries, error) = {
            let mut write = self.write_entries();
            let entries = write.guard_mut().map.take();
            let error = write.guard_mut().closed.get_or_insert(error).clone();
            (entries, error)
        };
        for (_, entry) in entries.into_iter().flatten() {
            let _ = entry.fail(error.clone());
        }
    }
    /// Removes the delivery under `tag`.
    pub fn withdraw(&self, tag: &DeliveryTag) -> Option<UnsettledMessage> {
        self.write_entries()
            .as_mut()
            .and_then(|map| map.swap_remove(tag))
    }
    /// Records the delivery state of the delivery under `tag`.
    pub fn set_state(&self, tag: &DeliveryTag, state: Option<DeliveryState>) {
        if let Some(message) = self
            .write_entries()
            .as_mut()
            .and_then(|map| map.get_mut(tag))
        {
            message.state = state;
        }
    }
}
/// A sender's store held open: see [`Store::open`].
pub(crate) struct Open<'a> {
    write: Write<'a, UnsettledMessage>,
}
impl Open<'_> {
    pub fn register(&mut self, tag: DeliveryTag, message: UnsettledMessage) {
        self.write
            .get_or_insert_with(UnsettledMap::new)
            .insert(tag, message);
    }
    pub fn withdraw(&mut self, tag: &DeliveryTag) -> Option<UnsettledMessage> {
        self.write.as_mut().and_then(|map| map.swap_remove(tag))
    }
    pub fn contains(&self, tag: &DeliveryTag) -> bool {
        self.write.as_ref().is_some_and(|map| map.contains_key(tag))
    }
}
pub(crate) struct Write<'a, T> {
    guard: Option<RwLockWriteGuard<'a, Entries<T>>>,
    revision: &'a watch::Sender<u64>,
}
impl<T> Write<'_, T> {
    fn guard_mut(&mut self) -> &mut Entries<T> {
        self.guard.as_mut().expect("held unsettled write")
    }
    fn closed(&self) -> &Option<LinkStateError> {
        &self.guard.as_ref().expect("held unsettled write").closed
    }
}
impl<T> Deref for Write<'_, T> {
    type Target = Option<UnsettledMap<T>>;
    fn deref(&self) -> &Self::Target {
        &self.guard.as_ref().expect("held unsettled write").map
    }
}
impl<T> DerefMut for Write<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard_mut().map
    }
}
impl<T> Drop for Write<'_, T> {
    fn drop(&mut self) {
        drop(self.guard.take());
        self.revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn changes_coalesce_without_queuing_dispositions_or_losing_latest_state() {
        let store = Store::<ReceiverDelivery>::new(None);
        let mut observer = store.subscribe();
        for n in 0..10000 {
            store
                .write()
                .get_or_insert_with(UnsettledMap::new)
                .insert(vec![(n % 256) as u8].into(), ReceiverDelivery::new(None));
        }
        observer.changed().await.unwrap();
        assert_eq!(store.read().as_ref().unwrap().len(), 256);
        store.write().as_mut().unwrap().clear();
        observer.changed().await.unwrap();
        assert!(store.read().as_ref().unwrap().is_empty());
        assert!(!observer.has_changed().unwrap());
    }
}
