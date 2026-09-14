//! A single engine-owned unsettled map with coalesced change notifications.
use super::UnsettledMap;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::{Deref, DerefMut};
use tokio::sync::watch;
#[derive(Debug)]
pub(crate) struct Store<T> {
    map: RwLock<Option<UnsettledMap<T>>>,
    revision: watch::Sender<u64>,
}
impl<T> Store<T> {
    pub fn new(value: Option<UnsettledMap<T>>) -> Self {
        Self {
            map: RwLock::new(value),
            revision: watch::channel(0).0,
        }
    }
    pub fn read(&self) -> RwLockReadGuard<'_, Option<UnsettledMap<T>>> {
        self.map.read()
    }
    pub fn write(&self) -> Write<'_, T> {
        Write {
            guard: Some(self.map.write()),
            revision: &self.revision,
        }
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.revision.subscribe()
    }
}
pub(crate) struct Write<'a, T> {
    guard: Option<RwLockWriteGuard<'a, Option<UnsettledMap<T>>>>,
    revision: &'a watch::Sender<u64>,
}
impl<T> Deref for Write<'_, T> {
    type Target = Option<UnsettledMap<T>>;
    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().expect("held unsettled write")
    }
}
impl<T> DerefMut for Write<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_mut().expect("held unsettled write")
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
        let store = Store::<Option<fe2o3_amqp_types::messaging::DeliveryState>>::new(None);
        let mut observer = store.subscribe();
        for n in 0..10000 {
            store
                .write()
                .get_or_insert_with(UnsettledMap::new)
                .insert(vec![(n % 256) as u8].into(), None);
        }
        observer.changed().await.unwrap();
        assert_eq!(store.read().as_ref().unwrap().len(), 256);
        store.write().as_mut().unwrap().clear();
        observer.changed().await.unwrap();
        assert!(store.read().as_ref().unwrap().is_empty());
        assert!(!observer.has_changed().unwrap());
    }
}
