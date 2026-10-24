//! One unsettled owner. All insertion and retained-state growth is checked here.
use super::{
    delivery::{DeliveryInfo, UnsettledMessage},
    receiver_delivery::ReceiverDelivery,
    IllegalLinkStateError, LinkStateError, UnsettledMap,
};
use fe2o3_amqp_types::{definitions::DeliveryTag, messaging::DeliveryState};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::watch;

/// Configured population and retained-content bounds for one link's unsettled state.
/// Bytes include tags, sender payloads, encoded local/remote states and retained
/// delivery-info tags. Entry count bounds structural storage separately. This is
/// a semantic retention bound, not a measurement of the allocator's heap use.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct UnsettledLimits {
    /// Maximum simultaneously retained delivery identities.
    pub max_entries: usize,
    /// Maximum aggregate retained-content bytes.
    pub max_bytes: usize,
}
impl UnsettledLimits {
    /// Set both bounds. Zero refuses any growth requiring that capacity.
    pub const fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            max_entries,
            max_bytes,
        }
    }
}
/// A link's unsettled owner cannot retain the proposed state. It is terminal;
/// freeing space or resuming does not reopen that owner.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Unsettled store capacity exceeded")]
pub struct UnsettledCapacityExceeded {
    /// The configured bounds, or addressable maxima for arithmetic overflow.
    pub limits: UnsettledLimits,
    /// Proposed entry count.
    pub entries: usize,
    /// Proposed retained bytes; None means size calculation overflowed.
    pub bytes: Option<usize>,
}

pub(crate) trait Retained {
    fn bytes(&self) -> Option<usize>;
    fn fail(self, error: LinkStateError);
}
fn state_bytes(state: &Option<DeliveryState>) -> Option<usize> {
    serde_amqp::serialized_size(state).ok()
}
impl Retained for UnsettledMessage {
    fn bytes(&self) -> Option<usize> {
        self.payload.len().checked_add(state_bytes(&self.state)?)
    }
    fn fail(self, error: LinkStateError) {
        let _ = UnsettledMessage::fail(self, error);
    }
}
impl Retained for ReceiverDelivery {
    fn bytes(&self) -> Option<usize> {
        state_bytes(&self.local)?
            .checked_add(state_bytes(&self.remote)?)?
            .checked_add(self.info.as_ref().map_or(0, |info| info.delivery_tag.len()))
    }
    fn fail(self, _: LinkStateError) {}
}
#[derive(Debug)]
struct Entries<T> {
    map: Option<UnsettledMap<T>>,
    closed: Option<LinkStateError>,
    bytes: usize,
    limits: Option<UnsettledLimits>,
}
#[derive(Debug)]
pub(crate) struct Store<T> {
    entries: RwLock<Entries<T>>,
    revision: watch::Sender<u64>,
    size: fn(&T) -> Option<usize>,
    fail: fn(T, LinkStateError),
    #[cfg(test)]
    before_receiver_write: BeforeReceiverWrite,
}
#[cfg(test)]
#[derive(Default)]
struct BeforeReceiverWrite(parking_lot::Mutex<Option<Box<dyn FnOnce() + Send>>>);
#[cfg(test)]
impl std::fmt::Debug for BeforeReceiverWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BeforeReceiverWrite")
    }
}
impl<T> Store<T> {
    pub fn new(
        value: Option<UnsettledMap<T>>,
        limits: Option<UnsettledLimits>,
    ) -> Result<Self, UnsettledCapacityExceeded>
    where
        T: Retained,
    {
        let store = Self {
            entries: RwLock::new(Entries {
                map: value,
                closed: None,
                bytes: 0,
                limits,
            }),
            revision: watch::channel(0).0,
            #[cfg(test)]
            before_receiver_write: BeforeReceiverWrite::default(),
            size: T::bytes,
            fail: T::fail,
        };
        {
            let mut write = store.write();
            let count = write.entries().map.as_ref().map_or(0, |map| map.len());
            let bytes =
                write
                    .entries()
                    .map
                    .iter()
                    .flatten()
                    .try_fold(0usize, |bytes, (tag, value)| {
                        bytes
                            .checked_add(tag.len())?
                            .checked_add((store.size)(value)?)
                    });
            let bytes = write.check(count, bytes)?;
            write.entries().bytes = bytes;
        }
        Ok(store)
    }
    pub fn read(&self) -> MappedRwLockReadGuard<'_, Option<UnsettledMap<T>>> {
        RwLockReadGuard::map(self.entries.read(), |entries| &entries.map)
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.revision.subscribe()
    }
    /// Keep a native wait tied to this owner's retained terminal cause.
    /// Subscribe before checking, and recheck when the operation completes so
    /// an input/queue closure cannot replace a concurrently published cap error.
    pub fn until_terminated<'a, F>(
        &'a self,
        operation: F,
    ) -> impl std::future::Future<Output = Result<F::Output, UnsettledCapacityExceeded>> + 'a
    where
        F: std::future::Future + 'a,
    {
        // Pin before constructing the guard future. Keeping the operation
        // inline multiplies the stack size of nested recovery/control futures.
        let mut operation = Box::pin(operation);
        async move {
            let mut changed = self.subscribe();
            loop {
                self.check()?;
                tokio::select! {
                    biased;
                    _ = changed.changed() => {},
                    value = &mut operation => {
                        self.check()?;
                        return Ok(value);
                    }
                }
            }
        }
    }
    pub fn check(&self) -> Result<(), UnsettledCapacityExceeded> {
        match &self.entries.read().closed {
            Some(LinkStateError::UnsettledCapacityExceeded(error)) => Err(error.clone()),
            _ => Ok(()),
        }
    }
    pub fn clear_for_reattach(&self) {
        let mut write = self.write();
        write.entries().map = None;
        write.entries().bytes = 0;
    }
    pub fn remove(&self, tag: &DeliveryTag) -> Option<T> {
        self.write().remove(tag)
    }
    pub fn retain(&self, mut keep: impl FnMut(&DeliveryTag, &T) -> bool) {
        let mut write = self.write();
        if let Some(map) = write.entries().map.as_mut() {
            map.as_inner_mut().retain(|tag, value| keep(tag, value));
        }
        let bytes = write
            .entries()
            .map
            .iter()
            .flatten()
            .map(|(tag, value)| {
                tag.len() + (self.size)(value).expect("previously admitted retained size")
            })
            .sum();
        write.entries().bytes = bytes;
    }
    fn write(&self) -> Mutation<'_, T> {
        #[cfg(test)]
        {
            let callback = self.before_receiver_write.0.lock().take();
            if let Some(callback) = callback { callback(); }
        }
        Mutation {
            guard: Some(self.entries.write()),
            store: self,
            failed: None,
            rejected: None,
        }
    }
    #[cfg(test)]
    pub fn usage(&self) -> (usize, usize) {
        let entries = self.entries.read();
        (
            entries.map.as_ref().map_or(0, |map| map.len()),
            entries.bytes,
        )
    }
}
impl Store<ReceiverDelivery> {
    #[cfg(test)]
    pub(crate) fn before_write_once(&self, callback: impl FnOnce() + Send + 'static) {
        let mut slot = self.before_receiver_write.0.lock();
        assert!(slot.is_none());
        *slot = Some(Box::new(callback));
    }
    pub fn insert(
        &self,
        tag: DeliveryTag,
        value: ReceiverDelivery,
    ) -> Result<(), UnsettledCapacityExceeded> {
        self.write().insert(tag, value)
    }
    pub fn update(
        &self,
        tag: &DeliveryTag,
        update: impl FnOnce(&mut ReceiverDelivery),
    ) -> Result<Option<ReceiverDelivery>, UnsettledCapacityExceeded> {
        let mut write = self.write();
        write.ensure_open()?;
        let Some(mut value) = write
            .entries()
            .map
            .as_ref()
            .and_then(|map| map.get(tag))
            .cloned()
        else {
            return Ok(None);
        };
        update(&mut value);
        write.insert(tag.clone(), value.clone())?;
        Ok(Some(value))
    }
    pub fn update_all(
        &self,
        mut update: impl FnMut(&DeliveryTag, &mut ReceiverDelivery),
    ) -> Result<(), UnsettledCapacityExceeded> {
        let tags: Vec<_> = self
            .read()
            .iter()
            .flatten()
            .map(|(tag, _)| tag.clone())
            .collect();
        for tag in tags {
            self.update(&tag, |value| update(&tag, value))?;
        }
        self.check()
    }
    pub fn dispose(
        &self,
        info: &DeliveryInfo,
        settled: bool,
        state: &DeliveryState,
    ) -> Result<Option<ReceiverDelivery>, IllegalLinkStateError> {
        let mut write = self.write();
        write.ensure_open()?;
        let Some(value) = write
            .entries()
            .map
            .as_ref()
            .and_then(|map| map.get(&info.delivery_tag))
        else {
            return Ok(None);
        };
        if !value
            .info
            .as_ref()
            .is_some_and(|current| current.same_delivery(info))
        {
            return Err(IllegalLinkStateError::IllegalState);
        }
        if settled {
            return Ok(write.remove(&info.delivery_tag));
        }
        let mut value = value.clone();
        value.local_state(state.clone());
        write.insert(info.delivery_tag.clone(), value.clone())?;
        Ok(Some(value))
    }
    pub fn dispose_many(
        &self,
        infos: &[DeliveryInfo],
        settled: bool,
        state: &DeliveryState,
    ) -> Result<(), IllegalLinkStateError> {
        let mut write = self.write();
        write.ensure_open()?;
        let map = write
            .entries()
            .map
            .as_ref()
            .ok_or(IllegalLinkStateError::IllegalState)?;
        if infos.iter().any(|info| {
            !map.get(&info.delivery_tag)
                .and_then(|value| value.info.as_ref())
                .is_some_and(|current| current.same_delivery(info))
        }) {
            return Err(IllegalLinkStateError::IllegalState);
        }
        // Validate all identities before any mutation under this same lock.
        for info in infos {
            if settled {
                write.remove(&info.delivery_tag);
            } else {
                let mut value = write
                    .entries()
                    .map
                    .as_ref()
                    .unwrap()
                    .get(&info.delivery_tag)
                    .unwrap()
                    .clone();
                value.local_state(state.clone());
                write.insert(info.delivery_tag.clone(), value)?;
            }
        }
        Ok(())
    }
}
impl Store<UnsettledMessage> {
    pub fn open(&self) -> Result<Open<'_>, LinkStateError> {
        let write = self.write();
        if let Some(error) = write.guard.as_ref().unwrap().closed.clone() {
            return Err(error);
        }
        Ok(Open { write })
    }
    #[cfg(test)]
    #[allow(
        clippy::result_large_err,
        reason = "return the unregistered delivery to its test owner"
    )]
    pub fn register(
        &self,
        tag: DeliveryTag,
        message: UnsettledMessage,
    ) -> Result<(), (LinkStateError, UnsettledMessage)> {
        match self.open() {
            Ok(mut open) => {
                // Check before moving the test's owner, as production admission does.
                if let Err(error) = open.check_registration(tag.len(), &message, None) {
                    return Err((error.into(), message));
                }
                open.register(tag, message)
                    .map_err(|_| unreachable!("admission held under the same lock"))
            }
            Err(error) => Err((error, message)),
        }
    }
    pub fn close(&self, error: LinkStateError) {
        let mut write = self.write();
        write.entries().closed.get_or_insert(error);
        write.failed = write.entries().map.take();
        write.entries().bytes = 0;
    }
    pub fn withdraw(&self, tag: &DeliveryTag) -> Option<UnsettledMessage> {
        self.remove(tag)
    }
    pub fn set_state(
        &self,
        tag: &DeliveryTag,
        state: Option<DeliveryState>,
    ) -> Result<(), UnsettledCapacityExceeded> {
        let mut write = self.write();
        write.ensure_open()?;
        let old = write.entries().map.as_ref().and_then(|map| map.get(tag));
        if let Some(old) = old {
            let old_size = (self.size)(old).expect("admitted delivery size");
            let size = old
                .payload
                .len()
                .checked_add(state_bytes(&state).unwrap_or(usize::MAX));
            let bytes = write
                .entries()
                .bytes
                .checked_sub(old_size)
                .and_then(|bytes| bytes.checked_add(size?));
            let count = write.entries().map.as_ref().unwrap().len();
            let bytes = write.check(count, bytes)?;
            write
                .entries()
                .map
                .as_mut()
                .unwrap()
                .get_mut(tag)
                .unwrap()
                .state = state;
            write.entries().bytes = bytes;
        }
        Ok(())
    }
}
pub(crate) struct Open<'a> {
    write: Mutation<'a, UnsettledMessage>,
}
impl Open<'_> {
    pub fn check_registration(
        &mut self,
        tag_len: usize,
        message: &UnsettledMessage,
        replacing: Option<&DeliveryTag>,
    ) -> Result<(), UnsettledCapacityExceeded> {
        let entries = self.write.entries();
        let old_size = replacing.and_then(|tag| {
            entries
                .map
                .as_ref()
                .and_then(|map| map.get(tag))
                .map(|value| tag.len() + value.bytes().expect("admitted delivery size"))
        });
        let count =
            entries.map.as_ref().map_or(0, |map| map.len()) + usize::from(old_size.is_none());
        let bytes = entries
            .bytes
            .checked_sub(old_size.unwrap_or(0))
            .and_then(|bytes| bytes.checked_add(tag_len)?.checked_add(message.bytes()?));
        self.write.check(count, bytes).map(|_| ())
    }
    pub fn check_move(
        &mut self,
        old: &DeliveryTag,
        tag_len: usize,
    ) -> Result<(), UnsettledCapacityExceeded> {
        let entries = self.write.entries();
        let count = entries.map.as_ref().map_or(0, |map| map.len());
        let bytes = if entries
            .map
            .as_ref()
            .is_some_and(|map| map.contains_key(old))
        {
            entries
                .bytes
                .checked_sub(old.len())
                .and_then(|bytes| bytes.checked_add(tag_len))
        } else {
            Some(entries.bytes)
        };
        self.write.check(count, bytes).map(|_| ())
    }
    pub fn register(
        &mut self,
        tag: DeliveryTag,
        message: UnsettledMessage,
    ) -> Result<(), UnsettledCapacityExceeded> {
        self.write.insert(tag, message)
    }
    pub fn withdraw(&mut self, tag: &DeliveryTag) -> Option<UnsettledMessage> {
        self.write.remove(tag)
    }
    pub fn contains(&self, tag: &DeliveryTag) -> bool {
        self.write
            .guard
            .as_ref()
            .unwrap()
            .map
            .as_ref()
            .is_some_and(|map| map.contains_key(tag))
    }
}
struct Mutation<'a, T> {
    guard: Option<RwLockWriteGuard<'a, Entries<T>>>,
    store: &'a Store<T>,
    failed: Option<UnsettledMap<T>>,
    rejected: Option<T>,
}
impl<T> Mutation<'_, T> {
    fn entries(&mut self) -> &mut Entries<T> {
        self.guard.as_mut().expect("held store mutation")
    }
    fn ensure_open(&mut self) -> Result<(), UnsettledCapacityExceeded> {
        match &self.entries().closed {
            Some(LinkStateError::UnsettledCapacityExceeded(error)) => Err(error.clone()),
            _ => Ok(()),
        }
    }
    fn check(
        &mut self,
        count: usize,
        bytes: Option<usize>,
    ) -> Result<usize, UnsettledCapacityExceeded> {
        self.ensure_open()?;
        let limits = self
            .entries()
            .limits
            .unwrap_or(UnsettledLimits::new(usize::MAX, usize::MAX));
        if count > limits.max_entries || bytes.is_none_or(|bytes| bytes > limits.max_bytes) {
            let error = UnsettledCapacityExceeded {
                limits,
                entries: count,
                bytes,
            };
            self.entries().closed = Some(LinkStateError::UnsettledCapacityExceeded(error.clone()));
            self.failed = self.entries().map.take();
            self.entries().bytes = 0;
            return Err(error);
        }
        Ok(bytes.unwrap())
    }
    fn insert(&mut self, tag: DeliveryTag, value: T) -> Result<(), UnsettledCapacityExceeded> {
        self.ensure_open()?;
        let size = self.store.size;
        let entries = self.entries();
        let previous = entries.map.as_ref().and_then(|map| map.get(&tag));
        let old_size =
            previous.map_or(0, |old| tag.len() + size(old).expect("admitted entry size"));
        let count =
            entries.map.as_ref().map_or(0, |map| map.len()) + usize::from(previous.is_none());
        let bytes = entries
            .bytes
            .checked_sub(old_size)
            .and_then(|bytes| bytes.checked_add(tag.len())?.checked_add(size(&value)?));
        let bytes = match self.check(count, bytes) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.rejected = Some(value);
                return Err(error);
            }
        };
        self.entries()
            .map
            .get_or_insert_with(UnsettledMap::new)
            .insert(tag, value);
        self.entries().bytes = bytes;
        Ok(())
    }
    fn remove(&mut self, tag: &DeliveryTag) -> Option<T> {
        let value = self.entries().map.as_mut()?.swap_remove(tag)?;
        let size = tag.len() + (self.store.size)(&value).expect("admitted entry size");
        self.entries().bytes -= size;
        Some(value)
    }
}
impl<T> Drop for Mutation<'_, T> {
    fn drop(&mut self) {
        let error = self
            .guard
            .as_ref()
            .and_then(|entries| entries.closed.clone());
        drop(self.guard.take());
        self.store
            .revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        if let Some(error) = error {
            for (_, value) in self.failed.take().into_iter().flatten() {
                (self.store.fail)(value, error.clone());
            }
            if let Some(value) = self.rejected.take() {
                (self.store.fail)(value, error);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fe2o3_amqp_types::{
        definitions::{AmqpError, Error},
        messaging::Rejected,
    };
    fn large_state() -> DeliveryState {
        Rejected {
            error: Some(Error::new(
                AmqpError::InternalError,
                Some("x".repeat(1024)),
                None,
            )),
        }
        .into()
    }
    #[test]
    fn both_roles_refuse_excess_count_and_bytes_and_stay_ended_after_clear() {
        for sender in [false, true] {
            if sender {
                let store = Store::<UnsettledMessage>::new(None, Some(UnsettledLimits::new(1, 32)))
                    .unwrap();
                let (tx, mut rx) = crate::completion::channel();
                store
                    .register(
                        vec![1].into(),
                        UnsettledMessage::new(bytes::Bytes::from_static(b"m"), None, 0, tx),
                    )
                    .unwrap();
                assert_eq!(store.usage().0, 1);
                assert!(store
                    .set_state(&vec![1].into(), Some(large_state()))
                    .is_err());
                assert!(matches!(
                    rx.try_recv(),
                    Ok(Err(LinkStateError::UnsettledCapacityExceeded(_)))
                ));
                assert_eq!(store.usage(), (0, 0));
                store.clear_for_reattach();
                assert!(store.open().is_err());
            } else {
                let store = Store::<ReceiverDelivery>::new(None, Some(UnsettledLimits::new(1, 32)))
                    .unwrap();
                store
                    .insert(vec![1].into(), ReceiverDelivery::new(None))
                    .unwrap();
                assert!(store
                    .insert(vec![2].into(), ReceiverDelivery::new(None))
                    .is_err());
                assert_eq!(store.usage(), (0, 0));
                store.clear_for_reattach();
                assert!(store
                    .insert(vec![3].into(), ReceiverDelivery::new(None))
                    .is_err());
            }
        }
    }
    #[test]
    fn receiver_updates_and_recovery_states_use_the_same_byte_bound() {
        for remote in [false, true] {
            let store =
                Store::<ReceiverDelivery>::new(None, Some(UnsettledLimits::new(4, 32))).unwrap();
            store
                .insert(vec![1].into(), ReceiverDelivery::new(None))
                .unwrap();
            let result = store.update(&vec![1].into(), |value| {
                if remote {
                    value.remote_state(Some(large_state()));
                } else {
                    value.local_state(large_state());
                }
            });
            assert!(result.is_err());
            assert_eq!(store.usage(), (0, 0));
        }
    }
    #[test]
    fn removal_replacement_and_shrinking_return_their_actual_room() {
        let store = Store::<ReceiverDelivery>::new(None, Some(UnsettledLimits::new(2, 6))).unwrap();
        store
            .insert(vec![1].into(), ReceiverDelivery::new(None))
            .unwrap();
        store
            .insert(vec![1].into(), ReceiverDelivery::new(None))
            .unwrap();
        assert_eq!(store.usage(), (1, 3));
        store
            .insert(vec![2].into(), ReceiverDelivery::new(None))
            .unwrap();
        store.remove(&vec![1].into());
        store
            .insert(vec![3].into(), ReceiverDelivery::new(None))
            .unwrap();
        assert_eq!(store.usage(), (2, 6));
    }
    #[tokio::test]
    async fn changes_coalesce_without_queuing_dispositions_or_losing_latest_state() {
        let store = Store::<ReceiverDelivery>::new(None, None).unwrap();
        let mut observer = store.subscribe();
        for n in 0..10000 {
            store
                .insert(vec![(n % 256) as u8].into(), ReceiverDelivery::new(None))
                .unwrap();
        }
        observer.changed().await.unwrap();
        assert_eq!(store.usage().0, 256);
        store.retain(|_, _| false);
        observer.changed().await.unwrap();
        assert_eq!(store.usage(), (0, 0));
        assert!(!observer.has_changed().unwrap());
    }
    #[tokio::test]
    async fn terminal_wait_wakes_and_retains_capacity_metadata() {
        use std::{
            future::Future,
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
            task::{Context, Poll, Wake, Waker},
        };
        struct Wakes(AtomicUsize);
        impl Wake for Wakes {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let store = Store::new(None, Some(UnsettledLimits::new(0, 32))).unwrap();
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = Waker::from(wakes.clone());
        let mut context = Context::from_waker(&waker);
        let mut waiting = Box::pin(store.until_terminated(std::future::pending::<()>()));
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        let error = store
            .insert(vec![1].into(), ReceiverDelivery::new(None))
            .unwrap_err();
        assert!(wakes.0.load(Ordering::Relaxed) > 0);
        assert_eq!(
            waiting.as_mut().poll(&mut context),
            Poll::Ready(Err(error.clone()))
        );
        assert_eq!(store.until_terminated(async {}).await, Err(error));
    }
}
