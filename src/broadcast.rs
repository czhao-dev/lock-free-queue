use crate::mpmc_queue::MpmcQueue;
use std::sync::{Arc, RwLock};

/// Error returned by [`BroadcastQueue::publish`].
pub(crate) enum BroadcastError {
    /// No subscribers are registered; the value was not delivered.
    NoSubscribers,
    /// At least one subscriber's receive channel is full (backpressure).
    SubscriberFull,
}

/// Fan-out broadcast primitive.
///
/// Each call to [`subscribe`](BroadcastQueue::subscribe) allocates a
/// dedicated [`MpmcQueue`] channel for that subscriber.  Publishing clones
/// `T` once per active subscriber, so storing `Arc<Payload>` as `T`
/// achieves effective zero-copy delivery — only the thin Arc pointer is
/// cloned, never the underlying payload bytes.
///
/// Subscribe/unsubscribe are write-lock operations (control-plane, cold
/// path).  Publishing takes a short-lived read lock to snapshot the
/// channel list, then pushes into each channel lock-free.
pub(crate) struct BroadcastQueue<T: Clone + Send + 'static> {
    channels: RwLock<Vec<Arc<MpmcQueue<T>>>>,
    capacity: usize,
}

impl<T: Clone + Send + 'static> BroadcastQueue<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        BroadcastQueue {
            channels: RwLock::new(Vec::new()),
            capacity,
        }
    }

    /// Allocate a new subscriber channel and register it.
    pub(crate) fn subscribe(&self) -> Arc<MpmcQueue<T>> {
        let channel = Arc::new(MpmcQueue::new(self.capacity));
        self.channels.write().unwrap().push(Arc::clone(&channel));
        channel
    }

    /// Remove a subscriber channel identified by pointer equality.
    pub(crate) fn unsubscribe(&self, channel: &Arc<MpmcQueue<T>>) {
        self.channels
            .write()
            .unwrap()
            .retain(|c| !Arc::ptr_eq(c, channel));
    }

    /// Deliver `value` to every currently registered subscriber.
    ///
    /// Returns [`BroadcastError::NoSubscribers`] when no subscribers exist.
    /// Returns [`BroadcastError::SubscriberFull`] if any channel is full;
    /// subscribers that were reached before the full one will have received
    /// the message (backpressure signal — caller should drain lagging
    /// consumers before retrying).
    pub(crate) fn publish(&self, value: T) -> Result<(), BroadcastError> {
        let channels = self.channels.read().unwrap();
        if channels.is_empty() {
            return Err(BroadcastError::NoSubscribers);
        }
        for ch in channels.iter() {
            // `value.clone()` is O(1) when T = Arc<Payload>.
            if ch.push(value.clone()).is_err() {
                return Err(BroadcastError::SubscriberFull);
            }
        }
        Ok(())
    }

    /// Returns the number of currently active subscribers.
    pub(crate) fn subscriber_count(&self) -> usize {
        self.channels.read().unwrap().len()
    }
}
