use crate::broadcast::{BroadcastError, BroadcastQueue};
use crate::mpmc_queue::MpmcQueue;
use std::sync::Arc;

/// A named, single-type broadcast stream.
///
/// `TopicStream` is the routing target that [`Publisher`](crate::Publisher)s
/// write to and [`Subscriber`](crate::Subscriber)s read from.  Internally
/// it manages a [`BroadcastQueue`] that fans every published value out to
/// each active subscriber's dedicated lock-free ring-buffer channel.
///
/// ## Capacity
/// `capacity` is the per-subscriber channel depth and must be a non-zero
/// power of two (required by the underlying [`MpmcQueue`]).
///
/// ## Usage
/// Most users obtain a `TopicStream` indirectly through
/// [`BrokerEngine::create_topic`](crate::BrokerEngine::create_topic).  For
/// single-topic use cases you can also construct one directly:
///
/// ```rust
/// use std::sync::Arc;
/// use oxide_broker::TopicStream;
///
/// let stream: Arc<TopicStream<u32>> = TopicStream::new("metrics", 512);
/// ```
pub struct TopicStream<T: Clone + Send + 'static> {
    name: String,
    broadcast: BroadcastQueue<T>,
}

impl<T: Clone + Send + 'static> TopicStream<T> {
    /// Create a new `TopicStream` with the given `name` and per-subscriber
    /// ring-buffer `capacity`.
    ///
    /// # Panics
    /// Panics if `capacity` is zero or not a power of two.
    pub fn new(name: impl Into<String>, capacity: usize) -> Arc<Self> {
        assert!(
            capacity > 0 && capacity.is_power_of_two(),
            "TopicStream capacity must be a non-zero power of two, got {capacity}"
        );
        Arc::new(TopicStream {
            name: name.into(),
            broadcast: BroadcastQueue::new(capacity),
        })
    }

    /// The name of this topic.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Publish `payload` to every active subscriber.
    ///
    /// Returns `Err` if no subscribers are registered or if any subscriber's
    /// channel is full.
    pub(crate) fn publish(&self, payload: T) -> Result<(), BroadcastError> {
        self.broadcast.publish(payload)
    }

    /// The number of currently active subscribers on this stream.
    pub fn subscriber_count(&self) -> usize {
        self.broadcast.subscriber_count()
    }

    /// Allocate and register a new subscriber receive channel.
    pub(crate) fn subscribe_channel(&self) -> Arc<MpmcQueue<T>> {
        self.broadcast.subscribe()
    }

    /// Deregister a subscriber channel by pointer identity.
    pub(crate) fn unsubscribe_channel(&self, channel: &Arc<MpmcQueue<T>>) {
        self.broadcast.unsubscribe(channel);
    }
}
