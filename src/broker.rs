use crate::broadcast::BroadcastError;
use crate::mpmc_queue::MpmcQueue;
use crate::topic::TopicStream;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// ──────────────────────────────────────────────────────────────────────────
// Error type
// ──────────────────────────────────────────────────────────────────────────

/// Error returned by [`Publisher::publish`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishError {
    /// The topic has no active subscribers; the value was not delivered.
    NoSubscribers,
    /// At least one subscriber's ring-buffer channel is full.
    /// Apply backpressure and retry, or drain lagging subscribers.
    SubscriberFull,
}

impl From<BroadcastError> for PublishError {
    fn from(e: BroadcastError) -> Self {
        match e {
            BroadcastError::NoSubscribers => PublishError::NoSubscribers,
            BroadcastError::SubscriberFull => PublishError::SubscriberFull,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// BrokerEngine
// ──────────────────────────────────────────────────────────────────────────

/// Top-level pub/sub message broker.
///
/// `BrokerEngine` owns the topic registry and is the factory for
/// [`Publisher`] and [`Subscriber`] handles.  All topics in one engine
/// share the same message type `T`.
///
/// Topic creation is a write-locked, cold-path operation.  The hot
/// publish/consume data plane is entirely lock-free.
///
/// # Example
///
/// ```rust
/// use oxide_broker::BrokerEngine;
///
/// let broker: BrokerEngine<u64> = BrokerEngine::new();
/// broker.create_topic("metrics", 4096);
///
/// let publisher = broker.publisher("metrics").unwrap();
/// let subscriber = broker.subscribe("metrics").unwrap();
///
/// publisher.publish(1).unwrap();
/// assert_eq!(subscriber.try_recv(), Some(1));
/// ```
pub struct BrokerEngine<T: Clone + Send + 'static> {
    topics: RwLock<HashMap<String, Arc<TopicStream<T>>>>,
}

impl<T: Clone + Send + 'static> BrokerEngine<T> {
    /// Create a new, empty `BrokerEngine`.
    pub fn new() -> Self {
        BrokerEngine {
            topics: RwLock::new(HashMap::new()),
        }
    }

    /// Register a topic with the given `name` and per-subscriber ring-buffer
    /// `capacity`, returning the (possibly pre-existing) stream.
    ///
    /// If a topic with `name` already exists its stream is returned
    /// unchanged regardless of `capacity`.
    ///
    /// # Panics
    /// Panics if `capacity` is zero or not a power of two.
    pub fn create_topic(&self, name: &str, capacity: usize) -> Arc<TopicStream<T>> {
        // Fast path: topic already exists.
        {
            let topics = self.topics.read().unwrap();
            if let Some(stream) = topics.get(name) {
                return Arc::clone(stream);
            }
        }
        // Slow path: double-checked locking inside the write guard.
        let mut topics = self.topics.write().unwrap();
        Arc::clone(
            topics
                .entry(name.to_owned())
                .or_insert_with(|| TopicStream::new(name, capacity)),
        )
    }

    /// Look up a topic by name.  Returns `None` if not found.
    pub fn get_topic(&self, name: &str) -> Option<Arc<TopicStream<T>>> {
        self.topics.read().unwrap().get(name).cloned()
    }

    /// Return a [`Publisher`] for the named topic, or `None` if the topic
    /// has not been created yet.
    pub fn publisher(&self, topic: &str) -> Option<Publisher<T>> {
        self.get_topic(topic).map(Publisher::new)
    }

    /// Subscribe to the named topic and return a [`Subscriber`] handle, or
    /// `None` if the topic has not been created yet.
    pub fn subscribe(&self, topic: &str) -> Option<Subscriber<T>> {
        self.get_topic(topic).map(Subscriber::new)
    }
}

impl<T: Clone + Send + 'static> Default for BrokerEngine<T> {
    fn default() -> Self {
        Self::new()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Publisher
// ──────────────────────────────────────────────────────────────────────────

/// A cloneable write handle for a [`TopicStream`].
///
/// `Publisher` is cheap to clone (`Arc` increment) — each clone refers to
/// the same underlying stream, so multiple threads can each hold their own
/// `Publisher` and publish concurrently without coordination.
#[derive(Clone)]
pub struct Publisher<T: Clone + Send + 'static> {
    stream: Arc<TopicStream<T>>,
}

impl<T: Clone + Send + 'static> Publisher<T> {
    /// Create a `Publisher` backed by `stream`.
    pub fn new(stream: Arc<TopicStream<T>>) -> Self {
        Publisher { stream }
    }

    /// Publish `payload` to every active subscriber on this topic.
    ///
    /// # Errors
    /// - [`PublishError::NoSubscribers`] — no subscribers registered.
    /// - [`PublishError::SubscriberFull`] — a subscriber's channel is full.
    pub fn publish(&self, payload: T) -> Result<(), PublishError> {
        self.stream.publish(payload).map_err(PublishError::from)
    }

    /// The name of the topic this publisher writes to.
    pub fn topic(&self) -> &str {
        self.stream.name()
    }

    /// The number of active subscribers currently registered on this topic.
    pub fn subscriber_count(&self) -> usize {
        self.stream.subscriber_count()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Subscriber
// ──────────────────────────────────────────────────────────────────────────

/// A receive handle for a [`TopicStream`].
///
/// Every `Subscriber` owns an exclusive ring-buffer channel.  Every message
/// published *after* the subscriber was created is enqueued into that
/// channel, so each subscriber independently receives the full message
/// stream (broadcast / fan-out semantics).
///
/// Multiple OS threads can share a single `Subscriber` via
/// `Arc<Subscriber>` to compete for (work-steal) messages from its channel
/// — a convenient pattern for scaling a single logical consumer across
/// cores.
///
/// Dropping a `Subscriber` automatically deregisters its channel from the
/// topic, stopping further delivery.
pub struct Subscriber<T: Clone + Send + 'static> {
    channel: Arc<MpmcQueue<T>>,
    topic_name: String,
    stream: Arc<TopicStream<T>>,
}

impl<T: Clone + Send + 'static> Subscriber<T> {
    /// Create a `Subscriber` for `stream`, registering a new receive channel.
    pub fn new(stream: Arc<TopicStream<T>>) -> Self {
        let topic_name = stream.name().to_owned();
        let channel = stream.subscribe_channel();
        Subscriber {
            channel,
            topic_name,
            stream,
        }
    }

    /// Non-blocking receive.
    ///
    /// Returns `Some(T)` if a message is available, `None` if the channel
    /// is currently empty.
    pub fn try_recv(&self) -> Option<T> {
        self.channel.pop()
    }

    /// The name of the topic this subscriber reads from.
    pub fn topic(&self) -> &str {
        &self.topic_name
    }
}

impl<T: Clone + Send + 'static> Drop for Subscriber<T> {
    fn drop(&mut self) {
        // Deregister the channel so the publisher stops delivering to it.
        self.stream.unsubscribe_channel(&self.channel);
    }
}
