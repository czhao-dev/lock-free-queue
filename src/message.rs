/// A typed message envelope that pairs a `payload` with a per-topic
/// monotonically increasing `sequence` number.
///
/// Use `Message<Payload>` as the type parameter `T` when you need
/// explicit ordering guarantees or gap detection on the consumer side.
///
/// # Example
///
/// ```rust
/// use oxide_broker::{BrokerEngine, Message};
///
/// let broker: BrokerEngine<Message<u64>> = BrokerEngine::new();
/// broker.create_topic("events", 1024);
///
/// let publisher = broker.publisher("events").unwrap();
/// let subscriber = broker.subscribe("events").unwrap();
///
/// publisher.publish(Message::new(42, 1)).unwrap();
/// let msg = subscriber.try_recv().unwrap();
/// assert_eq!(msg.payload, 42);
/// assert_eq!(msg.sequence, 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<T> {
    /// The application payload.
    pub payload: T,
    /// Monotonically increasing sequence number assigned by the publisher.
    pub sequence: u64,
}

impl<T> Message<T> {
    /// Wrap `payload` with the given `sequence` number.
    #[inline]
    pub fn new(payload: T, sequence: u64) -> Self {
        Message { payload, sequence }
    }
}
