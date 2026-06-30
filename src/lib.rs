//! `oxide_broker` — a high-throughput, in-memory pub/sub message engine
//! built on a lock-free MPMC ring buffer core.
//!
//! # Architecture
//!
//! | Type | Role |
//! |------|------|
//! | [`BrokerEngine`] | Top-level factory and topic registry |
//! | [`TopicStream`] | Named, single-type broadcast channel |
//! | [`Publisher`] | Cloneable write handle (multiple producers supported) |
//! | [`Subscriber`] | Exclusive receive handle; auto-deregisters on drop |
//! | [`Message`] | Optional metadata envelope (payload + sequence number) |
//!
//! # Quick Start
//!
//! ```rust
//! use oxide_broker::BrokerEngine;
//!
//! let broker: BrokerEngine<u64> = BrokerEngine::new();
//! broker.create_topic("events", 1024);
//!
//! let publisher = broker.publisher("events").unwrap();
//! let subscriber = broker.subscribe("events").unwrap();
//!
//! publisher.publish(42).unwrap();
//! assert_eq!(subscriber.try_recv(), Some(42));
//! ```
//!
//! # Zero-Copy Fan-Out
//!
//! Storing `Arc<Payload>` as `T` makes publishing O(1) per subscriber — only
//! the thin `Arc` pointer is cloned, never the underlying payload bytes:
//!
//! ```rust
//! use std::sync::Arc;
//! use oxide_broker::BrokerEngine;
//!
//! let broker: BrokerEngine<Arc<[u8; 256]>> = BrokerEngine::new();
//! broker.create_topic("telemetry", 4096);
//!
//! let publisher = broker.publisher("telemetry").unwrap();
//! let sub_a = broker.subscribe("telemetry").unwrap();
//! let sub_b = broker.subscribe("telemetry").unwrap();
//!
//! let payload = Arc::new([0u8; 256]);
//! publisher.publish(Arc::clone(&payload)).unwrap();
//!
//! // Both subscribers receive the same Arc — zero bytes copied.
//! assert!(sub_a.try_recv().is_some());
//! assert!(sub_b.try_recv().is_some());
//! ```
//!
//! # Low-Level Primitives
//!
//! The raw lock-free ring buffer is also exported for direct use:
//! - [`MpmcQueue`] — bounded, lock-free MPMC queue
//! - [`MutexQueue`] — mutex-backed baseline queue
//! - [`BoundedQueue`] — shared trait for generic testing/benchmarking

mod broadcast;
mod cache_padded;
mod mpmc_queue;
mod mutex_queue;

pub mod broker;
pub mod message;
pub mod topic;

pub use broker::{BrokerEngine, PublishError, Publisher, Subscriber};
pub use message::Message;
pub use mpmc_queue::MpmcQueue;
pub use mutex_queue::MutexQueue;
pub use topic::TopicStream;

/// Common interface shared by [`MpmcQueue`] and [`MutexQueue`], used to
/// write tests and benchmarks generically over both implementations.
pub trait BoundedQueue<T> {
    fn with_capacity(capacity: usize) -> Self;
    fn push(&self, value: T) -> Result<(), T>;
    fn pop(&self) -> Option<T>;
    fn capacity(&self) -> usize;
}

impl<T> BoundedQueue<T> for MpmcQueue<T> {
    fn with_capacity(capacity: usize) -> Self {
        MpmcQueue::new(capacity)
    }
    fn push(&self, value: T) -> Result<(), T> {
        MpmcQueue::push(self, value)
    }
    fn pop(&self) -> Option<T> {
        MpmcQueue::pop(self)
    }
    fn capacity(&self) -> usize {
        MpmcQueue::capacity(self)
    }
}

impl<T> BoundedQueue<T> for MutexQueue<T> {
    fn with_capacity(capacity: usize) -> Self {
        MutexQueue::new(capacity)
    }
    fn push(&self, value: T) -> Result<(), T> {
        MutexQueue::push(self, value)
    }
    fn pop(&self) -> Option<T> {
        MutexQueue::pop(self)
    }
    fn capacity(&self) -> usize {
        MutexQueue::capacity(self)
    }
}
