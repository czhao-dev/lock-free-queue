use oxide_broker::{BoundedQueue, BrokerEngine, Message, MpmcQueue, MutexQueue, PublishError, Subscriber};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex};
use std::thread;

// -----------------------------------------------------------------------
// Single-thread functional tests
// -----------------------------------------------------------------------

fn empty_and_full<Q: BoundedQueue<i32>>() {
    let queue = Q::with_capacity(4);

    assert_eq!(queue.capacity(), 4);
    assert!(queue.pop().is_none());

    assert!(queue.push(1).is_ok());
    assert!(queue.push(2).is_ok());
    assert!(queue.push(3).is_ok());
    assert!(queue.push(4).is_ok());
    assert!(queue.push(5).is_err());

    assert_eq!(queue.pop(), Some(1));
    assert_eq!(queue.pop(), Some(2));
    assert_eq!(queue.pop(), Some(3));
    assert_eq!(queue.pop(), Some(4));
    assert!(queue.pop().is_none());
}

fn wraparound_order<Q: BoundedQueue<i32>>() {
    let queue = Q::with_capacity(2);

    assert!(queue.push(10).is_ok());
    assert!(queue.push(20).is_ok());
    assert!(queue.push(30).is_err());

    assert_eq!(queue.pop(), Some(10));
    assert!(queue.push(30).is_ok());

    assert_eq!(queue.pop(), Some(20));
    assert_eq!(queue.pop(), Some(30));
    assert!(queue.pop().is_none());
}

// MutexQueue-specific: exercises an odd capacity (3) that MpmcQueue would
// reject at construction since it requires a power of two.
fn exact_capacity<Q: BoundedQueue<i32>>() {
    let queue = Q::with_capacity(3);

    assert_eq!(queue.capacity(), 3);
    assert!(queue.push(1).is_ok());
    assert!(queue.push(2).is_ok());
    assert!(queue.push(3).is_ok());
    assert!(queue.push(4).is_err());

    assert_eq!(queue.pop(), Some(1));
    assert!(queue.push(4).is_ok());

    assert_eq!(queue.pop(), Some(2));
    assert_eq!(queue.pop(), Some(3));
    assert_eq!(queue.pop(), Some(4));
    assert!(queue.pop().is_none());
}

fn move_only_values<Q: BoundedQueue<Box<i32>>>() {
    let queue = Q::with_capacity(2);

    assert!(queue.push(Box::new(42)).is_ok());
    let value = queue.pop();
    assert_eq!(value, Some(Box::new(42)));
    assert!(queue.pop().is_none());
}

#[test]
fn mpmc_empty_and_full() {
    empty_and_full::<MpmcQueue<i32>>();
}

#[test]
fn mutex_empty_and_full() {
    empty_and_full::<MutexQueue<i32>>();
}

#[test]
fn mpmc_wraparound_order() {
    wraparound_order::<MpmcQueue<i32>>();
}

#[test]
fn mutex_wraparound_order() {
    wraparound_order::<MutexQueue<i32>>();
}

#[test]
fn mutex_exact_capacity() {
    exact_capacity::<MutexQueue<i32>>();
}

#[test]
fn mpmc_move_only_values() {
    move_only_values::<MpmcQueue<Box<i32>>>();
}

#[test]
fn mutex_move_only_values() {
    move_only_values::<MutexQueue<Box<i32>>>();
}

#[test]
#[should_panic(expected = "non-zero power of two")]
fn mpmc_rejects_zero_capacity() {
    MpmcQueue::<i32>::new(0);
}

#[test]
#[should_panic(expected = "non-zero power of two")]
fn mpmc_rejects_non_power_of_two() {
    MpmcQueue::<i32>::new(3);
}

#[test]
#[should_panic(expected = "non-zero")]
fn mutex_rejects_zero_capacity() {
    MutexQueue::<i32>::new(0);
}

// -----------------------------------------------------------------------
// Concurrent stress tests
// -----------------------------------------------------------------------

/// `producers` threads each push a unique sequence of values; `consumers`
/// threads pop until all `producers * values_per_producer` items are
/// received. Verifies every value pushed was received exactly once (no
/// duplicates, no drops) — this catches the most common class of
/// lock-free bugs.
fn run_stress<Q>(capacity: usize, producers: usize, consumers: usize, values_per_producer: i32)
where
    Q: BoundedQueue<i32> + Send + Sync,
{
    let queue = Q::with_capacity(capacity);
    let total = producers as i32 * values_per_producer;

    let consumed = AtomicUsize::new(0);
    let received: Mutex<Vec<i32>> = Mutex::new(Vec::with_capacity(total as usize));

    thread::scope(|scope| {
        for p in 0..producers {
            let queue = &queue;
            scope.spawn(move || {
                let start = p as i32 * values_per_producer;
                let end = start + values_per_producer;
                for v in start..end {
                    let mut pending = v;
                    while let Err(returned) = queue.push(pending) {
                        pending = returned;
                    }
                }
            });
        }

        for _ in 0..consumers {
            let queue = &queue;
            let consumed = &consumed;
            let received = &received;
            scope.spawn(move || {
                let mut local = Vec::new();
                while consumed.load(Ordering::Relaxed) < total as usize {
                    if let Some(value) = queue.pop() {
                        local.push(value);
                        consumed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                received.lock().unwrap().extend(local);
            });
        }
    });

    let mut received = received.into_inner().unwrap();
    assert_eq!(received.len(), total as usize);
    received.sort_unstable();
    for (i, &value) in received.iter().enumerate() {
        assert_eq!(value, i as i32);
    }
}

const VPROD: i32 = 10_000;

#[test]
fn mpmc_stress_1p_1c() {
    run_stress::<MpmcQueue<i32>>(8, 1, 1, VPROD);
}

#[test]
fn mutex_stress_1p_1c() {
    run_stress::<MutexQueue<i32>>(8, 1, 1, VPROD);
}

#[test]
fn mpmc_stress_2p_2c() {
    run_stress::<MpmcQueue<i32>>(8, 2, 2, VPROD);
}

#[test]
fn mutex_stress_2p_2c() {
    run_stress::<MutexQueue<i32>>(8, 2, 2, VPROD);
}

#[test]
fn mpmc_stress_4p_4c() {
    run_stress::<MpmcQueue<i32>>(64, 4, 4, VPROD);
}

#[test]
fn mutex_stress_4p_4c() {
    run_stress::<MutexQueue<i32>>(64, 4, 4, VPROD);
}

#[test]
fn mpmc_stress_8p_8c() {
    run_stress::<MpmcQueue<i32>>(64, 8, 8, VPROD);
}

#[test]
fn mutex_stress_8p_8c() {
    run_stress::<MutexQueue<i32>>(64, 8, 8, VPROD);
}

#[test]
fn mpmc_stress_many_consumers() {
    // One producer, many consumers.
    run_stress::<MpmcQueue<i32>>(8, 1, 4, VPROD);
}

#[test]
fn mutex_stress_many_consumers() {
    run_stress::<MutexQueue<i32>>(8, 1, 4, VPROD);
}

#[test]
fn mpmc_stress_many_producers() {
    // Many producers, one consumer.
    run_stress::<MpmcQueue<i32>>(8, 4, 1, VPROD);
}

#[test]
fn mutex_stress_many_producers() {
    run_stress::<MutexQueue<i32>>(8, 4, 1, VPROD);
}

#[test]
fn mpmc_stress_small_capacity_high_contention() {
    // Small capacity forces wraparound and high contention.
    for _ in 0..5 {
        run_stress::<MpmcQueue<i32>>(2, 2, 2, 1000);
    }
}

#[test]
fn mutex_stress_small_capacity_high_contention() {
    for _ in 0..5 {
        run_stress::<MutexQueue<i32>>(2, 2, 2, 1000);
    }
}

// -----------------------------------------------------------------------
// Pub/sub correctness tests
// -----------------------------------------------------------------------

fn make_broker(capacity: usize) -> BrokerEngine<i32> {
    let broker = BrokerEngine::new();
    broker.create_topic("test", capacity);
    broker
}

#[test]
fn pubsub_single_subscriber_basic() {
    let broker = make_broker(16);
    let publisher = broker.publisher("test").unwrap();
    let subscriber = broker.subscribe("test").unwrap();

    publisher.publish(1).unwrap();
    publisher.publish(2).unwrap();
    publisher.publish(3).unwrap();

    assert_eq!(subscriber.try_recv(), Some(1));
    assert_eq!(subscriber.try_recv(), Some(2));
    assert_eq!(subscriber.try_recv(), Some(3));
    assert_eq!(subscriber.try_recv(), None);
}

#[test]
fn pubsub_no_subscribers_returns_error() {
    let broker = make_broker(16);
    let publisher = broker.publisher("test").unwrap();
    assert_eq!(publisher.publish(1), Err(PublishError::NoSubscribers));
}

#[test]
fn pubsub_fan_out_three_subscribers() {
    const N: i32 = 100;
    let broker = make_broker(256);
    let publisher = broker.publisher("test").unwrap();
    let sub_a = broker.subscribe("test").unwrap();
    let sub_b = broker.subscribe("test").unwrap();
    let sub_c = broker.subscribe("test").unwrap();

    for i in 0..N {
        while publisher.publish(i).is_err() {}
    }

    let drain = |sub: &Subscriber<i32>| -> Vec<i32> {
        let mut v = Vec::new();
        while let Some(x) = sub.try_recv() {
            v.push(x);
        }
        v
    };

    let a = drain(&sub_a);
    let b = drain(&sub_b);
    let c = drain(&sub_c);

    assert_eq!(a.len(), N as usize, "subscriber A missed messages");
    assert_eq!(b.len(), N as usize, "subscriber B missed messages");
    assert_eq!(c.len(), N as usize, "subscriber C missed messages");
    // All three receive the same ordered sequence.
    let expected: Vec<i32> = (0..N).collect();
    assert_eq!(a, expected);
    assert_eq!(b, expected);
    assert_eq!(c, expected);
}

#[test]
fn pubsub_topic_isolation() {
    let broker: BrokerEngine<i32> = BrokerEngine::new();
    broker.create_topic("alpha", 64);
    broker.create_topic("beta", 64);

    let pub_alpha = broker.publisher("alpha").unwrap();
    let pub_beta = broker.publisher("beta").unwrap();
    let sub_alpha = broker.subscribe("alpha").unwrap();
    let sub_beta = broker.subscribe("beta").unwrap();

    pub_alpha.publish(1).unwrap();
    pub_alpha.publish(2).unwrap();
    pub_beta.publish(10).unwrap();
    pub_beta.publish(20).unwrap();

    // alpha messages do not leak into beta and vice-versa.
    assert_eq!(sub_alpha.try_recv(), Some(1));
    assert_eq!(sub_alpha.try_recv(), Some(2));
    assert_eq!(sub_alpha.try_recv(), None);

    assert_eq!(sub_beta.try_recv(), Some(10));
    assert_eq!(sub_beta.try_recv(), Some(20));
    assert_eq!(sub_beta.try_recv(), None);
}

#[test]
fn pubsub_subscriber_drop_unregisters() {
    let broker = make_broker(16);
    let publisher = broker.publisher("test").unwrap();

    {
        let _sub = broker.subscribe("test").unwrap();
        assert_eq!(publisher.subscriber_count(), 1);
    } // _sub dropped here — should deregister itself

    assert_eq!(publisher.subscriber_count(), 0);
    assert_eq!(publisher.publish(1), Err(PublishError::NoSubscribers));
}

#[test]
fn pubsub_concurrent_publishers_fan_out() {
    const N_PRODUCERS: usize = 4;
    const OPS_PER_PRODUCER: i32 = 1_000;
    const N_SUBSCRIBERS: usize = 3;
    let total = N_PRODUCERS as i32 * OPS_PER_PRODUCER;

    let broker = BrokerEngine::<i32>::new();
    broker.create_topic("multi", 4096);

    let publisher = broker.publisher("multi").unwrap();
    let subscribers: Vec<Subscriber<i32>> = (0..N_SUBSCRIBERS)
        .map(|_| broker.subscribe("multi").unwrap())
        .collect();

    thread::scope(|scope| {
        for p in 0..N_PRODUCERS {
            let publisher = publisher.clone();
            scope.spawn(move || {
                let start = p as i32 * OPS_PER_PRODUCER;
                for v in start..start + OPS_PER_PRODUCER {
                    while publisher.publish(v).is_err() {}
                }
            });
        }
    });

    // Each subscriber must receive all `total` distinct values exactly once.
    for sub in &subscribers {
        let mut received = Vec::new();
        while let Some(v) = sub.try_recv() {
            received.push(v);
        }
        assert_eq!(received.len(), total as usize);
        received.sort_unstable();
        assert_eq!(received, (0..total).collect::<Vec<_>>());
    }
}

#[test]
fn pubsub_unknown_topic_returns_none() {
    let broker: BrokerEngine<i32> = BrokerEngine::new();
    assert!(broker.publisher("nonexistent").is_none());
    assert!(broker.subscribe("nonexistent").is_none());
}

#[test]
fn pubsub_message_envelope() {
    let broker: BrokerEngine<Message<&str>> = BrokerEngine::new();
    broker.create_topic("msgs", 16);

    let publisher = broker.publisher("msgs").unwrap();
    let subscriber = broker.subscribe("msgs").unwrap();

    publisher.publish(Message::new("hello", 1)).unwrap();
    publisher.publish(Message::new("world", 2)).unwrap();

    let first = subscriber.try_recv().unwrap();
    assert_eq!(first.payload, "hello");
    assert_eq!(first.sequence, 1);

    let second = subscriber.try_recv().unwrap();
    assert_eq!(second.payload, "world");
    assert_eq!(second.sequence, 2);

    assert!(subscriber.try_recv().is_none());
}
