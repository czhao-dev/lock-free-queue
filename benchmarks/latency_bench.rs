use oxide_broker::{BoundedQueue, BrokerEngine, MpmcQueue, MutexQueue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

/// Payload carries the nanosecond timestamp of when it was pushed so that
/// the consumer can measure end-to-end queue latency for each item.
#[derive(Clone, Copy)]
struct Item {
    push_ns: i64,
}

fn now_ns(epoch: Instant) -> i64 {
    epoch.elapsed().as_nanos() as i64
}

fn run_latency<Q>(label: &str, n_samples: usize, queue_capacity: usize)
where
    Q: BoundedQueue<Item> + Send + Sync,
{
    let queue = Q::with_capacity(queue_capacity);
    let epoch = Instant::now();
    let consumed = AtomicUsize::new(0);

    let latencies = thread::scope(|scope| {
        let queue_ref = &queue;
        let consumed_ref = &consumed;
        let consumer = scope.spawn(move || {
            let mut latencies = Vec::with_capacity(n_samples);
            while consumed_ref.load(Ordering::Relaxed) < n_samples {
                if let Some(item) = queue_ref.pop() {
                    let lat = now_ns(epoch) - item.push_ns;
                    latencies.push(if lat > 0 { lat } else { 0 });
                    consumed_ref.fetch_add(1, Ordering::Relaxed);
                }
            }
            latencies
        });

        for _ in 0..n_samples {
            let mut item = Item { push_ns: now_ns(epoch) };
            while let Err(returned) = queue.push(item) {
                item = returned;
                item.push_ns = now_ns(epoch);
            }
        }

        consumer.join().unwrap()
    });

    let mut latencies = latencies;
    latencies.sort_unstable();
    let pct = |p: f64| -> i64 {
        let mut idx = (latencies.len() as f64 * p) as usize;
        if idx >= latencies.len() {
            idx = latencies.len() - 1;
        }
        latencies[idx]
    };

    println!("{},{},{},{}", label, pct(0.50), pct(0.99), pct(0.999));
}

/// Measures end-to-end pub/sub latency: time from `Publisher::publish` to
/// `Subscriber::try_recv` returning `Some(item)` on a 1-producer/1-subscriber
/// topology.
fn run_pubsub_latency(label: &str, n_samples: usize, queue_capacity: usize) {
    let broker = BrokerEngine::<Item>::new();
    broker.create_topic("bench", queue_capacity);

    let publisher = broker.publisher("bench").unwrap();
    let subscriber = broker.subscribe("bench").unwrap();
    let epoch = Instant::now();
    let consumed = AtomicUsize::new(0);

    let latencies = thread::scope(|scope| {
        let subscriber = &subscriber;
        let consumed_ref = &consumed;
        let consumer = scope.spawn(move || {
            let mut latencies = Vec::with_capacity(n_samples);
            while consumed_ref.load(Ordering::Relaxed) < n_samples {
                if let Some(item) = subscriber.try_recv() {
                    let lat = now_ns(epoch) - item.push_ns;
                    latencies.push(if lat > 0 { lat } else { 0 });
                    consumed_ref.fetch_add(1, Ordering::Relaxed);
                }
            }
            latencies
        });

        for _ in 0..n_samples {
            let mut item = Item { push_ns: now_ns(epoch) };
            loop {
                match publisher.publish(item) {
                    Ok(()) => break,
                    Err(_) => item = Item { push_ns: now_ns(epoch) },
                }
            }
        }

        consumer.join().unwrap()
    });

    let mut latencies = latencies;
    latencies.sort_unstable();
    let pct = |p: f64| -> i64 {
        let idx = ((latencies.len() as f64 * p) as usize).min(latencies.len() - 1);
        latencies[idx]
    };

    println!("{},{},{},{}", label, pct(0.50), pct(0.99), pct(0.999));
}

fn main() {
    const SAMPLES: usize = 100_000;
    const CAPACITY: usize = 64;

    println!("queue,p50_ns,p99_ns,p999_ns");
    run_latency::<MpmcQueue<Item>>("mpmc_raw", SAMPLES, CAPACITY);
    run_latency::<MutexQueue<Item>>("mutex_raw", SAMPLES, CAPACITY);

    // ── Pub/Sub end-to-end latency ──────────────────────────────────────
    // Measures the round-trip time from Publisher::publish() to
    // Subscriber::try_recv() returning Some(item).
    println!();
    println!("queue,p50_ns,p99_ns,p999_ns");
    run_pubsub_latency("oxide_broker", SAMPLES, CAPACITY);
}
