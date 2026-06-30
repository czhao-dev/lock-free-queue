use oxide_broker::{BoundedQueue, BrokerEngine, MpmcQueue, MutexQueue};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

/// All threads spin on `go` until the main thread releases them, so that the
/// timer starts after thread creation overhead.
fn run_throughput<Q>(producers: usize, consumers: usize, ops_per_producer: i64) -> f64
where
    Q: BoundedQueue<i32> + Send + Sync,
{
    let queue = Q::with_capacity(1024);
    let total = producers as i64 * ops_per_producer;
    let consumed = AtomicI64::new(0);
    let ready = AtomicUsize::new(0);
    let go = AtomicBool::new(false);

    let start = thread::scope(|scope| {
        for _ in 0..producers {
            let queue = &queue;
            let ready = &ready;
            let go = &go;
            scope.spawn(move || {
                ready.fetch_add(1, Ordering::Relaxed);
                while !go.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                for _ in 0..ops_per_producer {
                    while queue.push(0).is_err() {}
                }
            });
        }

        for _ in 0..consumers {
            let queue = &queue;
            let ready = &ready;
            let go = &go;
            let consumed = &consumed;
            scope.spawn(move || {
                ready.fetch_add(1, Ordering::Relaxed);
                while !go.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                while consumed.load(Ordering::Relaxed) < total {
                    if queue.pop().is_some() {
                        consumed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }

        while ready.load(Ordering::Acquire) < (producers + consumers) {
            std::hint::spin_loop();
        }
        let start = Instant::now();
        go.store(true, Ordering::Release);
        start
    });

    let seconds = start.elapsed().as_secs_f64();
    total as f64 / seconds / 1e6
}

/// Measures total messages *delivered* per second across all subscribers.
///
/// Each subscriber gets its own dedicated channel (fan-out), so total
/// deliveries = n_producers × ops_per_producer × n_subscribers.
/// Using `Arc<[u8; 256]>` as the payload type: cloning an `Arc` is a single
/// atomic increment — no heap allocation or byte-copy on the hot path.
fn run_pubsub_throughput(
    n_producers: usize,
    n_subscribers: usize,
    ops_per_producer: i64,
) -> f64 {
    let broker = Arc::new(BrokerEngine::<Arc<[u8; 256]>>::new());
    broker.create_topic("bench", 4096);

    let publisher = broker.publisher("bench").unwrap();
    let subscribers: Vec<_> = (0..n_subscribers)
        .map(|_| broker.subscribe("bench").unwrap())
        .collect();

    let total_publishes = n_producers as i64 * ops_per_producer;
    let total_delivers = total_publishes * n_subscribers as i64;

    let delivered = AtomicI64::new(0);
    let ready = AtomicUsize::new(0);
    let go = AtomicBool::new(false);

    // Pre-allocate a shared payload so every publish is truly zero-copy.
    let payload: Arc<[u8; 256]> = Arc::new([0u8; 256]);

    let start = thread::scope(|scope| {
        for _ in 0..n_producers {
            let publisher = publisher.clone();
            let payload = Arc::clone(&payload);
            let ready = &ready;
            let go = &go;
            scope.spawn(move || {
                ready.fetch_add(1, Ordering::Relaxed);
                while !go.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                for _ in 0..ops_per_producer {
                    while publisher.publish(Arc::clone(&payload)).is_err() {
                        std::hint::spin_loop();
                    }
                }
            });
        }

        for sub in &subscribers {
            let ready = &ready;
            let go = &go;
            let delivered = &delivered;
            scope.spawn(move || {
                ready.fetch_add(1, Ordering::Relaxed);
                while !go.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                while delivered.load(Ordering::Relaxed) < total_delivers {
                    if sub.try_recv().is_some() {
                        delivered.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }

        while ready.load(Ordering::Acquire) < n_producers + n_subscribers {
            std::hint::spin_loop();
        }
        let start = Instant::now();
        go.store(true, Ordering::Release);
        start
    });

    let seconds = start.elapsed().as_secs_f64();
    total_delivers as f64 / seconds / 1e6
}

fn main() {
    println!(
        "# hardware_concurrency={}",
        thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
    );
    println!("threads,queue,total_ops,seconds,mops_per_sec");

    const TOTAL: i64 = 10_000_000;

    for n in [1usize, 2, 4, 8] {
        let ops = TOTAL / n as i64;
        let total = n as i64 * ops;

        let lfq_mops = run_throughput::<MpmcQueue<i32>>(n, n, ops);
        let mtx_mops = run_throughput::<MutexQueue<i32>>(n, n, ops);

        println!(
            "{}+{},lfqueue,{},{:.3},{:.1}",
            n,
            n,
            total,
            total as f64 / (lfq_mops * 1e6),
            lfq_mops
        );
        println!(
            "{}+{},mutex,{},{:.3},{:.1}",
            n,
            n,
            total,
            total as f64 / (mtx_mops * 1e6),
            mtx_mops
        );
    }

    // ── Pub/Sub fan-out benchmark ─────────────────────────────────────────
    // Reports total messages *delivered* per second (publish_rate × n_subscribers).
    // Using Arc<[u8; 256]> payloads to demonstrate zero-copy fan-out: the
    // Arc pointer is cloned per subscriber, but the 256 payload bytes are never
    // copied.
    println!();
    println!("# pub/sub fan-out (Arc<[u8; 256]> payloads)");
    println!("producers,subscribers,total_delivered,seconds,mops_delivered_per_sec");

    const PUBSUB_OPS_PER_PRODUCER: i64 = 2_000_000;

    for (n_prod, n_subs) in [(1, 1), (4, 1), (1, 4), (4, 4)] {
        let mops = run_pubsub_throughput(n_prod, n_subs, PUBSUB_OPS_PER_PRODUCER);
        let total_delivered = n_prod as i64 * PUBSUB_OPS_PER_PRODUCER * n_subs as i64;
        println!(
            "{},{},{},{:.3},{:.1}",
            n_prod,
            n_subs,
            total_delivered,
            total_delivered as f64 / (mops * 1e6),
            mops
        );
    }
}
