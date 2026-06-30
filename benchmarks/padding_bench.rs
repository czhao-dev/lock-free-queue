use oxide_broker::MpmcQueue;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Deliberately unpadded MPMC queue — head and tail share cache lines with
// each other and with buffer metadata, causing false sharing between
// producers and consumers. Kept here (not in the library) because it is
// only useful for demonstrating the performance cost of missing padding.
// ---------------------------------------------------------------------------
struct Slot<T> {
    sequence: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}

struct NoPadMpmcQueue<T> {
    capacity: usize,
    mask: usize,
    buffer: Box<[Slot<T>]>,
    // NOT cache-line padded — head and tail sit adjacent to each other and
    // to buffer's metadata.
    head: AtomicUsize,
    tail: AtomicUsize,
}

unsafe impl<T: Send> Send for NoPadMpmcQueue<T> {}
unsafe impl<T: Send> Sync for NoPadMpmcQueue<T> {}

impl<T> NoPadMpmcQueue<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity != 0 && (capacity & (capacity - 1)) == 0);
        let buffer: Box<[Slot<T>]> = (0..capacity)
            .map(|i| Slot {
                sequence: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            capacity,
            mask: capacity - 1,
            buffer,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    fn push(&self, value: T) -> Result<(), T> {
        let mut pos = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.buffer[pos & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - pos as isize;
            if diff == 0 {
                if self
                    .tail
                    .compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    unsafe {
                        (*slot.data.get()).write(value);
                    }
                    slot.sequence.store(pos + 1, Ordering::Release);
                    return Ok(());
                }
            } else if diff < 0 {
                return Err(value);
            } else {
                pos = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    fn pop(&self) -> Option<T> {
        let mut pos = self.head.load(Ordering::Relaxed);
        loop {
            let slot = &self.buffer[pos & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - (pos + 1) as isize;
            if diff == 0 {
                if self
                    .head
                    .compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    let value = unsafe { (*slot.data.get()).assume_init_read() };
                    slot.sequence.store(pos + self.capacity, Ordering::Release);
                    return Some(value);
                }
            } else if diff < 0 {
                return None;
            } else {
                pos = self.head.load(Ordering::Relaxed);
            }
        }
    }
}

impl<T> Drop for NoPadMpmcQueue<T> {
    fn drop(&mut self) {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        let mut pos = head;
        while pos != tail {
            let slot = &self.buffer[pos & self.mask];
            if slot.sequence.load(Ordering::Relaxed) == pos + 1 {
                unsafe {
                    (*slot.data.get()).assume_init_drop();
                }
            }
            pos += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Benchmark driver
// ---------------------------------------------------------------------------
trait Pushable<T> {
    fn try_push(&self, value: T) -> Result<(), T>;
    fn try_pop(&self) -> Option<T>;
}

impl<T> Pushable<T> for NoPadMpmcQueue<T> {
    fn try_push(&self, value: T) -> Result<(), T> {
        self.push(value)
    }
    fn try_pop(&self) -> Option<T> {
        self.pop()
    }
}

impl<T> Pushable<T> for MpmcQueue<T> {
    fn try_push(&self, value: T) -> Result<(), T> {
        self.push(value)
    }
    fn try_pop(&self) -> Option<T> {
        self.pop()
    }
}

fn run_throughput<Q>(queue: Q, producers: usize, consumers: usize, ops_per_producer: i64) -> f64
where
    Q: Pushable<i32> + Send + Sync,
{
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
                    while queue.try_push(0).is_err() {}
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
                    if queue.try_pop().is_some() {
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

fn main() {
    const P: usize = 4;
    const C: usize = 4;
    const TOTAL: i64 = 10_000_000;
    let ops = TOTAL / P as i64;
    let total = P as i64 * ops;

    let nopad_mops = run_throughput(NoPadMpmcQueue::<i32>::new(1024), P, C, ops);
    let pad_mops = run_throughput(MpmcQueue::<i32>::new(1024), P, C, ops);

    println!("configuration,total_ops,seconds,mops_per_sec");
    println!(
        "without_padding,{},{:.3},{:.1}",
        total,
        total as f64 / (nopad_mops * 1e6),
        nopad_mops
    );
    println!(
        "with_padding,{},{:.3},{:.1}",
        total,
        total as f64 / (pad_mops * 1e6),
        pad_mops
    );
}
