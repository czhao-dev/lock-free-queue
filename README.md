# lfqueue — Lock-Free Multi-Producer Multi-Consumer Queue

![C++](https://img.shields.io/badge/C%2B%2B-17-00599C.svg?logo=cplusplus)
![CMake](https://img.shields.io/badge/CMake-3.16%2B-064F8C.svg?logo=cmake)
![ThreadSanitizer](https://img.shields.io/badge/ThreadSanitizer-clean-brightgreen.svg)
![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

A bounded, lock-free MPMC (multi-producer multi-consumer) queue implemented in
C++ using atomic operations and explicit memory ordering. Benchmarked against a
mutex-protected queue across varying thread counts and contention levels, with
discussion of the correctness reasoning required for lock-free design.

---

## Overview

Most concurrent data structures are protected by a mutex: a thread acquires the
lock, performs its operation, and releases it. This is simple to reason about
but has a fundamental cost — under contention, threads block and the OS
scheduler must context-switch between them, and even without contention,
acquiring and releasing a lock involves atomic operations and potential cache
coherency traffic.

A **lock-free** data structure guarantees that at least one thread makes
progress in a bounded number of steps, regardless of what other threads are
doing — including if another thread is suspended mid-operation. No thread ever
blocks waiting for another. This is achieved using **atomic compare-and-swap
(CAS)** operations instead of locks, combined with careful **memory ordering**
to ensure operations on different cores become visible in a consistent order.

`lfqueue` implements a bounded ring-buffer queue supporting multiple concurrent
producers (`push`) and multiple concurrent consumers (`pop`), with no mutex
anywhere in the implementation.

### Why this problem

Lock-free programming is one of the most rigorous correctness exercises in
systems programming — bugs are subtle, non-deterministic, and often invisible
in testing but catastrophic in production under specific timing conditions.
Building one correctly (and being able to explain *why* it's correct) signals a
level of comfort with concurrent correctness reasoning that few candidates can
demonstrate. It is also a direct, practical complement to the memory allocator
project: high-performance allocators and runtime systems use lock-free queues
for work-stealing schedulers, thread pools, and inter-thread message passing.

---

## Architecture

```
                  head (next slot to pop)         tail (next slot to push)
                       │                                │
                       ▼                                ▼
        ┌────┬────┬────┬────┬────┬────┬────┬────┬────┐
slots:  │ E  │ E  │ F  │ F  │ F  │ E  │ E  │ E  │ E  │   E = empty, F = full
        └────┴────┴────┴────┴────┴────┴────┴────┴────┘
          0    1    2    3    4    5    6    7    8

Producers CAS-advance tail and write into the claimed slot.
Consumers CAS-advance head and read from the claimed slot.
Per-slot sequence numbers (not shown) coordinate producer/consumer handoff
without a separate "full/empty" flag race.
```

The queue is a fixed-size array of slots (capacity is a power of two for fast
modular indexing via bitmasking). Each slot carries a **sequence number** in
addition to its data — this is the key mechanism that makes the design correct
(see below), based on the design used in Dmitry Vyukov's bounded MPMC queue.

---

## Key Concepts

### 1. Compare-and-Swap (CAS)

CAS is the fundamental primitive: `compare_exchange(expected, desired)` atomically
checks whether a memory location currently holds `expected`; if so, it writes
`desired` and returns true; otherwise it returns false and updates `expected`
with the actual current value.

```cpp
std::atomic<size_t> tail;

size_t pos = tail.load(std::memory_order_relaxed);
while (!tail.compare_exchange_weak(pos, pos + 1,
                                    std::memory_order_relaxed)) {
    // pos was updated to the current value by compare_exchange_weak; retry
}
// This thread now "owns" slot `pos` for writing
```

The loop is the standard CAS retry pattern: if another thread won the race and
advanced `tail` first, this thread's CAS fails, `pos` is refreshed to the new
value, and it retries with the new claim. No thread ever blocks — a failed CAS
means "someone else made progress," which satisfies the lock-free progress
guarantee.

`compare_exchange_weak` vs `compare_exchange_strong`: `weak` may fail
spuriously (return false even when the value matches) on some architectures due
to how LL/SC (load-linked/store-conditional) instructions work, but is cheaper
in a retry loop. `strong` never fails spuriously but may cost more per call.
Inside a loop, `weak` is preferred.

### 2. The ABA Problem and Per-Slot Sequence Numbers

A naive lock-free queue using only head/tail indices is vulnerable to the **ABA
problem**: thread A reads a value `X` at location L, gets preempted; thread B
changes L to `Y` and back to `X`; thread A resumes and its CAS succeeds because
L still equals `X` — but the underlying state has changed in ways thread A's
logic didn't account for.

`lfqueue` avoids ABA by giving each slot a **sequence number** that increments
every time the slot is reused:

```cpp
struct Slot {
    std::atomic<size_t> sequence;
    T data;
};
```

A producer claiming slot `i` for the `n`-th time around the ring expects
`sequence == n`. After writing, it sets `sequence = n + 1`, signaling to a
consumer that the slot is ready to read. A consumer expects `sequence == n + 1`
before reading, and sets `sequence = n + capacity` after consuming, signaling
the slot is ready for the *next* producer pass. This turns a 2-state
full/empty flag into a monotonically increasing counter, eliminating the ABA
ambiguity entirely — the sequence number encodes *which lap* around the ring
the slot is on, not just its current state.

### 3. Memory Ordering

C++'s `std::memory_order` controls what guarantees an atomic operation makes
about the visibility and ordering of *other* memory operations around it — this
is where most lock-free bugs live, because the code can be "correct" on x86
(which has strong default ordering) and fail on ARM (which does not).

| Order               | Guarantee                                                          | Used for                          |
|---------------------|---------------------------------------------------------------------|------------------------------------|
| `relaxed`           | Atomicity only — no ordering with other memory ops                 | Counters with no dependent data    |
| `acquire`           | No subsequent read/write can be reordered *before* this load        | Reading a slot's sequence before reading its data |
| `release`           | No preceding read/write can be reordered *after* this store         | Publishing a slot's data before updating its sequence |
| `acq_rel`           | Both acquire and release semantics                                   | CAS operations that both read and write |
| `seq_cst`           | Total global ordering across all threads (default, most expensive) | Used only where reasoning requires it |

The critical pattern in `lfqueue` is **release-acquire pairing**: a producer
writes the slot's data, then performs a `release` store to the sequence number.
A consumer performs an `acquire` load of the sequence number, then reads the
data. The acquire/release pair guarantees that if the consumer observes the
updated sequence number, it is also guaranteed to observe the data write that
happened-before it — without this pairing, the consumer could read stale or
partially-written data even though the sequence number appears updated, because
without ordering constraints, the compiler or CPU may reorder the two stores.

```cpp
// Producer
slot.data = std::move(value);                                    // (1) plain write
slot.sequence.store(pos + 1, std::memory_order_release);         // (2) release

// Consumer
size_t seq = slot.sequence.load(std::memory_order_acquire);      // (3) acquire
if (seq == expected) {
    T value = std::move(slot.data);                              // (4) plain read
}
```

The release at (2) and acquire at (3) form a synchronizes-with relationship:
if (3) observes the value written by (2), then (1) is guaranteed visible to (4).
This is the mechanism — not a global lock — that makes the handoff safe.

### 4. False Sharing and Cache Line Padding

If `head` and `tail` (each updated by different sets of threads — consumers and
producers respectively) sit on the same cache line, every producer update
invalidates the cache line for consumers and vice versa, even though they touch
logically independent data. This is **false sharing**, and it can degrade
throughput by an order of magnitude under contention.

`lfqueue` pads `head` and `tail` to separate 64-byte cache lines:

```cpp
struct alignas(64) AlignedAtomic {
    std::atomic<size_t> value;
    char padding[64 - sizeof(std::atomic<size_t>)];
};
```

This is a small code change with a measurable effect — the benchmarks section
quantifies it.

---

## Design Decisions and Tradeoffs

**Bounded vs unbounded queue**

`lfqueue` is bounded (fixed capacity, set at construction). Unbounded lock-free
queues exist (e.g., the Michael-Scott queue using a linked list with CAS-based
node insertion) but introduce a harder problem: **safe memory reclamation**.
When a node is dequeued from a lock-free linked list, another thread might still
hold a pointer to it — freeing it immediately risks a use-after-free. Solving
this requires hazard pointers, epoch-based reclamation, or RCU, each of which is
a substantial topic on its own. A bounded ring buffer avoids the problem
entirely because slots are reused, never freed — at the cost of needing to
handle the "queue full" case (the design choice below).

**Behavior on full/empty: spin vs block**

`push` on a full queue and `pop` on an empty queue return `false` immediately
(non-blocking) rather than spinning or blocking. This keeps the data structure
itself fully lock-free and leaves the retry/backoff policy to the caller —
appropriate because different use cases want different policies (busy-spin for
ultra-low-latency, exponential backoff for CPU-friendly polling, or falling back
to a condition variable for a hybrid design).

**Lock-free vs wait-free**

This queue is lock-free but not wait-free: under pathological scheduling, a
thread's CAS could fail repeatedly if other threads continuously win the race
(though this is exceptionally rare in practice with `compare_exchange_weak` and
real scheduler behavior). True wait-free algorithms guarantee every thread
completes in a bounded number of steps regardless of other threads, but are
substantially more complex to implement correctly. Lock-free is the standard,
practical target for high-performance concurrent data structures.

**Why not just use a mutex?**

This is the question every interviewer will ask, and the honest answer is: for
most applications, a mutex-protected `std::deque` is the right choice. Mutex
overhead (tens of nanoseconds, uncontended) is negligible for most workloads,
and the implementation is trivially correct. Lock-free structures are justified
specifically when: (1) the queue is on a hot path called millions of times per
second, (2) priority inversion is a concern (a low-priority thread holding a
lock blocking a high-priority thread), or (3) the structure is used in a context
where blocking is unacceptable, such as a signal handler or real-time audio
callback. The benchmarks below quantify when the crossover happens.

---

## Benchmarks

**Hardware:** Apple M3, 8 cores (8 logical), macOS  
**Compiler:** Apple Clang 21.0.0, `-O2 -DNDEBUG` (CMake Release)  
**Operations:** 10,000,000 items transferred per configuration

### Throughput vs thread count (fixed 10 M total items)

| Threads (P+C) | lfqueue (Mops/sec) | mutex queue (Mops/sec) |
|:-------------:|:------------------:|:----------------------:|
| 1 + 1         | 57.2               | 45.7                   |
| 2 + 2         | 14.2               | 26.0                   |
| 4 + 4         | 5.0                | 18.0                   |
| 8 + 8         | 3.4                | 17.3                   |

**Interpretation:** The lock-free queue wins at 1+1 (no contention, avoids
lock overhead entirely). As thread count rises, the CAS-based design suffers
from cache-invalidation storms: every producer's CAS on `tail_` and every
consumer's CAS on `head_` broadcasts a cache-line invalidation to all other
cores. On Apple Silicon's strongly-ordered architecture, the mutex queue
serialises access more cheaply than repeated CAS failures under high
contention. This is a well-known result — lock-free does not mean
faster-under-all-conditions; it means progress without blocking. The
lock-free queue's advantages show clearly in the latency distribution below,
particularly at the tail.

### Effect of cache line padding (4 producers + 4 consumers)

| Configuration               | Throughput (Mops/sec) |
|:---------------------------:|:---------------------:|
| Without padding (false sharing) | 3.3               |
| With 64-byte padding            | 5.0               |

Padding `head_` and `tail_` onto separate cache lines gives a **52%
throughput improvement** with no algorithmic change — the only difference is
preventing the two counters from sharing a cache line and invalidating each
other on every operation.

### Latency distribution (single producer, single consumer, 100 k samples)

| Metric | lfqueue (ns) | mutex queue (ns) |
|:------:|:------------:|:----------------:|
| p50    | 4,959        | 5,375            |
| p99    | 7,167        | 18,291           |
| p999   | 15,958       | 57,917           |

The lock-free queue shows a **3.6× lower p999 latency** than the mutex queue.
This is the expected advantage: when a thread is preempted while holding a
mutex, every other thread stalls until it resumes. A lock-free queue never
blocks a waiter — worst case, a consumer spins briefly on a sequence number
before the producer publishes, but it never waits for an OS reschedule.

*Reproduce with `./benchmarks/run_all.sh` (builds Release binaries if needed).*

---

## Building and Running

### Requirements

- C++20 compiler (for `std::atomic::wait`/`notify`, if used in extensions)
  — core implementation works with C++17
- `cmake` 3.16+
- ThreadSanitizer support (Clang or GCC with `-fsanitize=thread`)

### Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

### Run benchmarks

```bash
./build/benchmarks/throughput_bench
./build/benchmarks/padding_bench
./build/benchmarks/latency_bench
```

### Run correctness tests

```bash
ctest --test-dir build --output-on-failure
```

### Run with ThreadSanitizer

```bash
cmake -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=thread"
cmake --build build-tsan
./build-tsan/tests/correctness_test
```

ThreadSanitizer is essential for this project — it detects data races that may
not manifest in normal testing but are real bugs under different scheduling or
hardware. **Every commit should pass under TSan** before being considered
correct; this is standard practice for any production lock-free code.

---

## Testing Strategy

Lock-free correctness cannot be fully validated by normal unit tests, because
race conditions are timing-dependent. The test suite uses several complementary
approaches:

**Functional correctness**: single-threaded push/pop sequences verify FIFO
ordering, full/empty boundary conditions, and wraparound behavior.

**Concurrent stress test**: N producers each push a unique sequence of values;
M consumers pop and record what they receive. After completion, verify that
every value pushed was received exactly once (no duplicates, no drops) — this
catches the most common class of lock-free bugs.

**ThreadSanitizer**: run the stress test under TSan to catch missing memory
ordering even when the functional test passes — TSan can detect a "correct by
luck" race that would fail on different hardware.

**Relacy / CDSChecker (optional, advanced)**: model-checking tools that
exhaustively explore thread interleavings for small test cases, providing much
stronger guarantees than running many random iterations. Mentioned here as a
stretch goal for demonstrating depth.

### Results

```
$ ctest --test-dir build-release --output-on-failure
Test project lfqueue/build-release
    Start 1: correctness_test
1/1 Test #1: correctness_test .................   Passed    0.73 sec
100% tests passed, 0 tests failed out of 1
```

The `correctness_test` binary covers, in order: empty/full boundary
conditions, wraparound ordering, zero-capacity rejection, constructor
validation, move-only value types, and exact-capacity behavior for the mutex
baseline — followed by six concurrent stress configurations run against
**both** `MPMCQueue` and `MutexQueue` (1+1, 2+2, 4+4, 8+8, 1 producer/4
consumers, 4 producers/1 consumer), each verifying every pushed value is
received exactly once with no drops or duplicates.

```
$ cmake -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=thread"
$ cmake --build build-tsan
$ ./build-tsan/tests/correctness_test
$ echo $?
0
```

ThreadSanitizer reports **zero data races** across every stress configuration
— the release/acquire pairing on the sequence number is sufficient to make
the producer/consumer handoff safe under TSan's race detector, not merely
"correct by luck" on a specific architecture's memory model.

---

## Relationship to Broader Systems Work

Lock-free queues are the backbone of high-performance concurrent systems:

- **Work-stealing schedulers** (used in thread pools, task-based parallelism
  frameworks like Intel TBB and Rust's Tokio) use lock-free deques so idle
  threads can steal work from busy threads without blocking them.

- **Inter-thread communication in low-latency systems** (audio processing, HFT,
  real-time control loops) requires bounded, predictable-latency message passing
  — exactly what this queue provides, in contrast to a mutex queue's occasional
  long tail latencies under preemption.

- **Memory allocators** (connecting to the companion allocator project):
  high-performance allocators use lock-free free-lists for cross-thread memory
  return — when thread A frees memory originally allocated by thread B, it must
  hand the memory back without locking B's allocator state.

- **Compiler and runtime internals**: garbage collectors and JIT compilers use
  lock-free structures for work queues (e.g., parallel marking phases in a GC)
  where mutex contention would directly translate to pause-time regressions.

The memory ordering reasoning in this project — release/acquire pairing,
happens-before relationships — is the same reasoning required to understand
data races in any concurrent system, including the kernel synchronization in
the character device driver project (where the kernel's locking primitives
handle this for you, but the underlying memory model is identical).

---

## Future Extensions

- **Unbounded variant with hazard pointers** — implement the Michael-Scott
  queue with a safe memory reclamation scheme, demonstrating the harder
  unbounded case
- **Backoff strategies** — implement and benchmark exponential backoff on CAS
  failure vs busy-spin, quantifying the CPU-usage/latency tradeoff
- **SPSC fast path** — a single-producer/single-consumer specialization can
  avoid CAS entirely (plain atomic loads/stores suffice when there's only one
  writer per index), and benchmarking SPSC vs MPMC quantifies the cost of
  generality
- **`std::atomic::wait`/`notify` (C++20)** — replace busy-polling consumers with
  futex-based blocking that still avoids a full mutex

---

## References

- Vyukov, D. *Bounded MPMC Queue* — the sequence-number design this
  implementation is based on (1024cores.net)
- Michael, M. & Scott, M. *Simple, Fast, and Practical Non-Blocking and
  Blocking Concurrent Queue Algorithms* (PODC 1996) — the unbounded queue
  referenced in Future Extensions
- Williams, A. *C++ Concurrency in Action, 2nd Edition* — the standard
  reference for `std::atomic` and memory ordering in C++
- *C++ Standard, `<atomic>` header* — `std::memory_order` definitions
- Preshing, J. — preshing.com blog series on lock-free programming and memory
  ordering (clear, practical explanations with diagrams)
