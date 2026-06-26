#include "lfqueue/mpmc_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <new>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

// ---------------------------------------------------------------------------
// Deliberately unpadded MPMC queue — head_ and tail_ share cache lines with
// buffer_ metadata, causing false sharing between producers and consumers.
// Kept here (not in the public header) because it is only useful for
// demonstrating the performance cost of missing padding.
// ---------------------------------------------------------------------------
template <typename T>
class NoPadMPMCQueue {
    static_assert(std::is_nothrow_move_constructible<T>::value, "");
    static_assert(std::is_nothrow_move_assignable<T>::value, "");

public:
    explicit NoPadMPMCQueue(std::size_t capacity)
        : capacity_(checked_pow2(capacity)), mask_(capacity_ - 1),
          buffer_(capacity_) {
        for (std::size_t i = 0; i < capacity_; ++i)
            buffer_[i].seq.store(i, std::memory_order_relaxed);
    }

    ~NoPadMPMCQueue() noexcept {
        std::size_t h = head_.load(std::memory_order_relaxed);
        std::size_t t = tail_.load(std::memory_order_relaxed);
        for (std::size_t p = h; p != t; ++p) {
            auto& slot = buffer_[p & mask_];
            if (slot.seq.load(std::memory_order_acquire) == p + 1)
                slot.data()->~T();
        }
    }

    NoPadMPMCQueue(const NoPadMPMCQueue&) = delete;
    NoPadMPMCQueue& operator=(const NoPadMPMCQueue&) = delete;

    bool push(T value) {
        std::size_t pos = tail_.load(std::memory_order_relaxed);
        for (;;) {
            auto& slot = buffer_[pos & mask_];
            std::size_t seq = slot.seq.load(std::memory_order_acquire);
            auto diff = static_cast<std::intptr_t>(seq) -
                        static_cast<std::intptr_t>(pos);
            if (diff == 0) {
                if (tail_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    slot.construct(std::move(value));
                    slot.seq.store(pos + 1, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = tail_.load(std::memory_order_relaxed);
            }
        }
    }

    bool pop(T& out) {
        std::size_t pos = head_.load(std::memory_order_relaxed);
        for (;;) {
            auto& slot = buffer_[pos & mask_];
            std::size_t seq = slot.seq.load(std::memory_order_acquire);
            auto diff = static_cast<std::intptr_t>(seq) -
                        static_cast<std::intptr_t>(pos + 1);
            if (diff == 0) {
                if (head_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    out = std::move(*slot.data());
                    slot.data()->~T();
                    slot.seq.store(pos + capacity_, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }
    }

private:
    using Storage = typename std::aligned_storage<sizeof(T), alignof(T)>::type;

    struct Slot {
        std::atomic<std::size_t> seq{0};
        Storage storage;
        void construct(T&& v) { ::new (static_cast<void*>(&storage)) T(std::move(v)); }
        T* data() noexcept { return std::launder(reinterpret_cast<T*>(&storage)); }
    };

    static std::size_t checked_pow2(std::size_t n) {
        if (n == 0 || (n & (n - 1)) != 0)
            throw std::invalid_argument("NoPadMPMCQueue capacity must be a non-zero power of two");
        return n;
    }

    const std::size_t capacity_;
    const std::size_t mask_;
    std::vector<Slot> buffer_;
    // NOT cache-line padded — sits adjacent to buffer_ metadata,
    // and head_/tail_ share a cache line with each other.
    std::atomic<std::size_t> head_{0};
    std::atomic<std::size_t> tail_{0};
};

// ---------------------------------------------------------------------------
// Benchmark driver
// ---------------------------------------------------------------------------
template <typename Queue>
double run_throughput(int P, int C, long ops_per_producer) {
    Queue queue(1024);
    const long total = static_cast<long>(P) * ops_per_producer;
    std::atomic<long> consumed{0};
    std::atomic<int>  ready{0};
    std::atomic<bool> go{false};

    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(P + C));

    for (int p = 0; p < P; ++p) {
        threads.emplace_back([&]() {
            ready.fetch_add(1, std::memory_order_relaxed);
            while (!go.load(std::memory_order_acquire)) {}
            for (long i = 0; i < ops_per_producer; ++i)
                while (!queue.push(0)) {}
        });
    }
    for (int c = 0; c < C; ++c) {
        threads.emplace_back([&]() {
            ready.fetch_add(1, std::memory_order_relaxed);
            while (!go.load(std::memory_order_acquire)) {}
            int val;
            while (consumed.load(std::memory_order_relaxed) < total)
                if (queue.pop(val))
                    consumed.fetch_add(1, std::memory_order_relaxed);
        });
    }

    while (ready.load(std::memory_order_acquire) < P + C) {}
    auto t0 = std::chrono::steady_clock::now();
    go.store(true, std::memory_order_release);
    for (auto& t : threads) t.join();
    auto t1 = std::chrono::steady_clock::now();

    double seconds = std::chrono::duration<double>(t1 - t0).count();
    return static_cast<double>(total) / seconds / 1e6;
}

int main() {
    constexpr int P = 4, C = 4;
    constexpr long kTotal = 10'000'000;
    long ops = kTotal / P;
    long total = static_cast<long>(P) * ops;

    double nopad_mops = run_throughput<NoPadMPMCQueue<int>>(P, C, ops);
    double pad_mops   = run_throughput<lfqueue::MPMCQueue<int>>(P, C, ops);

    std::puts("configuration,total_ops,seconds,mops_per_sec");
    std::printf("without_padding,%ld,%.3f,%.1f\n",
                total, total / (nopad_mops * 1e6), nopad_mops);
    std::printf("with_padding,%ld,%.3f,%.1f\n",
                total, total / (pad_mops * 1e6), pad_mops);
    return 0;
}
