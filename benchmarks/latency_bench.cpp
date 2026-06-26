#include "lfqueue/mpmc_queue.hpp"
#include "lfqueue/mutex_queue.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

// Payload carries the nanosecond timestamp of when it was pushed so that the
// consumer can measure end-to-end queue latency for each item.
struct Item {
    int64_t push_ns;
};
static_assert(std::is_nothrow_move_constructible<Item>::value, "");
static_assert(std::is_nothrow_move_assignable<Item>::value, "");

static int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

template <typename Queue>
void run_latency(const char* label, int n_samples, int queue_capacity) {
    Queue queue(static_cast<std::size_t>(queue_capacity));
    std::vector<int64_t> latencies;
    latencies.reserve(static_cast<std::size_t>(n_samples));

    std::atomic<int> consumed{0};

    std::thread consumer([&]() {
        Item item{0};
        while (consumed.load(std::memory_order_relaxed) < n_samples) {
            if (queue.pop(item)) {
                int64_t lat = now_ns() - item.push_ns;
                latencies.push_back(lat > 0 ? lat : 0);
                consumed.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    for (int i = 0; i < n_samples; ++i)
        while (!queue.push(Item{now_ns()})) {}

    consumer.join();

    std::sort(latencies.begin(), latencies.end());
    auto pct = [&](double p) -> int64_t {
        std::size_t idx = static_cast<std::size_t>(
            static_cast<double>(latencies.size()) * p);
        if (idx >= latencies.size()) idx = latencies.size() - 1;
        return latencies[idx];
    };

    std::printf("%s,%lld,%lld,%lld\n",
                label,
                static_cast<long long>(pct(0.50)),
                static_cast<long long>(pct(0.99)),
                static_cast<long long>(pct(0.999)));
}

int main() {
    constexpr int kSamples  = 100'000;
    constexpr int kCapacity = 64;

    std::puts("queue,p50_ns,p99_ns,p999_ns");
    run_latency<lfqueue::MPMCQueue<Item>>("lfqueue", kSamples, kCapacity);
    run_latency<lfqueue::MutexQueue<Item>>("mutex",   kSamples, kCapacity);
    return 0;
}
