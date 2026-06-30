#include "lfqueue/mpmc_queue.hpp"
#include "lfqueue/mutex_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

// All threads spin on `go` until the main thread releases them, so that the
// timer starts after thread creation overhead.
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
    std::printf("# hardware_concurrency=%u\n",
                std::thread::hardware_concurrency());
    std::puts("threads,queue,total_ops,seconds,mops_per_sec");

    constexpr long kTotal = 10'000'000;

    for (int N : {1, 2, 4, 8}) {
        long ops = kTotal / N;
        long total = static_cast<long>(N) * ops;

        double lfq_mops  = run_throughput<lfqueue::MPMCQueue<int>>(N, N, ops);
        double mtx_mops  = run_throughput<lfqueue::MutexQueue<int>>(N, N, ops);

        std::printf("%d+%d,lfqueue,%ld,%.3f,%.1f\n",
                    N, N, total, total / (lfq_mops * 1e6), lfq_mops);
        std::printf("%d+%d,mutex,%ld,%.3f,%.1f\n",
                    N, N, total, total / (mtx_mops * 1e6), mtx_mops);
    }
    return 0;
}
