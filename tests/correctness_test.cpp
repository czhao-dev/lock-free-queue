#include "lfqueue/mpmc_queue.hpp"
#include "lfqueue/mutex_queue.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

// -------------------------------------------------------------------
// Single-thread functional tests
// -------------------------------------------------------------------

template <template <typename> class Queue>
void test_empty_and_full() {
    Queue<int> queue(4);
    int value = -1;

    assert(queue.capacity() == 4);
    assert(!queue.pop(value));

    assert(queue.push(1));
    assert(queue.push(2));
    assert(queue.push(3));
    assert(queue.push(4));
    assert(!queue.push(5));

    assert(queue.pop(value));
    assert(value == 1);
    assert(queue.pop(value));
    assert(value == 2);
    assert(queue.pop(value));
    assert(value == 3);
    assert(queue.pop(value));
    assert(value == 4);
    assert(!queue.pop(value));
}

template <template <typename> class Queue>
void test_wraparound_order() {
    Queue<int> queue(2);
    int value = 0;

    assert(queue.push(10));
    assert(queue.push(20));
    assert(!queue.push(30));

    assert(queue.pop(value));
    assert(value == 10);

    assert(queue.push(30));

    assert(queue.pop(value));
    assert(value == 20);
    assert(queue.pop(value));
    assert(value == 30);
    assert(!queue.pop(value));
}

template <template <typename> class Queue>
void test_rejects_zero_capacity() {
    bool rejected_zero = false;
    try {
        Queue<int> queue(0);
    } catch (const std::invalid_argument&) {
        rejected_zero = true;
    }
    assert(rejected_zero);
}

void test_mpmc_constructor_validation() {
    bool rejected_non_power_of_two = false;
    try {
        lfqueue::MPMCQueue<int> queue(3);
    } catch (const std::invalid_argument&) {
        rejected_non_power_of_two = true;
    }
    assert(rejected_non_power_of_two);
}

void test_mutex_queue_exact_capacity() {
    lfqueue::MutexQueue<int> queue(3);
    int value = 0;

    assert(queue.capacity() == 3);
    assert(queue.push(1));
    assert(queue.push(2));
    assert(queue.push(3));
    assert(!queue.push(4));

    assert(queue.pop(value));
    assert(value == 1);
    assert(queue.push(4));

    assert(queue.pop(value));
    assert(value == 2);
    assert(queue.pop(value));
    assert(value == 3);
    assert(queue.pop(value));
    assert(value == 4);
    assert(!queue.pop(value));
}

template <template <typename> class Queue>
void test_move_only_values() {
    Queue<std::unique_ptr<int>> queue(2);
    std::unique_ptr<int> value;

    assert(queue.push(std::make_unique<int>(42)));
    assert(queue.pop(value));
    assert(value);
    assert(*value == 42);
    assert(!queue.pop(value));
}

template <template <typename> class Queue>
void test_basic_queue_behavior() {
    test_empty_and_full<Queue>();
    test_wraparound_order<Queue>();
    test_rejects_zero_capacity<Queue>();
    test_move_only_values<Queue>();
}

// -------------------------------------------------------------------
// Concurrent stress tests
// -------------------------------------------------------------------

// P producers each push [p*vprod, (p+1)*vprod). C consumers pop until
// all P*vprod items are received. Verify every value appears exactly once.
template <template <typename> class Queue>
void run_stress(std::size_t capacity, int P, int C, int vprod) {
    Queue<int> q(capacity);
    const int total = P * vprod;

    std::atomic<int> consumed{0};
    std::mutex mx;
    std::vector<int> received;
    received.reserve(static_cast<std::size_t>(total));

    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    producers.reserve(static_cast<std::size_t>(P));
    consumers.reserve(static_cast<std::size_t>(C));

    for (int p = 0; p < P; ++p) {
        producers.emplace_back([&q, p, vprod]() {
            for (int v = p * vprod, end = v + vprod; v < end; ++v)
                while (!q.push(v)) {}
        });
    }

    for (int c = 0; c < C; ++c) {
        consumers.emplace_back([&]() {
            std::vector<int> local;
            int val;
            while (consumed.load(std::memory_order_relaxed) < total) {
                if (q.pop(val)) {
                    local.push_back(val);
                    consumed.fetch_add(1, std::memory_order_relaxed);
                }
            }
            std::lock_guard<std::mutex> lk(mx);
            received.insert(received.end(), local.begin(), local.end());
        });
    }

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    assert(static_cast<int>(received.size()) == total);
    std::sort(received.begin(), received.end());
    for (int i = 0; i < total; ++i)
        assert(received[i] == i);
}

template <template <typename> class Queue>
void test_concurrent_stress() {
    constexpr int vprod = 10000;

    run_stress<Queue>(  8, 1, 1, vprod);
    run_stress<Queue>(  8, 2, 2, vprod);
    run_stress<Queue>( 64, 4, 4, vprod);
    run_stress<Queue>( 64, 8, 8, vprod);
    run_stress<Queue>(  8, 1, 4, vprod);  // many consumers, one producer
    run_stress<Queue>(  8, 4, 1, vprod);  // many producers, one consumer

    // Small capacity forces wraparound and high contention
    for (int i = 0; i < 5; ++i)
        run_stress<Queue>(2, 2, 2, 1000);
}

} // namespace

int main() {
    test_basic_queue_behavior<lfqueue::MPMCQueue>();
    test_basic_queue_behavior<lfqueue::MutexQueue>();
    test_mpmc_constructor_validation();
    test_mutex_queue_exact_capacity();
    test_concurrent_stress<lfqueue::MPMCQueue>();
    test_concurrent_stress<lfqueue::MutexQueue>();
}
