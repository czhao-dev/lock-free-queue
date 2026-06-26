#ifndef LFQUEUE_MPMC_QUEUE_HPP
#define LFQUEUE_MPMC_QUEUE_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace lfqueue {

template <typename T>
class MPMCQueue {
    static_assert(std::is_nothrow_move_constructible<T>::value,
                  "MPMCQueue<T> requires nothrow move construction");
    static_assert(std::is_nothrow_move_assignable<T>::value,
                  "MPMCQueue<T> requires nothrow move assignment");

public:
    explicit MPMCQueue(std::size_t capacity)
        : capacity_(checked_capacity(capacity)),
          mask_(capacity_ - 1),
          buffer_(capacity_) {
        for (std::size_t i = 0; i < capacity_; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }

        head_.value.store(0, std::memory_order_relaxed);
        tail_.value.store(0, std::memory_order_relaxed);
    }

    ~MPMCQueue() noexcept {
        destroy_remaining();
    }

    MPMCQueue(const MPMCQueue&) = delete;
    MPMCQueue& operator=(const MPMCQueue&) = delete;
    MPMCQueue(MPMCQueue&&) = delete;
    MPMCQueue& operator=(MPMCQueue&&) = delete;

    bool push(T value) {
        Slot* slot = nullptr;
        std::size_t pos = tail_.value.load(std::memory_order_relaxed);

        for (;;) {
            slot = &buffer_[pos & mask_];
            const std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            const auto diff = static_cast<std::intptr_t>(seq) -
                              static_cast<std::intptr_t>(pos);

            if (diff == 0) {
                if (tail_.value.compare_exchange_weak(pos, pos + 1,
                                                      std::memory_order_relaxed,
                                                      std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = tail_.value.load(std::memory_order_relaxed);
            }
        }

        slot->construct(std::move(value));
        slot->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& out) {
        Slot* slot = nullptr;
        std::size_t pos = head_.value.load(std::memory_order_relaxed);

        for (;;) {
            slot = &buffer_[pos & mask_];
            const std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            const auto diff = static_cast<std::intptr_t>(seq) -
                              static_cast<std::intptr_t>(pos + 1);

            if (diff == 0) {
                if (head_.value.compare_exchange_weak(pos, pos + 1,
                                                      std::memory_order_relaxed,
                                                      std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = head_.value.load(std::memory_order_relaxed);
            }
        }

        T* value = slot->data();
        out = std::move(*value);
        value->~T();
        slot->sequence.store(pos + capacity_, std::memory_order_release);
        return true;
    }

    std::size_t capacity() const noexcept {
        return capacity_;
    }

private:
    static constexpr std::size_t cache_line_size = 64;

    struct alignas(cache_line_size) PaddedAtomic {
        std::atomic<std::size_t> value{0};
    };

    struct Slot {
        using Storage = typename std::aligned_storage<sizeof(T), alignof(T)>::type;

        std::atomic<std::size_t> sequence{0};
        Storage storage;

        Slot() noexcept = default;
        Slot(const Slot&) = delete;
        Slot& operator=(const Slot&) = delete;

        void construct(T&& value) {
            ::new (static_cast<void*>(&storage)) T(std::move(value));
        }

        T* data() noexcept {
            return std::launder(reinterpret_cast<T*>(&storage));
        }
    };

    static_assert(sizeof(PaddedAtomic) >= cache_line_size,
                  "PaddedAtomic must occupy at least one cache line");

    static bool is_power_of_two(std::size_t value) noexcept {
        return value != 0 && (value & (value - 1)) == 0;
    }

    static std::size_t checked_capacity(std::size_t capacity) {
        if (!is_power_of_two(capacity)) {
            throw std::invalid_argument(
                "MPMCQueue capacity must be a non-zero power of two");
        }
        return capacity;
    }

    void destroy_remaining() noexcept {
        // Destruction must not race with producers or consumers.
        const std::size_t head = head_.value.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.value.load(std::memory_order_relaxed);

        for (std::size_t pos = head; pos != tail; ++pos) {
            Slot& slot = buffer_[pos & mask_];
            const std::size_t ready = pos + 1;
            if (slot.sequence.load(std::memory_order_acquire) == ready) {
                slot.data()->~T();
                slot.sequence.store(pos + capacity_, std::memory_order_relaxed);
            }
        }
    }

    const std::size_t capacity_;
    const std::size_t mask_;
    std::vector<Slot> buffer_;
    PaddedAtomic head_;
    PaddedAtomic tail_;
};

} // namespace lfqueue

#endif // LFQUEUE_MPMC_QUEUE_HPP
