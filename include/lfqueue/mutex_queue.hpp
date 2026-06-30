#ifndef LFQUEUE_MUTEX_QUEUE_HPP
#define LFQUEUE_MUTEX_QUEUE_HPP

#include <cstddef>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace lfqueue {

template <typename T>
class MutexQueue {
public:
    explicit MutexQueue(std::size_t capacity)
        : capacity_(checked_capacity(capacity)),
          buffer_(capacity_) {}

    MutexQueue(const MutexQueue&) = delete;
    MutexQueue& operator=(const MutexQueue&) = delete;
    MutexQueue(MutexQueue&&) = delete;
    MutexQueue& operator=(MutexQueue&&) = delete;

    bool push(T value) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (size_ == capacity_) {
            return false;
        }

        buffer_[tail_].emplace(std::move(value));
        tail_ = next_index(tail_);
        ++size_;
        return true;
    }

    bool pop(T& out) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (size_ == 0) {
            return false;
        }

        out = std::move(*buffer_[head_]);
        buffer_[head_].reset();
        head_ = next_index(head_);
        --size_;
        return true;
    }

    std::size_t capacity() const noexcept {
        return capacity_;
    }

private:
    static std::size_t checked_capacity(std::size_t capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("MutexQueue capacity must be non-zero");
        }
        return capacity;
    }

    std::size_t next_index(std::size_t index) const noexcept {
        ++index;
        return index == capacity_ ? 0 : index;
    }

    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::vector<std::optional<T>> buffer_;
    std::size_t head_{0};
    std::size_t tail_{0};
    std::size_t size_{0};
};

} // namespace lfqueue

#endif // LFQUEUE_MUTEX_QUEUE_HPP
