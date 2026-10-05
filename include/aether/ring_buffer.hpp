#ifndef INCLUDE_AETHER_RING_BUFFER_HPP
#define INCLUDE_AETHER_RING_BUFFER_HPP

#include <atomic>
#include <cstdint>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>

namespace aether {

// 1. DATA STRUCTURES: 64-byte cache-line aligned PacketDescriptor metadata struct
struct alignas(64) PacketDescriptor {
    uint64_t sequence_number;
    uint64_t timestamp_ns;
    uint32_t payload_length;
    uint32_t flags;
    void* payload_address;
    uint8_t reserved_padding[32]; // Pads the struct to exactly 64 bytes
};

static_assert(sizeof(PacketDescriptor) == 64, "PacketDescriptor must be exactly 64 bytes to match cache lines.");
static_assert(alignof(PacketDescriptor) == 64, "PacketDescriptor must be aligned to 64 bytes.");

// 2. CONCURRENCY & MEMORY LAYOUT: MPMC Lock-Free Circular Ring Buffer
// Uses power-of-two capacity masks and atomic sequence counters with acquire-release semantics.
template <typename T, size_t Capacity>
class RingBuffer {
    static_assert(Capacity > 0 && (Capacity & (Capacity - 1)) == 0, 
                  "RingBuffer capacity must be strictly a power of two.");

public:
    RingBuffer() : head_(0), tail_(0) {
        for (size_t i = 0; i < Capacity; ++i) {
            nodes_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~RingBuffer() = default;

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&&) = delete;
    RingBuffer& operator=(RingBuffer&&) = delete;

    // Push an item into the ring buffer (MPMC safe)
    bool push(const T& item) noexcept {
        Cell* cell;
        uint64_t head = head_.load(std::memory_order_relaxed);
        
        for (;;) {
            cell = &nodes_[head & kMask];
            uint64_t seq = cell->sequence.load(std::memory_order_acquire);
            int64_t diff = static_cast<int64_t>(seq) - static_cast<int64_t>(head);

            if (diff == 0) {
                if (head_.compare_exchange_weak(head, head + 1, 
                                                std::memory_order_relaxed, 
                                                std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                // Buffer is full (backpressure condition)
                return false;
            } else {
                head = head_.load(std::memory_order_relaxed);
            }
        }

        cell->data = item;
        cell->sequence.store(head + 1, std::memory_order_release);
        return true;
    }

    // Push an item into the ring buffer via move semantics (MPMC safe)
    bool push(T&& item) noexcept {
        Cell* cell;
        uint64_t head = head_.load(std::memory_order_relaxed);
        
        for (;;) {
            cell = &nodes_[head & kMask];
            uint64_t seq = cell->sequence.load(std::memory_order_acquire);
            int64_t diff = static_cast<int64_t>(seq) - static_cast<int64_t>(head);

            if (diff == 0) {
                if (head_.compare_exchange_weak(head, head + 1, 
                                                std::memory_order_relaxed, 
                                                std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                // Buffer is full (backpressure condition)
                return false;
            } else {
                head = head_.load(std::memory_order_relaxed);
            }
        }

        cell->data = std::move(item);
        cell->sequence.store(head + 1, std::memory_order_release);
        return true;
    }

    // Pop an item from the ring buffer (MPMC safe)
    bool pop(T& item) noexcept {
        Cell* cell;
        uint64_t tail = tail_.load(std::memory_order_relaxed);

        for (;;) {
            cell = &nodes_[tail & kMask];
            uint64_t seq = cell->sequence.load(std::memory_order_acquire);
            int64_t diff = static_cast<int64_t>(seq) - static_cast<int64_t>(tail + 1);

            if (diff == 0) {
                if (tail_.compare_exchange_weak(tail, tail + 1, 
                                                std::memory_order_relaxed, 
                                                std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                // Buffer is empty
                return false;
            } else {
                tail = tail_.load(std::memory_order_relaxed);
            }
        }

        item = std::move(cell->data);
        cell->sequence.store(tail + kMask + 1, std::memory_order_release);
        return true;
    }

    // Optional-based pop interface for convenience
    std::optional<T> pop() noexcept {
        T item;
        if (pop(item)) {
            return std::optional<T>(std::in_place, std::move(item));
        }
        return std::nullopt;
    }

    [[nodiscard]] constexpr size_t capacity() const noexcept {
        return Capacity;
    }

    // Approximate size calculation for monitoring/telemetry
    [[nodiscard]] size_t size_approx() const noexcept {
        uint64_t head = head_.load(std::memory_order_relaxed);
        uint64_t tail = tail_.load(std::memory_order_relaxed);
        if (head >= tail) {
            return static_cast<size_t>(head - tail);
        }
        return 0;
    }

    [[nodiscard]] bool empty() const noexcept {
        uint64_t tail = tail_.load(std::memory_order_relaxed);
        Cell* cell = &nodes_[tail & kMask];
        uint64_t seq = cell->sequence.load(std::memory_order_acquire);
        return static_cast<int64_t>(seq) - static_cast<int64_t>(tail + 1) < 0;
    }

private:
    struct alignas(64) Cell {
        std::atomic<uint64_t> sequence;
        T data;
    };

    static constexpr uint64_t kMask = Capacity - 1;

    // Cache-line separation to eliminate false sharing between producers and consumers
    alignas(64) std::atomic<uint64_t> head_;
    alignas(64) std::atomic<uint64_t> tail_;
    
    // Array of cells aligned to cache lines
    Cell nodes_[Capacity];
};

} // namespace aether

#endif // INCLUDE_AETHER_RING_BUFFER_HPP