#ifndef AETHER_TYPES_HPP
#define AETHER_TYPES_HPP

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <new>
#include <type_traits>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <cmath>

namespace aether {

// Alignas 64 bytes to prevent false sharing across cache lines
struct alignas(64) PacketDescriptor {
    uint64_t sequence_id;
    uint64_t timestamp_ns;
    uint32_t payload_len;
    uint32_t flags;
    uint8_t* payload_ptr;
    uint32_t flow_hash;
    uint32_t source_core;
    uint8_t reserved[24]; // Pad out to exactly 64 bytes
};

static_assert(sizeof(PacketDescriptor) == 64, "PacketDescriptor must be exactly 64 bytes");
static_assert(alignof(PacketDescriptor) == 64, "PacketDescriptor must be aligned to 64 bytes");

template <typename T, size_t Capacity>
class SPSCQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
public:
    SPSCQueue() : head_(0), tail_(0) {
        for (size_t i = 0; i < Capacity; ++i) {
            ring_[i] = T{};
        }
    }

    ~SPSCQueue() = default;

    SPSCQueue(const SPSCQueue&) = delete;
    SPSCQueue& operator=(const SPSCQueue&) = delete;
    SPSCQueue(SPSCQueue&&) = delete;
    SPSCQueue& operator=(SPSCQueue&&) = delete;

    bool push(const T& item) noexcept {
        const uint64_t current_tail = tail_.load(std::memory_order_relaxed);
        const uint64_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false; // Full
        }

        ring_[current_tail & kMask] = item;
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool push(T&& item) noexcept {
        const uint64_t current_tail = tail_.load(std::memory_order_relaxed);
        const uint64_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false; // Full
        }

        ring_[current_tail & kMask] = std::move(item);
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) noexcept {
        const uint64_t current_head = head_.load(std::memory_order_relaxed);
        const uint64_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head == current_tail) {
            return false; // Empty
        }

        item = std::move(ring_[current_head & kMask]);
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] size_t size() const noexcept {
        uint64_t tail = tail_.load(std::memory_order_acquire);
        uint64_t head = head_.load(std::memory_order_acquire);
        if (tail >= head) {
            return static_cast<size_t>(tail - head);
        }
        return 0;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    [[nodiscard]] constexpr size_t capacity() const noexcept {
        return Capacity;
    }

private:
    static constexpr uint64_t kMask = Capacity - 1;

    alignas(64) std::atomic<uint64_t> head_;
    alignas(64) std::atomic<uint64_t> tail_;
    alignas(64) T ring_[Capacity];
};

class HugePageArena {
public:
    explicit HugePageArena(size_t size_bytes) 
        : size_(align_to_page_size(size_bytes)), base_ptr_(nullptr), offset_(0) {
        
        void* ptr = mmap(nullptr, size_, 
                         PROT_READ | PROT_WRITE, 
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, 
                         -1, 0);
                         
        if (ptr == MAP_FAILED) {
            // Fallback to standard anonymous mmap if hugepages are unavailable
            ptr = mmap(nullptr, size_, 
                       PROT_READ | PROT_WRITE, 
                       MAP_PRIVATE | MAP_ANONYMOUS, 
                       -1, 0);
            if (ptr == MAP_FAILED) {
                base_ptr_ = nullptr;
                size_ = 0;
                return;
            }
        }
        base_ptr_ = static_cast<uint8_t*>(ptr);
    }

    ~HugePageArena() {
        if (base_ptr_ && base_ptr_ != MAP_FAILED) {
            munmap(base_ptr_, size_);
        }
    }

    HugePageArena(const HugePageArena&) = delete;
    HugePageArena& operator=(const HugePageArena&) = delete;
    HugePageArena(HugePageArena&& other) noexcept 
        : size_(other.size_), base_ptr_(other.base_ptr_), offset_(other.offset_) {
        other.base_ptr_ = nullptr;
        other.size_ = 0;
        other.offset_ = 0;
    }

    HugePageArena& operator=(HugePageArena&& other) noexcept {
        if (this != &other) {
            if (base_ptr_ && base_ptr_ != MAP_FAILED) {
                munmap(base_ptr_, size_);
            }
            size_ = other.size_;
            base_ptr_ = other.base_ptr_;
            offset_ = other.offset_;
            other.base_ptr_ = nullptr;
            other.size_ = 0;
            other.offset_ = 0;
        }
        return ori_self(*this);
    }

    [[nodiscard]] uint8_t* allocate(size_t bytes, size_t alignment = 64) noexcept {
        if (!base_ptr_) return nullptr;

        uintptr_t current_address = reinterpret_cast<uintptr_t>(base_ptr_ + offset_);
        uintptr_t aligned_address = (current_address + (alignment - 1)) & ~(alignment - 1);
        size_t padding = aligned_address - current_address;
        size_t total_required = bytes + padding;

        if (offset_ + total_required > size_) {
            return nullptr; // Exhaustion
        }

        uint8_t* allocated_ptr = reinterpret_cast<uint8_t*>(aligned_address);
        offset_ += total_required;
        return allocated_ptr;
    }

    void reset() noexcept {
        offset_ = 0;
    }

    [[nodiscard]] size_t available() const noexcept {
        return size_ > offset_ ? size_ - offset_ : 0;
    }

    [[nodiscard]] bool is_valid() const noexcept {
        return base_ptr_ != nullptr && base_ptr_ != MAP_FAILED;
    }

private:
    size_t size_;
    uint8_t* base_ptr_;
    size_t offset_;

    static constexpr size_t align_to_page_size(size_t sz) noexcept {
        constexpr size_t default_huge_size = 2 * 1024 * 1024; // 2MB
        if (sz < default_huge_size) {
            return default_huge_size;
        }
        return ((sz + default_huge_size - 1) / default_huge_size) * default_huge_size;
    }

    HugePageArena& ori_self(HugePageArena& s) noexcept { return s; }
};

class SlidingWindowFlowLedger {
public:
    explicit SlidingWindowFlowLedger(uint32_t window_size_ms = 1000)
        : window_size_ms_(window_size_ms),
          ema_jitter_ns_(0.0),
          alpha_(0.125), // Standard EWMA smoothing factor
          dropped_packets_(0),
          throttled_(false) {}

    ~SlidingWindowFlowLedger() = default;

    void record_packet(uint64_t timestamp_ns, bool dropped) noexcept {
        if (dropped) {
            dropped_packets_.fetch_add(1, std::memory_order_relaxed);
            throttled_.store(true, std::memory_order_release);
        } else {
            throttled_.store(false, std::memory_order_release);
        }
    }

    void update_jitter(uint64_t sample_jitter_ns) noexcept {
        double current_ema = ema_jitter_ns_.load(std::memory_order_relaxed);
        double new_ema = alpha_ * static_cast<double>(sample_jitter_ns) + (1.0 - alpha_) * current_ema;
        ema_jitter_ns_.store(new_ema, std::memory_order_relaxed);
    }

    [[nodiscard]] double get_ema_jitter_ns() const noexcept {
        return ema_jitter_ns_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] uint64_t get_dropped_count() const noexcept {
        return dropped_packets_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool is_throttled() const noexcept {
        return throttled_.load(std::memory_order_acquire);
    }

private:
    uint32_t window_size_ms_;
    std::atomic<double> ema_jitter_ns_;
    double alpha_;
    std::atomic<uint64_t> dropped_packets_;
    std::atomic<bool> throttled_;
};

} // namespace aether

#endif // AETHER_TYPES_HPP