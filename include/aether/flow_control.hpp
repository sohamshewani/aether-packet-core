#ifndef AETHER_FLOW_CONTROL_HPP
#define AETHER_FLOW_CONTROL_HPP

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <chrono>
#include <new>
#include <array>
#include <system_error>

namespace aether {

// Align structures to 64-byte cache lines to eliminate false sharing.
#ifdef __cpp_lib_hardware_interference_size
using std::hardware_destructive_interference_size;
constexpr std::size_t kCacheLineSize = hardware_destructive_interference_size;
#else
constexpr std::size_t kCacheLineSize = 64;
#endif

// 1. DATA STRUCTURES: Fixed-size PacketDescriptor metadata struct
struct alignas(kCacheLineSize) PacketDescriptor {
    uint64_t sequence_id;
    uint64_t timestamp_ns;
    uint32_t payload_len;
    uint32_t flags;
    uint8_t* payload_ptr;
    std::array<uint8_t, 32> reserved; // Padding to ensure exact cache line alignment and future metadata extension

    PacketDescriptor() noexcept
        : sequence_id(0), timestamp_ns(0), payload_len(0), flags(0), payload_ptr(nullptr), reserved{} {}

    PacketDescriptor(uint64_t seq, uint64_t ts, uint32_t len, uint32_t f, uint8_t* ptr) noexcept
        : sequence_id(seq), timestamp_ns(ts), payload_len(len), flags(f), payload_ptr(ptr), reserved{} {}
};

// 1. DATA STRUCTURES & 4. INVARIANTS: SPSC/MPMC lock-free circular ring buffer with power-of-two capacity
template <typename T, size_t Capacity>
class alignas(kCacheLineSize) LockFreeRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Ring buffer capacity must be strictly a power of two.");

public:
    LockFreeRingBuffer() noexcept : head_(0), tail_(0) {
        // Initialize sequence numbers for MPMC synchronization
        for (size_t i = 0; i < Capacity; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~LockFreeRingBuffer() noexcept = default;

    LockFreeRingBuffer(const LockFreeRingBuffer&) = delete;
    LockFreeRingBuffer& operator=(const LockFreeRingBuffer&) = delete;
    LockFreeRingBuffer(LockFreeRingBuffer&&) = delete;
    LockFreeRingBuffer& operator=(LockFreeRingBuffer&&) = delete;

    bool push(const Titem& item) noexcept {
        Cell* cell;
        uint64_t tail = tail_.load(std::memory_order_relaxed);
        for (;;) {
            cell = &buffer_[tail & kMask_];
            uint32_t seq = cell->sequence.load(std::memory_order_acquire);
            int64_t dif = static_cast<int64_t>(seq) - static_cast<int64_t>(tail);
            if (dif == 0) {
                if (tail_.compare_exchange_weak(tail, tail + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Buffer is full (Backpressure saturation)
                return false;
            } else {
                tail = tail_.load(std::memory_order_relaxed);
            }
        }
        cell->data = item;
        cell->sequence.store(tail + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) noexcept {
        Cell* cell;
        uint64_t head = head_.load(std::memory_order_relaxed);
        for (;;) {
            cell = &buffer_[head & kMask_];
            uint32_t seq = cell->sequence.load(std::memory_order_acquire);
            int64_t dif = static_cast<int64_t>(seq) - static_cast<int64_t>(head + 1);
            if (dif == 0) {
                if (head_.compare_exchange_weak(head, head + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Buffer is empty
                return false;
            } else {
                head = head_.load(std::memory_order_relaxed);
            }
        }
        item = cell->data;
        cell->sequence.store(head + kMask_ + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] size_t size() const noexcept {
        uint64_t head = head_.load(std::memory_order_relaxed);
        uint64_t tail = tail_.load(std::memory_order_relaxed);
        if (tail >= head) {
            return static_cast<size_t>(tail - head);
        }
        return 0;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_relaxed);
    }

    constexpr size_t capacity() const noexcept {
        return Capacity;
    }

private:
    struct Cell {
        std::atomic<uint64_t> sequence;
        T data;
    };

    static constexpr size_t kMask_ = Capacity - 1;
    alignas(kCacheLineSize) std::atomic<uint64_t> head_;
    alignas(kCacheLineSize) std::atomic<uint64_t> tail_;
    alignas(kCacheLineSize) std::array<Cell, Capacity> buffer_;

    using Titem = T;
};

// 1. DATA STRUCTURES: Sliding-window flow control ledger utilizing circular rate-buckets and EMA jitter estimators
class alignas(kCacheLineSize) FlowControlLedger {
public:
    explicit FlowControlLedger(uint64_t window_size_ms = 1000, uint64_t max_rate_pps = 100000) noexcept
        : window_size_ms_(window_size_ms),
          max_rate_pps_(max_rate_pps),
          current_bucket_idx_(0),
          last_timestamp_ms_(0),
          ema_jitter_ns_(0.0),
          dropped_packets_counter_(0),
          throttled_state_(false) {
        for (auto& bucket : buckets_) {
            bucket.packet_count.store(0, std::memory_order_relaxed);
            bucket.timestamp_ms.store(0, std::memory_order_relaxed);
        }
    }

    ~FlowControlLedger() noexcept = default;

    FlowControlLedger(const FlowControlLedger&) = delete;
    FlowControlLedger& operator=(const FlowControlLedger&) = delete;
    FlowControlLedger(FlowControlLedger&&) = delete;
    FlowControlLedger& operator=(FlowControlLedger&&) = delete;

    bool admit_packet(uint64_t current_timestamp_ms, uint64_t inter_arrival_jitter_ns) noexcept {
        update_window(current_timestamp_ms);
        update_jitter_ema(static_cast<double>(inter_arrival_jitter_ns));

        uint64_t total_packets = compute_window_total();
        if (total_packets >= max_rate_pps_) {
            throttled_state_.store(true, std::memory_order_relaxed);
            dropped_packets_counter_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        throttled_state_.store(false, std::memory_order_relaxed);
        size_t idx = current_bucket_idx_ % kNumBuckets;
        buckets_[idx].packet_count.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    void update_jitter_ema(double new_sample_ns) noexcept {
        if (ema_jitter_ns_ == 0.0) {
            ema_jitter_ns_ = new_sample_ns;
        } else {
            ema_jitter_ns_ = kAlpha * new_sample_ns + (1.0 - kAlpha) * ema_jitter_ns_;
        }
    }

    [[nodiscard]] double get_ema_jitter_ns() const noexcept {
        return ema_jitter_ns_;
    }

    [[nodiscard]] uint64_t get_dropped_count() const noexcept {
        return dropped_packets_counter_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool is_throttled() const noexcept {
        return throttled_state_.load(std::memory_order_relaxed);
    }

    void reset_counters() noexcept {
        for (auto& bucket : buckets_) {
            bucket.packet_count.store(0, std::memory_order_relaxed);
            bucket.timestamp_ms.store(0, std::memory_order_relaxed);
        }
        dropped_packets_counter_.store(0, std::memory_order_relaxed);
        throttled_state_.store(false, std::memory_order_relaxed);
        ema_jitter_ns_ = 0.0;
    }

private:
    struct alignas(kCacheLineSize) RateBucket {
        std::atomic<uint64_t> packet_count;
        std::atomic<uint64_t> timestamp_ms;
    };

    static constexpr size_t kNumBuckets = 10;
    static constexpr double kAlpha = 0.125; // Exponential Moving Average smoothing factor

    void update_window(uint64_t current_timestamp_ms) noexcept {
        if (current_timestamp_ms <= last_timestamp_ms_) {
            return;
        }
        uint64_t elapsed = current_timestamp_ms - last_timestamp_ms_;
        if (elapsed >= (window_size_ms_ / kNumBuckets)) {
            size_t steps = static_cast<size_t>(elapsed / (window_size_ms_ / kNumBuckets));
            for (size_t i = 0; i < steps && i < kNumBuckets; ++i) {
                current_bucket_idx_ = (current_bucket_idx_ + 1) % kNumBuckets;
                size_t idx = current_bucket_idx_;
                buckets_[idx].packet_count.store(0, std::memory_order_relaxed);
                buckets_[idx].timestamp_ms.store(current_timestamp_ms, std::memory_order_relaxed);
            }
            last_timestamp_ms_ = current_timestamp_ms;
        }
    }

    [[nodiscard]] uint64_t compute_window_total() const noexcept {
        uint64_t total = 0;
        for (size_t i = 0; i < kNumBuckets; ++i) {
            total += buckets_[i].packet_count.load(std::memory_order_relaxed);
        }
        return total;
    }

    uint64_t window_size_ms_;
    uint64_t max_rate_pps_;
    size_t current_bucket_idx_;
    uint64_t last_timestamp_ms_;
    double ema_jitter_ns_;
    std::atomic<uint64_t> dropped_packets_counter_;
    std::atomic<bool> throttled_state_;
    std::array<RateBucket, kNumBuckets> buckets_;
};

// 5. FAILURE RECOVERY MODES: Flow Control Manager consolidating system state and recovery hooks
class alignas(kCacheLineSize) FlowControlManager {
public:
    explicit FlowControlManager(uint64_t window_size_ms = 1000, uint64_t max_rate_pps = 100000) noexcept
        : ledger_(window_size_ms, max_rate_pps),
          system_healthy_(true) {}

    ~FlowControlManager() noexcept = default;

    FlowControlManager(const FlowControlManager&) = delete;
    FlowControlManager& operator=(const FlowControlManager&) = delete;
    FlowControlManager(FlowControlManager&&) = delete;
    FlowControlManager& operator=(FlowControlManager&&) = delete;

    [[nodiscard]] bool process_packet_admission(uint64_t timestamp_ms, uint64_t jitter_ns) noexcept {
        bool admitted = ledger_.admit_packet(timestamp_ms, jitter_ns);
        if (!admitted) {
            handle_ring_saturation();
        }
        return admitted;
    }

    void handle_ring_saturation() noexcept {
        // Trigger sliding-window throttle signals without stalling event loops
        system_healthy_.store(false, std::memory_order_relaxed);
    }

    void handle_arena_exhaustion_graceful_degradation() noexcept {
        // Priority-based drop policy flag activation
        system_healthy_.store(false, std::memory_order_relaxed);
    }

    [[nodiscard]] bool is_system_healthy() const noexcept {
        return system_healthy_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] FlowControlLedger& get_ledger() noexcept {
        return ledger_;
    }

private:
    FlowControlLedger ledger_;
    std::atomic<bool> system_healthy_;
};

} // namespace aether

#endif // AETHER_FLOW_CONTROL_HPP