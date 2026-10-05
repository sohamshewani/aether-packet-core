/**
 * @file test_aether_packet_core.cpp
 * @brief Exhaustive Unit, Invariant, and Concurrent Stress-Test Suite for Aether Packet Core
 * 
 * Compiled via C++20 standard thread pools, atomic primitives, and invariant assertions.
 * Output strictly raw C++ source code.
 */

#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <cassert>
#include <cstring>
#include <memory>
#include <queue>
#include <mutex>
#include <random>
#include <algorithm>
#include <bitset>
#include <span>

// ============================================================================
// SIMULATED AETHER PACKET CORE IMPLEMENTATION
// ============================================================================

namespace aether {

constexpr size_t MAX_PACKET_SIZE = 9216; // Jumbo frame support
constexpr size_t RING_BUFFER_SIZE = 4096; // Must be power of 2

enum class PacketType : uint8_t {
    DATA = 0x01,
    CONTROL = 0x02,
    HEARTBEAT = 0x03,
    POISON = 0xFF
};

struct PacketHeader {
    uint32_t magic;
    uint32_t sequence_id;
    uint16_t length;
    PacketType type;
    uint8_t flags;
};

struct Packet {
    PacketHeader header;
    uint8_t payload[MAX_PACKET_SIZE];

    size_t total_size() const {
        return sizeof(PacketHeader) + header.length;
    }
};

class PacketRingBuffer {
private:
    struct Node {
        Packet packet;
        std::atomic<bool> ready{false};
    };

    Node buffer_[RING_BUFFER_SIZE];
    std::atomic<size_t> head_{0};
    std::atomic<size_t> tail_{0};

public:
    bool push(const Packet& pkt) {
        size_t current_head = head_.load(std::memory_order_relaxed);
        size_t current_tail = tail_.load(std::memory_order_relaxed);

        if ((current_head - current_tail) >= RING_BUFFER_SIZE) {
            return false; // Full
        }

        if (!head_.compare_exchange_weak(current_head, current_head + 1, 
                                         std::memory_order_release, 
                                         std::memory_order_relaxed)) {
            return false;
        }

        size_t index = current_head & (RING_BUFFER_SIZE - 1);
        std::memcpy(&buffer_[index].packet, &pkt, sizeof(Packet));
        buffer_[index].ready.store(true, std::memory_order_release);
        return true;
    }

    bool pop(Packet& pkt) {
        size_t current_tail = tail_.load(std::memory_order_relaxed);
        size_t current_head = head_.load(std::memory_order_acquire);

        if (current_tail == current_head) {
            return false; // Empty
        }

        size_t index = current_tail & (RING_BUFFER_SIZE - 1);
        if (!buffer_[index].ready.load(std::memory_order_acquire)) {
            return false; // Not yet written fully
        }

        std::memcpy(&pkt, &buffer_[index].packet, sizeof(Packet));
        buffer_[index].ready.store(false, std::memory_order_relaxed);
        
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    void reset() {
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
        for (auto& node : buffer_) {
            node.ready.store(false, std::memory_order_relaxed);
        }
    }

    size_t size() const {
        size_t h = head_.load(std::memory_order_acquire);
        size_t t = tail_.load(std::memory_order_acquire);
        return (h >= t) ? (h - t) : 0;
    }
};

class PacketCoreEngine {
private:
    PacketRingBuffer rx_ring_;
    PacketRingBuffer tx_ring_;
    std::atomic<uint32_t> dropped_packets_{0};
    std::atomic<uint32_t> processed_packets_{0};
    std::atomic<bool> is_running_{false};

public:
    void start() {
        is_running_.store(true, std::memory_order_release);
    }

    void stop() {
        is_running_.store(false, std::memory_order_release);
    }

    bool inject_rx(const Packet& pkt) {
        if (!is_running_.load(std::memory_order_relaxed)) return false;
        
        // Invariant check: packet length bounds
        if (pkt.header.length > MAX_PACKET_SIZE || pkt.header.magic != 0xAE7C0001) {
            dropped_packets_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        bool success = rx_ring_.push(pkt);
        if (!success) {
            dropped_packets_.fetch_add(1, std::memory_order_relaxed);
        }
        return success;
    }

    bool poll_tx(Packet& pkt) {
        if (!is_running_.load(std::memory_order_relaxed)) return false;
        return tx_ring_.pop(pkt);
    }

    void process_cycle() {
        Packet pkt;
        if (rx_ring_.pop(pkt)) {
            // Core processing logic (e.g., echo or mutate)
            pkt.header.flags |= 0x80; // Mark processed
            while (!tx_ring_.push(pkt) && is_running_.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            processed_packets_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void reset_state() {
        rx_ring_.reset();
        tx_ring_.reset();
        dropped_packets_.store(0, std::memory_order_relaxed);
        processed_packets_.store(0, std::memory_order_relaxed);
    }

    uint32_t get_dropped() const { return dropped_packets_.load(std::memory_order_relaxed); }
    uint32_t get_processed() const { return processed_packets_.load(std::memory_order_relaxed); }
};

} // namespace aether

// ============================================================================
// TEST SUITE IMPLEMENTATION
// ============================================================================

#define RUN_TEST(test_func) \
    do { \
        std::cout << "[RUNNING] " << #test_func << "..." << std::endl; \
        try { \
            test_func(); \
            std::cout << "[PASSED]  " << #test_func << "\n" << std::endl; \
        } catch (const std::exception& e) { \
            std::cerr << "[FAILED]  " << #test_func << " with exception: " << e.what() << "\n" << std::endl; \
            std::exit(1); \
        } \
    } while(0)

// 1. Boundary & Edge-Case Assertions
void test_packet_boundaries() {
    aether::PacketCoreEngine engine;
    engine.start();

    // Edge Case A: Zero-length packet with valid magic
    aether::Packet zero_pkt{};
    zero_pkt.header.magic = 0xAE7C0001;
    zero_pkt.header.sequence_id = 1;
    zero_pkt.header.length = 0;
    zero_pkt.header.type = aether::PacketType::DATA;

    assert(engine.inject_rx(zero_pkt) == true);
    
    // Edge Case B: Max-length packet
    aether::Packet max_pkt{};
    max_pkt.header.magic = 0xAE7C0001;
    max_pkt.header.sequence_id = 2;
    max_pkt.header.length = aether::MAX_PACKET_SIZE;
    max_pkt.header.type = aether::PacketType::DATA;
    std::memset(max_pkt.payload, 0xAA, aether::MAX_PACKET_SIZE);

    assert(engine.inject_rx(max_pkt) == true);

    // Edge Case C: Over-sized packet (Boundary violation)
    aether::Packet invalid_pkt{};
    invalid_pkt.header.magic = 0xAE7C0001;
    invalid_pkt.header.sequence_id = 3;
    invalid_pkt.header.length = aether::MAX_PACKET_SIZE + 1; // Out of bounds
    invalid_pkt.header.type = aether::PacketType::DATA;

    assert(engine.inject_rx(invalid_pkt) == false);
    assert(engine.get_dropped() == 1);

    // Edge Case D: Invalid Magic Number
    aether::Packet bad_magic_pkt{};
    bad_magic_pkt.header.magic = 0xDEADBEEF;
    bad_magic_pkt.header.sequence_id = 4;
    bad_magic_pkt.header.length = 64;
    bad_magic_pkt.header.type = aether::PacketType::DATA;

    assert(engine.inject_rx(bad_magic_pkt) == false);
    assert(engine.get_dropped() == 2);

    engine.stop();
}

// 2. Invariant Verification
void test_core_invariants() {
    aether::PacketCoreEngine engine;
    engine.start();

    // Invariant: Sequence preservation through pipeline execution
    const uint32_t target_seq = 9999;
    aether::Packet pkt{};
    pkt.header.magic = 0xAE7C0001;
    pkt.header.sequence_id = target_seq;
    pkt.header.length = 128;
    pkt.header.type = aether::PacketType::HEARTBEAT;
    std::memset(pkt.payload, 0x55, 128);

    assert(engine.inject_rx(pkt) == true);
    
    // Run cycle
    engine.process_cycle();

    aether::Packet out_pkt{};
    assert(engine.poll_tx(out_pkt) == true);

    // Verify invariants
    assert(out_pkt.header.sequence_id == target_seq);
    assert(out_pkt.header.magic == 0xAE7C0001);
    assert((out_pkt.header.flags & 0x80) == 0x80); // Processed flag set
    assert(engine.get_processed() == 1);

    engine.stop();
}

// 3. Multi-threaded Contention & Race Condition Stress Testing
void test_concurrent_stress() {
    aether::PacketCoreEngine engine;
    engine.start();

    constexpr int NUM_PRODUCERS = 4;
    constexpr int NUM_CONSUMERS = 4;
    constexpr int NUM_WORKERS = 2;
    constexpr int PACKETS_PER_PRODUCER = 5000;

    std::atomic<bool> start_signal{false};
    std::atomic<uint64_t> total_injected{0};
    std::atomic<uint64_t> total_popped{0};

    // Producer threads (Injecting RX)
    std::vector<std::thread> producers;
    for (int i = 0; i < NUM_PRODUCERS; ++i) {
        producers.emplace_back([&, i]() {
            while (!start_signal.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (int j = 0; j < PACKETS_PER_PRODUCER; ++j) {
                aether::Packet pkt{};
                pkt.header.magic = 0xAE7C0001;
                pkt.header.sequence_id = (i * PACKETS_PER_PRODUCER) + j;
                pkt.header.length = 64;
                pkt.header.