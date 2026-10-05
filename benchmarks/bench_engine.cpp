#include <chrono>
#include <iostream>
#include <vector>
#include <numeric>
#include <algorithm>
#include <iomanip>
#include <cstdint>
#include <cstring>

// -----------------------------------------------------------------------------
// Mocking the 'aether-packet-core' types and functions for a self-contained,
// highly optimized, and runnable microbenchmark.
// -----------------------------------------------------------------------------
namespace aether::packet_core {

constexpr std::size_t MAX_PACKET_SIZE = 2048;

struct Packet {
    uint8_t data[MAX_PACKET_SIZE];
    uint16_t length;
    uint16_t flags;
};

enum class Status {
    Success,
    Drop,
    Error
};

class PacketPipeline {
public:
    PacketPipeline() = default;
    ~PacketPipeline() = default;

    // Inline process function simulating deep packet inspection and forwarding
    // logic native to aether-packet-core.
    inline Status process(Packet& pkt) noexcept {
        // Simulate minor payload transformation / header check
        if (pkt.length == 0 || pkt.length > MAX_PACKET_SIZE) {
            return Status::Error;
        }
        
        // Dummy stateless packet parsing loop
        uint32_t checksum = 0;
        for (uint16_t i = 0; i < pkt.length; ++i) {
            checksum += pkt.data[i];
        }

        // Drop condition based on checksum bit
        if ((checksum & 0xFF) == 0x42) {
            return Status::Drop;
        }

        // Simulate header rewrite
        pkt.flags |= 0x0001;
        return Status::Success;
    }
};

} // namespace aether::packet_core

// -----------------------------------------------------------------------------
// Benchmark Harness
// -----------------------------------------------------------------------------
int main() {
    using namespace aether::packet_core;

    constexpr uint64_t WARMUP_ITERATIONS = 100'000;
    constexpr uint64_t MEASUREMENT_ITERATIONS = 5'000'000;

    PacketPipeline pipeline;
    Packet test_packet{};
    test_packet.length = 128;
    test_packet.flags = 0;
    std::memset(test_packet.data, 0xAB, test_packet.length);

    // Pre-allocate latency tracking vector to avoid dynamic allocations during measurement
    std::vector<uint64_t> latencies_ns;
    latencies_ns.reserve(MEASUREMENT_ITERATIONS);

    // -------------------------------------------------------------------------
    // Warmup Phase
    // -------------------------------------------------------------------------
    for (uint64_t i = 0; i < WARMUP_ITERATIONS; ++i) {
        volatile Status st = pipeline.process(test_packet);
        (void)st;
    }

    // -------------------------------------------------------------------------
    // Measurement Phase (Throughput & Latency)
    // -------------------------------------------------------------------------
    auto start_time = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 0; i < MEASUREMENT_ITERATIONS; ++i) {
        auto op_start = std::chrono::high_resolution_clock::now();
        
        Status st = pipeline.process(test_packet);
        
        auto op_end = std::chrono::high_resolution_clock::now();
        
        // Prevent dead-code elimination
        if (st == Status::Error) {
            test_packet.flags ^= 0x8000;
        }

        auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
        latencies_ns.push_back(static_cast<uint64_t>(duration_ns));
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_duration_s = std::chrono::duration<double>(end_time - start_time).count();

    // -------------------------------------------------------------------------
    // Statistical Analysis
    // -------------------------------------------------------------------------
    double ops_sec = static_cast<double>(MEASUREMENT_ITERATIONS) / total_duration_s;

    // Sort latencies for percentile calculations
    std::sort(latencies_ns.begin(), latencies_ns.end());

    uint64_t p50 = latencies_ns[MEASUREMENT_ITERATIONS * 50 / 100];
    uint64_t p90 = latencies_ns[MEASUREMENT_ITERATIONS * 90 / 100];
    uint64_t p99 = latencies_ns[MEASUREMENT_ITERATIONS * 99 / 100];
    uint64_t p999 = latencies_ns[static_cast<size_t>(MEASUREMENT_ITERATIONS * 99.9 / 100)];
    
    uint64_t min_lat = latencies_ns.front();
    uint64_t max_lat = latencies_ns.back();
    
    double avg_lat = static_cast<double>(
        std::accumulate(latencies_ns.begin(), latencies_ns.end(), uint64_t{0})
    ) / MEASUREMENT_ITERATIONS;

    // -------------------------------------------------------------------------
    // Formatted Tabular Output
    // -------------------------------------------------------------------------
    std::cout << "\n";
    std::cout << "===================================================================\n";
    std::cout << "           AETHER-PACKET-CORE MICROBENCHMARK RESULTS              \n";
    std::cout << "===================================================================\n";
    std::cout << " Metric                     | Value                                \n";
    std::cout << "----------------------------+--------------------------------------\n";
    std::cout << " Total Iterations           | " << std::setw(15) << MEASUREMENT_ITERATIONS << "                   \n";
    std::cout << " Total Time (s)             | " << std::setw(15) << std::fixed << std::setprecision(4) << total_duration_s << " s                \n";
    std::cout << " Throughput (ops/sec)       | " << std::setw(15) << std::fixed << std::setprecision(2) << ops_sec << " ops/sec         \n";
    std::cout << "----------------------------+--------------------------------------\n";
    std::cout << " Min Latency                | " << std::setw(10) << min_lat << " ns                        \n";
    std::cout << " Mean Latency               | " << std::setw(10) << std::fixed << std::setprecision(2) << avg_lat << " ns                        \n";
    std::cout << " p50 Latency                | " << std::setw(10) << p50 << " ns                        \n";
    std::cout << " p90 Latency                | " << std::setw(10) << p90 << " ns                        \n";
    std::cout << " p99 Latency                | " << std::setw(10) << p99 << " ns                        \n";
    std::cout << " p99.9 Latency              | " << std::setw(10) << p999 << " ns                        \n";
    std::cout << " Max Latency                | " << std::setw(10) << max_lat << " ns                        \n";
    std::cout << "===================================================================\n";
    std::cout << std::endl;

    return 0;
}