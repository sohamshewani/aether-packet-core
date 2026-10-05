#include "engine.hpp"

#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cerrno>
#include <iostream>
#include <new>

namespace aether {
namespace core {

// ============================================================================
// MemoryArena Implementation
// ============================================================================

MemoryArena::MemoryArena(size_t total_size)
    : capacity_(total_size), offset_(0), mapped_memory_(nullptr) {
    // Ensure capacity is aligned to 2MB hugepage boundary
    const size_t hugepage_size = 2UL * 1024UL * 1024UL;
    if (capacity_ % hugepage_size != 0) {
        capacity_ = ((capacity_ + hugepage_size - 1) / hugepage_size) * hugepage_size;
    }

    void* addr = mmap(
        nullptr,
        capacity_,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
        -1,
        0
    );

    if (addr == MAP_FAILED) {
        // Fallback to standard anonymous mmap if hugepages are unavailable
        addr = mmap(
            nullptr,
            capacity_,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS,
            -1,
            0
        );
        if (addr == MAP_FAILED) {
            throw std::system_error(errno, std::generic_category(), "MemoryArena mmap failed");
        }
    }

    mapped_memory_ = static_cast<uint8_t*>(addr);
}

MemoryArena::~MemoryArena() {
    if (mapped_memory_ && mapped_memory_ != MAP_FAILED) {
        munmap(mapped_memory_, capacity_);
    }
}

uint8_t* MemoryArena::allocate(size_t size) {
    // Align request to 64 bytes
    const size_t alignment = 64;
    size_t aligned_size = (size + alignment - 1) & ~(alignment - 1);

    size_t current_offset = offset_.load(std::memory_order_relaxed);
    while (true) {
        if (current_offset + aligned_size > capacity_) {
            return nullptr; // Arena exhausted
        }
        if (offset_.compare_exchange_weak(
                current_offset,
                current_offset + aligned_size,
                std::memory_order_release,
                std::memory_order_relaxed)) {
            return mapped_memory_ + current_offset;
        }
    }
}

void MemoryArena::reset() {
    offset_.store(0, std::memory_order_release);
}

size_t MemoryArena::available() const {
    size_t current_offset = offset_.load(std::memory_order_relaxed);
    return (current_offset < capacity_) ? (capacity_ - current_offset) : 0;
}

// ============================================================================
// FlowControlLedger Implementation
// ============================================================================

FlowControlLedger::FlowControlLedger(uint64_t window_size_us)
    : window_size_us_(window_size_us),
      smoothed_rtt_us_(1000.0),
      jitter_us_(0.0),
      dropped_packets_(0),
      passed_packets_(0) {}

bool FlowControlLedger::admit_packet(uint64_t current_time_us, uint32_t priority) {
    // Sliding window cleanup of old buckets
    while (!rate_buckets_.empty() && (current_time_us - rate_buckets_.front().timestamp_us > window_size_us_)) {
        rate_buckets_.pop_front();
    }

    // Compute current load within window
    uint64_t current_window_volume = 0;
    for (const auto& bucket : rate_buckets_) {
        current_window_volume += bucket.byte_count;
    }

    // Dynamic threshold based on priority
    uint64_t capacity_limit = 1024 * 1024 * 100; // 100MB baseline window limit
    if (priority == 0 && current_window_volume > (capacity_limit * 80 / 100)) {
        // Drop low priority telemetry/bulk traffic early
        dropped_packets_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    if (current_window_volume >= capacity_limit) {
        dropped_packets_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    passed_packets_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

void FlowControlLedger::record_sample(uint64_t current_time_us, size_t bytes, uint64_t rtt_us) {
    if (rate_buckets_.empty() || rate_buckets_.back().timestamp_us != current_time_us) {
        rate_buckets_.push_back({current_time_us, bytes});
    } else {
        rate_buckets_.back().byte_count += bytes;
    }

    // Exponential Moving Average (EMA) for Jitter and RTT
    const double alpha = 0.125;
    double rtt_diff = static_cast<double>(rtt_us) > smoothed_rtt_us_ ? 
                      static_cast<double>(rtt_us) - smoothed_rtt_us_ : 
                      smoothed_rtt_us_ - static_cast<double>(rtt_us);

    smoothed_rtt_us_ = (1.0 - alpha) * smoothed_rtt_us_ + alpha * static_cast<double>(rtt_us);
    jitter_us_ = (1.0 - alpha) * jitter_us_ + alpha * rtt_diff;
}

uint64_t FlowControlLedger::dropped_count() const {
    return dropped_packets_.load(std::memory_order_relaxed);
}

uint64_t FlowControlLedger::passed_count() const {
    return passed_packets_.load(std::memory_order_relaxed);
}

// ============================================================================
// PacketEngine Implementation
// ============================================================================

PacketEngine::PacketEngine(int socket_fd, size_t arena_size)
    : socket_fd_(socket_fd),
      epoll_fd_(-1),
      running_(false),
      arena_(arena_size),
      ledger_(1000000) { // 1 second sliding window
    
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == -1) {
        throw std::system_error(errno, std::generic_category(), "epoll_create1 failed");
    }

    // Set socket to non-blocking
    int flags = fcntl(socket_fd_, F_GETFL, 0);
    if (flags == -1) flags = 0;
    if (fcntl(socket_fd_, F_SETFL, flags | O_NONBLOCK) == -1) {
        throw std::system_error(errno, std::generic_category(), "fcntl non-blocking failed");
    }

    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLERR;
    ev.data.fd = socket_fd_;

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, socket_fd_, &ev) == -1) {
        throw std::system_error(errno, std::generic_category(), "epoll_ctl failed");
    }
}

PacketEngine::~PacketEngine() {
    stop();
    if (epoll_fd_ != -1) {
        close(epoll_fd_);
    }
}

void PacketEngine::start() {
    running_.store(true, std::memory_order_release);
    worker_thread_ = std::thread(&PacketEngine::event_loop, this);
}

void PacketEngine::stop() {
    if (running_.exchange(false, std::memory_order_acq_rel)) {
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
    }
}

void PacketEngine::event_loop() {
    const int max_events = 64;
    struct epoll_event events[max_events];

    while (running_.load(std::memory_order_acquire)) {
        int nfds = epoll_wait(epoll_fd_, events, max_events, 10);
        if (nfds < 0) {
            if (errno == EINTR) {
                continue;
            }
            break; // Fatal epoll error
        }

        for (int i = 0; i < nfds; ++i) {
            if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLRDHUP)) {
                // Handle socket error or hangup
                running_.store(false, std::memory_order_release);
                break;
            }

            if (events[i].events & EPOLLIN) {
                poll_socket_rx();
            }
        }
    }
}

void PacketEngine::poll_socket_rx() {
    // Batch receive using recvmmsg or standard recv depending on availability
    const size_t batch_size = 32;
    struct mmsghdr msgs[batch_size];
    struct iovec iovecs[batch_size];
    
    // Pre-allocate storage for headers if needed
    for (size_t i = 0; i < batch_size; ++i) {
        uint8_t* slot = arena_.allocate(2048); // Standard MTU slot
        if (!slot) {
            // Arena exhausted: trigger graceful degradation
            arena_exhaustion_recovery();
            break;
        }

        std::memset(&iovecs[i], 0, sizeof(iovecs[i]));
        iovecs[i].iov_base = slot;
        iovecs[i].iov_len = 2048;

        std::memset(&msgs[i], 0, sizeof(msgs[i]));
        msgs[i].msg_hdr.msg_iov = &iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }

    // Non-blocking read loop handling EAGAIN/EWOULDBLOCK
    while (running_.load(std::memory_order_acquire)) {
        int retval = recvmmsg(socket_fd_, msgs, batch_size, MSG_DONTWAIT, nullptr);
        if (retval < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break; // No more packets available currently
            }
            if (errno == EINTR) {
                continue; // Retry interrupted syscall
            }
            // Handle unexpected socket failure
            break;
        }

        if (retval == 0) {
            break;
        }

        uint64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();

        for (int i = 0; i < retval; ++i) {
            size_t bytes_received = msgs[i].msg_len;
            uint8_t* packet_data = static_cast<uint8_t*>(iovecs[i].iov_base);

            // Flow control ledger check
            if (!ledger_.admit_packet(now_us, 1)) {
                // Packet dropped by ledger policy, reclaim arena memory implicitly on reset
                continue;
            }

            // Construct descriptor mapped directly over zero-copy memory
            PacketDescriptor desc;
            desc.sequence = ledger_.passed_count();
            desc.timestamp_us = now_us;
            desc.length = static_cast<uint16_t>(bytes_received);
            desc.flags = 0;
            desc.payload = packet_data;

            // Push to ring buffer
            while (!rx_ring_.push(desc)) {
                // Ring buffer saturation: backpressure handling
                backpressure_handler();
                // Yield thread slice to allow consumer progress
                std::this_thread::yield();
            }

            ledger_.record_sample(now_us, bytes_received, 50); // Simulated RTT sample
        }
        
        break; // Processed current batch edge-trigger
    }
}

void PacketEngine::backpressure_handler() {
    // Increment telemetry for ring buffer saturation
    ledger_.admit_packet(0, 0); // Triggers drop metric increment
}

void PacketEngine::arena_exhaustion_recovery() {
    // Reset arena safely when single writer consumption falls behind
    arena_.reset();
}

} // namespace core
} // namespace aether