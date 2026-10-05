#ifndef AETHER_ARENA_HPP
#define AETHER_ARENA_HPP

#include <cstdint>
#include <cstddef>
#include <new>
#include <atomic>
#include <system_error>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>

namespace aether {

enum class ArenaError : int {
    Success = 0,
    HugePageAllocationFailed,
    OutOfMemory,
    InvalidAlignment,
    InvalidSize
};

class ArenaErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override { return "AetherArena"; }
    std::string message(int ev) const override {
        switch (static_cast<ArenaError>(ev)) {
            case ArenaError::Success: return "Success";
            case ArenaError::HugePageAllocationFailed: return "Failed to allocate 2MB hugepage via mmap";
            case ArenaError::OutOfMemory: return "Arena capacity exhausted";
            case ArenaError::InvalidAlignment: return "Requested alignment is invalid or not a power of two";
            case ArenaError::InvalidSize: return "Requested allocation size is invalid";
            default: return "Unknown Arena Error";
        }
    }
};

inline const std::error_category& get_arena_error_category() noexcept {
    static ArenaErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(ArenaError e) noexcept {
    return {static_cast<int>(e), get_arena_error_category()};
}

} // namespace aether

namespace std {
template <>
struct is_error_code_enum<aether::ArenaError> : true_type {};
} // namespace std

namespace aether {

class alignas(64) PacketArena {
public:
    static constexpr size_t HUGEPAGE_SIZE = 2 * 1024 * 1024; // 2MB Hugepage
    static constexpr size_t CACHE_LINE_SIZE = 64;

    explicit PacketArena(size_t capacity_bytes = HUGEPAGE_SIZE) 
        : capacity_(capacity_bytes), 
          offset_(0), 
          base_address_(nullptr), 
          allocation_failed_(false) {
        
        if (capacity_ == 0) {
            allocation_failed_ = true;
            return;
        }

        // Round up capacity to page boundaries if necessary, minimum 2MB for hugepages
        if (capacity_ < HUGEPAGE_SIZE) {
            capacity_ = HUGEPAGE_SIZE;
        }

        void* ptr = mmap(
            nullptr,
            capacity_,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
            -1,
            0
        );

        if (ptr == MAP_FAILED) {
            // Fallback to standard 4KB pages if hugepages are unavailable, while preserving alignment semantics
            ptr = mmap(
                nullptr,
                capacity_,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS,
                -1,
                0
            );

            if (ptr == MAP_FAILED) {
                allocation_failed_ = true;
                base_address_ = nullptr;
                return;
            }
        }

        base_address_ = static_cast<std::byte*>(ptr);
        allocation_failed_ = false;
    }

    ~PacketArena() noexcept {
        if (base_address_ != nullptr && base_address_ != MAP_FAILED) {
            munmap(base_address_, capacity_);
            base_address_ = nullptr;
        }
    }

    PacketArena(const PacketArena&) = delete;
    PacketArena& operator=(const PacketArena&) = delete;

    PacketArena(PacketArena&& other) noexcept 
        : capacity_(other.capacity_),
          offset_(other.offset_.load(std::memory_order_relaxed)),
          base_address_(other.base_address_),
          allocation_failed_(other.allocation_failed_) {
        other.base_address_ = nullptr;
        other.capacity_ = 0;
        other.offset_.store(0, std::memory_order_relaxed);
        other.allocation_failed_ = true;
    }

    PacketArena& operator=(PacketArena&& other) noexcept {
        if (this != &other) {
            if (base_address_ != nullptr && base_address_ != MAP_FAILED) {
                munmap(base_address_, capacity_);
            }
            capacity_ = other.capacity_;
            offset_.store(other.offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            base_address_ = other.base_address_;
            allocation_failed_ = other.allocation_failed_;

            other.base_address_ = nullptr;
            other.capacity_ = 0;
            other.offset_.store(0, std::memory_order_relaxed);
            other.allocation_failed_ = true;
        }
        return *this;
    }

    bool is_valid() const noexcept {
        return !allocation_failed_ && base_address_ != nullptr;
    }

    template <typename T, typename... Args>
    T* allocate_object(Args&&... args) {
        void* mem = allocate(sizeof(T), alignof(T));
        if (mem == nullptr) {
            return nullptr;
        }
        return ::new (mem) T(std::forward<Args>(args)...);
    }

    void* allocate(size_t size, size_t alignment = CACHE_LINE_SIZE) noexcept {
        if (allocation_failed_ || base_address_ == nullptr) {
            return nullptr;
        }

        if (size == 0) {
            return nullptr;
        }

        if ((alignment & (alignment - 1)) != 0) {
            return nullptr; // Alignment must be a power of two
        }

        size_t current_offset = offset_.load(std::memory_order_relaxed);

        while (true) {
            std::byte* current_ptr = base_address_ + current_offset;
            size_t space = capacity_ - current_offset;
            void* aligned_ptr = current_ptr;
            size_t space_val = space;

            if (std::align(alignment, size, aligned_ptr, space_val) == nullptr) {
                return nullptr; // Out of memory in current arena chunk
            }

            size_t padding = static_cast<size_t>(static_cast<std::byte*>(aligned_ptr) - current_ptr);
            size_t next_offset = current_offset + padding + size;

            if (next_offset > capacity_) {
                return nullptr;
            }

            if (offset_.compare_exchange_weak(
                current_offset,
                next_offset,
                std::memory_order_release,
                std::memory_order_relaxed)) {
                return aligned_ptr;
            }
        }
    }

    void reset() noexcept {
        offset_.store(0, std::memory_order_release);
    }

    size_t bytes_allocated() const noexcept {
        return offset_.load(std::memory_order_relaxed);
    }

    size_t capacity() const noexcept {
        return capacity_;
    }

    std::byte* base() const noexcept {
        return base_address_;
    }

private:
    size_t capacity_;
    std::atomic<size_t> offset_;
    std::byte* base_address_;
    bool allocation_failed_;
};

} // namespace aether

#endif // AETHER_ARENA_HPP