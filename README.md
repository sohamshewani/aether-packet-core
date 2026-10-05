# aether-packet-core
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/aether-packet-core/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/aether-packet-core/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/aether-packet-core) [![Stars](https://img.shields.io/github/stars/sohamshewani/aether-packet-core?style=flat)](https://github.com/sohamshewani/aether-packet-core)

## 1. Executive Summary & Problem Space

`aether-packet-core` is an ultra-low-latency, high-throughput packet processing engine engineered in modern C++20 for high-frequency networking, cloud-native service meshes, and carrier-grade edge routers. 

Traditional networking stacks suffer from severe performance degradation at line rate due to three fundamental bottlenecks:
1. **Dynamic Memory Allocation Overhead:** Standard heap allocators (`malloc`/`new`) introduce unpredictable system call latencies and severe lock contention under multi-threaded load.
2. **Cache Coherency Misses:** Standard STL containers scatter nodes across non-contiguous memory, causing constant CPU cache invalidations.
3. **Synchronization Latency:** Mutex-based queueing structures trigger context switches and kernel-level scheduling overheads when crossing thread boundaries.

`aether-packet-core` solves these bottlenecks by implementing a zero-allocation, lock-free architecture utilizing custom memory arenas, lock-free multi-producer multi-consumer (MPMC) ring buffers, and cache-aligned packet filtering mechanisms.

---

## 2. Structural ASCII Architecture / Dataflow Diagram

```
 +------------------------------------------------------------------------+
 |                         RAW PACKET INGESTION                           |
 +------------------------------------------------------------------------+
                                     |
                                     v
 +------------------------------------------------------------------------+
 |                       Packet Filter (`packet_filter.hpp`)              |
 |  - Layer 2/3/4 Header Inspection                                       |
 |  - Zero-Copy Bitmask Rule Matching                                     |
 +------------------------------------------------------------------------+
                                     |
                                     v
 +------------------------------------------------------------------------+
 |                       Flow Control (`flow_control.hpp`)                |
 |  - Rate Limiting & Token Bucket Enforcement                            |
 |  - Backpressure Signaling                                              |
 +------------------------------------------------------------------------+
                                     |
                                     v
 +------------------------------------------------------------------------+
 |                       Ring Buffer (`ring_buffer.hpp`)                  |
 |  - Lock-Free MPMC Atomic SPSC/MPMC Channelling                         |
 |  - Cache-Aligned Ring Slots (False Sharing Prevention)                 |
 +------------------------------------------------------------------------+
                                     |
       +-----------------------------+-----------------------------+
       | (Allocated via Arena)                                     | (Allocated via Arena)
       v                                                           v
 +---------------------------------------+   +---------------------------------------+
 |          Memory Arena (Worker 1)      |   |          Memory Arena (Worker N)      |
 |  (`include/aether/arena.hpp`)         |   |  (`include/aether/arena.hpp`)         |
 |  - Monotonic Bump Pointer Allocator   |   |  - Monotonic Bump Pointer Allocator   |
 |  - O(1) Deallocation / Reset          |   |  - O(1) Deallocation / Reset          |
 +---------------------------------------+   +---------------------------------------+
```

---

## 3. Concurrency Model & Memory Invariants

### Thread Safety & Lock Contention Prevention
`aether-packet-core` strictly avoids blocking synchronization primitives (`std::mutex`, `std::shared_mutex`, condition variables) on hot execution paths. Instead, it relies on lock-free atomic primitives and thread-local state isolation:
- **MPMC Ring Buffers (`include/aether/ring_buffer.hpp`):** Implemented using acquire-release memory semantics (`std::memory_order_acquire`, `std::memory_order_release`) over cache-aligned atomic sequence counters, preventing false sharing across processor cores.
- **Worker Isolation:** Ingestion threads operate within isolated execution domains, communicating exclusively through lock-free ring buffers without acquiring locks.

### Memory Ownership & Arena Allocation
To eliminate heap fragmentation and allocator overhead, `aether-packet-core` mandates deterministic memory lifecycles:
- **Arena Allocator (`include/aether/arena.hpp`):** Pre-allocates contiguous memory blocks during initialization. Dynamic allocations within the engine operate as bump-pointer increments running in $O(1)$ time complexity.
- **RAII Lifecycles:** Arena blocks are reclaimed in bulk upon request lifecycle completion, bypassing fine-grained `free()` operations entirely.

---

## 4. Performance Characteristics & Algorithmic Complexity Table

| Component / Path | Algorithmic Complexity | Time Complexity (Best/Worst) | Space Complexity | Latency Estimate (P99) |
| :--- | :--- | :--- | :--- | :--- |
| **Arena Allocation** | Monotonic Bump Pointer | $O(1)$ / $O(1)$ | $O(N)$ fixed pool | $< 5$ ns |
| **Ring Buffer Push/Pop** | CAS / Atomic Seq Load | $O(1)$ / $O(1)$ | $O(N)$ ring size | $< 25$ ns |
| **Packet Filter Match** | Bitmask Evaluation | $O(1)$ / $O(K)$ rules | $O(K)$ rule table | $< 15$ ns |
| **Flow Control Token Check** | Atomic Token Bucket | $O(1)$ / $O(1)$ | $O(1)$ state | $< 10$ ns |
| **Engine Execution Cycle** | End-to-End Pipeline | $O(1)$ per packet | $O(N)$ arena/ring | $< 80$ ns |

---

## 5. Security Architecture & Threat Model

`aether-packet-core` is engineered for deployment in high-trust, high-threat edge environments where packet corruption, malformed frame injection, and denial-of-service (DoS) attempts are expected.

### Input Boundaries & Sanitization
- **Strict Bounds Checking:** All incoming packet buffers parsed via `include/aether/types.hpp` undergo rigorous size validation against declared Layer 2/3/4 header lengths before pointer arithmetic is performed.
- **Type-Safe Wrappers:** Raw pointer manipulation is encapsulated inside strongly-typed structures, mitigating buffer overflow and buffer over-read vectors.

### Memory Corruption Protections
- **No Untrusted Dynamic Allocation:** Because allocations are constrained within pre-initialized `Arena` blocks, heap exploitation vectors (such as heap spraying or use-after-free via dangling pointers) are eliminated by design.
- **Compiler Hardening Flags:** The build configuration enforces stack protector strong (`-fstack-protector-strong`), position-independent executables (`-fPIE`), and strict strict-aliasing rules.

---

## 6. Build, Verification & Benchmarking Recipes

### Prerequisites
- **Compiler:** GCC 11+ or Clang 13+ supporting C++20.
- **Build System:** GNU Make 4.0+.
- **Dependencies:** Standard POSIX environment.

### Build Recipes

#### 1. Compile the Library and Binaries
```bash
make clean
make all
```

#### 2. Run the Test Suite (Invariants & Unit Tests)
Executes deterministic invariant verification tests (`tests/test_invariants.cpp`):
```bash
make test
```

#### 3. Execute Performance Benchmarks
Runs micro-benchmarks (`benchmarks/bench_engine.cpp`) to measure throughput, allocation speed, and pipeline latency:
```bash
make bench