#pragma once
// The hand-rolled SPSC busy-spin ring — the LMAX pattern in ~40 lines.

#include <atomic>
#include <cstdint>
#include <vector>

// ---------------------------------------------------------------- SPSC ring
// The LMAX pattern in miniature: a power-of-two ring of pre-allocated events,
// a producer cursor and a consumer cursor, busy-spin waits on both sides.

struct Event {
    uint8_t kind, side;
    int64_t price, qty;
    uint64_t id;
    uint64_t ts; // scheduled-publish nanos since run epoch; 0 = don't measure
};

constexpr uint64_t RING_SIZE = 65536;

struct Ring {
    std::vector<Event> buf{RING_SIZE};
    uint64_t mask = RING_SIZE - 1;
    alignas(64) std::atomic<uint64_t> prod{0};
    alignas(64) std::atomic<uint64_t> cons{0};

    Event* claim() {
        uint64_t next = prod.load(std::memory_order_relaxed) + 1;
        while (next - cons.load(std::memory_order_acquire) > RING_SIZE) {
            // busy-spin: ring full
        }
        return &buf[next & mask];
    }
    void publish() {
        prod.store(prod.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }
};

