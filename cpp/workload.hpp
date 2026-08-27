#pragma once
// splitmix64 workload generator — bit-identical across all four languages.

#include <algorithm>
#include <cstdint>

// ---------------------------------------------------------------- workload
// splitmix64, bit-identical to the Java/Rust/Go generators.

struct SplitMix64 {
    uint64_t state;
    uint64_t next() {
        state += 0x9E3779B97F4A7C15ULL;
        uint64_t z = state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    }
};

constexpr uint8_t KIND_LIMIT = 0;
constexpr uint8_t KIND_CANCEL = 1;
constexpr uint8_t KIND_REPORT = 2;
constexpr uint8_t SIDE_BID = 0;

struct Gen {
    SplitMix64 rng;
    int64_t mid = 1000;
    uint64_t last_id = 0;

    // out-params of next_op (no allocation per call)
    uint8_t kind = 0, side = 0;
    int64_t price = 0, qty = 0;
    uint64_t id = 0;

    explicit Gen(uint64_t seed) : rng{seed} {}

    void next_op() {
        uint64_t r = rng.next();
        if (r % 7 == 0) {
            mid += static_cast<int64_t>((r >> 40) % 3) - 1;
            mid = std::clamp<int64_t>(mid, 500, 1500);
        }
        uint64_t c = r % 100;
        if (c < 85) {
            side = static_cast<uint8_t>((r >> 8) & 1);
            bool aggressive = c >= 55;
            int64_t offset = aggressive
                ? static_cast<int64_t>((r >> 16) % 10)
                : 1 + static_cast<int64_t>((r >> 16) % 20);
            price = ((side == SIDE_BID) != aggressive) ? mid - offset : mid + offset;
            qty = 1 + static_cast<int64_t>((r >> 32) % 100);
            id = ++last_id;
            kind = KIND_LIMIT;
        } else {
            uint64_t back = (r >> 16) % 5000;
            id = last_id > back ? last_id - back : 0; // saturating
            kind = KIND_CANCEL;
            side = 0;
            price = 0;
            qty = 0;
        }
    }
};

