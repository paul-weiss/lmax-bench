// CLOB matching engine on the LMAX pattern in C++, benchmarked.
//
// Architecture: hand-rolled single-producer/single-consumer busy-spin ring
// buffer (C++ has no canonical Disruptor library; the pattern is ~60 lines)
// feeding a single-writer matching core that owns all book state. The exact
// same workload, book algorithm, and measurement protocol are implemented in
// the Java, Rust, and Go versions — all four print the same deterministic
// book checksum on the same workload.
//
// Latency percentiles are exact: every measured sample is stored in a
// preallocated vector and sorted at the end (no histogram approximation).
//
// Modes: throughput <ops> | latency <ops> <rate> | all

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <new>
#include <string>
#include <thread>
#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif
#include <unordered_map>
#include <vector>

// ---------------------------------------------------------------- allocation counter

static std::atomic<uint64_t> g_allocs{0};

void* operator new(std::size_t sz) {
    g_allocs.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(sz)) return p;
    throw std::bad_alloc{};
}
void* operator new[](std::size_t sz) { return operator new(sz); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

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

// ---------------------------------------------------------------- the book
// Price-time priority; lazy cancellation (cancel removes from the id map,
// stale ids are skipped at the head of a level queue).

struct Resting {
    int64_t price;
    int64_t qty;
};

struct Book {
    std::map<int64_t, std::deque<uint64_t>> bids;
    std::map<int64_t, std::deque<uint64_t>> asks;
    std::unordered_map<uint64_t, Resting> orders;
    uint64_t fills = 0;
    uint64_t volume = 0;

    void limit(uint8_t side, int64_t price, int64_t qty, uint64_t id) {
        while (qty > 0) {
            // best opposite level that crosses, else rest the residual
            std::map<int64_t, std::deque<uint64_t>>::iterator it;
            if (side == SIDE_BID) {
                it = asks.begin();
                if (it == asks.end() || it->first > price) break;
            } else {
                if (bids.empty()) break;
                it = std::prev(bids.end());
                if (it->first < price) break;
            }
            auto& lvl = it->second;
            while (qty > 0) {
                if (lvl.empty()) break;
                uint64_t head = lvl.front();
                auto oit = orders.find(head);
                if (oit == orders.end()) { // lazily-cancelled order
                    lvl.pop_front();
                    continue;
                }
                int64_t m = std::min(qty, oit->second.qty);
                oit->second.qty -= m;
                qty -= m;
                fills++;
                volume += static_cast<uint64_t>(m);
                if (oit->second.qty == 0) {
                    orders.erase(oit);
                    lvl.pop_front();
                }
            }
            if (lvl.empty()) {
                (side == SIDE_BID ? asks : bids).erase(it);
            }
        }
        if (qty > 0) {
            auto& book_side = (side == SIDE_BID) ? bids : asks;
            book_side[price].push_back(id);
            orders.emplace(id, Resting{price, qty});
        }
    }

    void cancel(uint64_t id) { orders.erase(id); }

    // checksum over live orders in (price ascending, FIFO) order per side —
    // must be identical to the other implementations on the same workload.
    uint64_t checksum() const {
        uint64_t acc = 0;
        auto fold = [&](const std::map<int64_t, std::deque<uint64_t>>& side) {
            for (const auto& [p, lvl] : side) {
                for (uint64_t id : lvl) {
                    auto oit = orders.find(id);
                    if (oit != orders.end()) {
                        SplitMix64 s{acc ^ id ^
                                     (static_cast<uint64_t>(oit->second.price) << 20) ^
                                     static_cast<uint64_t>(oit->second.qty)};
                        acc = s.next();
                    }
                }
            }
        };
        fold(bids);
        fold(asks);
        return acc;
    }
};

// ---------------------------------------------------------------- tuned book
// Same semantics as Book (identical fills/volume/checksum on the same
// stream), engineered for the hot path: dense array-backed price ladder with
// best-bid/best-ask pointers instead of a tree, slice-FIFO levels (head
// index, cleared not freed), identity-hashed id map with reserved capacity
// (ids are already well-distributed sequence numbers). Steady state
// approaches zero allocation.

constexpr int64_t LADDER_BASE = 0;
constexpr size_t LADDER_LEN = 4096; // workload prices stay well inside [480, 1521]

struct IdHash {
    size_t operator()(uint64_t v) const noexcept {
        // Identity hash, deliberately: order ids are unique sequence numbers,
        // and identity clusters them into adjacent buckets — sequential ids
        // stay prefetch-friendly. A "well-mixed" hash here scatters every
        // lookup across the table and measurably LOSES throughput.
        return static_cast<size_t>(v);
    }
};

struct FlatLevel {
    std::vector<uint64_t> ids;
    size_t head = 0;
    bool empty() const { return head == ids.size(); }
    void reset() { ids.clear(); head = 0; }
};

struct TunedBook {
    std::vector<FlatLevel> bids{LADDER_LEN};
    std::vector<FlatLevel> asks{LADDER_LEN};
    int64_t best_bid = -1;
    int64_t best_ask = INT64_MAX;
    std::unordered_map<uint64_t, Resting, IdHash> orders;
    uint64_t fills = 0;
    uint64_t volume = 0;

    TunedBook() { orders.reserve(1u << 21); }

    void limit(uint8_t side, int64_t price, int64_t qty, uint64_t id) {
        while (qty > 0) {
            int64_t best = (side == SIDE_BID) ? best_ask : best_bid;
            bool crosses = (side == SIDE_BID) ? best <= price : best >= price;
            if (best < LADDER_BASE || best >= LADDER_BASE + (int64_t)LADDER_LEN || !crosses)
                break;
            FlatLevel& lvl = (side == SIDE_BID) ? asks[size_t(best - LADDER_BASE)]
                                                : bids[size_t(best - LADDER_BASE)];
            while (qty > 0) {
                if (lvl.empty()) break;
                uint64_t head = lvl.ids[lvl.head];
                auto oit = orders.find(head);
                if (oit == orders.end()) { // lazily-cancelled order
                    lvl.head++;
                    continue;
                }
                int64_t m = std::min(qty, oit->second.qty);
                oit->second.qty -= m;
                qty -= m;
                fills++;
                volume += static_cast<uint64_t>(m);
                if (oit->second.qty == 0) {
                    orders.erase(oit);
                    lvl.head++;
                }
            }
            if (lvl.empty()) {
                lvl.reset();
                // advance the best pointer to the next occupied level
                if (side == SIDE_BID) {
                    int64_t p = best + 1;
                    while (p < LADDER_BASE + (int64_t)LADDER_LEN &&
                           asks[size_t(p - LADDER_BASE)].empty())
                        p++;
                    best_ask = p < LADDER_BASE + (int64_t)LADDER_LEN ? p : INT64_MAX;
                } else {
                    int64_t p = best - 1;
                    while (p >= LADDER_BASE && bids[size_t(p - LADDER_BASE)].empty()) p--;
                    best_bid = p;
                }
            }
        }
        if (qty > 0) {
            size_t idx = size_t(price - LADDER_BASE);
            if (side == SIDE_BID) {
                bids[idx].ids.push_back(id);
                best_bid = std::max(best_bid, price);
            } else {
                asks[idx].ids.push_back(id);
                best_ask = std::min(best_ask, price);
            }
            orders.emplace(id, Resting{price, qty});
        }
    }

    void cancel(uint64_t id) { orders.erase(id); }

    uint64_t checksum() const {
        uint64_t acc = 0;
        auto fold = [&](const std::vector<FlatLevel>& side) {
            for (const auto& lvl : side) {
                for (size_t i = lvl.head; i < lvl.ids.size(); i++) {
                    auto oit = orders.find(lvl.ids[i]);
                    if (oit != orders.end()) {
                        SplitMix64 s{acc ^ lvl.ids[i] ^
                                     (static_cast<uint64_t>(oit->second.price) << 20) ^
                                     static_cast<uint64_t>(oit->second.qty)};
                        acc = s.next();
                    }
                }
            }
        };
        fold(bids);
        fold(asks);
        return acc;
    }
};

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

// ---------------------------------------------------------------- harness

struct Report {
    uint64_t fills = 0, volume = 0, checksum = 0;
    size_t resting = 0;
    std::vector<uint32_t> samples; // latency ns
};

using Clock = std::chrono::steady_clock;

static uint64_t nanos_since(Clock::time_point epoch) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - epoch).count());
}

struct RunResult {
    Report rep;
    double wall = 0;
    uint64_t allocs = 0;
};

// Pin the calling thread to a core (Linux only; no-op elsewhere).
static void pin_current(int core) {
#ifdef __linux__
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#else
    (void)core;
#endif
}

static int env_core(const char* name) {
    const char* v = std::getenv(name);
    return v ? std::atoi(v) : -1;
}

template <class BookT>
static RunResult run_impl(uint64_t total_ops, uint64_t warmup_ops, uint64_t rate) {
    auto epoch = Clock::now();
    Ring ring;
    Report rep;
    if (rate > 0) rep.samples.reserve(total_ops - warmup_ops + 16);
    int pin_cons = env_core("PIN_CONS");
    int pin_prod = env_core("PIN_PROD");
    if (pin_prod >= 0) pin_current(pin_prod);

    std::thread consumer([&] {
        if (pin_cons >= 0) pin_current(pin_cons);
        BookT book;
        uint64_t seq = 0;
        for (;;) {
            while (ring.prod.load(std::memory_order_acquire) == seq) {
                // busy-spin
            }
            seq++;
            Event& e = ring.buf[seq & ring.mask];
            switch (e.kind) {
                case KIND_LIMIT: book.limit(e.side, e.price, e.qty, e.id); break;
                case KIND_CANCEL: book.cancel(e.id); break;
                default:
                    rep.fills = book.fills;
                    rep.volume = book.volume;
                    rep.resting = book.orders.size();
                    rep.checksum = book.checksum();
                    return;
            }
            if (e.ts != 0) {
                uint64_t lat = nanos_since(epoch) - e.ts;
                rep.samples.push_back(static_cast<uint32_t>(std::min<uint64_t>(
                    std::max<uint64_t>(lat, 1), UINT32_MAX)));
            }
            ring.cons.store(seq, std::memory_order_release);
        }
    });

    Gen wl(42);
    uint64_t interval = rate > 0 ? 1'000'000'000ULL / rate : 0;
    uint64_t next_due = nanos_since(epoch);

    uint64_t allocs0 = g_allocs.load(std::memory_order_relaxed);
    auto t0 = Clock::now();
    for (uint64_t i = 0; i < total_ops; i++) {
        wl.next_op();
        uint64_t ts = 0;
        if (interval > 0) {
            next_due += interval;
            while (nanos_since(epoch) < next_due) {
                // busy-spin pacing
            }
            if (i >= warmup_ops) ts = next_due;
        }
        Event* e = ring.claim();
        e->kind = wl.kind;
        e->side = wl.side;
        e->price = wl.price;
        e->qty = wl.qty;
        e->id = wl.id;
        e->ts = ts;
        ring.publish();
    }
    Event* e = ring.claim();
    e->kind = KIND_REPORT;
    e->ts = 0;
    ring.publish();
    consumer.join();
    double wall = std::chrono::duration<double>(Clock::now() - t0).count();
    uint64_t allocs = g_allocs.load(std::memory_order_relaxed) - allocs0;

    return RunResult{std::move(rep), wall, allocs};
}

static RunResult run(uint64_t total_ops, uint64_t warmup_ops, uint64_t rate) {
    const char* eng = std::getenv("ENGINE");
    bool tuned = eng && std::strcmp(eng, "tuned") == 0;
    int pp = env_core("PIN_PROD"), pc = env_core("PIN_CONS");
    std::printf("  engine=%s pin_prod=%d pin_cons=%d\n",
                tuned ? "tuned" : "idiomatic", pp, pc);
    return tuned ? run_impl<TunedBook>(total_ops, warmup_ops, rate)
                 : run_impl<Book>(total_ops, warmup_ops, rate);
}

static void print_result(RunResult& r, uint64_t total_ops, bool latency) {
    std::printf("  heap allocations during run: %llu\n",
                static_cast<unsigned long long>(r.allocs));
    std::printf("  ops=%llu wall=%.3fs throughput=%.0f ops/s\n",
                static_cast<unsigned long long>(total_ops), r.wall, total_ops / r.wall);
    std::printf("  fills=%llu volume=%llu resting=%zu checksum=%016llx\n",
                static_cast<unsigned long long>(r.rep.fills),
                static_cast<unsigned long long>(r.rep.volume), r.rep.resting,
                static_cast<unsigned long long>(r.rep.checksum));
    if (latency) {
        auto& s = r.rep.samples;
        std::sort(s.begin(), s.end());
        auto at = [&](double q) {
            size_t idx = static_cast<size_t>(q * (s.size() - 1));
            return s[idx] / 1000.0;
        };
        std::printf(
            "  latency µs: p50=%.3f p90=%.3f p99=%.3f p99.9=%.1f p99.99=%.1f max=%.1f (n=%zu)\n",
            at(0.50), at(0.90), at(0.99), at(0.999), at(0.9999),
            s.back() / 1000.0, s.size());
    }
}

int main(int argc, char** argv) {
    std::string mode = argc > 1 ? argv[1] : "all";
    auto arg_u = [&](int i, uint64_t d) {
        return argc > i ? std::strtoull(argv[i], nullptr, 10) : d;
    };

    if (mode == "throughput" || mode == "all") {
        uint64_t ops = mode == "throughput" ? arg_u(2, 10'000'000) : 10'000'000;
        std::printf("[cpp] throughput mode\n");
        RunResult r = run(ops, ops, 0);
        print_result(r, ops, false);
    }
    if (mode == "latency" || mode == "all") {
        uint64_t ops = 2'000'000, rate = 250'000;
        if (mode == "latency") {
            ops = arg_u(2, ops);
            rate = arg_u(3, rate);
        }
        std::printf("[cpp] latency mode (%llu ops/s paced)\n",
                    static_cast<unsigned long long>(rate));
        RunResult r = run(ops, ops / 5, rate);
        print_result(r, ops, true);
    }
    return 0;
}
