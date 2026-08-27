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

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <thread>
#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

#include "book.hpp"
#include "ring.hpp"
#include "tuned.hpp"
#include "workload.hpp"

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
