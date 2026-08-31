#pragma once
// Tuned book: array ladder + best pointers + id-indexed order array.

#include <cstdint>
#include <vector>

#include "book.hpp"

// ---------------------------------------------------------------- tuned book
// Same semantics as Book (identical fills/volume/checksum on the same
// stream), engineered for the hot path: dense array-backed price ladder with
// best-bid/best-ask pointers instead of a tree, slice-FIFO levels (head
// index, cleared not freed), and the id map replaced outright by a
// preallocated array indexed by order id (ids are dense sequence numbers;
// qty == 0 marks a dead slot). Steady state is zero-allocation.
//
// The id store's history, preserved because the temptation recurs:
//   1. std::unordered_map with a splitmix-mixed hash — textbook, and 25%
//      SLOWER than the idiomatic engine: well-mixed hashing scattered every
//      lookup across a 32MB table, a cache miss each.
//   2. Identity hash — sequential ids land in adjacent buckets, lookups went
//      cache-resident, throughput recovered. But std::unordered_map is
//      node-based BY THE STANDARD (stable element addresses), so reserve()
//      only sizes the bucket array: every emplace still mallocs a node and
//      every erase frees one — ~1M hidden allocations per 2M-op run, and the
//      allocator's slow paths landed exactly where this engine's tail was.
//   3. No hash at all — the array below. The ids are the indexes.

constexpr int64_t LADDER_BASE = 0;
constexpr size_t LADDER_LEN = 4096; // workload prices stay well inside [480, 1521]

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
    std::vector<Resting> orders; // indexed by id; qty == 0 = no such order
    size_t live = 0;
    uint64_t fills = 0;
    uint64_t volume = 0;

    // max_id bounds the ids the workload can emit (one per op at most).
    // Zero-initializing the array also touches every page up front, so no
    // first-touch fault lands in the measured region.
    explicit TunedBook(uint64_t max_id) : orders(max_id + 2) {}

    size_t resting() const { return live; }

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
                Resting& r = orders[head];
                if (r.qty == 0) { // lazily-cancelled order
                    lvl.head++;
                    continue;
                }
                int64_t m = std::min(qty, r.qty);
                r.qty -= m;
                qty -= m;
                fills++;
                volume += static_cast<uint64_t>(m);
                if (r.qty == 0) {
                    live--;
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
            orders[id] = Resting{price, qty};
            live++;
        }
    }

    void cancel(uint64_t id) {
        Resting& r = orders[id]; // id 0 (saturated cancel) hits the dead slot 0
        if (r.qty != 0) {
            r.qty = 0;
            live--;
        }
    }

    uint64_t checksum() const {
        uint64_t acc = 0;
        auto fold = [&](const std::vector<FlatLevel>& side) {
            for (const auto& lvl : side) {
                for (size_t i = lvl.head; i < lvl.ids.size(); i++) {
                    const Resting& r = orders[lvl.ids[i]];
                    if (r.qty != 0) {
                        SplitMix64 s{acc ^ lvl.ids[i] ^
                                     (static_cast<uint64_t>(r.price) << 20) ^
                                     static_cast<uint64_t>(r.qty)};
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

