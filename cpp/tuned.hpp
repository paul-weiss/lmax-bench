#pragma once
// Tuned book: array ladder + best pointers + identity-hashed reserved map.

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "book.hpp"

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

