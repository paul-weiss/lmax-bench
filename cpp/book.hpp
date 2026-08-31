#pragma once
// The idiomatic price-time priority book; lazy cancellation.

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <unordered_map>

#include "workload.hpp"

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

    // max_id is unused here (the map grows as it likes); the parameter exists
    // so Book and TunedBook construct identically in the templated harness.
    explicit Book(uint64_t max_id) { (void)max_id; }

    size_t resting() const { return orders.size(); }

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

