// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <array>
#include <cassert>
#include <cstddef>
#include <functional>
#include <utility>

namespace db0

{

    template <typename KeyT, typename ValueT, std::size_t N, typename EqualT = std::equal_to<KeyT> >
    class SieveCache
    {
    public:
        static_assert(N > 0, "SieveCache capacity must be greater than zero");

        template <typename FactoryT>
        const ValueT &getOrCreate(const KeyT &key, FactoryT &&factory)
        {
            if (auto *value = tryGet(key)) {
                return *value;
            }

            auto &entry = allocateEntry();
            entry.key = key;
            entry.value = std::forward<FactoryT>(factory)();
            entry.occupied = true;
            entry.visited = false;
            return entry.value;
        }

        ValueT *tryGet(const KeyT &key)
        {
            for (std::size_t i = 0; i < m_size; ++i) {
                auto &entry = m_entries[i];
                if (entry.occupied && m_equal(entry.key, key)) {
                    entry.visited = true;
                    return &entry.value;
                }
            }
            return nullptr;
        }

        const ValueT *tryGet(const KeyT &key) const
        {
            for (std::size_t i = 0; i < m_size; ++i) {
                const auto &entry = m_entries[i];
                if (entry.occupied && m_equal(entry.key, key)) {
                    return &entry.value;
                }
            }
            return nullptr;
        }

        std::size_t size() const
        {
            return m_size;
        }

    private:
        struct Entry
        {
            KeyT key = {};
            ValueT value = {};
            bool occupied = false;
            bool visited = false;
        };

        Entry &allocateEntry()
        {
            if (m_size < N) {
                return m_entries[m_size++];
            }

            for (;;) {
                auto &entry = m_entries[m_hand];
                if (entry.visited) {
                    entry.visited = false;
                    advanceHand();
                    continue;
                }
                advanceHand();
                return entry;
            }
        }

        void advanceHand()
        {
            ++m_hand;
            if (m_hand == N) {
                m_hand = 0;
            }
        }

        std::array<Entry, N> m_entries;
        std::size_t m_size = 0;
        std::size_t m_hand = 0;
        EqualT m_equal;
    };

}
