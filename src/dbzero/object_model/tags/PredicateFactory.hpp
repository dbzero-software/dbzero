// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#pragma once

#include "ObjectIterable.hpp"
#include <dbzero/core/memory/config.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace db0::object_model

{

    class PredicateFactory
    {
    public:
        using ObjectPtr = ObjectIterable::ObjectPtr;
        using ObjectSharedPtr = ObjectIterable::ObjectSharedPtr;

        static constexpr std::size_t DEFAULT_CAPACITY = 1024;

        explicit PredicateFactory(std::optional<std::size_t> capacity = {});

        // Return the cached predicate iterable identified by its language object
        // pointer. The key must unwrap to a predicate-only ObjectIterable. The
        // factory stores the serialized predicate bytes once, then recreates the
        // cached native iterable whenever the prefix state number moves.
        std::shared_ptr<ObjectIterable> get(ObjectPtr key);

        std::size_t size() const;
        std::size_t getCapacity() const;

    private:
        struct CacheItem
        {
            ObjectSharedPtr m_key_ref;
            std::shared_ptr<ObjectIterable> m_iterable;
            std::vector<std::byte> m_bytes;
            StateNumType m_state_num = 0;
        };

        const std::size_t m_capacity;
        mutable std::shared_mutex m_mutex;
        std::size_t m_size = 0;
        std::vector<CacheItem> m_cache;
        mutable std::vector<CacheItem>::iterator m_evict_hand;
        mutable std::vector<CacheItem>::iterator m_insert_hand;
        mutable std::vector<bool> m_visited;
        std::unordered_map<ObjectPtr, std::uint32_t> m_key_to_index;

        bool isFull() const;
        std::shared_ptr<ObjectIterable> deserialize(db0::swine_ptr<Fixture> fixture,
            const std::vector<std::byte> &bytes) const;
        std::optional<std::uint32_t> evictOne();
        std::optional<std::uint32_t> findEmptySlot();
    };

}
