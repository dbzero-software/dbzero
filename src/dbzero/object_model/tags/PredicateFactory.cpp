// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include "PredicateFactory.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::object_model

{

    PredicateFactory::PredicateFactory(std::optional<std::size_t> capacity)
        : m_capacity(capacity.value_or(DEFAULT_CAPACITY))
        , m_cache(m_capacity)
        , m_evict_hand(m_cache.begin())
        , m_insert_hand(m_cache.begin())
        , m_visited(m_capacity)
    {
        assert(m_capacity > 0);
    }

    bool PredicateFactory::isFull() const
    {
        return m_size == m_capacity;
    }

    std::shared_ptr<ObjectIterable> PredicateFactory::deserialize(
        db0::swine_ptr<Fixture> fixture, const std::vector<std::byte> &bytes) const
    {
        auto iter = bytes.cbegin();
        return ObjectIterable::deserialize(fixture, iter, bytes.cend());
    }

    std::shared_ptr<ObjectIterable> PredicateFactory::get(ObjectPtr key)
    {
        if (!key) {
            THROWF(db0::InputException) << "Invalid predicate key";
        }

        const auto &predicate = ObjectIterable::LangToolkit::getPredicateIterable(key);
        auto fixture = predicate.getFixture();
        auto stateNum = fixture->getPrefix().getStateNum();
        std::unique_lock<std::shared_mutex> lock(m_mutex);

        auto it = m_key_to_index.find(key);
        if (it != m_key_to_index.end()) {
            auto slotId = it->second;
            auto &item = m_cache[slotId];
            assert(item.m_key_ref.get() == key);
            m_visited[slotId] = true;
            if (item.m_state_num != stateNum) {
                item.m_iterable = deserialize(fixture, item.m_bytes);
                item.m_state_num = stateNum;
            }
            return item.m_iterable;
        }

        std::optional<std::uint32_t> slot;
        if (isFull()) {
            slot = evictOne();
            if (!slot) {
                // Sieve gives every visited item a second chance. If all entries
                // were marked visited, a second pass must find an evictable slot.
                slot = evictOne();
            }
        } else {
            slot = findEmptySlot();
        }
        assert(slot);

        std::vector<std::byte> bytes;
        predicate.serialize(bytes);

        auto slotId = *slot;
        auto &item = m_cache[slotId];
        assert(!item.m_key_ref.get());
        item.m_key_ref = ObjectSharedPtr(key);
        item.m_bytes = std::move(bytes);
        item.m_iterable = deserialize(fixture, item.m_bytes);
        item.m_state_num = stateNum;
        m_visited[slotId] = true;
        m_key_to_index[key] = slotId;
        ++m_size;
        return item.m_iterable;
    }

    std::optional<std::uint32_t> PredicateFactory::evictOne()
    {
        if (m_size == 0) {
            return std::nullopt;
        }

        assert(m_evict_hand != m_cache.end());
        auto end = m_evict_hand;
        ++m_evict_hand;
        for (; m_evict_hand != end; ++m_evict_hand) {
            if (m_evict_hand == m_cache.end()) {
                m_evict_hand = m_cache.begin();
                if (m_evict_hand == end) {
                    return std::nullopt;
                }
            }
            if (!m_evict_hand->m_key_ref.get()) {
                continue;
            }
            auto slotId = m_evict_hand - m_cache.begin();
            if (m_visited[slotId]) {
                m_visited[slotId] = false;
                continue;
            }

            m_key_to_index.erase(m_evict_hand->m_key_ref.get());
            *m_evict_hand = CacheItem{};
            --m_size;
            return slotId;
        }
        return std::nullopt;
    }

    std::optional<std::uint32_t> PredicateFactory::findEmptySlot()
    {
        auto end = m_insert_hand;
        for (;;) {
            if (m_insert_hand == m_cache.end()) {
                m_insert_hand = m_cache.begin();
            }
            if (!m_insert_hand->m_key_ref.get()) {
                return m_insert_hand - m_cache.begin();
            }
            ++m_insert_hand;
            if (m_insert_hand == end) {
                return std::nullopt;
            }
        }
    }

    std::size_t PredicateFactory::size() const
    {
        std::shared_lock<std::shared_mutex> lock(m_mutex);
        return m_size;
    }

    std::size_t PredicateFactory::getCapacity() const
    {
        return m_capacity;
    }

}
