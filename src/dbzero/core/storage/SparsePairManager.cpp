// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SparsePairManager.hpp"
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/MS_MetaPrefix.hpp>
#include <utility>

namespace db0

{

    SparsePairManager::SparsePairManager(MS_MetaSpace &metaspace, AccessType access_type, StorageFlags flags)
        : m_prefix(metaspace.getMSPrefixPtr())
        , m_allocator(metaspace.getMSAllocatorPtr())
        , m_access_type(access_type)
        , m_flags(flags)
    {
    }

    SparsePair *SparsePairManager::tryGetCached(Allocator::SlotId slot_id) noexcept
    {
        if (m_hot_pair && m_hot_slot_id == slot_id) {
            return m_hot_pair;
        }

        auto it = m_pairs.find(slot_id);
        if (it == m_pairs.end()) {
            return nullptr;
        }
        cacheHotPair(slot_id, *it->second);
        return it->second.get();
    }

    SparsePair &SparsePairManager::getOrCreate(Allocator::SlotId slot_id)
    {
        if (auto *cached = tryGetCached(slot_id)) {
            return *cached;
        }

        auto dram_pair = createDRAMPair(slot_id);
        auto root_address = m_allocator->tryFirstAlloc(slot_id);
        auto sparse_pair = root_address
            ? std::make_unique<SparsePair>(dram_pair, m_access_type, *root_address, m_flags, slot_id)
            : std::make_unique<SparsePair>(SparsePair::tag_create(), dram_pair, slot_id);
        auto *result = sparse_pair.get();
        m_pairs.emplace(slot_id, std::move(sparse_pair));
        cacheHotPair(slot_id, *result);
        return *result;
    }

    DRAM_Pair SparsePairManager::createDRAMPair(Allocator::SlotId slot_id) const
    {
        (void)slot_id;
        return { m_prefix, m_allocator };
    }

    void SparsePairManager::cacheHotPair(Allocator::SlotId slot_id, SparsePair &sparse_pair) noexcept
    {
        m_hot_slot_id = slot_id;
        m_hot_pair = &sparse_pair;
    }

}
