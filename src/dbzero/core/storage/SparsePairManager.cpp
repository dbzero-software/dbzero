// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SparsePairManager.hpp"
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/MS_MetaPrefix.hpp>
#include <unordered_set>
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

    PlainSparsePair *SparsePairManager::tryGetCached(Allocator::SlotId slot_id, AccessType access_type) const noexcept
    {
        if (m_hot_pair && m_hot_slot_id == slot_id && canUseCached(m_hot_access_type, access_type)) {
            return m_hot_pair;
        }

        auto it = m_pairs.find(slot_id);
        if (it == m_pairs.end()) {
            return nullptr;
        }
        if (!canUseCached(it->second.m_access_type, access_type)) {
            return nullptr;
        }
        cacheHotPair(slot_id, *it->second.m_pair, it->second.m_access_type);
        return it->second.m_pair.get();
    }

    PlainSparsePair *SparsePairManager::tryGetCached(Allocator::SlotId slot_id) const noexcept
    {
        return tryGetCached(slot_id, m_access_type);
    }

    PlainSparsePair &SparsePairManager::getOrCreate(Allocator::SlotId slot_id)
    {
        if (auto *existing = tryGetExisting(slot_id, m_access_type)) {
            return *existing;
        }

        auto dram_pair = createDRAMPair(slot_id);
        auto sparse_pair = std::make_unique<PlainSparsePair>(
            PlainSparsePair::tag_create(), dram_pair, slot_id, &m_change_log);
        auto *result = sparse_pair.get();
        m_pairs.insert_or_assign(slot_id, PairEntry { std::move(sparse_pair), m_access_type });
        cacheHotPair(slot_id, *result, m_access_type);
        return *result;
    }

    PlainSparsePair *SparsePairManager::tryGetExisting(Allocator::SlotId slot_id, AccessType access_type) const
    {
        auto cached_it = m_pairs.find(slot_id);
        if (cached_it != m_pairs.end() && canUseCached(cached_it->second.m_access_type, access_type)) {
            cacheHotPair(slot_id, *cached_it->second.m_pair, cached_it->second.m_access_type);
            return cached_it->second.m_pair.get();
        }

        auto root_address = m_allocator->tryFirstAlloc(slot_id);
        if (!root_address) {
            return nullptr;
        }

        auto dram_pair = createDRAMPair(slot_id);
        auto sparse_pair = std::make_unique<PlainSparsePair>(
            dram_pair, access_type, *root_address, m_flags, slot_id, &m_change_log);
        auto *result = sparse_pair.get();
        m_pairs.insert_or_assign(slot_id, PairEntry { std::move(sparse_pair), access_type });
        cacheHotPair(slot_id, *result, access_type);
        return result;
    }

    PlainSparsePair *SparsePairManager::tryGetExisting(Allocator::SlotId slot_id) const
    {
        return tryGetExisting(slot_id, m_access_type);
    }

    void SparsePairManager::evictSlot(Allocator::SlotId slot_id)
    {
        auto pair_it = m_pairs.find(slot_id);
        if (pair_it == m_pairs.end()) {
            return;
        }
        if (m_hot_pair == pair_it->second.m_pair.get()) {
            m_hot_pair = nullptr;
        }
        pair_it->second.m_pair->detach();
        m_pairs.erase(pair_it);
    }
    
    void SparsePairManager::refreshPages(const std::vector<std::uint64_t> &page_nums)
    {
        if (page_nums.empty()) {
            return;
        }

        // Refresh pages from a single specific slot only
        auto refresh_slot = [&](std::uint64_t slot_id, const std::uint64_t *begin, const std::uint64_t *end) -> bool
        {
            auto sparse_pair = tryGetCached(slot_id);
            if (!sparse_pair) {
                // not cached, might need to be loaded if mapping policy == eager
                return false;
            }

            if (begin == end) {
                // no pages to refresh, just return
                return true;
            }

            // detach before reloading
            sparse_pair->detach();
            db0::load(*m_prefix, begin, end);
            sparse_pair->refresh();

            // also update the allocator
            auto updater = m_allocator->beginUpdate(slot_id);
            for (;begin != end; ++begin) {
                // update with the local address
                updater(MS_Address::from(*begin << m_ps_shift).local_address());
            }
             return true;
        };

        // page_nums are sorted
        // we can scan them refreshing slot by slot, only existing slots need refreshing
        // but newly added slots should be loaded when the mapping policy == eager
        const std::uint64_t *current = page_nums.data();
        const std::uint64_t *end = current;
        std::uint64_t last_slot_id = 0;
        for (auto page_num: page_nums) {
            auto slot_id = MS_Address::from(page_num << m_ps_shift).slot_id();
            if (slot_id != last_slot_id) {
                assert(slot_id > last_slot_id);
                refresh_slot(last_slot_id, current, end);
                // move on to the next slot
                last_slot_id = slot_id;
                current = end;
            } else {
                ++end;
            }            
        }

        refresh_slot(last_slot_id, current, end);
        m_prefix->refresh();
    }

    void SparsePairManager::forCachedPairs(std::function<void(Allocator::SlotId, PlainSparsePair &)> callback)
    {
        for (auto &item: m_pairs) {
            callback(item.first, *item.second.m_pair);
        }
    }

    std::size_t SparsePairManager::getChangeLogSize() const
    {
        return m_change_log.size();
    }

    SparsePairManager::ChangeLogT SparsePairManager::extractChangeLogPages()
    {
        ChangeLogT page_nums;
        page_nums.swap(m_change_log);
        return page_nums;
    }

    bool SparsePairManager::commit()
    {
        if (m_change_log.empty()) {
            return false;
        }
        
        std::unordered_set<SlotId> committed_slots;
        for (auto entry: m_change_log) {
            auto slot_id = PlainSparsePair::changeLogEntrySlotId(entry);
            if (!committed_slots.insert(slot_id).second) {
                continue;
            }

            auto pair_it = m_pairs.find(slot_id);
            if (pair_it != m_pairs.end()) {
                pair_it->second.m_pair->commit();
            }
        }
        return true;
    }

    DRAM_Pair SparsePairManager::createDRAMPair(Allocator::SlotId slot_id) const
    {
        (void)slot_id;
        return { m_prefix, m_allocator };
    }

    bool SparsePairManager::canUseCached(AccessType cached_access_type, AccessType requested_access_type) noexcept
    {
        return requested_access_type == AccessType::READ_ONLY || cached_access_type == AccessType::READ_WRITE;
    }

    void SparsePairManager::cacheHotPair(Allocator::SlotId slot_id, PlainSparsePair &sparse_pair,
        AccessType access_type) const noexcept
    {
        m_hot_slot_id = slot_id;
        m_hot_pair = &sparse_pair;
        m_hot_access_type = access_type;
    }
        
}
