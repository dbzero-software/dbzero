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
            return m_hot_pair->isOpen() ? m_hot_pair : nullptr;
        }

        auto it = m_pairs.find(slot_id);
        if (it == m_pairs.end()) {
            return nullptr;
        }
        if (!canUseCached(it->second.m_access_type, access_type)) {
            return nullptr;
        }
        if (!it->second.m_pair->isOpen()) {
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
            if (cached_it->second.m_pair->isOpen()) {
                cacheHotPair(slot_id, *cached_it->second.m_pair, cached_it->second.m_access_type);
                return cached_it->second.m_pair.get();
            }
            if (!m_allocator->tryFirstAlloc(slot_id)) {
                return nullptr;
            }
            cached_it->second.m_pair->refresh();
            if (cached_it->second.m_pair->isOpen()) {
                cacheHotPair(slot_id, *cached_it->second.m_pair, cached_it->second.m_access_type);
                return cached_it->second.m_pair.get();
            }
            return nullptr;
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

    void SparsePairManager::beginRefreshLog()
    {
        m_refresh_pages.clear();
    }

    void SparsePairManager::recordRefreshPage(std::uint64_t entry)
    {
        m_refresh_pages.insert(entry);
    }

    void SparsePairManager::completeRefreshLog()
    {
        if (m_refresh_pages.empty()) {
            return;
        }

        std::vector<std::uint64_t> page_nums(m_refresh_pages.begin(), m_refresh_pages.end());
        m_refresh_pages.clear();
        refreshPages(page_nums);
    }

    void SparsePairManager::cancelRefreshLog()
    {
        m_refresh_pages.clear();
    }

    void SparsePairManager::beginRefreshPages()
    {
        m_flags = m_flags & ~StorageFlags { StorageFlagOption::NO_LOAD };
        m_prefix->refreshState();
    }

    void SparsePairManager::refreshPages(const std::vector<std::uint64_t> &page_nums)
    {
        if (page_nums.empty()) {
            return;
        }

        beginRefreshPages();
        std::unordered_map<Allocator::SlotId, std::vector<std::uint64_t> > pages_by_slot;
        for (auto entry: page_nums) {
            auto slot_id = PlainSparsePair::changeLogEntrySlotId(entry);
            auto page_num = PlainSparsePair::changeLogEntryPageNum(entry);
            pages_by_slot[slot_id].push_back(page_num);
        }

        for (auto &[slot_id, slot_page_nums]: pages_by_slot) {
            auto pair_it = m_pairs.find(slot_id);
            if (pair_it != m_pairs.end()) {
                std::unordered_set<std::uint64_t> reloaded_pages;
                auto reload_address = [this, &reloaded_pages](Address address) {
                    auto page_num = address.getOffset() / m_prefix->getPageSize();
                    if (!reloaded_pages.insert(page_num).second) {
                        return true;
                    }
                    return m_prefix->reloadPage(page_num);
                };
                pair_it->second.m_pair->refreshPages(slot_page_nums, reload_address);
                cacheHotPair(slot_id, *pair_it->second.m_pair, pair_it->second.m_access_type);
            } else {
                m_allocator->detachSlot(slot_id);
            }
        }
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
