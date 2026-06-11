// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "SparsePair.hpp"
#include <dbzero/core/dram/MetaSpace.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace db0

{

    class DRAM_Prefix;
    class MS_MetaAllocator;

    /**
     * Owns per-slot SparsePair instances stored inside one MS_MetaSpace.
     *
     * Each managed SparsePair uses the shared MS_MetaSpace prefix, but all of
     * its internal sparse/diff index allocations are forced into the requested
     * MS_MetaSpace slot. This lets callers keep independent sparse mappings for
     * sparse slot ids while preserving the MetaSpace-level persistence and flush
     * behavior.
     *
     * The manager requires a typed MS_MetaSpace, not a generic Memspace, because
         * it needs access to MS_MetaAllocator slot metadata to open an existing
     * SparsePair root allocation without scanning unrelated slots. Repeated
     * lookups are optimized for the common same-slot case with a last-hit
     * pointer before falling back to the slot-id map.
     *
     * SparsePairManager is scoped to one MS_MetaSpace instance and does not add
     * synchronization; callers must provide external locking if they share it
     * across threads.
     */
    class SparsePairManager
    {
    public:
        using ChangeLogT = PlainSparsePair::ChangeLogT;
        using SlotId = Allocator::SlotId;

        SparsePairManager(MS_MetaSpace &metaspace, AccessType access_type = AccessType::READ_WRITE,
            StorageFlags flags = {});

        PlainSparsePair &getOrCreate(SlotId slot_id);

        PlainSparsePair *tryGetExisting(SlotId slot_id, AccessType access_type) const;

        PlainSparsePair *tryGetExisting(SlotId slot_id) const;

        PlainSparsePair *tryGetCached(SlotId slot_id) const noexcept;

        PlainSparsePair *tryGetCached(SlotId slot_id, AccessType access_type) const noexcept;

        void evictSlot(SlotId slot_id);
        
        void recordRefreshPage(std::uint64_t entry);

        void completeRefreshLog();

        void cancelRefreshLog();

        void refreshPages(const std::vector<std::uint64_t> &page_nums);

        void forCachedPairs(std::function<void(SlotId, PlainSparsePair &)> callback);

        std::size_t getChangeLogSize() const;

        ChangeLogT extractChangeLogPages();

        bool commit();

    private:
        std::shared_ptr<MS_MetaPrefix> m_prefix;
        std::shared_ptr<MS_MetaAllocator> m_allocator;
        AccessType m_access_type;
        StorageFlags m_flags;
        // shared change log for all managed pairs, cleared on commit
        // it contains page numbers which after translating to MS_Address also reveal slot IDs
        mutable ChangeLogT m_change_log;

        struct PairEntry
        {
            std::unique_ptr<PlainSparsePair> m_pair;
            AccessType m_access_type;
        };

        mutable std::unordered_map<SlotId, PairEntry> m_pairs;
        mutable SlotId m_hot_slot_id = 0;
        mutable PlainSparsePair *m_hot_pair = nullptr;
        mutable AccessType m_hot_access_type = AccessType::READ_ONLY;

        DRAM_Pair createDRAMPair(SlotId slot_id) const;

        void beginRefreshPages();

        static bool canUseCached(AccessType cached_access_type, AccessType requested_access_type) noexcept;

        void cacheHotPair(SlotId slot_id, PlainSparsePair &sparse_pair,
            AccessType access_type) const noexcept;
    };

}
