// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "SparsePair.hpp"
#include <dbzero/core/dram/MetaSpace.hpp>
#include <memory>
#include <unordered_map>

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
     * it needs access to MS_MetaAllocator slot metadata to reopen an existing
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
        SparsePairManager(MS_MetaSpace &metaspace, AccessType access_type = AccessType::READ_WRITE,
            StorageFlags flags = {});

        SparsePair &getOrCreate(Allocator::SlotId slot_id);

        SparsePair *tryGetCached(Allocator::SlotId slot_id) noexcept;

    private:
        std::shared_ptr<DRAM_Prefix> m_prefix;
        std::shared_ptr<MS_MetaAllocator> m_allocator;
        AccessType m_access_type;
        StorageFlags m_flags;
        std::unordered_map<Allocator::SlotId, std::unique_ptr<SparsePair> > m_pairs;
        Allocator::SlotId m_hot_slot_id = 0;
        SparsePair *m_hot_pair = nullptr;

        DRAM_Pair createDRAMPair(Allocator::SlotId slot_id) const;

        void cacheHotPair(Allocator::SlotId slot_id, SparsePair &sparse_pair) noexcept;
    };

}
