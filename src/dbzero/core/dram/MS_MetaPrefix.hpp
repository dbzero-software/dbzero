// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "MS_Address.hpp"
#include "MS_MetaAllocator.hpp"
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/dram/MetaPrefix.hpp>
#include <dbzero/core/memory/Allocator.hpp>
#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <unordered_map>
#include <dbzero/core/storage/SparsePairFwd.hpp>

namespace db0

{

    struct MS_MetaSpace;

    enum class MappingPolicy
    {
        eager,
        lazy
    };
    
    class MS_MetaPrefix: public MetaPrefix
    {
    public:
        using SlotId = Allocator::SlotId;        

        /**
         * Creates a metadata prefix over the shared sparse mapping.
         * page_io reference is required for lazy / mixed slot loading policy
         */
        MS_MetaPrefix(std::size_t page_size, SparsePair &sparse_pair,
            RandomIO_Stream &page_io, MappingPolicy mapping_policy = MappingPolicy::eager);

        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) override;
        
        // Evict dirty and unused slot (must be flushed and detached)
        bool evictSlot(SlotId);
        
        // Get slot associated desc-io logical begin / end page pair
        std::pair<std::uint64_t, std::uint64_t> getPageRange(SlotId) const;

    private:
        friend struct MS_MetaSpace;
        
        const std::uint32_t m_ps_shift;
        RandomIO_Stream &m_page_io;
        const MappingPolicy m_mapping_policy;
        // the loaded slot IDs
        std::unordered_set<SlotId> m_slot_ids;

        void ensureSlot(SlotId);
        void loadSlot(SlotId);

        friend void load(MS_MetaPrefix &, const std::uint64_t *, const std::uint64_t *);
    };
    
    void load(MS_MetaPrefix &, const std::uint64_t *page_num, const std::uint64_t *end);

}
