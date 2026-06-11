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

    enum class MetaSpaceLoadPolicy
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
         * diff_io reference is required for lazy / mixed slot loading policy
         */
        MS_MetaPrefix(std::size_t page_size, SparsePair &sparse_pair, 
            const Diff_IO *diff_io = nullptr);

        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) override;
        
        // Evict dirty and unused slot (must be flushed and detached)
        bool evictSlot(SlotId);
        
        // Get slot associated begin / end page pair
        static std::pair<std::uint64_t, std::uint64_t> getPageRange(SlotId);

    private:
        friend struct MS_MetaSpace;
        
        const std::uint32_t m_ps_shift;
        const Diff_IO *m_diff_io_ptr;
        // the loaded slot IDs
        std::unordered_set<SlotId> m_slot_ids;

        void ensureSlot(SlotId);
        void loadSlot(SlotId);
    };

}
