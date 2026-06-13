// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "MS_Address.hpp"
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

    // MS_MetaAllocator organizes allocations into independently managed slots
    // Slot ID is encoded in the high bits of the returned address (with 40 / 24 bit split)
    // this leaves ~16M slot capacity which is sufficient for meta-data (e.g. single SLAB metadata)
    // but needs to be monitored to avoid unexpected exhaustion.
    class MS_MetaAllocator: public DRAM_Allocator
    {
    public:
        using SlotId = Allocator::SlotId;
        MS_MetaAllocator(SparsePair &parent_index, std::size_t page_size);

        std::optional<Address> tryAlloc(std::size_t size, Allocator::SlotId slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;

        void free(Address address) override;

        std::size_t getAllocSize(Address address) const override;

        bool isAllocated(Address address, std::size_t *size_of_result = nullptr) const override;

        AllocationInfo findAllocation(Address address) const override;

        void commit() const override;

        void detach() const override;

        std::optional<Address> tryFirstAlloc(SlotId);
        Address firstAlloc(SlotId) const override;

        void evictSlot(SlotId);
        
        // For scoped refresh / updates of the allocator state
        // NOTE: the no-op updater will be returned if the slot was restored and fully initialized
        DRAM_Allocator::Updater tryBeginUpdate(SlotId);
        
        // This version will expose a non-initialized allocator's updater if not found
        DRAM_Allocator::Updater beginUpdate(SlotId);
        
    private:
        SparsePair &m_parent_index;
        const std::size_t m_page_size;
        const std::uint32_t m_ps_shift;
        std::unordered_map<SlotId, std::shared_ptr<DRAM_Allocator> > m_allocators;

        void initializeAllocators();

        DRAM_Allocator &ensureAllocator(SlotId, bool *is_newly_created = nullptr);

        const DRAM_Allocator *tryFindAllocator(SlotId) const;
    };

}
