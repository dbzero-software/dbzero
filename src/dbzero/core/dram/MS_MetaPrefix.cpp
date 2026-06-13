// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MS_MetaPrefix.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/utils.hpp>
#include <dbzero/core/storage/SparsePair.hpp>
#include <algorithm>
#include <cassert>
#include <cstring>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace db0

{

    static_assert(sizeof(MS_Address) == sizeof(std::uint64_t));
    static_assert(alignof(MS_Address) == alignof(std::uint64_t));
    static_assert(std::is_standard_layout_v<MS_Address>);
    
    MS_MetaPrefix::MS_MetaPrefix(
        std::size_t page_size, SparsePair &parent_index, RandomIO_Stream &page_io)
        : MetaPrefix(page_size, parent_index)
        , m_ps_shift(db0::getPageShift(page_size))        
        , m_page_io(page_io)
    {    
    }

    std::pair<std::uint64_t, std::uint64_t> MS_MetaPrefix::getPageRange(Allocator::SlotId slot_id) const
    {
        assert(slot_id < MS_Address::SLOT_ID_COUNT);
        auto first_addr = MS_Address::encode(slot_id, 0);
        auto end_addr = MS_Address::encode(slot_id + 1, 0);
        return { first_addr >> m_ps_shift, end_addr >> m_ps_shift };
    }

    bool MS_MetaPrefix::evictSlot(Allocator::SlotId slot_id)
    {
        if (m_slot_ids.erase(slot_id) == 0) {
            return false;
        }
        auto [first_page_num, end_page_num] = getPageRange(slot_id);
        // NOTE: this is sufficiently fast becuse DRAM_Prefix prunes the range internally        
        evictPageRange(first_page_num, end_page_num);
        return true;
    }
    
    bool MS_MetaPrefix::tryLoadSlot(SlotId slot_id, MS_MetaAllocator &allocator)
    {        
        // FIXME: implement
        THROWF(db0::InternalException) << "not implemented yet";
        /*
        m_slot_ids.insert(slot_id);
        auto [first_page_num, end_page_num] = getPageRange(slot_id);
        // Collect slot page numbers
        std::vector<std::uint64_t> slot_page_nums;
        m_sparse_pair.getSparseIndex().forUniquePageRange(first_page_num, end_page_num, [&](const SI_Item &item) {
            slot_page_nums.push_back(item.m_page_num);
        });
        auto updater = allocator.beginUpdate(slot_id);
        db0::load(*this, slot_page_nums.data(), slot_page_nums.data() + slot_page_nums.size(), std::move(updater));
        */
        return false;
    }

    void load(MS_MetaPrefix &prefix, const std::uint64_t *page_num, const std::uint64_t *end,
        DRAM_Allocator::Updater &&updater)
    {
        load(prefix, prefix.m_page_io, page_num, end);
        if (!updater) {
            return;
        }
        for (; page_num != end; ++page_num) {
            updater(MS_Address::from(*page_num << prefix.m_ps_shift).local_address());
        }
    }
    
}
