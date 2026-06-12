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
    
    MS_MetaPrefix::MS_MetaPrefix(std::size_t page_size,
        SparsePair &sparse_pair, Diff_IO &diff_io, MappingPolicy mapping_policy)
        : MetaPrefix(page_size, sparse_pair)
        , m_ps_shift(db0::getPageShift(page_size))        
        , m_diff_io(diff_io)
        , m_mapping_policy(mapping_policy)
    {    
    }

    std::pair<std::uint64_t, std::uint64_t> MS_MetaPrefix::getPageRange(Allocator::SlotId slot_id) const
    {
        assert(slot_id < MS_Address::SLOT_ID_COUNT);
        auto first_addr = MS_Address::encode(slot_id, 0);
        auto end_addr = MS_Address::encode(slot_id + 1, 0);
        return { first_addr >> m_ps_shift, end_addr >> m_ps_shift };
    }

    void MS_MetaPrefix::ensureSlot(Allocator::SlotId slot_id)
    {
        if (m_slot_ids.insert(slot_id).second) {
            loadSlot(slot_id);
        }
    }

    MemLock MS_MetaPrefix::mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> access_mode)
    {
        ensureSlot(MS_Address::from(address).slot_id());
        return MetaPrefix::mapRange(address, size, access_mode);
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
    
    void MS_MetaPrefix::loadSlot(SlotId slot_id)
    {
        auto [first_page_num, end_page_num] = getPageRange(slot_id);
        // Collect slot page numbers
        std::vector<std::uint64_t> slot_page_nums;            
        std::uint64_t last_page_num = 0;
        m_sparse_pair.getSparseIndex().forPageRange(first_page_num, end_page_num, [&](const SI_Item &item) {
            if (!item || item.m_page_num == 0 || item.m_page_num == last_page_num) {
                return;
            }
            slot_page_nums.push_back(item.m_page_num);
            last_page_num = item.m_page_num;
        });
        db0::load(*this, m_diff_io, slot_page_nums);
    }

    void load(MS_MetaPrefix &prefix, const std::uint64_t *page_num, const std::uint64_t *end)
    {
        load(prefix, prefix.m_diff_io, page_num, end);
    }
    
}
