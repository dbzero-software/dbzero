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
        auto [first_page_num, end_page_num] = getPageRange(slot_id);
        // Collect slot-specific storage (logical) page numbers first
        std::vector<std::uint64_t> slot_page_nums;
        m_parent_index.forUniquePageRange(first_page_num, end_page_num, [&](std::uint64_t page_num) {
            slot_page_nums.push_back(page_num);
        });
        auto updater = allocator.beginUpdate(slot_id);
        db0::load(*this, slot_page_nums.data(), slot_page_nums.data() + slot_page_nums.size(), std::move(updater));
        m_slot_ids.insert(slot_id);
        return false;
    }

    void load(MS_MetaPrefix &prefix, const std::uint64_t *page_num, const std::uint64_t *end,
        DRAM_Allocator::Updater &&updater)
    {
        db0::load(prefix, prefix.m_page_io, page_num, end);
        if (!updater) {
            return;
        }
        for (; page_num != end; ++page_num) {
            updater(MS_Address::from(*page_num << prefix.m_ps_shift).local_address());
        }
    }
    
    void load(MS_MetaPrefix &prefix, MS_MetaAllocator &allocator)
    {
        std::vector<std::uint64_t> page_nums;
        Allocator::SlotId current_slot_id = 0;

        auto load_current_slot = [&]() {
            if (!page_nums.empty()) {
                auto updater = allocator.beginUpdate(current_slot_id);
                db0::load(prefix, prefix.m_page_io, page_nums.data(), page_nums.data() + page_nums.size(), std::move(updater));
                prefix.m_slot_ids.insert(current_slot_id);
            }
        };

        // Iterate all known pages and load on a per-slot basis
        prefix.m_parent_index.forUniquePageRange([&](std::uint64_t page_num) {
            auto slot_id = MS_Address::from(page_num << prefix.m_ps_shift).slot_id();
            if (slot_id != current_slot_id) {
                load_current_slot();
                page_nums.clear();
                current_slot_id = slot_id;
            }
            page_nums.push_back(page_num);
        });

        load_current_slot();
    }

}
