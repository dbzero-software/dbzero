// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MetaSpace.hpp"
#include "MetaPrefix.hpp"
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/storage/Diff_IO.hpp>
#include <dbzero/core/storage/SparsePair.hpp>
#include <algorithm>
#include <utility>

namespace db0

{
    Memspace MetaSpace::create(std::size_t page_size, SparsePair &sparse_pair, Diff_IO &page_io)
    {
        auto prefix = std::make_shared<MetaPrefix>(page_size, sparse_pair);
        load(*prefix, page_io);
        auto allocator = std::make_shared<DRAM_Allocator>(
            [&](DRAM_Allocator::AddressSinkFunction sink) {
                prefix->forAllocatedAddresses(sink);
            },
            page_size
        );
        return { prefix, allocator };
    }

    MS_MetaSpace::MS_MetaSpace(std::shared_ptr<MS_MetaPrefix> prefix, std::shared_ptr<MS_MetaAllocator> allocator)
        : Memspace(std::move(prefix), std::move(allocator))
    {
    }

    MS_MetaSpace MS_MetaSpace::create(std::size_t page_size, SparsePair &sparse_pair, Diff_IO &page_io)
    {
        return create(page_size, sparse_pair, page_io, MappingPolicy::eager);
    }

    MS_MetaSpace MS_MetaSpace::create(std::size_t page_size, SparsePair &sparse_pair, Diff_IO &page_io,
        MappingPolicy mapping_policy)
    {
        auto prefix = mapping_policy == MappingPolicy::lazy
            ? std::make_shared<MS_MetaPrefix>(page_size, sparse_pair, createSlotLoadFunction(sparse_pair, page_io))
            : std::make_shared<MS_MetaPrefix>(page_size, sparse_pair);
        if (mapping_policy == MappingPolicy::eager) {
            load(*prefix, page_io, [prefix](std::uint64_t page_num) {
                auto slot_id = MS_MetaPrefix::slotIdFromPageNum(page_num);
                auto &highest_page_num = prefix->m_loaded_slot_high_watermarks[slot_id];
                highest_page_num = std::max(highest_page_num, page_num);
            });
        }
        auto allocator = std::make_shared<MS_MetaAllocator>(sparse_pair, page_size);
        return { prefix, allocator };
    }

    std::shared_ptr<MS_MetaPrefix> MS_MetaSpace::getMSPrefixPtr() const
    {
        return std::static_pointer_cast<MS_MetaPrefix>(m_prefix);
    }

    std::shared_ptr<MS_MetaAllocator> MS_MetaSpace::getMSAllocatorPtr() const
    {
        return std::static_pointer_cast<MS_MetaAllocator>(m_allocator);
    }

    MS_MetaPrefix::SlotLoadFunction MS_MetaSpace::createSlotLoadFunction(SparsePair &sparse_pair, Diff_IO &page_io)
    {
        return [&sparse_pair, &page_io](MS_MetaPrefix &prefix, Allocator::SlotId slot_id) {
            auto [first_page_num, last_page_num] = MS_MetaPrefix::pageRangeForSlot(slot_id);
            auto state_num = prefix.getStateNum();
            std::uint64_t previous_page_num = 0;

            sparse_pair.getSparseIndex().forPageRange(first_page_num, last_page_num, [&](const SI_Item &item) {
                if (!item || item.m_page_num == 0 || item.m_page_num == previous_page_num) {
                    return;
                }
                auto page_buffer = prefix.update(item.m_page_num, false);
                if (prefix.readPage(page_io, item.m_page_num, state_num, page_buffer)) {
                    auto &highest_page_num = prefix.m_loaded_slot_high_watermarks[slot_id];
                    highest_page_num = std::max(highest_page_num, item.m_page_num);
                    previous_page_num = item.m_page_num;
                }
            });
        };
    }

}
