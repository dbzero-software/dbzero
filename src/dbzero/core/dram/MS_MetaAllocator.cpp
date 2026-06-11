// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MS_MetaAllocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
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
    
    inline Address ms_external_address(Allocator::SlotId slot_id, Address local_addr)
    {        
        // external address = slot ID + local address
        return Address::fromOffset(MS_Address::encode(slot_id, local_addr));
    }
    
    MS_MetaAllocator::MS_MetaAllocator(SparsePair &sparse_pair, std::size_t page_size)
        : DRAM_Allocator(page_size)
        , m_sparse_pair(sparse_pair)
        , m_page_size(page_size)
        , m_ps_shift(db0::getPageShift(page_size))
    {
        initializeAllocators();
    }

    void MS_MetaAllocator::initializeAllocators()
    {
        std::optional<Allocator::SlotId> current_slot_id;
        // Current slot-local assigned addresses
        std::unordered_set<std::uint64_t> local_allocs;

        auto create_slot_allocator = [&]() {
            if (!current_slot_id) {
                return;
            }
            auto allocator = std::make_shared<DRAM_Allocator>(local_allocs, m_page_size);            
            m_allocators.emplace(*current_slot_id, std::move(allocator));
            local_allocs.clear();
        };

        // NOTE: sorted iteration exposes slot-ordered page number
        std::uint64_t last_addr = 0;
        for (auto it = m_sparse_pair.getSparseIndex().cbegin(); !it.is_end(); ++it) {
            auto item = *it;
            if (!item || item.m_page_num == 0) {
                continue;
            }
            
            // page-shift to obtain actual address
            auto ext_addr = item.m_page_num << m_ps_shift;
            auto &address = MS_Address::from(ext_addr);
            auto local_addr = address.local_address();
            if (local_addr == 0) {
                continue;
            }

            auto slot_id = address.slot_id();
            if (current_slot_id && slot_id != *current_slot_id) {
                // next slot ID encountered
                create_slot_allocator();
                last_addr = 0;
            }
            current_slot_id = slot_id;
            // NOTE: the same address will be repeated with multiple different state numbers
            if (local_addr != last_addr) {
                local_allocs.insert(local_addr);
                last_addr = local_addr;
            }            
        }
        create_slot_allocator();
    }

    void MS_MetaAllocator::forAllocatedAddresses(
        Allocator::SlotId slot_id, std::function<void(std::uint64_t)> sink) const
    {
        auto first_addr = MS_Address::encode(slot_id, 0);
        auto last_addr = slot_id + 1 == MS_Address::SLOT_ID_COUNT
            ? std::numeric_limits<std::uint64_t>::max()
            : MS_Address::encode(slot_id + 1, 0);
        std::uint64_t last_addr = 0;
        // iterate range of address-related pages
        m_sparse_pair.getSparseIndex().forPageRange(first_addr >> m_ps_shift, last_addr >> m_ps_shift, [&](const SI_Item &item) {
            if (!item || item.m_page_num == 0) {
                return;
            }

            auto ext_addr = item.m_page_num << m_ps_shift;
            auto &address = MS_Address::from(ext_addr);
            auto local_addr = address.local_address();
            if (local_addr != 0 && local_addr != last_addr) {
                sink(local_addr);
                last_addr = local_addr;
            }
        });
    }
    
    DRAM_Allocator &MS_MetaAllocator::ensureAllocator(Allocator::SlotId slot_id)
    {
        auto it = m_allocators.find(slot_id);
        if (it != m_allocators.end()) {
            return *it->second;
        }

        auto allocator = std::make_shared<DRAM_Allocator>(m_page_size);
        // initialize allocator with the updater
        {
            auto updater = allocator->beginUpdate();
            forAllocatedAddresses(slot_id, [&](std::uint64_t local_addr) {
                updater(local_addr);
            });
        }

        auto [new_it, inserted] = m_allocators.emplace(slot_id, std::move(allocator));
        (void)inserted;
        return *new_it->second;
    }

    const DRAM_Allocator *MS_MetaAllocator::tryFindAllocator(Allocator::SlotId slot_id) const
    {
        auto it = m_allocators.find(slot_id);
        if (it == m_allocators.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    std::optional<Address> MS_MetaAllocator::tryAlloc(std::size_t size, Allocator::SlotId slot_num,
        bool aligned, unsigned char realm_id, unsigned char locality)
    {
        auto local_addr = ensureAllocator(slot_num).tryAlloc(size, 0, aligned, realm_id, locality);        
        if (!local_addr) {
            return std::nullopt;
        }
        return ms_external_address(slot_num, *local_addr);
    }

    void MS_MetaAllocator::free(Address address)
    {
        auto &ms_addr = MS_Address::from(address);
        ensureAllocator(ms_addr.slot_id()).free(ms_addr.local_address());
    }

    std::size_t MS_MetaAllocator::getAllocSize(Address address) const
    {
        auto &ms_addr = MS_Address::from(address);
        auto allocator = tryFindAllocator(ms_addr.slot_id());
        if (!allocator) {
            THROWF(db0::BadAddressException) << "Invalid MS_MetaSpace slot address: " << address;
        }
        return allocator->getAllocSize(ms_addr.local_address());
    }

    bool MS_MetaAllocator::isAllocated(Address address, std::size_t *size_of_result) const
    {
        auto &ms_addr = MS_Address::from(address);
        auto allocator = tryFindAllocator(ms_addr.slot_id());
        if (!allocator) {
            return false;
        }
        return allocator->isAllocated(ms_addr.local_address(), size_of_result);
    }

    Allocator::AllocationInfo MS_MetaAllocator::findAllocation(Address address) const
    {
        auto &ms_addr = MS_Address::from(address);
        auto allocator = tryFindAllocator(ms_addr.slot_id());
        if (!allocator) {
            THROWF(db0::BadAddressException) << "Invalid MS_MetaSpace slot address: " << address;
        }
        auto local_info = allocator->findAllocation(ms_addr.local_address());
        return {
            ms_external_address(ms_addr.slot_id(), local_info.address),
            local_info.size
        };
    }

    std::optional<Address> MS_MetaAllocator::tryFirstAlloc(Allocator::SlotId slot_id)
    {        
        auto local_addr = ensureAllocator(slot_id).tryFirstAlloc();
        if (!local_addr) {
            return std::nullopt;
        }
        return ms_external_address(slot_id, *local_addr);
    }

    void MS_MetaAllocator::evictSlot(Allocator::SlotId slot_id)
    {
        m_allocators.erase(slot_id);
    }

    void MS_MetaAllocator::commit() const
    {
    }

    void MS_MetaAllocator::detach() const
    {
    }

}
