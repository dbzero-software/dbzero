// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MS_MetaPrefix.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/storage/Diff_IO.hpp>
#include <dbzero/core/storage/SparsePair.hpp>
#include <cassert>
#include <limits>
#include <optional>
#include <type_traits>
#include <vector>

namespace db0

{

    static_assert(sizeof(MS_Address) == sizeof(std::uint64_t));
    static_assert(alignof(MS_Address) == alignof(std::uint64_t));
    static_assert(std::is_standard_layout_v<MS_Address>);

    std::uint32_t ms_page_size_shift(std::uint64_t page_size)
    {
        if (page_size == 0 || (page_size & (page_size - 1)) != 0) {
            THROWF(db0::InternalException) << "MS_MetaSpace: page size must be a power of two";
        }
        std::uint32_t shift = 0;
        while ((1ull << shift) != page_size) {
            ++shift;
        }
        return shift;
    }

    inline std::uint64_t ms_external_page_num(Address address, std::uint32_t ps_shift)
    {
        return address.getOffset() >> ps_shift;
    }

    inline std::uint64_t ms_page_offset(Address address, std::uint32_t ps_shift)
    {
        return address.getOffset() & ((1ull << ps_shift) - 1);
    }

    inline Address ms_local_address(const MS_Address &address, std::uint32_t ps_shift, std::uint64_t page_offset = 0)
    {
        return Address::fromOffset((address.local_page_num() << ps_shift) + page_offset);
    }

    inline Address ms_external_address(Allocator::SlotId slot_id, Address local_address, std::uint32_t ps_shift)
    {
        auto local_page_num = local_address.getOffset() >> ps_shift;
        return Address::fromOffset(MS_Address::encode(slot_id, local_page_num) << ps_shift);
    }

    MS_MetaPrefix::MS_MetaPrefix(std::size_t page_size, SparsePair &sparse_pair, Diff_IO &page_io)
        : MetaPrefix(page_size, sparse_pair)
    {
        load(*this, page_io);
    }

    MS_MetaAllocator::MS_MetaAllocator(std::shared_ptr<MS_MetaPrefix> prefix)
        : m_prefix(std::move(prefix))
        , m_ps_shift(ms_page_size_shift(m_prefix->getPageSize()))
    {
        initializeAllocators();
    }

    void MS_MetaAllocator::initializeAllocators()
    {
        std::optional<Allocator::SlotId> current_slot_id;
        std::vector<std::size_t> local_addresses;

        auto create_slot_allocator = [&]() {
            if (!current_slot_id) {
                return;
            }
            auto allocator = std::make_shared<DRAM_Allocator>(
                [&local_addresses](DRAM_Allocator::AddressSinkFunction sink) {
                    for (auto local_address: local_addresses) {
                        sink(local_address);
                    }
                },
                m_prefix->getPageSize()
            );
            m_allocators.emplace(*current_slot_id, std::move(allocator));
            local_addresses.clear();
        };

        for (auto it = m_prefix->m_sparse_pair.getSparseIndex().cbegin(); !it.is_end(); ++it) {
            auto item = *it;
            if (!item || item.m_page_num == 0) {
                continue;
            }

            auto encoded_page_num = item.m_page_num;
            auto &address = MS_Address::from(encoded_page_num);
            auto local_page_num = address.local_page_num();
            if (local_page_num == 0) {
                continue;
            }

            auto slot_id = address.slot_id();
            if (current_slot_id && slot_id != *current_slot_id) {
                create_slot_allocator();
            }
            current_slot_id = slot_id;
            auto local_address = local_page_num << m_ps_shift;
            if (local_addresses.empty() || local_address != local_addresses.back()) {
                local_addresses.push_back(local_address);
            }
        }
        create_slot_allocator();
    }

    void MS_MetaAllocator::forAllocatedAddresses(Allocator::SlotId slot_id, DRAM_Allocator::AddressSinkFunction sink) const
    {
        auto first_page_num = MS_Address::encode(slot_id, 0);
        auto last_page_num = slot_id + 1 == MS_Address::SLOT_ID_COUNT
            ? std::numeric_limits<std::uint64_t>::max()
            : MS_Address::encode(slot_id + 1, 0);
        std::uint64_t previous_local_address = 0;
        m_prefix->m_sparse_pair.getSparseIndex().forPageRange(first_page_num, last_page_num, [&](const SI_Item &item) {
            if (!item || item.m_page_num == 0) {
                return;
            }

            auto encoded_page_num = item.m_page_num;
            auto &address = MS_Address::from(encoded_page_num);
            auto local_address = address.local_page_num() << m_ps_shift;
            if (local_address != 0 && local_address != previous_local_address) {
                sink(local_address);
                previous_local_address = local_address;
            }
        });
    }

    DRAM_Allocator &MS_MetaAllocator::ensureAllocator(Allocator::SlotId slot_id)
    {
        auto it = m_allocators.find(slot_id);
        if (it != m_allocators.end()) {
            return *it->second;
        }

        auto allocator = std::make_shared<DRAM_Allocator>(
            [this, slot_id](DRAM_Allocator::AddressSinkFunction sink) {
                forAllocatedAddresses(slot_id, std::move(sink));
            },
            m_prefix->getPageSize()
        );
        auto [new_it, inserted] = m_allocators.emplace(slot_id, std::move(allocator));
        (void)inserted;
        return *new_it->second;
    }

    const DRAM_Allocator *MS_MetaAllocator::findAllocator(Allocator::SlotId slot_id) const
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
        auto &allocator = ensureAllocator(slot_num);
        auto local_address = allocator.tryAlloc(size, 0, aligned, realm_id, locality);
        if (!local_address) {
            return std::nullopt;
        }
        return ms_external_address(slot_num, *local_address, m_ps_shift);
    }

    void MS_MetaAllocator::free(Address address)
    {
        auto encoded_page_num = ms_external_page_num(address, m_ps_shift);
        auto &ms_address = MS_Address::from(encoded_page_num);
        auto local_address = ms_local_address(ms_address, m_ps_shift, ms_page_offset(address, m_ps_shift));
        ensureAllocator(ms_address.slot_id()).free(local_address);
    }

    std::size_t MS_MetaAllocator::getAllocSize(Address address) const
    {
        auto encoded_page_num = ms_external_page_num(address, m_ps_shift);
        auto &ms_address = MS_Address::from(encoded_page_num);
        auto allocator = findAllocator(ms_address.slot_id());
        if (!allocator) {
            THROWF(db0::BadAddressException) << "Invalid MS_MetaSpace slot address: " << address;
        }
        return allocator->getAllocSize(ms_local_address(ms_address, m_ps_shift, ms_page_offset(address, m_ps_shift)));
    }

    bool MS_MetaAllocator::isAllocated(Address address, std::size_t *size_of_result) const
    {
        auto encoded_page_num = ms_external_page_num(address, m_ps_shift);
        auto &ms_address = MS_Address::from(encoded_page_num);
        auto allocator = findAllocator(ms_address.slot_id());
        if (!allocator) {
            return false;
        }
        return allocator->isAllocated(ms_local_address(ms_address, m_ps_shift, ms_page_offset(address, m_ps_shift)),
            size_of_result);
    }

    Allocator::AllocationInfo MS_MetaAllocator::findAllocation(Address address) const
    {
        auto encoded_page_num = ms_external_page_num(address, m_ps_shift);
        auto &ms_address = MS_Address::from(encoded_page_num);
        auto allocator = findAllocator(ms_address.slot_id());
        if (!allocator) {
            THROWF(db0::BadAddressException) << "Invalid MS_MetaSpace slot address: " << address;
        }
        auto local_info = allocator->findAllocation(
            ms_local_address(ms_address, m_ps_shift, ms_page_offset(address, m_ps_shift)));
        return {
            ms_external_address(ms_address.slot_id(), local_info.address, m_ps_shift),
            local_info.size
        };
    }

    void MS_MetaAllocator::commit() const
    {
    }

    void MS_MetaAllocator::detach() const
    {
    }

}
