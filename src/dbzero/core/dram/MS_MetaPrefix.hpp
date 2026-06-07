// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/dram/MetaPrefix.hpp>
#include <dbzero/core/memory/Allocator.hpp>
#include <cassert>
#include <cstdint>
#include <memory>
#include <unordered_map>

namespace db0

{

    class Diff_IO;
    class SparsePair;

    class MS_Address
    {
    public:
        static MS_Address &from(std::uint64_t &encoded_address);

        static const MS_Address &from(const std::uint64_t &encoded_address);

        static std::uint64_t encode(Allocator::SlotId slot_id, std::uint64_t local_page_num);

        Allocator::SlotId slot_id() const;

        std::uint64_t local_page_num() const;

    private:
        friend class MS_MetaAllocator;

        static constexpr std::uint64_t LOCAL_PAGE_BITS = 24;
        static constexpr std::uint64_t SLOT_ID_BITS = 40;
        static constexpr std::uint64_t LOCAL_PAGE_COUNT = 1ull << LOCAL_PAGE_BITS;
        static constexpr std::uint64_t SLOT_ID_COUNT = 1ull << SLOT_ID_BITS;
        static constexpr std::uint64_t LOCAL_PAGE_MASK = LOCAL_PAGE_COUNT - 1;

        std::uint64_t m_encoded_address;
    };

    inline MS_Address &MS_Address::from(std::uint64_t &encoded_address)
    {
        return reinterpret_cast<MS_Address &>(encoded_address);
    }

    inline const MS_Address &MS_Address::from(const std::uint64_t &encoded_address)
    {
        return reinterpret_cast<const MS_Address &>(encoded_address);
    }

    inline std::uint64_t MS_Address::encode(Allocator::SlotId slot_id, std::uint64_t local_page_num)
    {
        assert(slot_id < SLOT_ID_COUNT);
        assert(local_page_num < LOCAL_PAGE_COUNT);
        return (static_cast<std::uint64_t>(slot_id) << LOCAL_PAGE_BITS) | local_page_num;
    }

    inline Allocator::SlotId MS_Address::slot_id() const
    {
        return m_encoded_address >> LOCAL_PAGE_BITS;
    }

    inline std::uint64_t MS_Address::local_page_num() const
    {
        return m_encoded_address & LOCAL_PAGE_MASK;
    }

    class MS_MetaPrefix: public MetaPrefix
    {
    public:
        MS_MetaPrefix(std::size_t page_size, SparsePair &sparse_pair, Diff_IO &page_io);

    private:
        friend class MS_MetaAllocator;
    };

    class MS_MetaAllocator: public Allocator
    {
    public:
        explicit MS_MetaAllocator(std::shared_ptr<MS_MetaPrefix> prefix);

        std::optional<Address> tryAlloc(std::size_t size, Allocator::SlotId slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;

        void free(Address address) override;

        std::size_t getAllocSize(Address address) const override;

        bool isAllocated(Address address, std::size_t *size_of_result = nullptr) const override;

        AllocationInfo findAllocation(Address address) const override;

        void commit() const override;

        void detach() const override;

    private:
        std::shared_ptr<MS_MetaPrefix> m_prefix;
        std::uint32_t m_ps_shift;
        std::unordered_map<Allocator::SlotId, std::shared_ptr<DRAM_Allocator> > m_allocators;

        void initializeAllocators();

        void forAllocatedAddresses(Allocator::SlotId slot_id, DRAM_Allocator::AddressSinkFunction sink) const;

        DRAM_Allocator &ensureAllocator(Allocator::SlotId slot_id);

        const DRAM_Allocator *findAllocator(Allocator::SlotId slot_id) const;
    };

}
