// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/dram/MetaPrefix.hpp>
#include <dbzero/core/memory/Allocator.hpp>
#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <unordered_map>

namespace db0

{

    class SparsePair;
    struct MS_MetaSpace;

    enum class MS_MetaMappingPolicy
    {
        eager,
        lazy
    };

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
        friend class MS_MetaPrefix;

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
        using SlotLoadFunction = std::function<void(MS_MetaPrefix &, Allocator::SlotId)>;

        /**
         * Creates a metadata prefix over the shared sparse mapping.
         *
         * Without slot_load, the prefix assumes persisted contents are populated
         * externally, for example by load(MetaPrefix &, Diff_IO &) during eager
         * setup. With slot_load, mapRange invokes the callback once per slot on
         * first access; the callback should populate pages for that slot with
         * update(page_num, false).
         */
        MS_MetaPrefix(std::size_t page_size, SparsePair &sparse_pair, SlotLoadFunction slot_load = {});

        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) override;

        bool evictSlot(Allocator::SlotId slot_id);

        static Allocator::SlotId slotIdFromPageNum(std::uint64_t page_num);

        static std::pair<std::uint64_t, std::uint64_t> pageRangeForSlot(Allocator::SlotId slot_id);

    private:
        friend struct MS_MetaSpace;

        SlotLoadFunction m_slot_load;
        std::unordered_map<Allocator::SlotId, std::uint64_t> m_loaded_slot_high_watermarks;

        void ensureSlotLoaded(Allocator::SlotId slot_id, std::uint64_t page_num);
    };

    class MS_MetaAllocator: public Allocator
    {
    public:
        MS_MetaAllocator(SparsePair &sparse_pair, std::size_t page_size);

        std::optional<Address> tryAlloc(std::size_t size, Allocator::SlotId slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;

        void free(Address address) override;

        std::size_t getAllocSize(Address address) const override;

        bool isAllocated(Address address, std::size_t *size_of_result = nullptr) const override;

        AllocationInfo findAllocation(Address address) const override;

        void commit() const override;

        void detach() const override;

    private:
        SparsePair &m_sparse_pair;
        std::size_t m_page_size;
        std::uint32_t m_ps_shift;
        std::unordered_map<Allocator::SlotId, std::shared_ptr<DRAM_Allocator> > m_allocators;

        void initializeAllocators();

        void forAllocatedAddresses(Allocator::SlotId slot_id, DRAM_Allocator::AddressSinkFunction sink) const;

        DRAM_Allocator &ensureAllocator(Allocator::SlotId slot_id);

        const DRAM_Allocator *findAllocator(Allocator::SlotId slot_id) const;
    };

}
