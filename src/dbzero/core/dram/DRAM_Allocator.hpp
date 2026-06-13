// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/memory/Allocator.hpp>
#include <unordered_set>

namespace db0

{

    /**
     * In-memory only allocator, allocates only whole memory pages
    */
    class DRAM_Allocator: public Allocator
    {
    public:
        DRAM_Allocator(std::size_t page_size);

        /**
         * Create pre-populated with existing allocations
        */
        DRAM_Allocator(const std::unordered_set<std::size_t> &allocs, std::size_t page_size);

        struct Updater
        {
            DRAM_Allocator *m_allocator = nullptr;
            std::uint64_t m_max_page_id = FIRST_PAGE_ID;
            const std::size_t m_page_size = 0;

            // no-op updater
            Updater() = default;
            Updater(DRAM_Allocator &);
            // must be called after all updates to finalize the state
            ~Updater();

            // must be populated in address-ascending order
            void operator()(std::size_t addr);
            bool operator!() const;
        };
        
        // Allows populating the initial state, only allowed when the allocator is empty
        // expecting a complete list of allocated addresses (e.g. from the underlying storage) 
        // and to be provided in ascending order
        Updater beginUpdate();
        
        /**
         * Update with externally provided list of allocations (add new allocations)
         */
        void update(const std::unordered_set<std::size_t> &allocs);

        void reset();

        std::optional<Address> tryAlloc(std::size_t size, SlotId slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;

        void free(Address) override;

        std::size_t getAllocSize(Address) const override;

        bool isAllocated(Address, std::size_t *size_of_result = nullptr) const override;

        AllocationInfo findAllocation(Address) const override;

        void commit() const override;

        void detach() const override;
        
        /**
         * Get address of the 1st allocation
        */
        virtual Address firstAlloc(SlotId = 0) const;

        bool empty() const;

    private:
        static constexpr std::size_t FIRST_PAGE_ID = 1;
        const std::size_t m_page_size;
        // note that addr = 0x0 is reserved for the root allocation
        std::size_t m_next_page_id = FIRST_PAGE_ID;
        std::unordered_set<std::size_t> m_free_pages;
    };

}
