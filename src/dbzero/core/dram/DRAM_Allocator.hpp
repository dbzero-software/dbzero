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
        
        /**
         * Update with externally provided list of allocations (add new allocations)
         */
        void update(const std::unordered_set<std::size_t> &allocs);

        std::optional<Address> tryAlloc(std::size_t size, std::uint32_t slot_num = 0, 
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;
        
        void free(Address) override;

        std::size_t getAllocSize(Address) const override;

        bool isAllocated(Address, std::size_t *size_of_result = nullptr) const override;
        
        void commit() const override;

        void detach() const override;
        
        /**
         * Get address of the 1st allocation
        */
        Address firstAlloc() const;

    private:
        static constexpr std::size_t FIRST_PAGE_ID = 1;
        const std::size_t m_page_size;
        // note that addr = 0x0 is reserved for the root allocation
        std::size_t m_next_page_id = FIRST_PAGE_ID;
        std::unordered_set<std::size_t> m_free_pages;
    };
    
}