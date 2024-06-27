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

        std::optional<std::uint64_t> tryAlloc(std::size_t size, std::uint32_t slot_num = 0) override;
        
        void free(std::uint64_t address) override;

        std::size_t getAllocSize(std::uint64_t address) const override;
        
        void commit() override;

        void detach() override;
        
        /**
         * Get address of the 1st allocation
        */
        std::uint64_t firstAlloc() const;

    private:
        static constexpr std::size_t FIRST_PAGE_ID = 1;
        const std::size_t m_page_size;
        // note that addr = 0x0 is reserved for the root allocation
        std::size_t m_next_page_id = FIRST_PAGE_ID;
        std::unordered_set<std::size_t> m_free_pages;
    };

}