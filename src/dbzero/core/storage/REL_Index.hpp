#pragma once

#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>

namespace db0

{
    
    // REL_Index holds a complete mapping from relative to absolute Page IO addresses
    // (aka storage page numbers)
    // it only holds the location of the entire step of blocks
    // since relative addresses are from a continous space, they're represeted by vector indices
    // NOTE: REL_Index must be initialized with a known "step size" (must be power of 2)
    class REL_Index: protected db0::v_bvector<std::uint64_t>
    {
    public:
        using super_t = db0::v_bvector<std::uint64_t>;
        
        // as null
        REL_Index() = default;
        REL_Index(const REL_Index &) = delete;
        REL_Index(Memspace &, std::uint32_t step_size);
        REL_Index(mptr, std::uint32_t step_size);
        
        // Add a new mapping from relative page num to storage page num
        // @param page_num relative page num
        // @param page_io_address absolute storage page num
        void add(std::uint64_t page_num, std::uint64_t page_io_address);
        
        // Retrieve storage page num for a given relative page num
        std::uint64_t get(std::uint64_t page_num) const;
        
        void detach() const;
        void commit() const;
        
    private:
        std::shared_ptr<DRAM_Prefix> m_dram_prefix;
        std::shared_ptr<DRAM_Allocator> m_dram_allocator;
        Memspace m_dram_space;
        const AccessType m_access_type;
        
        std::uint32_t m_step_size = 0;
        std::uint32_t m_shift = 0;
    };
    
}