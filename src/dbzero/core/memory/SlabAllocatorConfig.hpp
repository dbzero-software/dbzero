#pragma once

#include <cstdint>
#include <cstdlib>
#include <functional>
#include "AccessOptions.hpp"
#include <dbzero/core/memory/Address.hpp>

namespace db0

{
    
    struct SlabAllocatorConfig
    {
        // 4KB pages
        static constexpr std::size_t DEFAULT_PAGE_SIZE = 4096;
        static constexpr std::size_t DEFAULT_SLAB_SIZE = 128u << 20;
        
        static constexpr unsigned int SLAB_BITSPACE_SIZE() {
            // Must equal the number of data pages in the entire slab
            return DEFAULT_SLAB_SIZE / DEFAULT_PAGE_SIZE;            
        }
        
        // Minimum operational capacity in bytes
        // i.e. slabs below this capacity will not be considered for allocation
        static constexpr std::size_t MIN_OP_CAPACITY() {
            return DEFAULT_SLAB_SIZE / 16;
        }
    };
    
}