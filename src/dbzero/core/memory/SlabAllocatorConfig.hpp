// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
        static constexpr std::size_t DEFAULT_SLAB_SIZE = 64u << 20;
        
        static constexpr unsigned int SLAB_BITSPACE_SIZE() {
            // Must equal the number of data pages in the entire slab            
            return DEFAULT_SLAB_SIZE / DEFAULT_PAGE_SIZE;            
        }
        
        // Minimum operational capacity in bytes
        // i.e. slabs with remaining capacity below this value will not be considered for allocation
        static std::size_t MIN_OP_CAPACITY(std::size_t slab_size) {
            // NOTE: 1/2 may seem very high but it helps improve performance under heavy fragmentation
            return slab_size / 2;
        }
        
        // The number of alloc attempts from existing slabs before
        // resorting to adding a new slab
        static constexpr int NUM_EXISTING_SLAB_ALLOC_ATTEMPTS = 2;
    };
    
}