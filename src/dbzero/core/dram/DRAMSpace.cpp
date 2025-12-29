// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "DRAMSpace.hpp"
#include "DRAM_Prefix.hpp"
#include "DRAM_Allocator.hpp"

namespace db0

{   

    Memspace DRAMSpace::create(std::size_t page_size, std::function<void(DRAM_Pair)> callback) 
    {        
        auto dram_pair = std::make_pair(
            std::make_shared<DRAM_Prefix>(page_size),
            std::make_shared<DRAM_Allocator>(page_size) );
        if (callback) {
            callback(dram_pair);
        }
        return DRAMSpace::create(dram_pair);
    }      
    
    Memspace DRAMSpace::create(DRAM_Pair dram_pair)
    {
        assert((dram_pair.first && dram_pair.second) || (!dram_pair.first && !dram_pair.second));
        if (!dram_pair.first || !dram_pair.second) {
            THROWF(db0::InternalException) << "Invalid DRAM_Pair provided to DRAMSpace::create";
        }
        return { dram_pair.first, dram_pair.second };
    }

    Memspace DRAMSpace::tryCreate(DRAM_Pair dram_pair)
    {
        assert((dram_pair.first && dram_pair.second) || (!dram_pair.first && !dram_pair.second));
        if (dram_pair.first && dram_pair.second) {         
            return { dram_pair.first, dram_pair.second };
        } else {
            return {};        
        }
    }

}