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
    
    Memspace DRAMSpace::create(DRAM_Pair dram_pair) {
        return { dram_pair.first, dram_pair.second };
    }

}