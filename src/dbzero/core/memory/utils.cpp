// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "utils.hpp"
    
namespace db0

{   

    unsigned int getPageShift(std::size_t page_size, bool validate_pow_2)
    {
        auto initial_size = page_size;
        unsigned int shift = 0;
        while (page_size > 1) {
            if (page_size & 0x01) {
                // page size is not a power of 2
                if (validate_pow_2) {
                    THROWF(db0::InternalException) << "Page size is not a power of 2: " << initial_size << THROWF_END;
                }
                return 0;                
            }
            page_size >>= 1;
            ++shift;
        }
        return shift;
    }

    std::uint64_t getPageMask(std::size_t page_size) {
        return page_size - 1;
    }

    Address alignAddress(Address addr, std::size_t page_size, int direction)
    {
        if (addr % page_size != 0)
            return addr - (addr % page_size) + (direction > 0 ? page_size : 0);
        return addr;
    }

    Address alignWideRange(Address addr, std::size_t size, std::size_t page_size, int direction) {
        return size > (page_size << 1) ? alignAddress(addr, page_size, direction) : addr;
    }

    void printBuffer(unsigned char *data, std::size_t len)
    {
        std::cout << "buffer: ";
        for (std::size_t i = 0; i < len; ++i) {
            std::cout << (int)data[i] << " ";
        }
        std::cout << std::endl;
    }
    
    void checkPoisonedOp(unsigned int &counter)
    {
        if (counter > 0) {            
            if (--counter == 0) {
                std::abort();
            }
        }
    }

}