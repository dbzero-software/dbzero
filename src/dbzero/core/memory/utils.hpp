#pragma once

#include <cstdlib>
#include <cstdint>

namespace db0

{

    /**
     * Compute bitewise shift corresponding to a page size
    */
    unsigned int getPageShift(std::size_t page_size);

    std::uint64_t getPageMask(std::size_t page_size);

    /**
     * Unconditionally align address to the page boundary
     * @param addr the address to align
     * @param page_size the page size
     * @param direction either 1 or -1 (the direction in which the addresses are allocated)
    */
    std::uint64_t alignAddress(std::uint64_t addr, std::size_t page_size, int direction);
    
    /**
     * Apply the wide range alignment rules (i.e. align only if size > page_size / 2)
     * @param addr the address to align
     * @param size the size of the memory range
     * @param page_size the page size
     * @param direction either 1 or -1 (the direction in which the addresses are allocated)
    */
    std::uint64_t alignWideRange(std::uint64_t addr, std::size_t size, std::size_t page_size, int direction = 1);

}
