#include "utils.hpp"
    
namespace db0

{

    unsigned int getPageShift(std::size_t page_size)
    {
        unsigned int shift = 0;
        while (page_size > 1)
        {
            page_size >>= 1;
            ++shift;
        }
        return shift;
    }

    std::uint64_t getPageMask(std::size_t page_size)
    {
        return page_size - 1;
    }

    std::uint64_t alignAddress(std::uint64_t addr, std::size_t page_size, int direction)
    {
        if (addr % page_size != 0)
            return addr - (addr % page_size) + (direction > 0 ? page_size : 0);
        return addr;
    }

    std::uint64_t alignWideRange(std::uint64_t addr, std::size_t size, std::size_t page_size, int direction) 
    {
        return size > (page_size << 1) ? alignAddress(addr, page_size, direction) : addr;
    }

}