#pragma once

#include <cstdlib>
#include <cstdint>
#include <cassert>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    /**
     * Compute bitewise shift corresponding to a page size
    */
    unsigned int getPageShift(std::size_t page_size);

    std::uint64_t getPageMask(std::size_t page_size);

    void printBuffer(unsigned char *data, std::size_t len);
    
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

    template <typename StorageT> std::uint64_t findUniqueMutation(const StorageT &storage, std::uint64_t first_page, 
        std::uint64_t end_page, std::uint64_t state_num)
    {
        assert(end_page > first_page);
        std::uint64_t result = 0;
        for (; first_page < end_page; ++first_page) {
            auto mutation_id = storage.findMutation(first_page, state_num);
            if (result == 0) {
                result = mutation_id;
            } else if (result != mutation_id) {
                THROWF(db0::InternalException) << "Inconsistent mutations found in a wide range" << THROWF_END;
            }
        }
        return result;
    }
    
    template <typename StorageT> bool tryFindUniqueMutation(const StorageT &storage, std::uint64_t first_page,
        std::uint64_t end_page, std::uint64_t state_num, std::uint64_t &mutation_id)
    {
        std::uint64_t result = 0;
        bool has_mutation = false;
        for (;first_page < end_page; ++first_page) {
            if (storage.tryFindMutation(first_page, state_num, result)) {
                if (has_mutation && result != mutation_id) {
                    THROWF(db0::InternalException) << "Inconsistent mutations found in a wide range" << THROWF_END;
                }
                mutation_id = result;
                has_mutation = true;
            }
        }
        return has_mutation;
    }

}
