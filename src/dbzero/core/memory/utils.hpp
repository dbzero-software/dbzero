// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdlib>
#include <cstdint>
#include <cassert>
#include "config.hpp"
#include "Address.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <optional>

namespace db0

{
    
    /**
     * Compute bitewise shift corresponding to a page size
     * @param validate_pow_2 if false and page size is not a power of 2, the function will return 0
    */
    unsigned int getPageShift(std::size_t page_size, bool validate_pow_2 = true);
        
    std::uint64_t getPageMask(std::size_t page_size);

    void printBuffer(unsigned char *data, std::size_t len);
    
    /**
     * Unconditionally align address to the page boundary
     * @param addr the address to align
     * @param page_size the page size
     * @param direction either 1 or -1 (the direction in which the addresses are allocated)
    */
    Address alignAddress(Address addr, std::size_t page_size, int direction);
    
    /**
     * Apply the wide range alignment rules (i.e. align only if size > page_size / 2)
     * @param addr the address to align
     * @param size the size of the memory range
     * @param page_size the page size
     * @param direction either 1 or -1 (the direction in which the addresses are allocated)
    */
    Address alignWideRange(Address addr, std::size_t size, std::size_t page_size, int direction = 1);

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
        std::uint64_t end_page, StateNumType state_num, StateNumType &mutation_id)
    {
        StateNumType result = 0;
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
    
    template <typename T>
    std::optional<T> optional_max(const std::optional<T> &a, const std::optional<T> &b) 
    {
        if (a && b) {
            return std::max(*a, *b);
        } else if (a) {
            return a;
        } else {
            return b;
        }
        return {};
    }

    // std::abort or decrement the counter (nothing if counter is 0)
    void checkPoisonedOp(unsigned int &counter);
    
}
