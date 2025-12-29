// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "AlgoAllocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <cassert>

namespace db0

{

    AlgoAllocator::AlgoAllocator(AddressPoolF f, ReverseAddressPoolF rf, std::size_t alloc_size)
        : m_address_pool_f(f)
        , m_reverse_address_pool_f(rf)
        , m_alloc_size(alloc_size)        
    {
    }
    
    std::optional<Address> AlgoAllocator::tryAlloc(std::size_t size, std::uint32_t slot_num,
        bool aligned, unsigned char, unsigned char)
    {
        assert(slot_num == 0);
        assert(!aligned && "AlgoAllocator: aligned allocation not supported");
        assert(size == m_alloc_size && "AlgoAllocator: invalid alloc size requested");
        return m_address_pool_f(m_next_i++);
    }
    
    void AlgoAllocator::free(Address address)
    {
        if (address % m_alloc_size != 0) {
            // allow sub-allocations
            return;
        }
        auto i = m_reverse_address_pool_f(address);
        if (i >= m_next_i) {
            THROWF(db0::BadAddressException) << "AlgoAllocator: invalid address " << address;
        }
        if (i == m_next_i - 1) {
            --m_next_i;
        }
    }
    
    std::size_t AlgoAllocator::getAllocSize(Address address) const
    {
        auto offset = address % m_alloc_size;
        auto i = m_reverse_address_pool_f(address - offset);
        if (i >= m_next_i) {
            THROWF(db0::BadAddressException) << "AlgoAllocator: invalid address " << address;
        }
        return m_alloc_size - offset;
    }
    
    bool AlgoAllocator::isAllocated(Address address, std::size_t *size_of_result) const
    {
        auto offset = address % m_alloc_size;
        auto i = m_reverse_address_pool_f(address - offset);
        if (i >= m_next_i) {
            return false;
        }
        if (size_of_result) {
            *size_of_result = m_alloc_size - offset;
        }
        return true;
    }
    
    void AlgoAllocator::reset() {
        m_next_i = 0;
    }
    
    Address AlgoAllocator::getRootAddress() const {
        return m_address_pool_f(0);
    }
    
    void AlgoAllocator::setMaxAddress(Address max_address)
    {
        auto offset = max_address % m_alloc_size;
        m_next_i = m_reverse_address_pool_f(max_address - offset) + 1;
    }
    
    void AlgoAllocator::commit() const
    {
    }
    
    void AlgoAllocator::detach() const
    {
    }

}