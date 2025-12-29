// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "EmbeddedAllocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    std::optional<Address> EmbeddedAllocator::tryAlloc(std::size_t size, std::uint32_t slot_num, 
        bool aligned, unsigned char, unsigned char)
    {
        auto new_address = Address::fromOffset(4096 * ++m_count);
        m_allocations[new_address] = size;
        if (m_alloc_callback) {
            m_alloc_callback(size, slot_num, aligned, new_address);
        }
        return new_address;
    }
    
    void EmbeddedAllocator::free(Address address)
    {
        auto it = m_allocations.find(address);
        if (it == m_allocations.end()) {
            THROWF(db0::InternalException) << "address not found: " << address;
        }
        m_allocations.erase(it);        
    }

    std::size_t EmbeddedAllocator::getAllocSize(Address address) const
    {
        auto it = m_allocations.find(address);
        if (it == m_allocations.end()) {
            THROWF(db0::InternalException) << "address not found: " << address;
        }
        return it->second;
    }
    
    bool EmbeddedAllocator::isAllocated(Address address, std::size_t *size_of_result) const
    {
        auto it = m_allocations.find(address);
        if (it == m_allocations.end()) {
            return false;
        }
        if (size_of_result) {
            *size_of_result = it->second;
        }
        return true;        
    }
    
    void EmbeddedAllocator::commit() const {
        // nothing to do
    }

    void EmbeddedAllocator::detach() const {
        // nothing to do
    }
    
    void EmbeddedAllocator::setAllocCallback(AllocCallbackT callback) {
        this->m_alloc_callback = callback;
    }
    
}