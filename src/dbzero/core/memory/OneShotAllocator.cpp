// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "OneShotAllocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <cassert>

namespace db0

{

    OneShotAllocator::OneShotAllocator(Address addr, std::size_t size)
        : m_addr(addr)        
        , m_size(size)
    {
    }
    
    std::optional<Address> OneShotAllocator::tryAlloc(std::size_t size, std::uint32_t slot_num,
        bool aligned, unsigned char, unsigned char)
    {
        assert(slot_num == 0);
        assert(!aligned && "OneShotAllocator: aligned allocation not supported");        
        if (size != m_size || m_allocated) {
            return std::nullopt;
        }
        m_allocated = true;
        return m_addr;
    }
    
    void OneShotAllocator::free(Address address)
    {
        if (address != m_addr || !m_allocated) {
            THROWF(db0::BadAddressException) << "OneShotAllocator invalid address: " << address;
        }
        m_allocated = false;
    }
    
    std::size_t OneShotAllocator::getAllocSize(Address address) const 
    {
        if (address != m_addr || !m_allocated) {
            THROWF(db0::BadAddressException) << "OneShotAllocator invalid address: " << address;
        }
        return m_size;
    }
    
    bool OneShotAllocator::isAllocated(Address address, std::size_t *size_of_result) const
    {
        if (address != m_addr || !m_allocated) {
            return false;
        }
        if (size_of_result) {
            *size_of_result = m_size;
        }
        return true;
    }
    
    void OneShotAllocator::commit() const {
        // nothing to do
    }

    void OneShotAllocator::detach() const {
        // nothing to do
    }

}