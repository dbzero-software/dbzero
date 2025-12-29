// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Allocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{
    
    std::optional<UniqueAddress> Allocator::tryAllocUnique(
        std::size_t, std::uint32_t, bool, unsigned char, unsigned char)
    {
        THROWF(InternalException) 
            << "Allocator: unique allocation not supported by: " << typeid(*this).name() << THROWF_END;
    }
    
    Address Allocator::alloc(std::size_t size, std::uint32_t slot_num, bool aligned, 
        unsigned char realm_id, unsigned char locality)
    {
        auto result = tryAlloc(size, slot_num, aligned, realm_id, locality);
        if (!result) {
            THROWF(InternalException) << "Allocator: out of memory" << THROWF_END;
        }
        return *result;
    }
    
    UniqueAddress Allocator::allocUnique(std::size_t size, std::uint32_t slot_num, bool aligned, 
        unsigned char realm_id, unsigned char locality)
    {
        auto result = tryAllocUnique(size, slot_num, aligned, realm_id, locality);
        if (!result) {
            THROWF(InternalException) << "Allocator: out of memory" << THROWF_END;
        }
        return *result;
    }
    
    void Allocator::flush() const {
    }
    
    bool Allocator::inRange(Address) const {
        return true;
    }
    
    std::size_t Allocator::getAllocSize(Address address, unsigned char realm_id) const {
        return getAllocSize(address);
    }
    
    bool Allocator::isAllocated(Address address, unsigned char, std::size_t *size_of_result) const {
        return isAllocated(address, size_of_result);
    }
    
    std::pair<Address, std::optional<Address> > Allocator::getRange(std::uint32_t slot_num) const
    {
        if (slot_num != 0) {
            THROWF(InternalException) << "Invalid / unsupported slot number";        
        }
        return { Address::fromOffset(0), std::nullopt };
    }

    void Allocator::close()
    {
    }

}