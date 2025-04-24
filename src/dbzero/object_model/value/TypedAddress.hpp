#pragma once

#include <cstdint>
#include <cassert>
#include <dbzero/core/memory/Allocator.hpp>
#include "StorageClass.hpp"

namespace db0::object_model

{

    // A struct that combines StorageClass (14bit) + address (50bits) in a single 64bit value
    // but ignores the address-embedded instance_id
    struct [[gnu::packed]] TypedAddress
    {
        std::uint64_t m_value;

        TypedAddress() = default;

        inline TypedAddress(Address address)
            : m_value(address.getOffset())
        {
        }
        
        inline TypedAddress(StorageClass type, Address address)
            : m_value((static_cast<std::uint64_t>(type) << 50) | address.getOffset())
        {            
        }

        inline StorageClass getType() const {
            return static_cast<StorageClass>(m_value >> 50);
        }
        
        inline Address getAddress() const {
            return Address::fromOffset(m_value & 0x3FFFFFFFFFFFFF);
        }
                
        inline operator Address() const {
            return getAddress();
        }

        void setAddress(Address);
        void setType(StorageClass type);

        bool operator==(const TypedAddress &other) const;
        bool operator<(const TypedAddress &other) const;
    };
    
}