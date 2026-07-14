// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cassert>
#include <dbzero/core/memory/Allocator.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include "StorageClass.hpp"

namespace db0::object_model

{

DB0_PACKED_BEGIN

    // A struct that combines StorageClass (14bit) + address (50bits) in a single 64bit value
    // but ignores the address-embedded instance_id
    struct DB0_PACKED_ATTR TypedAddress
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
            return Address::fromOffset(m_value & 0x3FFFFFFFFFFFF);
        }
        
        inline operator Address() const {
            return getAddress();
        }

        void setAddress(Address);
        void setType(StorageClass type);

        inline bool operator==(const TypedAddress &other) const {
            return m_value == other.m_value;
        }

        inline bool operator<(const TypedAddress &other) const {
            return m_value < other.m_value;
        }
    };
    
    TypedAddress toTypedAddress(const std::pair<UniqueAddress, StorageClass> &);
    
DB0_PACKED_END

}

namespace std

{
    
    template <>
    struct hash<db0::object_model::TypedAddress>
    {
        std::size_t operator()(const db0::object_model::TypedAddress& k) const {
            return std::hash<std::uint64_t>()(k.m_value);
        }
    };

}