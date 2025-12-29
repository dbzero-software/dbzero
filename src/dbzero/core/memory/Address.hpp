// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cassert>
#include <functional>
#include <ostream>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

DB0_PACKED_BEGIN
    template <typename store_t> class DB0_PACKED_ATTR AddressType
    {
    public:
        using offset_t = store_t;

        // Construct as null / invalid
        AddressType() = default;
        
        // make address from offset
        static inline AddressType fromOffset(std::uint64_t offset) {
            return AddressType(offset);
        }

        static inline AddressType fromValue(std::uint64_t value) {
            return AddressType(value);
        }

        inline bool operator!() const {
            return m_value == 0;
        }
        
        inline bool isValid() const {
            return m_value != 0; 
        }

        inline std::uint64_t getOffset() const {
            return m_value;
        }

        inline std::uint64_t getValue() const {
            return m_value;
        }

        inline bool operator==(const AddressType& other) const {
            return m_value == other.m_value;
        }

        inline bool operator!=(const AddressType& other) const {
            return m_value != other.m_value;
        }

        inline bool operator<(const AddressType& other) const {
            return m_value < other.m_value;
        }

        inline bool operator>(const AddressType& other) const {
            return m_value > other.m_value;
        }
        
        // Offset cast
        inline operator store_t() const {
            return m_value;
        }

        inline AddressType operator+(offset_t offset) const {
            return AddressType(m_value + offset);
        }

        inline AddressType operator-(offset_t offset) const {
           return AddressType(m_value - offset);
        }

        template <typename T = offset_t, typename = std::enable_if_t<!std::is_same_v<T, std::size_t>>>
        inline AddressType operator+(std::size_t offset) const {
            return AddressType(m_value + offset);
        }

        template <typename T = offset_t, typename = std::enable_if_t<!std::is_same_v<T, std::size_t>>>
        inline AddressType operator-(std::size_t offset) const {
            return AddressType(m_value - offset);
        }

        
    private:
        store_t m_value = 0;
        
        inline AddressType(std::uint64_t value)
            : m_value(value)
        {            
        }
    };
DB0_PACKED_END
    using Address = AddressType<std::uint64_t>;
    
    // The UniqueAddress combines memory offset and instance ID
    // by definition the UniqueAddress will not be assigned more than once throughut the lifetime of the prefix
DB0_PACKED_BEGIN    
    class DB0_PACKED_ATTR UniqueAddress
    {
    public:
        static constexpr std::size_t INSTANCE_ID_SHIFT = 14;
        static constexpr std::size_t INSTANCE_ID_MASK = (1ULL << INSTANCE_ID_SHIFT) - 1;     
        static constexpr std::size_t INSTANCE_ID_MAX = (1ULL << INSTANCE_ID_SHIFT) - 1;

        // Construct as null / invalid
        UniqueAddress() = default;
        
        inline UniqueAddress(Address address, std::uint16_t instance_id)
            : m_value((address.getOffset() << INSTANCE_ID_SHIFT) | static_cast<std::uint64_t>(instance_id))
        {
            assert(instance_id > 0);
            assert(address.getOffset() < (1ULL << 50));
            assert(instance_id < (1ULL << INSTANCE_ID_SHIFT));
        }
        
        static inline UniqueAddress fromValue(std::uint64_t value) {
            return UniqueAddress(value);
        }
        
        inline bool isValid() const {
            return m_value != 0; 
        }
        
        inline std::uint64_t getOffset() const {
            return m_value >> INSTANCE_ID_SHIFT; 
        }

        inline std::uint16_t getInstanceId() const {
            assert(m_value & INSTANCE_ID_MASK);
            return static_cast<std::uint16_t>(m_value & INSTANCE_ID_MASK); 
        }

        bool hasInstanceId() const {
            return (m_value & INSTANCE_ID_MASK) != 0;
        }

        inline bool operator==(const UniqueAddress& other) const {
            return m_value == other.m_value;
        }

        inline bool operator!=(const UniqueAddress& other) const {
            return m_value != other.m_value;
        }

        inline bool operator<(const UniqueAddress& other) const {
            return m_value < other.m_value;
        }

        inline bool operator<=(const UniqueAddress& other) const {
            return m_value <= other.m_value;
        }

        inline bool operator>(const UniqueAddress& other) const {
            return m_value > other.m_value;
        }

        inline bool operator>=(const UniqueAddress& other) const {
            return m_value > other.m_value;
        }
        
        // operator<<
        inline friend std::ostream &operator<<(std::ostream &os, const UniqueAddress &addr) {
            os << addr.m_value;
            return os;
        }

        // Get offset + instance ID encoded in a single 64-bit value
        inline std::uint64_t getValue() const {
            return m_value;
        }

        inline Address getAddress() const {
            return Address::fromOffset(getOffset());
        }
        
        // Address cast
        inline operator Address() const {
            return Address::fromOffset(getOffset());
        }
        
    private:
        std::uint64_t m_value = 0;

        inline UniqueAddress(std::uint64_t value)
            : m_value(value)
        {            
        }
    };
DB0_PACKED_END    

    UniqueAddress makeUniqueAddr(std::uint64_t offset, std::uint16_t id);

}

namespace std

{

    // std::hash specialization for db0::Address
    template <> struct hash<db0::Address> {
        std::size_t operator()(const db0::Address &address) const noexcept {
            return std::hash<std::uint64_t>()(address.getValue());
        }
    };

    template <> struct hash<db0::UniqueAddress> {
        std::size_t operator()(const db0::UniqueAddress &address) const noexcept {
            return std::hash<std::uint64_t>()(address.getValue());
        }
    };

}