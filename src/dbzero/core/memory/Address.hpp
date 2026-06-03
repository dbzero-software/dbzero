// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cassert>
#include <functional>
#include <ostream>
#include <type_traits>
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

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR TagAddress
    {
    public:
        // TagAddress is used by the short-tag index, whose key space contains
        // both real memory addresses and packed non-address tags. Address-backed
        // tags use only low 50 bits, matching UniqueAddress's address payload.
        // EnumValue_UID tags reserve bit 63 to distinguish enum tags from
        // address-backed tags, so passive tags must not use or mask that bit.
        // PASSIVE_BIT is only recognized when no non-address high bits are set.
        static constexpr std::uint64_t ENUM_BIT = 1ULL << 63;
        static constexpr std::uint64_t PASSIVE_BIT = 1ULL << 62;
        static constexpr std::uint64_t ADDRESS_MASK = (1ULL << 50) - 1;

        TagAddress() = default;

        static inline TagAddress fromValue(std::uint64_t value) {
            return TagAddress(value);
        }

        static inline TagAddress fromOffset(std::uint64_t offset) {
            return TagAddress(offset);
        }

        static inline TagAddress fromAddress(Address address) {
            return fromOffset(address.getOffset());
        }

        inline bool operator!() const {
            return regularValue(m_value) == 0;
        }

        inline bool isValid() const {
            return regularValue(m_value) != 0;
        }

        inline bool isPassive() const {
            return isPassiveValue(m_value);
        }

        inline TagAddress asPassive() const {
            auto regular_value = regularValue(m_value);
            // Non-address tag encodings, such as enum and field-def tags, are
            // not eligible for passive storage. Returning them unchanged keeps
            // their packed identity intact.
            if ((regular_value & ~ADDRESS_MASK) != 0) {
                return *this;
            }
            return TagAddress(regular_value | PASSIVE_BIT);
        }

        inline TagAddress asRegular() const {
            return TagAddress(regularValue(m_value));
        }

        inline std::uint64_t getOffset() const {
            return regularValue(m_value);
        }

        inline std::uint64_t getValue() const {
            return m_value;
        }

        inline Address getAddress() const {
            return Address::fromOffset(getOffset());
        }

        inline operator Address() const {
            return getAddress();
        }

        inline operator std::uint64_t() const {
            return regularValue(m_value);
        }

        inline bool operator==(const TagAddress &other) const {
            return regularValue(m_value) == regularValue(other.m_value);
        }

        inline bool operator!=(const TagAddress &other) const {
            return regularValue(m_value) != regularValue(other.m_value);
        }

        inline bool operator<(const TagAddress &other) const {
            return regularValue(m_value) < regularValue(other.m_value);
        }

        inline bool operator>(const TagAddress &other) const {
            return regularValue(m_value) > regularValue(other.m_value);
        }

        inline bool operator<=(const TagAddress &other) const {
            return regularValue(m_value) <= regularValue(other.m_value);
        }

        inline bool operator>=(const TagAddress &other) const {
            return regularValue(m_value) >= regularValue(other.m_value);
        }

        static inline bool isPassiveValue(std::uint64_t value) {
            return (value & PASSIVE_BIT) != 0 && (value & ~(PASSIVE_BIT | ADDRESS_MASK)) == 0;
        }

        static inline std::uint64_t regularValue(std::uint64_t value) {
            return isPassiveValue(value) ? (value & ADDRESS_MASK) : value;
        }

        inline friend std::ostream &operator<<(std::ostream &os, const TagAddress &address) {
            os << address.m_value;
            return os;
        }

    private:
        std::uint64_t m_value = 0;

        explicit inline TagAddress(std::uint64_t value)
            : m_value(value)
        {
        }
    };
DB0_PACKED_END

    static_assert(sizeof(TagAddress) == sizeof(std::uint64_t));
    static_assert(std::is_trivially_copyable_v<TagAddress>);

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

    template <> struct hash<db0::TagAddress> {
        std::size_t operator()(const db0::TagAddress &address) const noexcept {
            return std::hash<std::uint64_t>()(address.getOffset());
        }
    };

}
