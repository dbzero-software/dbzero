#pragma once

#include <cstdint>
#include <cassert>
#include <functional>

namespace db0

{

    template <typename store_t> class [[gnu::packed]] AddressType
    {
    public:
        // Construct as null / invalid
        AddressType() = default;
        
        // make address from offset
        static inline AddressType fromOffset(std::uint64_t offset) {
            return AddressType(offset);
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

        inline bool operator==(const Address& other) const {
            return m_value == other.m_value;
        }

        inline bool operator!=(const Address& other) const {
            return m_value != other.m_value;
        }

        inline bool operator<(const Address& other) const {
            return m_value < other.m_value;
        }

        inline bool operator>(const Address& other) const {
            return m_value > other.m_value;
        }
        
        // Offset cast
        inline operator store_t() const {
            return m_store;
        }

        Address operator+(std::uint64_t offset) const;
        Address operator-(std::uint64_t offset) const;
        
    private:
        store_t m_value = 0;
        
        inline AddressType(std::uint64_t offset)
            : m_value(offset)
        {            
        }
    };
    
    using Address = AddressType<std::uint64_t>;

    // The UniqueAddress combines memory offset and instance ID
    // by definition the UniqueAddress will not be assigned more than once throughut the lifetime of the prefix
    class [[gnu::packed]] UniqueAddress
    {
    public:
        // Construct as null / invalid
        UniqueAddress() = default;
        
        inline UniqueAddress(Address address, std::uint16_t instance_id)
            : m_value((address.getOffset() << 14) | static_cast<std::uint64_t>(instance_id))
        {
            assert(instance_id > 0);
            assert(address.getOffset() < (1ULL << 50));
            assert(instance_id < (1ULL << 14));
        }
        
        static inline UniqueAddress fromValue(std::uint64_t value) {
            return UniqueAddress(value);
        }

        inline bool isValid() const {
            return m_value != 0; 
        }
        
        inline std::uint64_t getOffset() const {
            return m_value >> 14; 
        }

        inline std::uint16_t getInstanceId() const {
            assert(m_value & 0x3FFF);
            return static_cast<std::uint16_t>(m_value & 0x3FFF); 
        }

        bool hasInstanceId() const {
            return (m_value & 0x3FFF) != 0;
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

        inline bool operator>(const UniqueAddress& other) const {
            return m_value > other.m_value;
        }

        // Get offset + instance ID encoded in a single 64-bit value
        inline std::uint64_t getValue() const {
            return m_value;
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