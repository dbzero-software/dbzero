#pragma once

#include <cstdint>
#include <cassert>
#include <functional>

namespace db0

{

    // The "logical" address object consisting of 50-bit memory offset + 14 bit instance ID (optional)
    class [[gnu::packed]] Address
    {
    public:
        // Construct as null / invalid
        Address() = default;
        
        inline Address(std::uint64_t offset, std::uint16_t instance_id)
            : m_value((offset << 14) | static_cast<std::uint64_t>(instance_id))
        {
            assert(instance_id > 0);
            assert(offset < (1ULL << 50));
            assert(instance_id < (1ULL << 14));
        }
        
        // make address from offset, ignoring instance ID
        static inline Address fromOffset(std::uint64_t offset) {
            return Address(offset);
        }

        static inline Address fromValue(std::uint64_t value) {
            return Address(tag_from_value(), value);
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

        // Get offset + instance ID encoded in a single 64-bit value
        inline std::uint64_t getValue() const {
            return m_value;
        }
        
        // NOTE: the instance ID part is cleared when adding an offset
        Address operator+(std::uint64_t offset) const;
        
    private:
        std::uint64_t m_value = 0;

        inline Address(std::uint64_t offset)
            : m_value(offset << 14)
        {
            assert(offset < (1ULL << 50));            
        }

        struct tag_from_value {};
        inline Address(tag_from_value, std::uint64_t value)
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
    
}