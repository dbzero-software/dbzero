#pragma once

#include <cstdint>
#include <dbzero/core/metaprog/binary_cast.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace db0::object_model

{

    struct [[gnu::packed]] Value
    {
        Value() = default;

        inline Value(Address address)
            : m_store(address.getValue())
        {
        }

        inline Value(UniqueAddress address)
            : m_store(address.getValue())
        {
        }

        inline Value(std::uint64_t value)
            : m_store(value)
        {
        }

        template <typename T> inline T cast() const {
            return db0::binary_cast<T, std::uint64_t>()(m_store);
        }

        inline Address asAddress() const {
            return Address::fromValue(m_store);
        }

        inline UniqueAddress asUniqueAddress() const {
            return UniqueAddress::fromValue(m_store);
        }
        
        // Assign (merge) a lo-fi type value using a mask
        inline void assign(const Value &other, std::uint64_t mask) {
            m_store = (m_store & ~mask) | (other.m_store & mask);
        }
        
        bool operator==(const Value &other) const;
        
        std::uint64_t m_store = 0;
    };
       
}