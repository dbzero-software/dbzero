#pragma once

#include <cstdint>
#include <dbzero/core/metaprog/binary_cast.hpp>
#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model

{

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR Value
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

        bool operator==(const Value &other) const;
        
        std::uint64_t m_store = 0;
    };
DB0_PACKED_END
       
}