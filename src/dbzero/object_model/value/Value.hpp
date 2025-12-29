// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
        // common constants
        static constexpr std::uint64_t NONE = 0x00;
        static constexpr std::uint64_t FALSE = 0x01;
        static constexpr std::uint64_t TRUE = 0x02;
        static constexpr std::uint64_t DELETED = 0x03;

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
        
        inline bool operator==(const Value &other) const {
            return m_store == other.m_store;
        }

        inline bool operator!=(const Value &other) const {
            return m_store != other.m_store;
        }

        inline bool operator==(std::uint64_t other) const {
            return m_store == other;
        }

        inline bool operator!=(std::uint64_t other) const {
            return m_store != other;
        }

        std::uint64_t m_store = 0;
    };
DB0_PACKED_END
       
}