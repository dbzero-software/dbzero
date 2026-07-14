// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Value.hpp"
#include "StorageClass.hpp"
#include <dbzero/core/compiler_attributes.hpp>
#include <array>
#include <cassert>

namespace db0::object_model

{

    /**
     * Typed value + 24bit index
    */
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR XValue
    {
        std::array<std::uint8_t, 3> m_index = {};
        StorageClass m_type = StorageClass::UNDEFINED;
        Value m_value;

        XValue() = default;
        
        inline XValue(std::uint32_t index)
        {
            assert(index < 0x1000000);
            std::memcpy(m_index.data(), &index, 3);
        }
        
        inline XValue(std::uint32_t index, StorageClass type, Value value)
            : m_type(type)
            , m_value(value)
        {
            assert(index < 0x1000000);
            std::memcpy(m_index.data(), &index, 3);
        }
        
        inline std::uint32_t getIndex() const
        {
            std::uint32_t index = 0;
            std::memcpy(&index, m_index.data(), 3);
            return index;
        }

        bool operator<(const XValue &other) const;

        bool operator<(std::uint32_t index) const;

        bool operator==(std::uint32_t index) const;
        
        // NOTE: index-only comparison
        bool operator==(const XValue &) const;
        bool operator!=(const XValue &) const;
        
        // bitwise comparison
        // @param offset - required for lo-fi types (2 bits)
        bool equalTo(const XValue &other, unsigned int offset) const;
        
        // bitwise compare the entire contents
        bool equalTo(const XValue &other) const;
    };
DB0_PACKED_END
    
}
