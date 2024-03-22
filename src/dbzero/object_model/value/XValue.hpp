#pragma once

#include "Value.hpp"
#include "StorageClass.hpp"
#include <array>
#include <cassert>

namespace db0::object_model

{

    /**
     * Typed value + 24bit index
    */
    struct [[gnu::packed]] XValue
    {
        std::array<std::uint8_t, 3> m_index;
        StorageClass m_type;
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

        bool operator==(const XValue &) const;
    };
    
}