// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cassert>
#include <utility>
#include <iostream>

namespace db0::object_model

{

    class FieldID
    {
    public:
        static constexpr std::uint32_t MAX_INDEX = 0x40000 - 1; // max 2^22 fields
        static constexpr std::uint32_t MAX_OFFSET = 0x40 - 1;
        
        FieldID() = default;
        FieldID(std::pair<std::uint32_t, std::uint32_t> loc) {
            assert(loc.second <= MAX_OFFSET);
            assert(loc.first <= MAX_INDEX);
            m_value = (loc.first << 6) + loc.second + 1;
        }
        
        inline operator bool() const {
            return m_value != 0;
        }

        // get a zero-based field index
        inline std::uint32_t getIndex() const {
            assert(m_value);
            return (m_value - 1) >> 6;
        }

        inline std::uint32_t getOffset() const {
            assert(m_value);
            return (m_value - 1) & 0x3F;
        }
        
        // get offset if assigned, otherwise return 0
        std::uint32_t maybeOffset() const;

        // unpack to index and offset
        inline std::pair<std::uint32_t, std::uint32_t> getIndexAndOffset() const {
            assert(m_value);
            return { (m_value - 1) >> 6, (m_value - 1) & 0x3F };
        }
        
        // create FieldID from a zero-based index and offset
        // @param field index in the layout
        // @param optional offset (for low-fidelity types e.g. bool)
        static FieldID fromIndex(std::uint32_t index, std::uint32_t offset = 0) {
            assert(offset <= MAX_OFFSET);
            assert(index <= MAX_INDEX);
            return FieldID((index << 6) + offset + 1);
        }
        
        bool operator==(const FieldID &other) const {
            return m_value == other.m_value;
        }

        bool operator!=(const FieldID &other) const {
            return m_value != other.m_value;
        }
        
        // @return full index (index + offset) as a single integer
        inline std::uint32_t getLongIndex() const {
            return m_value;
        }
        
    private:
        std::uint32_t m_value = 0;
        
        FieldID(std::uint32_t value) : m_value(value) {}
    };
    
    // FieldID + fidelity
    using FieldInfo = std::pair<FieldID, unsigned int>;
    
}

namespace std

{

    std::ostream &operator<<(std::ostream &os, const db0::object_model::FieldID &field_id);

}