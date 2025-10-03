#pragma once

#include <cstdint>
#include <cassert>

namespace db0::object_model

{

    class FieldID
    {
    public:
        static constexpr std::uint32_t MAX_INDEX = 0x40000 - 1; // max 2^22 fields
        static constexpr std::uint32_t MAX_OFFSET = 0x40 - 1;
        
        FieldID() = default;
        
        inline operator bool() const {
            return m_value != 0;
        }

        // get a zero-based field index
        inline std::uint32_t getIndex() const {
            assert(m_value);
            return (m_value - 1) >> 6;
        }

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
        
        // get alternative index for low-fidelity types (e.g. bool)
        inline std::uint32_t getAltIndex() const {
            assert(m_value);
            return m_value + 1;
        }

    private:
        std::uint32_t m_value = 0;
        
        FieldID(std::uint32_t value) : m_value(value) {}
    };
    
}