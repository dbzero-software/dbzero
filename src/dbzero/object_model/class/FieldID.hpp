#pragma once

#include <cstdint>
#include <cassert>

namespace db0::object_model

{

    class FieldID
    {
    public:
        FieldID() = default;

        inline operator bool() const {
            return m_value != 0;
        }

        // get a zero-based field index
        inline std::uint32_t getIndex() const {
            assert(m_value);
            return m_value - 1;
        }
        
        static FieldID fromIndex(std::uint32_t index) {
            return FieldID(index + 1);
        }

    private:
        std::uint32_t m_value = 0;
        
        FieldID(std::uint32_t value) : m_value(value) {}
    };
    
}