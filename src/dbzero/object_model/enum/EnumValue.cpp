#include "EnumValue.hpp"

namespace db0::object_model

{

    EnumValue_UID::EnumValue_UID(std::uint32_t enum_uid, LP_String value)
        : m_enum_uid(enum_uid)  
        , m_value(value)
    {
    }

    EnumValue_UID::EnumValue_UID(std::uint64_t uid)
        : m_enum_uid(static_cast<std::uint32_t>(uid >> 32) & 0x7FFFFFFF)
        , m_value(static_cast<std::uint32_t>(uid & 0xFFFFFFFF))
    {
        assert((uid >> 32) & 0x80000000);
    }
    
    std::uint64_t EnumValue_UID::asULong() const {
        // set the highest bit to 1 to distinguish from a regular address
        return (static_cast<std::uint64_t>(m_enum_uid | 0x80000000) << 32) | m_value.m_value;
    }
    
    EnumValue_UID EnumValue::getUID() const {
        return { m_enum_uid, m_value };
    }

}