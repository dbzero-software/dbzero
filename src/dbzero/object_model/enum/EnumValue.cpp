#include "EnumValue.hpp"

namespace db0::object_model

{

    std::uint64_t EnumValue::getUID() const 
    {
        // set the highest bit to 1 to distinguish from regular addresses
        return (static_cast<std::uint64_t>(m_enum_uid | 0x80000000) << 32) | m_value.m_value;
    }

}