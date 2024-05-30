#pragma once

#include <string>
#include <dbzero/core/collections/pools/StringPools.hpp>

namespace db0::object_model

{
    
    using LP_String = db0::LP_String;
    
    struct EnumValue_UID
    {
        // the associated enum's UID (i.e. its address from the dedicated address pool)
        std::uint32_t m_enum_uid;
        LP_String m_value;

        EnumValue_UID(std::uint32_t enum_uid, LP_String value);
        EnumValue_UID(std::uint64_t);

        std::uint64_t asULong() const;
    };
    
    struct EnumValue
    {
        // associated fixture UUID (for context validation purposes)
        std::uint64_t m_fixture_uuid;
        // the associated enum's UID (i.e. its address from the dedicated address pool)
        std::uint32_t m_enum_uid;
        LP_String m_value;
        // the string representation
        std::string m_str_repr;

        // get unique tag identifier (unique within its prefix)
        EnumValue_UID getUID() const;
    };
    
}