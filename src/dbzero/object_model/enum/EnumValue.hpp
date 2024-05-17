#pragma once

#include <string>
#include <dbzero/core/collections/pools/StringPools.hpp>

namespace db0::object_model

{

    using LP_String = db0::LP_String;
    
    struct EnumValue
    {
        // associated fixture UUID (for context validation purposes)
        std::uint64_t m_fixture_uuid;
        LP_String m_value;
        // the string representation
        std::string m_str_repr;

        // get unique tag identifier (unique within its prefix)
        std::uint64_t getUID() const;
    };

}