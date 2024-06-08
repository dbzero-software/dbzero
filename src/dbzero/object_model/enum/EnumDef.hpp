#pragma once

#include <vector>
#include <string>

namespace db0::object_model

{
        
    struct EnumDef
    {
        // user assigned enum name
        const std::string m_name;
        // a module where the enum is defined
        const std::string m_module_name;
        // user assigned enum values
        const std::vector<std::string> m_values;
    };

}
