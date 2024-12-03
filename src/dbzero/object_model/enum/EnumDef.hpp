#pragma once

#include <vector>
#include <string>
#include <optional>
#include <iostream>

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
    
    // Full enum type definition
    struct EnumTypeDef
    {
        EnumDef m_enum_def;
        std::optional<std::string> m_type_id;
        std::optional<std::string> m_prefix_name;

        EnumTypeDef(const EnumDef &, const char *type_id, const char *prefix_name);

        bool hasPrefix() const;
        const std::string &getPrefixName() const;

        bool hasTypeId() const;
        const char *getTypeId() const;

        // @return nullptr if type-id has not been assigned
        const char *tryGetTypeId() const;
    };
    
}

namespace std

{

    ostream &operator<<(ostream &, const db0::object_model::EnumDef &);
    ostream &operator<<(ostream &, const db0::object_model::EnumTypeDef &);
    
}
