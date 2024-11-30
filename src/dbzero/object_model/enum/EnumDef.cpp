#include "EnumDef.hpp"
#include <cassert>

namespace db0::object_model

{

    EnumTypeDef::EnumTypeDef(const EnumDef &enum_def, const char *type_id, const char *prefix_name)
        : m_enum_def(enum_def)
        , m_type_id(type_id ? std::optional<std::string>(type_id) : std::nullopt)
        , m_prefix_name(prefix_name ? std::optional<std::string>(prefix_name) : std::nullopt)
    {
    }

    bool EnumTypeDef::hasPrefix() const {
        return m_prefix_name.has_value();
    }

    const std::string &EnumTypeDef::getPrefixName() const 
    {
        assert(hasPrefix());
        return m_prefix_name.value();
    }

    bool EnumTypeDef::hasTypeId() const {
        return m_type_id.has_value();
    }

    const char *EnumTypeDef::getTypeId() const 
    {
        assert(hasTypeId());
        return m_type_id.value().c_str();
    }
    
    const char *EnumTypeDef::tryGetTypeId() const {
        return m_type_id.has_value() ? m_type_id.value().c_str() : nullptr;
    }
    
}

namespace std

{
    
    ostream &operator<<(ostream &os, const db0::object_model::EnumDef &enum_def)
    {
        os << "EnumDef {" << enum_def.m_name << ", module_name: " << enum_def.m_module_name << ", values: [";
        bool is_first = true;
        for (const auto &value : enum_def.m_values) {
            if (!is_first) {
                os << ", ";
            }
            os << value;
            is_first = false;
        }
        os << "]}";
        return os;
    }
    
    ostream &operator<<(ostream &os, const db0::object_model::EnumTypeDef &enum_type_def)
    {   
        os << "EnumTypeDef {" << enum_type_def.m_enum_def.m_name << ", module_name: " 
            << enum_type_def.m_enum_def.m_module_name << ", values: [";
        bool is_first = true;
        for (const auto &value : enum_type_def.m_enum_def.m_values) {
            if (!is_first) {
                os << ", ";
            }
            os << value;
            is_first = false;
        }
        os << "], type_id: "<< (enum_type_def.hasTypeId() ? enum_type_def.getTypeId() : "null") 
            << ", prefix_name: " << (enum_type_def.hasPrefix() ? enum_type_def.getPrefixName() : "null") << "}";
        return os;
    }

}