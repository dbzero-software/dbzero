#include "EnumDef.hpp"
#include <cassert>

namespace db0::object_model

{
    
    std::uint32_t getHashOf(const std::vector<std::string> &values)
    {
        std::uint32_t hash = 0;
        for (const auto &value : values) {
            hash ^= std::hash<std::string>{}(value);
        }
        return hash;
    }
    
    EnumDef::EnumDef(const std::string &name, const std::string &module_name, const std::vector<std::string> &values,
        const char *type_id)
        : m_name(name)
        , m_module_name(module_name)
        , m_values(values)
        , m_hash(getHashOf(values))
        , m_type_id(type_id ? std::optional<std::string>(type_id) : std::nullopt)
    {
    }

    EnumDef::EnumDef(const std::string &name, const std::string &module_name, const std::vector<std::string> &values,
        std::optional<std::string> type_id)
        : m_name(name)
        , m_module_name(module_name)
        , m_values(values)
        , m_hash(getHashOf(values))
        , m_type_id(type_id)
    {
    }

    bool EnumDef::operator==(const EnumDef &other) const
    {
        // any 2 of the 3 components should match
        unsigned int matched = 0;
        if (m_name == other.m_name) {
            ++matched;
        }
        if (m_module_name == other.m_module_name) {
            ++matched;
        }
        if (matched == 2) {
            return true;
        }
        if (matched < 1) {
            return false;
        }
        // compare values finally
        if (m_hash != other.m_hash) {
            return false;
        }
        // FIXME: compare as order independent
        return m_values == other.m_values;
    }
    
    bool EnumDef::operator!=(const EnumDef &other) const {
        return !(*this == other);
    }

    EnumTypeDef::EnumTypeDef(const EnumDef &enum_def, const char *prefix_name)
        : m_enum_def(enum_def)        
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

    bool EnumDef::hasTypeId() const {
        return m_type_id.has_value();
    }

    const char *EnumDef::getTypeId() const 
    {
        assert(hasTypeId());
        return m_type_id.value().c_str();
    }
    
    const char *EnumDef::tryGetTypeId() const {
        return m_type_id.has_value() ? m_type_id.value().c_str() : nullptr;
    }
    
    const char *EnumTypeDef::getPrefixNamePtr() const {
        return m_prefix_name.has_value() ? m_prefix_name.value().c_str() : nullptr;
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
        os << "], type_id: "<< (enum_type_def.m_enum_def.hasTypeId() ? enum_type_def.m_enum_def.getTypeId() : "null") 
            << ", prefix_name: " << (enum_type_def.hasPrefix() ? enum_type_def.getPrefixName() : "null") << "}";
        return os;
    }
    
}