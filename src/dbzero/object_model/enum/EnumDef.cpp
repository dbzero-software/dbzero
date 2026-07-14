// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "EnumDef.hpp"
#include <dbzero/core/serialization/Serializable.hpp>
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

    o_enum_def::o_enum_def(const char *name, const char *module_name, std::uint32_t hash,  const char *type_id)
        : m_hash(hash)
    {
        this->arrangeMembers()
            (o_string::type(), name)
            (o_string::type(), module_name)
            (o_nullable_string::type(), type_id);
    }

    o_enum_def::o_enum_def(const EnumDef &enum_def)
        : o_enum_def(enum_def.m_name.c_str(), enum_def.m_module_name.c_str(), enum_def.m_hash, enum_def.tryGetTypeId())
    {
    }

    const o_string &o_enum_def::name() const {
        return this->getDynFirst(o_string::type());
    }

    const o_string &o_enum_def::module_name() const {
        return this->getDynAfter(this->name(), o_string::type());
    }

    const o_nullable_string &o_enum_def::type_id() const {
        return this->getDynAfter(this->module_name(), o_nullable_string::type());
    }
    
    std::size_t o_enum_def::measure(const char *name, const char *module_name, std::uint32_t hash, 
        const char *type_id)
    {
        return measureMembers()
            (o_string::type(), name)
            (o_string::type(), module_name)
            (o_nullable_string::type(), type_id);
    }

    std::size_t o_enum_def::measure(const EnumDef &enum_def) 
    {
        return measure(enum_def.m_name.c_str(), enum_def.m_module_name.c_str(), enum_def.m_hash, 
            enum_def.tryGetTypeId());
    }
    
    EnumDef o_enum_def::get() const 
    {
        return EnumDef(
            this->name(),
            this->module_name(),
            m_hash,
            this->type_id().isNull() ? std::nullopt : std::optional<std::string>(this->type_id())
        );
    }
    
    EnumDef::EnumDef(const std::string &name, const std::string &module_name, std::uint32_t hash, 
        const char *type_id)
        : m_name(name)
        , m_module_name(module_name)
        , m_hash(hash)
        , m_type_id(type_id ? std::optional<std::string>(type_id) : std::nullopt)
    {
    }
    
    EnumDef::EnumDef(const std::string &name, const std::string &module_name, std::uint32_t hash, 
        std::optional<std::string> type_id)
        : m_name(name)
        , m_module_name(module_name)
        , m_hash(hash)
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
        return m_hash == other.m_hash;
    }
    
    bool EnumDef::operator!=(const EnumDef &other) const {
        return !(*this == other);
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

    void EnumDef::serialize(std::vector<std::byte> &buffer) const
    {
        db0::serial::emplaceBack<o_enum_def>(buffer, m_name.c_str(), m_module_name.c_str(), m_hash, 
            m_type_id.has_value() ? m_type_id.value().c_str() : nullptr
        );
    }

    EnumFullDef::EnumFullDef(const std::string &name, const std::string &module_name, const std::vector<std::string> &values,
        const char *type_id)
        : EnumDef(name, module_name, getHashOf(values), type_id)
        , m_values(values)        
    {
    }

    EnumFullDef::EnumFullDef(const std::string &name, const std::string &module_name, const std::vector<std::string> &values,
        std::optional<std::string> type_id)
        : EnumDef(name, module_name, getHashOf(values), type_id)
        , m_values(values)        
    {
    }

    bool EnumFullDef::operator==(const EnumFullDef &other) const
    {
        if (EnumDef::operator!=(other)) {
            return false;
        }

        // compare values finally
        // FIXME: compare as order independent
        return m_values == other.m_values;
    }
    
    bool EnumFullDef::operator!=(const EnumFullDef &other) const {
        return !(*this == other);
    }

    EnumTypeDef::EnumTypeDef(const EnumFullDef &enum_def, const char *prefix_name)
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
    
    const char *EnumTypeDef::getPrefixNamePtr() const {
        return m_prefix_name.has_value() ? m_prefix_name.value().c_str() : nullptr;
    }
    
}

namespace std

{
 
    ostream &operator<<(ostream &os, const db0::object_model::o_enum_def &enum_def)
    {
        return os << "o_enum_def {" << enum_def.name() << ", module_name: " << enum_def.module_name() 
            << ", hash: " << enum_def.m_hash 
            << ", type_id: " << (enum_def.type_id().isNull() ? "null" : enum_def.type_id().extract()) << "}";
    }
    
    ostream &operator<<(ostream &os, const db0::object_model::EnumDef &enum_def) {
        return os << "EnumDef {" << enum_def.m_name << ", module_name: " << enum_def.m_module_name << "}";
    }

    ostream &operator<<(ostream &os, const db0::object_model::EnumFullDef &enum_def)
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