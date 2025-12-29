// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include <string>
#include <optional>
#include <iostream>
#include <cstdint>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model

{

DB0_PACKED_BEGIN
    
    // Enum Type definition without enum values
    class EnumDef
    {
    public:
        // user assigned enum name
        const std::string m_name;
        // a module where the enum is defined
        const std::string m_module_name;
        // combined hash computed from enum values
        const std::uint32_t m_hash;
        std::optional<std::string> m_type_id;
        
        EnumDef(const std::string &name, const std::string &module_name, std::uint32_t hash, 
            const char *type_id = nullptr);
        EnumDef(const std::string &name, const std::string &module_name, std::uint32_t hash, 
            std::optional<std::string> type_id);
        
        bool hasTypeId() const;
        const char *getTypeId() const;
        
        // @return nullptr if type-id has not been assigned
        const char *tryGetTypeId() const;
        
        // Compare enum type definitions
        bool operator==(const EnumDef &) const;
        bool operator!=(const EnumDef &) const;

        void serialize(std::vector<std::byte> &buffer) const;
    };
    
    class DB0_PACKED_ATTR o_enum_def: public db0::o_base<o_enum_def, 0, true>
    {
    protected:
        using self = o_enum_def;
        using super_t = db0::o_base<o_enum_def, 0, true>;
        using o_string = db0::o_string;
        friend super_t;

        // new empty enum definition
        o_enum_def(const char *name, const char *module_name, std::uint32_t hash, 
            const char *type_id = nullptr);
        o_enum_def(const EnumDef &);
            
    public:
        const std::uint32_t m_hash;

        const o_string &name() const;

        const o_string &module_name() const;

        const o_nullable_string &type_id() const;
        
        static std::size_t measure(const char *name, const char *module_name, std::uint32_t hash, 
            const char *type_id = nullptr);
        static std::size_t measure(const EnumDef &);
        
        template <typename T> static std::size_t safeSizeOf(T buf)
        {
            return sizeOfMembers(buf)
                (o_string::type())
                (o_string::type())
                (o_nullable_string::type());
        }
        
        EnumDef get() const;
    };
    
    // Full enum type definition (includes enum values)
    class EnumFullDef: public EnumDef
    {
    public:
        // user assigned enum values
        const std::vector<std::string> m_values;
        
        EnumFullDef(const std::string &name, const std::string &module_name, const std::vector<std::string> &values,
            const char *type_id = nullptr);
        EnumFullDef(const std::string &name, const std::string &module_name, const std::vector<std::string> &values,
            std::optional<std::string> type_id);
                
        // Compare enum type definitions
        bool operator==(const EnumFullDef &) const;
        bool operator!=(const EnumFullDef &) const;
    };
    
    // Full enum type definition
    struct EnumTypeDef
    {
        EnumFullDef m_enum_def;
        std::optional<std::string> m_prefix_name;
        
        EnumTypeDef(const EnumFullDef &, const char *prefix_name);

        bool hasPrefix() const;
        const std::string &getPrefixName() const;
        const char *getPrefixNamePtr() const;   
    };
    
DB0_PACKED_END

}

namespace std

{

    ostream &operator<<(ostream &, const db0::object_model::o_enum_def &);
    ostream &operator<<(ostream &, const db0::object_model::EnumDef &);
    ostream &operator<<(ostream &, const db0::object_model::EnumFullDef &);
    ostream &operator<<(ostream &, const db0::object_model::EnumTypeDef &);
    
}
