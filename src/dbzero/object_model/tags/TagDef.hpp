// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace db0::object_model

{
    
    class ClassFactory;

    // Short tag definition, is just a wrapper over 64-bit address
    class TagDef
    {
    public:
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using Address = db0::Address;

        struct type_as_tag {};
        
        TagDef(TypeObjectPtr type, type_as_tag);
        TagDef(std::uint64_t fixture_uuid, Address, ObjectPtr);

        bool operator==(const TagDef &other) const;
        bool operator!=(const TagDef &other) const;
        
        Address getAddress(ClassFactory &) const;
        // @retrun fixture UUID + Address
        std::pair<std::uint64_t, Address> getLongAddress(ClassFactory &) const;

        // NOTE: returns 0 if fixture UUID is not defined (e.g. for type as tag)
        std::uint64_t tryGetFixtureUUID() const;

        std::uint64_t getHash() const;
        
    private:
        mutable std::uint64_t m_fixture_uuid = 0;
        mutable Address m_address;
        bool m_is_type_tag = false;
        // the tag associated @memo object or a memo type object
        ObjectSharedPtr m_object;
    };
    
}