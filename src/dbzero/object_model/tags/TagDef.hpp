#pragma once

#include <cstdint>
#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace db0::object_model

{

    // Short tag definition, is just a wrapper over 64-bit address
    struct TagDef
    {
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using Address = db0::Address;

        std::uint64_t m_fixture_uuid;
        Address m_address;
        // the tag associated @memo object
        ObjectSharedPtr m_object;
        
        TagDef(std::uint64_t fixture_uuid, Address, ObjectPtr);    

        bool operator==(const TagDef &other) const;
        bool operator!=(const TagDef &other) const;
    };
    
}