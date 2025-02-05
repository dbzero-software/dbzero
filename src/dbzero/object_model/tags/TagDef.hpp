#pragma once

#include <cstdint>
#include <dbzero/object_model/LangConfig.hpp>

namespace db0::object_model

{

    // Short tag definition, is just a wrapper over 64-bit address
    struct TagDef
    {
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;

        std::uint64_t m_fixture_uuid;
        std::uint64_t m_value;
        // the tag associated @memo object
        ObjectSharedPtr m_object;
        
        TagDef(std::uint64_t fixture_uuid, std::uint64_t value, ObjectPtr);
                
        static TagDef &makeNew(void *at_ptr, std::uint64_t fixture_uuid, std::uint64_t value, ObjectPtr);

        bool operator==(const TagDef &other) const;
        bool operator!=(const TagDef &other) const;
    };
    
}