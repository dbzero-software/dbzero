// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <optional>
#include <dbzero/bindings/python/PyTypes.hpp>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/enum/EnumDef.hpp>
#include <unordered_map>

namespace db0 {

    class Snapshot;

}

namespace db0::object_model {

    class Enum;
    class EnumFactory;

}

namespace db0::python

{

    using Enum = db0::object_model::Enum;
    using EnumDef = db0::object_model::EnumDef;
    using EnumFullDef = db0::object_model::EnumFullDef;
    using EnumTypeDef = db0::object_model::EnumTypeDef;
    
    // must store EnumDef for deferred creation
    class PyEnumData
    {
    public:
        using Snapshot = db0::Snapshot;
        using EnumFactory = db0::object_model::EnumFactory;
        
        // shared_ptr to be able to associated this element with EnumValueRepr elemenst
        std::shared_ptr<EnumTypeDef> m_enum_type_def;

        PyEnumData(const EnumFullDef &, const char *prefix_name);
        
        // tryCreate may fail if enum is first accessed and prefix is not opened for read/write
        Enum *tryCreate();
        // when first accessed, tries pulling existing or creating a new enum in the current fixture
        Enum &create();
        
        // get an existing enum
        const Enum &get() const;        

        void close();

        // check if the underlying enum instance exists
        bool exists() const;

        bool hasValue(const char *value) const;
        
        // get values from their definitions
        const std::vector<std::string> &getValueDefs() const;
        
        std::size_t size() const;
            
    private:
        // enum specific fixture UUID (for scoped enums) or 0 to use the current fixture
        mutable std::optional<std::uint64_t> m_fixture_uuid;
        // prefix-specific enum instances
        mutable std::unordered_map<std::uint64_t, std::shared_ptr<Enum> > m_enum_cache;
        
        // resolve a concrete fixture UUID (!=0)
        std::uint64_t getFixtureUUID() const;
        // return 0 if resolving a concrete fixture UUID was not possible
        std::uint64_t tryGetFixtureUUID() const;

        static std::optional<std::uint64_t> tryGetFixtureUUID(const char *prefix_name);
    };
    
    using PyEnum = PyWrapper<PyEnumData, false>;
    
    std::optional<std::string> getEnumKeyVariant(const EnumDef &, int variant_id);
    std::optional<std::string> getEnumKeyVariant(const PyEnumData &, int variant_id);

}