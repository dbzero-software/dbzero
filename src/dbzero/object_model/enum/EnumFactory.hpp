// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <unordered_map>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/object_model/has_fixture.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <optional>
#include "EnumDef.hpp"
#include "EnumValue.hpp"

namespace db0::object_model

{

DB0_PACKED_BEGIN
    
    class Enum;
    using EnumPtr = db0::db0_ptr<Enum>;
    using VEnumMap = db0::v_map<db0::o_string, EnumPtr, o_string::comp_t>;

    struct DB0_PACKED_ATTR o_enum_factory: public o_fixed_versioned<o_enum_factory>
    {
        // 4 variants of enum identification
        db0::db0_ptr<VEnumMap> m_enum_map_ptrs[4];
        
        o_enum_factory(Memspace &memspace);
    };
    
    class EnumFactory: public db0::has_fixture<v_object<o_enum_factory> >
    {
    public:
        using super_t = db0::has_fixture<v_object<o_enum_factory> >;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = typename LangToolkit::TypeObjectSharedPtr;

        EnumFactory(db0::swine_ptr<Fixture> &);

        EnumFactory(db0::swine_ptr<Fixture> &, Address address);

        /**
         * Get existing class (or raise exception if not found)
         * @param lang_type the language specific type object (e.g. Python class)
         * @param typeid the user assigned type ID (optional)
        */
        std::shared_ptr<Enum> getExistingEnum(const EnumTypeDef &) const;        
        std::shared_ptr<Enum> getExistingEnum(const EnumDef &, const char *prefix_name = nullptr) const;
        
        /**
         * A non-throwing version of getExistingType
         * @return nullptr if the class is not found
        */
        std::shared_ptr<Enum> tryGetExistingEnum(const EnumTypeDef &) const;
        std::shared_ptr<Enum> tryGetExistingEnum(const EnumDef &, const char *prefix_name = nullptr) const;
        
        /**
         * Get existing or create a new dbzero enum instance
         * @param enum_def enum definition
         * @param type_id optional user assigned type ID
        */
        std::shared_ptr<Enum> getOrCreateEnum(const EnumFullDef &);
        std::shared_ptr<Enum> getOrCreateEnum(const EnumTypeDef &);
        
        std::shared_ptr<Enum> tryGetOrCreateEnum(const EnumFullDef &);
        std::shared_ptr<Enum> tryGetOrCreateEnum(const EnumTypeDef &);
        
        // reference the dbzero object model's enum by its pointer
        std::shared_ptr<Enum> getEnumByPtr(EnumPtr) const;
        
        // reference the dbzero object model's enum by its 32-bit UID
        std::shared_ptr<Enum> getEnumByUID(std::uint32_t enum_uid) const;
        
        /**
         * Migrate / translate enum value to the one managed by this fixture/factory
         * @param enum_value enum value from a different fixture
         * @return enum value as a language specific object or nullptr if failed to migrate due to read-only prefix
         */
        ObjectSharedPtr tryMigrateEnumLangValue(const EnumValue &enum_value);
        ObjectSharedPtr migrateEnumLangValue(const EnumValue &enum_value);        

        EnumValue migrateEnumValue(const EnumValue &enum_value);
        std::optional<EnumValue> tryMigrateEnumValue(const EnumValue &enum_value);

        // Checks if specific enum value requires migration / translation to a different prefix
        bool isMigrateRequired(const EnumValue &) const;
        
        // Try converting EnumValueRepr to this EnumFactory's associated EnumValue
        std::optional<EnumValue> tryGetEnumValue(const EnumValueRepr &);
        EnumValue getEnumValue(const EnumValueRepr &);
        // try converting to enum's related language specific object
        ObjectSharedPtr tryGetEnumLangValue(const EnumValueRepr &);

        void commit() const;
        
        void detach() const;

    private:
        // enum maps in 4 variants: 0: type ID, 1: name + module, 2: name + values: 3: module + values
        std::array<VEnumMap, 4> m_enum_maps;
        // Language specific type to dbzero class mapping
        mutable std::unordered_map<EnumPtr, std::shared_ptr<Enum> > m_ptr_cache;

        // Pull through by-pointer cache
        std::shared_ptr<Enum> getEnum(EnumPtr, std::shared_ptr<Enum>);
        
        // Locate enum by definition
        EnumPtr tryFindEnumPtr(const EnumDef &) const;
        // get translated enum corresponding to enum_value
        std::shared_ptr<Enum> tryGetMigratedEnum(const EnumValue &enum_value);
        std::shared_ptr<Enum> getMigratedEnum(const EnumValue &enum_value);
    };
    
    std::optional<std::string> getEnumKeyVariant(const EnumDef &, const char *type_id, int variant_id);

DB0_PACKED_END

}