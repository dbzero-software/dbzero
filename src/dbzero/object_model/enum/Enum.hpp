// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "EnumValue.hpp"
#include "EnumDef.hpp"
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/object_model/object_header.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model

{

DB0_PACKED_BEGIN

    using namespace db0::pools;
    using LP_String = db0::LP_String;

    struct DB0_PACKED_ATTR o_enum: public o_fixed_versioned<o_enum>
    {
        db0::o_object_header m_header;
        // enum type name
        LP_String m_name;
        LP_String m_module_name;
        LP_String m_type_id;
        // enum values
        db0::db0_ptr<db0::v_bindex<LP_String> > m_values;
        db0::db0_ptr<db0::v_bvector<LP_String> > m_ordered_values;
        
        o_enum(Memspace &);
    };
    
    /**
     * NOTE: enum types use SLOT_NUM = TYPE_SLOT_NUM
     * NOTE: enum allocations are NOT unique
    */
    class Enum: public db0::ObjectBase<Enum, db0::v_object<o_enum, Fixture::TYPE_SLOT_NUM>, StorageClass::DB0_ENUM_TYPE_REF, false>
    {
        // GC0 specific declarations
        GC0_Declare
    public:
        static constexpr std::uint32_t SLOT_NUM = Fixture::TYPE_SLOT_NUM;
        using super_t = db0::ObjectBase<Enum, db0::v_object<o_enum, SLOT_NUM>, StorageClass::DB0_ENUM_TYPE_REF, false>;
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        
        Enum(const Enum &) = delete;
        Enum(Enum &&) = delete;
        Enum(db0::swine_ptr<Fixture> &, Address);
        Enum(db0::swine_ptr<Fixture> &, const std::string &name, const std::string &module_name, 
            const std::vector<std::string> &values, const char *type_id = nullptr);
        ~Enum();
        
        std::string getName() const;
        std::string getModuleName() const;
        
        // exception thrown if value not found
        LP_String find(const char *value) const;
        LP_String tryFind(const char *value) const;
                
        // Get unique 32-bit identifier
        // it's implemented as a relative address from the underlying SLOT
        std::uint32_t getUID() const { return m_uid; }
        
        EnumValue tryGet(const char *value) const;
        EnumValue get(const char *value) const;
        EnumValue get(EnumValue_UID) const;
        
        // Get enum value as a language-specific type
        ObjectSharedPtr getLangValue(const char *value) const;
        ObjectSharedPtr getLangValue(EnumValue_UID) const;
        ObjectSharedPtr getLangValue(const EnumValue &) const;
        // retrieve value by its ordinal index
        ObjectSharedPtr getLangValue(unsigned int at) const;
        // returns nullptr if a value does not exist
        ObjectSharedPtr tryGetLangValue(const char *value) const;
        
        // Retrieve all enum defined values ordered by index
        std::vector<EnumValue> getValues() const;
        
        const EnumFullDef &getEnumDef() const;
        std::optional<std::string> getTypeID() const;
        
        // Fetch specific enum value's string representation
        std::string fetchValue(LP_String) const;
        
        std::size_t size() const;

        void detach() const;
        
        void commit() const;
        
        // NOTE: this is for type compatibility only, Enum objects don't have instance_id
        UniqueAddress getUniqueAddress() const;
        
    private:
        const std::uint64_t m_fixture_uuid;
        const std::uint32_t m_uid;     
        // NOTE: string-pool reference only available with the corresponding Fixture
        RC_LimitedStringPool &m_string_pool_ref;
        db0::v_bindex<LP_String> m_values;
        // values in order defined by the user
        db0::v_bvector<LP_String> m_ordered_values;
        // enum-values cache (lang objects)
        mutable std::unordered_map<std::string, ObjectSharedPtr> m_cache;
        // cache by ordinal values
        mutable std::unordered_map<unsigned int, ObjectSharedPtr> m_ord_cache;
        const EnumFullDef m_enum_def;
        
        std::uint32_t fetchUID() const;
        
        EnumFullDef makeEnumDef() const;
        
        const RC_LimitedStringPool &getStringPool(db0::swine_ptr<Fixture> &lock) const;
    };

    std::optional<std::string> getEnumKeyVariant(std::optional<std::string> type_id, std::optional<std::string> enum_name,
        std::optional<std::string> module_name, std::uint32_t hash, int variant_id);
    
DB0_PACKED_END

}

namespace std

{

    ostream &operator<<(ostream &os, const db0::object_model::Enum &);

}