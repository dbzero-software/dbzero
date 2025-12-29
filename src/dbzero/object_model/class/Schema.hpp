// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "FieldID.hpp"
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/collections/vector/v_sorted_vector.hpp>
#include <dbzero/core/collections/vector/VLimitedMatrix.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/object_model/object/lofi_store.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model

{

DB0_PACKED_BEGIN

    enum class SchemaTypeId: std::uint16_t
    {
        // undefined value (not set)
        UNDEFINED = static_cast<int>(StorageClass::UNDEFINED),
        DELETED = static_cast<int>(StorageClass::DELETED),
        // null value
        NONE = static_cast<int>(StorageClass::NONE),
        // reference to the string instance
        STRING = static_cast<int>(StorageClass::STRING_REF),
        INT = static_cast<int>(StorageClass::INT64),
        TIMESTAMP = static_cast<int>(StorageClass::PTIME64),
        FLOAT = static_cast<int>(StorageClass::FP_NUMERIC64),
        DATE = static_cast<int>(StorageClass::DATE),
        DATETIME = static_cast<int>(StorageClass::DATETIME),
        DATETIME_TZ = static_cast<int>(StorageClass::DATETIME_TZ),
        TIME = static_cast<int>(StorageClass::TIME),
        TIME_TZ = static_cast<int>(StorageClass::TIME_TZ),
        DECIMAL = static_cast<int>(StorageClass::DECIMAL),
        OBJECT = static_cast<int>(StorageClass::OBJECT_REF),
        LIST = static_cast<int>(StorageClass::DB0_LIST),
        DICT = static_cast<int>(StorageClass::DB0_DICT),
        SET = static_cast<int>(StorageClass::DB0_SET),
        TUPLE = static_cast<int>(StorageClass::DB0_TUPLE),
        CLASS = static_cast<int>(StorageClass::DB0_CLASS),
        INDEX = static_cast<int>(StorageClass::DB0_INDEX),
        BYTES = static_cast<int>(StorageClass::DB0_BYTES),
        BYTES_ARRAY = static_cast<int>(StorageClass::DB0_BYTES_ARRAY),
        ENUM_TYPE = static_cast<int>(StorageClass::DB0_ENUM_TYPE_REF),
        ENUM = static_cast<int>(StorageClass::DB0_ENUM_VALUE),        
        BOOLEAN = static_cast<int>(StorageClass::BOOLEAN),        
        WEAK_REF = static_cast<int>(StorageClass::OBJECT_WEAK_REF),
    };
    
    // NOTE: this version is only capable of handling full types (e.g. PACK_2 will raise an exception)
    SchemaTypeId getSchemaTypeId(StorageClass);
    // This version is capable of handling all storage classes 
    // but requires the additional "value" parameter (unpacked)
    SchemaTypeId getSchemaTypeId(StorageClass, Value);
    
    // convert to a common type ID
    db0::bindings::TypeId getTypeId(SchemaTypeId);
    std::string getTypeName(SchemaTypeId);
    
    struct DB0_PACKED_ATTR o_type_item: public db0::o_fixed_versioned<o_type_item>
    {        
        using FieldLoc = std::pair<std::uint32_t, std::uint32_t>;
        SchemaTypeId m_type_id = SchemaTypeId::UNDEFINED;
        // the number of occurences of the specific type ID
        std::uint32_t m_count = 0;
        
        o_type_item() = default;
        o_type_item(SchemaTypeId, std::uint32_t count = 0);
        o_type_item(SchemaTypeId, int count);

        bool operator!() const;

        // match by type ID only
        inline bool operator==(const o_type_item &other) const {
            return (m_type_id == other.m_type_id);
        }

        inline bool operator<(const o_type_item &other) const {
            return (m_type_id < other.m_type_id);
        }
        
        // assign from type ID and count (FieldID is ignored)
        o_type_item &operator=(std::tuple<FieldLoc, SchemaTypeId, int>);
    };
    
    struct DB0_PACKED_ATTR o_schema: public db0::o_fixed_versioned<o_schema>
    {        
        using TypeVector = db0::v_sorted_vector<o_type_item>;
        using total_func = std::function<std::uint32_t()>;
        using FieldLoc = std::pair<std::uint32_t, std::uint32_t>;

        // the primary type ID (e.g. db0::bindings::StorageClass::STRING, NONE inclusive)
        SchemaTypeId m_primary_type_id = SchemaTypeId::UNDEFINED;
        // the secondary (second most common)
        o_type_item m_secondary_type;
        // total number of occurrences of all extra type IDs
        std::uint32_t m_total_extra = 0;
        // optional type vector for storing additional, less common type IDs
        // sorted by type ID for fast updates
        db0::db0_ptr<TypeVector> m_type_vector_ptr;

        o_schema() = default;
        // construct populated with values (type ID + occurrence count)
        // NOTE: FieldLoc is for type compatibility only, it is ignored
        o_schema(Memspace &memspace,
            std::vector<std::tuple<FieldLoc, SchemaTypeId, int> >::const_iterator begin,
            std::vector<std::tuple<FieldLoc, SchemaTypeId, int> >::const_iterator end
        );

        // evaluate primary type based on the current collection size
        SchemaTypeId getPrimaryType(std::uint32_t collection_size) const;

        // get last known primary & secondary type for a given field ID
        std::pair<SchemaTypeId, SchemaTypeId> getType() const;

        // get all types from the most to least common
        std::vector<SchemaTypeId> getAllTypes(Memspace &) const;
        
        // NOTE: FieldLoc is for type compatibility only, it is ignored
        void update(Memspace &memspace,
            std::vector<std::tuple<FieldLoc, SchemaTypeId, int> >::const_iterator begin,
            std::vector<std::tuple<FieldLoc, SchemaTypeId, int> >::const_iterator end,
            std::uint32_t collection_size
        );

        // Update to reflect the collection size change only
        void update(Memspace &, std::uint32_t collection_size);

    protected:
        friend class Schema;
        
        bool isPrimarySwapRequired(std::uint32_t collection_size) const;

    private:
        void update(Memspace &, TypeVector &, std::uint32_t collection_size);
        void initTypeVector(Memspace &, TypeVector &);
    };
    
DB0_PACKED_END

    class Schema: protected db0::VLimitedMatrix<o_schema, lofi_store<2>::size()>
    {
    public:        
        using super_t = db0::VLimitedMatrix<o_schema, lofi_store<2>::size()>;
        using total_func = std::function<std::uint32_t()>;
        
        // as null instsance
        Schema();
        
        // @param total_func - a function that returns the total number of instance occurrences in the schema
        // it is required for the primary type ID occurrence calculation / only invoked on need-to-know basis
        // it is mandatory but can be configured later with postInit()
        Schema(Memspace &, total_func = {});
        Schema(mptr, total_func = {});
        ~Schema();
        
        void postInit(total_func);

        // add occurrence of a specicifc type (as a specific field ID)
        void add(FieldID, SchemaTypeId);
        void remove(FieldID, SchemaTypeId);

        // flush updates from the associated builder
        void flush() const;

        // discard all updates not yet flushed to the schema
        void rollback();

        // Get primary / most likely type (avoids returning None if other types are present)
        // NOTE that it may be TypeID::UNKNOWN
        SchemaTypeId getPrimaryType(FieldID) const;
        
        // get primary & secondary type for a given field ID
        std::pair<SchemaTypeId, SchemaTypeId> getType(FieldID) const;
        // get all types from the most to least common for a given field ID        
        std::vector<SchemaTypeId> getAllTypes(FieldID) const;
        
        db0::Address getAddress() const;
        void detach() const;
        void commit() const;
        
    private:
        using FieldLoc = std::pair<std::uint32_t, std::uint32_t>;
        class Builder;
        friend class Builder;
        mutable std::unique_ptr<Builder> m_builder;
        mutable std::uint32_t m_last_collection_size = 0;
        total_func m_get_total;
        
        Builder &getBuilder() const;
        
        // batch update a specific field's statistics
        // field ID, type ID, update count
        void update(FieldLoc field_loc,
            std::vector<std::tuple<FieldLoc, SchemaTypeId, int> >::const_iterator begin,
            std::vector<std::tuple<FieldLoc, SchemaTypeId, int> >::const_iterator end, 
            std::uint32_t collection_size
        );
        
        // Update to reflect the collection size change only
        void update(std::uint32_t collection_size);
    };

}