#pragma once

#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/collections/vector/v_sorted_vector.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include "FieldID.hpp"

namespace db0::object_model

{   

    enum class SchemaTypeId: std::uint16_t
    {
        // undefined value (not set)
        UNDEFINED = static_cast<int>(StorageClass::UNDEFINED),
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
    
    SchemaTypeId getSchemaTypeId(StorageClass storage_class);
    std::string getTypeName(SchemaTypeId);

    struct [[gnu::packed]] o_type_item: public db0::o_fixed<o_type_item>
    {        
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
        
        // assign from field ID, type ID and count
        o_type_item &operator=(std::tuple<unsigned int, SchemaTypeId, int>);
    };
    
    struct [[gnu::packed]] o_schema: public db0::o_fixed<o_schema>
    {        
        using TypeVector = db0::v_sorted_vector<o_type_item>;
        using total_func = std::function<std::uint32_t()>;

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
        // construct populated with values
        o_schema(Memspace &memspace,
            std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator begin,
            std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator end
        );

        // get primary & secondary type for a given field ID
        std::pair<SchemaTypeId, SchemaTypeId> getType() const;

        // get all types from the most to least common
        std::vector<SchemaTypeId> getAllTypes(Memspace &) const;

        void update(Memspace &memspace,
            std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator begin,
            std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator end,
            const total_func &get_total
        );
    };
    
    class Schema: protected db0::v_bvector<o_schema>
    {
    public:        
        using super_t = db0::v_bvector<o_schema>;
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
        void add(unsigned int field_id, SchemaTypeId);
        void remove(unsigned int field_id, SchemaTypeId);

        // flush updates from the associated builder
        void flush();

        // Get primary / most likely type (avoids returning None if other types are present)
        // NOTE that it may be TypeID::UNKNOWN
        SchemaTypeId getPrimaryType(unsigned int field_id) const;
        SchemaTypeId getPrimaryType(FieldID) const;
        
        // get primary & secondary type for a given field ID
        std::pair<SchemaTypeId, SchemaTypeId> getType(unsigned int field_id) const;
        std::pair<SchemaTypeId, SchemaTypeId> getType(FieldID) const;

        // get all types from the most to least common for a given field ID
        std::vector<SchemaTypeId> getAllTypes(unsigned int field_id) const;
        std::vector<SchemaTypeId> getAllTypes(FieldID) const;
        
        db0::Address getAddress() const;
        void detach() const;
        void commit() const;

    private:
        class Builder;
        friend class Builder;
        std::unique_ptr<Builder> m_builder;
        total_func m_get_total;

        Builder &getBuilder();

        // batch update a specific field's statistics
        // field ID, type ID, update count
        void update(unsigned int field_id,             
            std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator begin,
            std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator end
        );
    };
    
}