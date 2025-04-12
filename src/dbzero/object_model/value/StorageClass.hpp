#pragma once

#include <cstdint>
#include <limits>
#include <vector>
#include <dbzero/bindings/TypeId.hpp>

namespace db0::object_model

{
    
    /**
     * StorageClass defines possible types for object values     
    */
    enum class StorageClass: std::uint8_t
    {
        // undefined value (not set)
        UNDEFINED = 0,
        // null value
        NONE = 1,
        // reference to the string instance
        STRING_REF = 2,
        // reference to the pooled string item
        POOLED_STRING = 3,
        INT64 = 4,
        PTIME64 = 5,
        FP_NUMERIC64 = 6,
        DATE = 7,
        DATETIME = 8,
        DATETIME_TZ = 9,
        TIME = 10,
        TIME_TZ = 11,
        DECIMAL = 12,
        // reference to other dbzero object (Memo)
        OBJECT_REF = 13,
        DB0_LIST = 14,
        DB0_DICT = 15,
        DB0_SET = 16,
        DB0_TUPLE = 17,
        // string value encoded in 64 bits
        STR64 = 18,
        DB0_BLOCK = 19,
        DB0_PANDAS_DATAFRAME = 20,
        DB0_CLASS = 21,
        DB0_INDEX = 22,
        DB0_BYTES = 23,
        // dbzero object serialized to a byte array
        DB0_SERIALIZED = 24,
        DB0_BYTES_ARRAY = 25,
        DB0_ENUM_TYPE_REF = 26,
        DB0_ENUM_VALUE = 27,
        // BOOL
        BOOLEAN = 28,
        // weak reference to other (Memo) instance on the same prefix
        OBJECT_WEAK_REF = 29,
        // weak reference to other (Memo) instance from a foreign prefix
        OBJECT_LONG_WEAK_REF = 30,
        // COUNT used to determine size of the StorageClass associated arrays
        COUNT = 31,
        
        // invalid / reserved value, never used in objects
        INVALID = std::numeric_limits<std::uint8_t>::max()
    };
    
    bool isReference(StorageClass);
    
    class StorageClassMapper
    {
    public:
        using TypeId = db0::bindings::TypeId;

        StorageClassMapper();
        // Get storage class corresponding to a specifc common language model type ID
        StorageClass getStorageClass(TypeId) const;
        
    private:
        std::vector<StorageClass> m_storage_class_map;
        
        void addMapping(TypeId, StorageClass);
    };
    
}

namespace std

{

    ostream &operator<<(ostream &, db0::object_model::StorageClass);
        
}
