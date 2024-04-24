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
        // reference to other DBZero object (Memo)
        OBJECT_REF = 8,
        DB0_LIST = 9,
        DB0_DICT = 10,
        DB0_SET = 11,
        DB0_TUPLE = 12,
        // string value encoded in 64 bits
        STR64 = 13,
        DB0_BLOCK= 14,
        DB0_PANDAS_DATAFRAME= 15,
        DB0_CLASS = 16,
        DB0_INDEX = 17,    
        DB0_BYTES = 18,
        // DBZero object serialized to a byte array
        DB0_SERIALIZED = 19,
        // COUNT used to determine size of the StorageClass associated arrays
        COUNT = 20,
        DB0_BYTES_ARRAY = 21,
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
