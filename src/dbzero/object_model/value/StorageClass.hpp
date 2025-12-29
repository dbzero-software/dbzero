// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <limits>
#include <vector>
#include <cassert>
#include <dbzero/bindings/TypeId.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/object_model/LangConfig.hpp>

namespace db0::object_model

{
    
    /**
     * NOTE: PreStorageClass does not define OBJECT_LONG_WEAK_REF
     * which needs to be determined separately in case of detecting OBJECT_WEAK_REF
    */
    enum class PreStorageClass: std::uint8_t
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
       // Fidelity-2 packed storage (e.g. 2-bit boolean or None)
       PACK_2 = 29,
       // weak reference to other (Memo) instance on the same prefix
       OBJECT_WEAK_REF = 30,
       // deleted value (placeholder)
       DELETED = 31,
       COUNT = std::numeric_limits<std::uint8_t>::max() - 32,
       // invalid / reserved value, never used in objects
       INVALID = std::numeric_limits<std::uint8_t>::max()
    };
    
    /**
     * StorageClass defines possible types for object values
    */
    enum class StorageClass: std::uint8_t
    {
        // undefined value (not set)
        UNDEFINED = static_cast<int>(PreStorageClass::UNDEFINED),
        // null value
        NONE = static_cast<int>(PreStorageClass::NONE),
        // reference to the string instance
        STRING_REF = static_cast<int>(PreStorageClass::STRING_REF),
        // reference to the pooled string item
        POOLED_STRING = static_cast<int>(PreStorageClass::POOLED_STRING),
        INT64 = static_cast<int>(PreStorageClass::INT64),
        PTIME64 = static_cast<int>(PreStorageClass::PTIME64),
        FP_NUMERIC64 = static_cast<int>(PreStorageClass::FP_NUMERIC64),
        DATE = static_cast<int>(PreStorageClass::DATE),
        DATETIME = static_cast<int>(PreStorageClass::DATETIME),
        DATETIME_TZ = static_cast<int>(PreStorageClass::DATETIME_TZ),
        TIME = static_cast<int>(PreStorageClass::TIME),
        TIME_TZ = static_cast<int>(PreStorageClass::TIME_TZ),
        DECIMAL = static_cast<int>(PreStorageClass::DECIMAL),
        // reference to other dbzero object (Memo)
        OBJECT_REF = static_cast<int>(PreStorageClass::OBJECT_REF),
        DB0_LIST = static_cast<int>(PreStorageClass::DB0_LIST),
        DB0_DICT = static_cast<int>(PreStorageClass::DB0_DICT),
        DB0_SET = static_cast<int>(PreStorageClass::DB0_SET),
        DB0_TUPLE = static_cast<int>(PreStorageClass::DB0_TUPLE),
        // string value encoded in 64 bits
        STR64 = static_cast<int>(PreStorageClass::STR64),
        DB0_CLASS = static_cast<int>(PreStorageClass::DB0_CLASS),
        DB0_INDEX = static_cast<int>(PreStorageClass::DB0_INDEX),
        DB0_BYTES = static_cast<int>(PreStorageClass::DB0_BYTES),
        // dbzero object serialized to a byte array
        DB0_SERIALIZED = static_cast<int>(PreStorageClass::DB0_SERIALIZED),
        DB0_BYTES_ARRAY = static_cast<int>(PreStorageClass::DB0_BYTES_ARRAY),
        DB0_ENUM_TYPE_REF = static_cast<int>(PreStorageClass::DB0_ENUM_TYPE_REF),
        DB0_ENUM_VALUE = static_cast<int>(PreStorageClass::DB0_ENUM_VALUE),
        // BOOL
        BOOLEAN = static_cast<int>(PreStorageClass::BOOLEAN),
        PACK_2 = static_cast<int>(PreStorageClass::PACK_2),
        // weak reference to other (Memo) instance on the same prefix
        OBJECT_WEAK_REF = static_cast<int>(PreStorageClass::OBJECT_WEAK_REF),
        DELETED = static_cast<int>(PreStorageClass::DELETED),
        // weak reference to other (Memo) instance from a foreign prefix
        OBJECT_LONG_WEAK_REF = static_cast<int>(PreStorageClass::COUNT),
        // COUNT used to determine size of the StorageClass associated arrays
        COUNT = static_cast<int>(PreStorageClass::COUNT) + 1,
        // invalid / reserved value, never used in objects
        INVALID = std::numeric_limits<std::uint8_t>::max()
    };
    
    class StorageClassMapper
    {
    public:
        using TypeId = db0::bindings::TypeId;

        StorageClassMapper();
        // Get storage class corresponding to a specifc common language model type ID
        // @param allow_packed if true, BOOLEAN and NONE map to PACK_2
        PreStorageClass getPreStorageClass(TypeId, bool allow_packed) const;
        TypeId getTypeId(PreStorageClass) const;
        
    private:
        std::vector<PreStorageClass> m_storage_class_map;
        // a mapping with packed types (BOOLEAN, NONE) mapped to PACK_2
        std::vector<PreStorageClass> m_storage_class_packed_map;
        std::vector<TypeId> m_type_id_map;
        
        void addMapping(TypeId, PreStorageClass);
        // adds reverse mapping only
        void addReverseMapping(PreStorageClass, TypeId);
    };
    
    // Get storage class / type name for schema reporting purposes
    std::string getTypeName(StorageClass);
    // @retrun 0 for default fidelity (e.g. 64bit), otherwise the number of bits
    unsigned int getStorageFidelity(StorageClass);
    
}

namespace db0

{

    using ObjectPtr = db0::object_model::LangConfig::ObjectPtr;
    using PreStorageClass = db0::object_model::PreStorageClass;
    using StorageClass = db0::object_model::StorageClass;
    
    // This version is valid for all cases except for OBJECT_WEAK_REF
    inline StorageClass getStorageClass(PreStorageClass pre_storage_class) 
    {
        assert(pre_storage_class != PreStorageClass::OBJECT_WEAK_REF);
        return static_cast<StorageClass>(static_cast<int>(pre_storage_class));
    }
    
    // Get the closest pre-storage class
    inline PreStorageClass getPreStorageClass(StorageClass storage_class) 
    {
        if (storage_class == StorageClass::OBJECT_LONG_WEAK_REF) {
            return PreStorageClass::OBJECT_WEAK_REF;
        }
        return static_cast<PreStorageClass>(static_cast<int>(storage_class));
    }
    
    // This version should only be called for PreStorageClass::OBJECT_WEAK_REF 
    // to distinguish between short and long weak reference
    StorageClass getStorageClass(PreStorageClass, db0::swine_ptr<db0::Fixture> &, ObjectPtr);
    StorageClass getStorageClass(PreStorageClass, const db0::Fixture &, ObjectPtr);

}

namespace std

{

    ostream &operator<<(ostream &, db0::object_model::StorageClass);
        
}
