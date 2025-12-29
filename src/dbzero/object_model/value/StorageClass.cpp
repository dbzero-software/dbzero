// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "StorageClass.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/object/ObjectAnyImpl.hpp>

namespace db0::object_model

{
    
    StorageClassMapper::StorageClassMapper()
    {
        m_type_id_map.reserve(static_cast<std::size_t>(PreStorageClass::COUNT));
        addMapping(TypeId::NONE, PreStorageClass::NONE);
        // NOTE: None can be stored as 2-bit packed value
        addMapping(TypeId::NONE, PreStorageClass::PACK_2);
        addMapping(TypeId::STRING, PreStorageClass::STRING_REF);
        addReverseMapping(PreStorageClass::POOLED_STRING, TypeId::STRING);
        addReverseMapping(PreStorageClass::STR64, TypeId::STRING);
        addMapping(TypeId::FLOAT, PreStorageClass::FP_NUMERIC64);
        addMapping(TypeId::INTEGER, PreStorageClass::INT64);
        addMapping(TypeId::DATETIME, PreStorageClass::DATETIME);
        addMapping(TypeId::DATETIME_TZ, PreStorageClass::DATETIME_TZ);
        addMapping(TypeId::DATE, PreStorageClass::DATE);
        addMapping(TypeId::TIME, PreStorageClass::TIME);
        addMapping(TypeId::TIME_TZ, PreStorageClass::TIME_TZ);
        addMapping(TypeId::DECIMAL, PreStorageClass::DECIMAL);
        addMapping(TypeId::LIST, PreStorageClass::DB0_LIST);
        addMapping(TypeId::DICT, PreStorageClass::DB0_DICT);
        addMapping(TypeId::SET, PreStorageClass::DB0_SET);
        addMapping(TypeId::TUPLE, PreStorageClass::DB0_TUPLE);
        addMapping(TypeId::BYTES, PreStorageClass::DB0_BYTES);
        addMapping(TypeId::MEMO_OBJECT, PreStorageClass::OBJECT_REF);
        addMapping(TypeId::DB0_CLASS, PreStorageClass::DB0_CLASS);
        // storage class for memo types is DB0_CLASS
        addMapping(TypeId::MEMO_TYPE, PreStorageClass::DB0_CLASS);
        addMapping(TypeId::DB0_LIST, PreStorageClass::DB0_LIST);
        addMapping(TypeId::DB0_DICT, PreStorageClass::DB0_DICT);
        addMapping(TypeId::DB0_SET, PreStorageClass::DB0_SET);
        addMapping(TypeId::DB0_TUPLE, PreStorageClass::DB0_TUPLE);
        addMapping(TypeId::DB0_INDEX, PreStorageClass::DB0_INDEX);
        addMapping(TypeId::OBJECT_ITERABLE, PreStorageClass::DB0_SERIALIZED);
        addMapping(TypeId::DB0_ENUM_VALUE, PreStorageClass::DB0_ENUM_VALUE);
        // NOTE: enum value-reprs are converted to materialized enums on storage
        addMapping(TypeId::DB0_ENUM_VALUE_REPR, PreStorageClass::DB0_ENUM_VALUE);
        addMapping(TypeId::BOOLEAN, PreStorageClass::BOOLEAN);
        // NOTE: booleans can be packed as 2-bit values
        addMapping(TypeId::BOOLEAN, PreStorageClass::PACK_2);
        addMapping(TypeId::DB0_BYTES_ARRAY, PreStorageClass::DB0_BYTES_ARRAY);
        // Note: DB0_WEAK_PROXY by default maps to OBJECT_WEAK_REF but can also be OBJECT_LONG_WEAK_REF which needs to be checked
        addMapping(TypeId::DB0_WEAK_PROXY, PreStorageClass::OBJECT_WEAK_REF);
    }
    
    PreStorageClass StorageClassMapper::getPreStorageClass(TypeId type_id, bool allow_packed) const
    {
        if (type_id == TypeId::STRING) {
            // determine string type dynamically
            return PreStorageClass::STRING_REF;
        }
        
        auto storage_map = allow_packed ? &m_storage_class_packed_map : &m_storage_class_map;
        auto int_id = static_cast<std::size_t>(type_id);
        if (int_id < storage_map->size()) {
            assert((*storage_map)[int_id] != PreStorageClass::INVALID);
            return (*storage_map)[int_id];
        }
        THROWF(db0::InputException)
            << "Storage class unknown for common language type ID: " << static_cast<int>(type_id) << THROWF_END;
    }
    
    void StorageClassMapper::addMapping(TypeId type_id, PreStorageClass storage_class)
    {
        auto int_id = static_cast<unsigned int>(type_id);
        for (auto storage_map: {&m_storage_class_map, &m_storage_class_packed_map}) {
            if (storage_class == PreStorageClass::PACK_2 
                && storage_map != &m_storage_class_packed_map) 
            {
                // PACK_2 only in packed map
                continue;
            }
            while (storage_map->size() <= int_id) {
                storage_map->push_back(PreStorageClass::INVALID);
            }
            storage_map->at(int_id) = storage_class;
            if (storage_class != PreStorageClass::PACK_2) {
                // reverse mapping only for non-packed types
                addReverseMapping(storage_class, type_id);
            }
        }        
    }

    void StorageClassMapper::addReverseMapping(PreStorageClass storage_class, TypeId type_id)
    {
        auto int_storage_class = static_cast<unsigned int>(storage_class);
        if (m_type_id_map.size() <= int_storage_class) {
            m_type_id_map.resize(int_storage_class + 1, TypeId::UNKNOWN);
        }
        m_type_id_map[int_storage_class] = type_id;
    }
    
    StorageClassMapper::TypeId StorageClassMapper::getTypeId(PreStorageClass storage_class) const
    {
        auto int_storage_class = static_cast<unsigned int>(storage_class);
        if (int_storage_class < m_type_id_map.size()) {
            assert(m_type_id_map[int_storage_class] != TypeId::UNKNOWN);
            return m_type_id_map[int_storage_class];
        }
        THROWF(db0::InputException)
            << "Type ID unknown for storage class: " << static_cast<int>(storage_class) << THROWF_END;
    }
    
    unsigned int getStorageFidelity(StorageClass storage_class)
    {
        switch (storage_class) {
            case StorageClass::PACK_2:
                return 2; // 2 bits per boolean or None
            default:
                return 0; // default fidelity (e.g. 64bit)
        }
    }
    
}

namespace std

{
    
    using StorageClass = db0::object_model::StorageClass;
    ostream &operator<<(ostream &os, StorageClass type) 
    {
        switch (type) {
            case StorageClass::UNDEFINED: return os << "UNDEFINED";
            case StorageClass::DELETED: return os << "DELETED";
            case StorageClass::NONE: return os << "NONE";
            case StorageClass::STRING_REF: return os << "STRING_REF";
            case StorageClass::POOLED_STRING: return os << "POOLED_STRING";
            case StorageClass::INT64: return os << "INT64";
            case StorageClass::PTIME64: return os << "PTIME64";
            case StorageClass::FP_NUMERIC64: return os << "FP_NUMERIC64";
            case StorageClass::DATE: return os << "DATE";
            case StorageClass::DATETIME: return os << "DATETIME";
            case StorageClass::DATETIME_TZ: return os << "DATETIME_TZ";
            case StorageClass::TIME: return os << "TIME";
            case StorageClass::TIME_TZ: return os << "TIME_TZ";
            case StorageClass::DECIMAL: return os << "DECIMAL";    
            case StorageClass::OBJECT_REF: return os << "OBJECT_REF";
            case StorageClass::DB0_LIST: return os << "DB0_LIST";
            case StorageClass::DB0_DICT: return os << "DB0_DICT";
            case StorageClass::DB0_SET: return os << "DB0_SET";
            case StorageClass::DB0_TUPLE: return os << "DB0_TUPLE";
            case StorageClass::STR64: return os << "STR64";
            case StorageClass::DB0_CLASS: return os << "DB0_CLASS";
            case StorageClass::DB0_INDEX: return os << "DB0_INDEX";        
            case StorageClass::DB0_SERIALIZED: return os << "DB0_SERIALIZED";
            case StorageClass::DB0_BYTES: return os << "BYTES";
            case StorageClass::DB0_BYTES_ARRAY: return os << "BYTES_ARRAY";
            case StorageClass::DB0_ENUM_TYPE_REF: return os << "DB0_ENUM_TYPE_REF";
            case StorageClass::DB0_ENUM_VALUE: return os << "DB0_ENUM_VALUE";        
            case StorageClass::BOOLEAN: return os << "BOOLEAN";
            case StorageClass::PACK_2: return os << "PACK_2";
            case StorageClass::OBJECT_WEAK_REF: return os << "OBJECT_WEAK_REF";
            case StorageClass::OBJECT_LONG_WEAK_REF: return os << "OBJECT_LONG_WEAK_REF";
            case StorageClass::INVALID: return os << "INVALID";
            default: return os << "ERROR!";
        }
        return os;
    }

}

namespace db0

{

    using LangToolkit = db0::object_model::LangConfig::LangToolkit;

    db0::object_model::StorageClass getStorageClass(db0::object_model::PreStorageClass pre_storage_class,
        db0::swine_ptr<db0::Fixture> &fixture, ObjectPtr lang_value)
    {
        assert(pre_storage_class == PreStorageClass::OBJECT_WEAK_REF);
        const auto &obj = LangToolkit::getTypeManager().extractAnyObject(lang_value);
        if (*obj.getFixture() != *fixture.get()) {
            // must use long weak-ref instead, since referenced object is from a foreign prefix
            return StorageClass::OBJECT_LONG_WEAK_REF;
        }
        return StorageClass::OBJECT_WEAK_REF;
    }

    db0::object_model::StorageClass getStorageClass(db0::object_model::PreStorageClass pre_storage_class,
        const db0::Fixture &fixture, ObjectPtr lang_value)
    {
        assert(pre_storage_class == PreStorageClass::OBJECT_WEAK_REF);
        const auto &obj = LangToolkit::getTypeManager().extractAnyObject(lang_value);
        if (*obj.getFixture() != fixture) {
            // must use long weak-ref instead, since referenced object is from a foreign prefix
            return StorageClass::OBJECT_LONG_WEAK_REF;
        }
        return StorageClass::OBJECT_WEAK_REF;
    }    
       
}