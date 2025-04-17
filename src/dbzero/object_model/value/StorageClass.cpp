#include "StorageClass.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/object/Object.hpp>

namespace db0::object_model

{
    
    StorageClassMapper::StorageClassMapper()
    {
        addMapping(TypeId::NONE, PreStorageClass::NONE);
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
        addMapping(TypeId::DB0_LIST, PreStorageClass::DB0_LIST);
        addMapping(TypeId::DB0_DICT, PreStorageClass::DB0_DICT);
        addMapping(TypeId::DB0_SET, PreStorageClass::DB0_SET);
        addMapping(TypeId::DB0_TUPLE, PreStorageClass::DB0_TUPLE);
        addMapping(TypeId::DB0_INDEX, PreStorageClass::DB0_INDEX);
        addMapping(TypeId::OBJECT_ITERABLE, PreStorageClass::DB0_SERIALIZED);
        addMapping(TypeId::DB0_ENUM_VALUE, PreStorageClass::DB0_ENUM_VALUE);
        addMapping(TypeId::BOOLEAN, PreStorageClass::BOOLEAN);
        addMapping(TypeId::DB0_BYTES_ARRAY, PreStorageClass::DB0_BYTES_ARRAY);
        // Note: DB0_WEAK_PROXY by default maps to OBJECT_WEAK_REF but can also be OBJECT_LONG_WEAK_REF which needs to be checked
        addMapping(TypeId::DB0_WEAK_PROXY, PreStorageClass::OBJECT_WEAK_REF);
    }
    
    PreStorageClass StorageClassMapper::getPreStorageClass(TypeId type_id) const
    {
        if (type_id == TypeId::STRING) {
            // determine string type dynamically
            return PreStorageClass::STRING_REF;
        }
        
        auto int_id = static_cast<std::size_t>(type_id);
        if (int_id < m_storage_class_map.size()) {
            return m_storage_class_map[int_id];
        }
        THROWF(db0::InputException)
            << "Storage class unknown for common language type ID: " << static_cast<int>(type_id) << THROWF_END;
    }
    
    void StorageClassMapper::addMapping(TypeId type_id, PreStorageClass storage_class) 
    {
        auto int_id = static_cast<unsigned int>(type_id);
        while (m_storage_class_map.size() <= int_id) {
            m_storage_class_map.push_back(PreStorageClass::INVALID);
        }
        m_storage_class_map[int_id] = storage_class;
    }
    
    bool isReference(StorageClass type)
    {
        switch (type) {
            case StorageClass::OBJECT_REF:
            case StorageClass::DB0_LIST:
            case StorageClass::DB0_DICT:
            case StorageClass::DB0_SET:
            case StorageClass::DB0_TUPLE:
            case StorageClass::DB0_CLASS:
            case StorageClass::DB0_INDEX:
            case StorageClass::DB0_BYTES:
            case StorageClass::DB0_BYTES_ARRAY:
            case StorageClass::OBJECT_WEAK_REF:
            case StorageClass::OBJECT_LONG_WEAK_REF:            
                return true;
            default:
                return false;
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
        const auto &obj = LangToolkit::getTypeManager().extractObject(lang_value);
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
        const auto &obj = LangToolkit::getTypeManager().extractObject(lang_value);
        if (*obj.getFixture() != fixture) {
            // must use long weak-ref instead, since referenced object is from a foreign prefix
            return StorageClass::OBJECT_LONG_WEAK_REF;
        }
        return StorageClass::OBJECT_WEAK_REF;
    }
    
}