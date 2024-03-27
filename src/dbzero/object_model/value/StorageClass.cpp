#include "StorageClass.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::object_model 

{

    StorageClassMapper::StorageClassMapper()
    {
        addMapping(TypeId::NONE, StorageClass::NONE);
        addMapping(TypeId::FLOAT, StorageClass::FP_NUMERIC64);
        addMapping(TypeId::INTEGER, StorageClass::INT64);
        addMapping(TypeId::DATETIME, StorageClass::DB0_DATETIME);
        addMapping(TypeId::DATE, StorageClass::DATE);    
        addMapping(TypeId::LIST, StorageClass::DB0_LIST);
        addMapping(TypeId::DICT, StorageClass::DB0_DICT);
        addMapping(TypeId::SET, StorageClass::DB0_SET);
        addMapping(TypeId::TUPLE, StorageClass::DB0_TUPLE);
        addMapping(TypeId::MEMO_OBJECT, StorageClass::OBJECT_REF);
        addMapping(TypeId::DB0_LIST, StorageClass::DB0_LIST);
        addMapping(TypeId::DB0_DICT, StorageClass::DB0_DICT);
        addMapping(TypeId::DB0_SET, StorageClass::DB0_SET);
        addMapping(TypeId::DB0_BLOCK, StorageClass::DB0_BLOCK);
        addMapping(TypeId::DB0_INDEX, StorageClass::DB0_INDEX);
        addMapping(TypeId::DB0_PANDAS_DATAFRAME, StorageClass::DB0_PANDAS_DATAFRAME);
        addMapping(TypeId::DB0_DATETIME, StorageClass::DB0_DATETIME);
    }
    
    StorageClass StorageClassMapper::getStorageClass(TypeId type_id) const
    {
        if (type_id == TypeId::STRING) {
            // determine string type dynamically
            return StorageClass::STRING_REF;
        }
        
        auto int_id = static_cast<std::size_t>(type_id);
        if (int_id < m_storage_class_map.size()) {
            return m_storage_class_map[int_id];
        }
        THROWF(db0::InputException) 
            << "Storage class unknown for common language type ID: " << type_id << THROWF_END;
    }

    void StorageClassMapper::addMapping(TypeId type_id, StorageClass storage_class) 
    {
        auto int_id = static_cast<unsigned int>(type_id);
        while (m_storage_class_map.size() <= int_id) {
            m_storage_class_map.push_back(StorageClass::INVALID);
        }
        m_storage_class_map[int_id] = storage_class;
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
            case StorageClass::OBJECT_REF: return os << "OBJECT_REF";
            case StorageClass::DB0_LIST: return os << "DB0_LIST";
            case StorageClass::DB0_DICT: return os << "DB0_DICT";
            case StorageClass::DB0_SET: return os << "DB0_SET";
            case StorageClass::DB0_TUPLE: return os << "DB0_TUPLE";
            case StorageClass::DB0_DATETIME: return os << "DB0_DATETIME";
            case StorageClass::STR64: return os << "STR64";
            case StorageClass::INVALID: return os << "INVALID";
            default: return os << "ERROR!";
        }
        return os;
    }

}