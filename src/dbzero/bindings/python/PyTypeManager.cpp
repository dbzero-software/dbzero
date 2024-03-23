#include "PyTypeManager.hpp"
#include "Memo.hpp"
#include "List.hpp"
#include "Set.hpp"
#include "Tuple.hpp"
#include "Dict.hpp"
#include "Index.hpp"
#include <Python.h>
#include "PyObjectIterator.hpp"
#include <chrono>
#include <dbzero/bindings/python/Pandas/PandasBlock.hpp>
#include <dbzero/bindings/python/Pandas/PandasDataFrame.hpp>
#include <dbzero/bindings/python/PyTagSet.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/object_model/pandas/Block.hpp>
#include <dbzero/object_model/pandas/Dataframe.hpp>
#include <dbzero/object_model/index/Index.hpp>

#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0::python

{
    
    PyTypeManager::PyTypeManager()
    {
        Py_Initialize();
    
        // register well known static types, including DB0 extension types
        addStaticType(&PyLong_Type, TypeId::INTEGER);
        addStaticType(&PyFloat_Type, TypeId::FLOAT);
        addStaticType(&_PyNone_Type, TypeId::NONE);
        addStaticType(&PyUnicode_Type, TypeId::STRING);
        // add python list type
        addStaticType(&PyList_Type, TypeId::LIST);
        addStaticType(&PySet_Type, TypeId::SET);
        addStaticType(&PyDict_Type, TypeId::DICT);
        addStaticType(&TagSetType, TypeId::DB0_TAG_SET);
        addStaticType(&IndexObjectType, TypeId::DB0_INDEX);
        addStaticType(&PyObjectIteratorType, TypeId::OBJECT_ITERATOR);
        addStaticType(&PyTypedObjectIteratorType, TypeId::TYPED_OBJECT_ITERATOR);
    }
    
    PyTypeManager::TypeId PyTypeManager::getTypeId(ObjectPtr ptr) const
    {
        if (!ptr) {
            return TypeId::UNKNOWN;
        }

        auto py_type = Py_TYPE(ptr);
        // check with static types first
        auto it = m_id_map.find(reinterpret_cast<PyObject*>(py_type));
        if (it != m_id_map.end()) {
            // return a known registered type
            return it->second;
        }
        
        // check if memo class
        if (PyMemo_Check(ptr)) {
            return TypeId::MEMO_OBJECT;
        }

         // check if block class
        if (PandasBlock_Check(ptr)) {
            return TypeId::DB0_BLOCK;
        }

        // check if data frame class
        if (PandasDataFrame_Check(ptr)) {
            return TypeId::DB0_PANDAS_DATAFRAME;
        }

        if (PyUnicode_Check(ptr)) {
            return TypeId::STRING;
        } else if (PyLong_Check(ptr)) {
            return TypeId::INTEGER;
        } else if (PyFloat_Check(ptr)) {
            return TypeId::FLOAT;
        } else if (ListObject_Check(ptr)) {
            return TypeId::DB0_LIST;
        } else if (SetObject_Check(ptr)) {
            return TypeId::DB0_SET;
        } else if (DictObject_Check(ptr)) {
            return TypeId::DB0_DICT;
        } else if (TupleObject_Check(ptr)) {
            return TypeId::DB0_TUPLE;
        }
        
        // Raise exception, type unknown
        THROWF(db0::InputException) << "Type unsupported by DBZero: " << py_type->tp_name << THROWF_END;
    }
    
    db0::object_model::Object &PyTypeManager::extractObject(ObjectPtr memo_ptr) const
    {
        if (!PyMemo_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a memo object" << THROWF_END;
        }
        return reinterpret_cast<MemoObject*>(memo_ptr)->ext();
    }
    
    db0::object_model::List &PyTypeManager::extractList(ObjectPtr list_ptr) const
    {
        if (!ListObject_Check(list_ptr)) {
            THROWF(db0::InputException) << "Expected a list object" << THROWF_END;
        }
        return reinterpret_cast<ListObject*>(list_ptr)->ext();
    }
    
    db0::object_model::Set &PyTypeManager::extractSet(ObjectPtr set_ptr) const
    {
        if (!SetObject_Check(set_ptr)) {
            THROWF(db0::InputException) << "Expected a set object" << THROWF_END;
        }
        return reinterpret_cast<SetObject*>(set_ptr)->ext();
    }

    db0::object_model::pandas::Block &PyTypeManager::extractBlock(ObjectPtr memo_ptr) const
    {
        if (!PandasBlock_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a Block object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::PandasBlockObject*>(memo_ptr)->ext();
    }

    db0::object_model::Tuple &PyTypeManager::extractTuple(ObjectPtr memo_ptr) const
    {
        if (!TupleObject_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a Tuple object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::TupleObject*>(memo_ptr)->ext();
    }

    db0::object_model::Dict &PyTypeManager::extractDict(ObjectPtr memo_ptr) const
    {
        if (!DictObject_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a Dict object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::DictObject*>(memo_ptr)->ext();
    }

    db0::object_model::TagSet &PyTypeManager::extractTagSet(ObjectPtr tag_set_ptr) const
    {
        if (!TagSet_Check(tag_set_ptr)) {
            THROWF(db0::InputException) << "Expected a TagSet object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::PyTagSet*>(tag_set_ptr)->m_tag_set;
    }
    
    db0::object_model::Index &PyTypeManager::extractIndex(ObjectPtr index_ptr) const 
    {
        if (!IndexObject_Check(index_ptr)) {
            THROWF(db0::InputException) << "Expected an Index object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::IndexObject*>(index_ptr)->ext();
    }

    const char *PyTypeManager::getPooledString(const std::string &str)
    {
        m_string_pool.push_back(str);
        return m_string_pool.back().c_str();
    }
    
    void PyTypeManager::addMemoType(TypeObjectPtr type, const char *type_id)
    {        
        // register type with up to 4 key variants
        for (unsigned int i = 0; i < 4; ++i) {
            auto variant_name = db0::object_model::getNameVariant(type, type_id, i);
            if (variant_name) {
                m_type_cache.insert({*variant_name, TypeObjectSharedPtr(type)});
            }
        }
    }
    
    PyTypeManager::TypeObjectPtr PyTypeManager::findType(const std::string &variant_name) const
    {
        auto it = m_type_cache.find(variant_name);
        if (it != m_type_cache.end()) {
            return it->second.get();
        }
        return nullptr;
    }
    
    std::int64_t PyTypeManager::extractInt64(ObjectPtr int_ptr) const
    {
        if (!PyLong_Check(int_ptr)) {
            THROWF(db0::InputException) << "Expected an integer object" << THROWF_END;
        }
        return PyLong_AsLongLong(int_ptr);
    }

    PyTypeManager::TypeObjectPtr PyTypeManager::getTypeObject(ObjectPtr py_type) const 
    {
        assert(PyType_Check(py_type));
        return reinterpret_cast<TypeObjectPtr>(py_type);
    }

    PyTypeManager::ObjectIterator &PyTypeManager::extractObjectIterator(ObjectPtr obj_ptr) const
    {
        if (!ObjectIterator_Check(obj_ptr)) {
            THROWF(db0::InputException) << "Expected an ObjectIterator object" << THROWF_END;
        }
        return reinterpret_cast<PyObjectIterator*>(obj_ptr)->ext();
    }

}