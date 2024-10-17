#include "PyTypeManager.hpp"
#include "Memo.hpp"
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/PySet.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/collections/PyDict.hpp>
#include <dbzero/bindings/python/collections/PyIndex.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
#include <Python.h>
#include <datetime.h>
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
#include <dbzero/bindings/python/types/DateTime.hpp>
#include "PyClassFields.hpp"
#include "PyEnum.hpp"
#include "PyClass.hpp"

namespace db0::python

{
    
    PyTypeManager::PyTypeManager()
    {
        if (!Py_IsInitialized()) {
            Py_InitializeEx(0);
        }

        // date time initialize
        PyDateTime_IMPORT;
        
        // register well known static types, including DB0 extension types
        addStaticType(&PyLong_Type, TypeId::INTEGER);
        addStaticType(&PyFloat_Type, TypeId::FLOAT);
        addStaticType(&_PyNone_Type, TypeId::NONE);
        addStaticType(&PyUnicode_Type, TypeId::STRING);
        // add python list type
        addStaticType(&PyList_Type, TypeId::LIST);
        addStaticType(&PySet_Type, TypeId::SET);
        addStaticType(&PyDict_Type, TypeId::DICT);
        addStaticType(&PyTuple_Type, TypeId::TUPLE);
        addStaticType(&TagSetType, TypeId::DB0_TAG_SET);
        addStaticType(&IndexObjectType, TypeId::DB0_INDEX);
        addStaticType(&ListObjectType, TypeId::DB0_LIST);
        addStaticType(&SetObjectType, TypeId::DB0_SET);
        addStaticType(&DictObjectType, TypeId::DB0_DICT);
        addStaticType(&TupleObjectType, TypeId::DB0_TUPLE);
        addStaticType(&ClassObjectType, TypeId::DB0_CLASS);
        addStaticType(&PyObjectIteratorType, TypeId::OBJECT_ITERATOR);
        addStaticType(&PyBytes_Type, TypeId::BYTES);
        addStaticType(&PyEnumType, TypeId::DB0_ENUM);
        addStaticType(&PyEnumValueType, TypeId::DB0_ENUM_VALUE);
        addStaticType(&PyEnumValueReprType, TypeId::DB0_ENUM_VALUE_REPR);
        addStaticType(&PyFieldDefType, TypeId::DB0_FIELD_DEF);
        addStaticType(&PandasBlockObjectType, TypeId::DB0_BLOCK);
        addStaticType(&PandasDataFrameObjectType, TypeId::DB0_PANDAS_DATAFRAME);
        // Python datetime type
        addStaticType(PyDateTimeAPI->DateTimeType, TypeId::DATETIME);
        m_py_bad_prefix_error = PyErr_NewException("dbzero_ce.BadPrefixError", NULL, NULL);
        m_py_class_not_found_error = PyErr_NewException("dbzero_ce.ClassNotFoundError", NULL, NULL);
    }
    
    PyTypeManager::~PyTypeManager()
    {
        for (auto &str: m_string_pool) {
            delete str;
        }
    }
    
    PyTypeManager::TypeId PyTypeManager::getTypeId(TypeObjectPtr py_type) const
    {
        if (!py_type) {
            return TypeId::UNKNOWN;
        }

        // check if a memo class first
        if (PyMemoType_Check(py_type)) {
            return TypeId::MEMO_OBJECT;
        }

        // check with the static types next
        auto it = m_id_map.find(reinterpret_cast<PyObject*>(py_type));
        if (it == m_id_map.end()) {            
            THROWF(db0::InputException) << "Type unsupported by DBZero: " << py_type->tp_name;
        }

        // return a known registered type
        return it->second;
    }
    
    PyTypeManager::TypeId PyTypeManager::getTypeId(ObjectPtr ptr) const
    {
        if (!ptr) {
            return TypeId::UNKNOWN;
        }

        return getTypeId(Py_TYPE(ptr));
    }
    
    const db0::object_model::Object &PyTypeManager::extractObject(ObjectPtr memo_ptr) const
    {
        if (!PyMemo_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a memo object" << THROWF_END;
        }
        return reinterpret_cast<const MemoObject*>(memo_ptr)->ext();
    }

    db0::object_model::Object &PyTypeManager::extractMutableObject(ObjectPtr memo_ptr) const
    {
        if (!PyMemo_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a memo object" << THROWF_END;
        }
        return reinterpret_cast<MemoObject*>(memo_ptr)->modifyExt();
    }

    const db0::object_model::Object *PyTypeManager::tryExtractObject(ObjectPtr memo_ptr) const
    {
        if (!PyMemo_Check(memo_ptr)) {
            return nullptr;
        }
        return &reinterpret_cast<const MemoObject*>(memo_ptr)->ext();
    }

    db0::object_model::Object *PyTypeManager::tryExtractMutableObject(ObjectPtr memo_ptr) const
    {
        if (!PyMemo_Check(memo_ptr)) {
            return nullptr;
        }
        return &reinterpret_cast<MemoObject*>(memo_ptr)->modifyExt();
    }

    const db0::object_model::List &PyTypeManager::extractList(ObjectPtr list_ptr) const
    {
        if (!ListObject_Check(list_ptr)) {
            THROWF(db0::InputException) << "Expected a list object" << THROWF_END;
        }
        return reinterpret_cast<const ListObject*>(list_ptr)->ext();
    }

    db0::object_model::List &PyTypeManager::extractMutableList(ObjectPtr list_ptr) const
    {
        if (!ListObject_Check(list_ptr)) {
            THROWF(db0::InputException) << "Expected a list object" << THROWF_END;
        }
        return reinterpret_cast<ListObject*>(list_ptr)->modifyExt();
    }

    const db0::object_model::Set &PyTypeManager::extractSet(ObjectPtr set_ptr) const
    {
        if (!SetObject_Check(set_ptr)) {
            THROWF(db0::InputException) << "Expected a set object" << THROWF_END;
        }
        return reinterpret_cast<const SetObject*>(set_ptr)->ext();
    }

    db0::object_model::Set &PyTypeManager::extractMutableSet(ObjectPtr set_ptr) const
    {
        if (!SetObject_Check(set_ptr)) {
            THROWF(db0::InputException) << "Expected a set object" << THROWF_END;
        }
        return reinterpret_cast<SetObject*>(set_ptr)->modifyExt();
    }

    const db0::object_model::pandas::Block &PyTypeManager::extractBlock(ObjectPtr memo_ptr) const
    {
        if (!PandasBlock_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a Block object" << THROWF_END;
        }
        return reinterpret_cast<const db0::python::PandasBlockObject*>(memo_ptr)->ext();
    }

    db0::object_model::pandas::Block &PyTypeManager::extractMutableBlock(ObjectPtr memo_ptr) const
    {
        if (!PandasBlock_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a Block object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::PandasBlockObject*>(memo_ptr)->modifyExt();
    }

    const db0::object_model::Tuple &PyTypeManager::extractTuple(ObjectPtr memo_ptr) const
    {
        if (!TupleObject_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a Tuple object" << THROWF_END;
        }
        return reinterpret_cast<const db0::python::TupleObject*>(memo_ptr)->ext();
    }

    db0::object_model::Tuple &PyTypeManager::extractMutableTuple(ObjectPtr memo_ptr) const
    {
        if (!TupleObject_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a Tuple object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::TupleObject*>(memo_ptr)->modifyExt();
    }

    const db0::object_model::Dict &PyTypeManager::extractDict(ObjectPtr memo_ptr) const
    {
        if (!DictObject_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a Dict object" << THROWF_END;
        }
        return reinterpret_cast<const db0::python::DictObject*>(memo_ptr)->ext();
    }

    db0::object_model::Dict &PyTypeManager::extractMutableDict(ObjectPtr memo_ptr) const
    {
        if (!DictObject_Check(memo_ptr)) {
            THROWF(db0::InputException) << "Expected a Dict object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::DictObject*>(memo_ptr)->modifyExt();
    }

    db0::object_model::TagSet &PyTypeManager::extractTagSet(ObjectPtr tag_set_ptr) const
    {
        if (!TagSet_Check(tag_set_ptr)) {
            THROWF(db0::InputException) << "Expected a TagSet object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::PyTagSet*>(tag_set_ptr)->m_tag_set;
    }
    
    const db0::object_model::Index &PyTypeManager::extractIndex(ObjectPtr index_ptr) const 
    {
        if (!IndexObject_Check(index_ptr)) {
            THROWF(db0::InputException) << "Expected an Index object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::IndexObject*>(index_ptr)->ext();
    }

    db0::object_model::Index &PyTypeManager::extractMutableIndex(ObjectPtr index_ptr) const
    {
        if (!IndexObject_Check(index_ptr)) {
            THROWF(db0::InputException) << "Expected an Index object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::IndexObject*>(index_ptr)->modifyExt();
    }

    const char *PyTypeManager::getPooledString(std::string str)
    {
        m_string_pool.push_back(new std::string(str));
        return m_string_pool.back()->c_str();
    }
    
    const char *PyTypeManager::getPooledString(const char *str)
    {
        if (!str) {
            return nullptr;
        }
        return getPooledString(std::string(str));
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
        // identify the MemoBase Type
        if (type_id && std::string(type_id) == "Division By Zero/dbzero_ce/MemoBase") {
            m_memo_base_type = type;
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
            THROWF(db0::InputException) << "Expected an integer object, got " 
                << PyToolkit::getTypeName(int_ptr) << THROWF_END;
        }
        return PyLong_AsLongLong(int_ptr);
    }
    
    std::uint64_t PyTypeManager::extractUInt64(ObjectPtr obj_ptr) const {
        return extractUInt64(getTypeId(obj_ptr), obj_ptr);
    }
    
    std::uint64_t PyTypeManager::extractUInt64(TypeId type_id, ObjectPtr obj_ptr) const
    {
        switch (type_id) {
            case TypeId::DATETIME:
                return pyDateTimeToToUint64(obj_ptr);
            case TypeId::INTEGER:
                return PyLong_AsUnsignedLongLong(obj_ptr);
            default:
                THROWF(db0::InputException) << "Unable to convert object of type " << PyToolkit::getTypeName(obj_ptr) 
                    << " to UInt64" << THROWF_END;
        }
    }

    PyTypeManager::TypeObjectPtr PyTypeManager::getTypeObject(ObjectPtr py_type) const 
    {
        assert(PyType_Check(py_type));
        return reinterpret_cast<TypeObjectPtr>(py_type);
    }

    PyTypeManager::ObjectIterator &PyTypeManager::extractObjectIterator(ObjectPtr obj_ptr) const
    {
        if (!PyObjectIterator_Check(obj_ptr)) {
            THROWF(db0::InputException) << "Expected an ObjectIterator object" << THROWF_END;
        }
        return *reinterpret_cast<PyObjectIterator*>(obj_ptr)->modifyExt();
    }
    
    bool PyTypeManager::isNull(ObjectPtr obj_ptr) const {
        return !obj_ptr || obj_ptr == Py_None;
    }
    
    db0::object_model::EnumValue PyTypeManager::extractEnumValue(ObjectPtr enum_value_ptr) const
    {
        if (!PyEnumValue_Check(enum_value_ptr)) {
            THROWF(db0::InputException) << "Expected an EnumValue object" << THROWF_END;
        }
        return reinterpret_cast<PyEnumValue*>(enum_value_ptr)->ext();
    }

    db0::object_model::FieldDef &PyTypeManager::extractFieldDef(ObjectPtr py_object) const
    {
        if (!PyFieldDef_Check(py_object)) {
            THROWF(db0::InputException) << "Expected a FieldDef object" << THROWF_END;
        }
        return reinterpret_cast<PyFieldDef*>(py_object)->modifyExt();
    }

    void PyTypeManager::addEnum(PyEnum *py_enum) {
        m_enum_cache.push_back(reinterpret_cast<PyObject*>(py_enum));
    }
    
    void PyTypeManager::close()
    {
        // close Enum's but don't remove from cache
        // this is to allow future creation / retrieval of DBZero enums while keeping python objects alive
        for (auto &obj : m_enum_cache) {
            PyEnum *py_enum = reinterpret_cast<PyEnum*>(obj.get());
            py_enum->modifyExt().close();
        }
        for (auto &memo_type: m_type_cache) {
            PyMemoType_close(memo_type.second.get());
        }
    }
    
    PyTypeManager::ObjectPtr PyTypeManager::getBadPrefixError() const {
        return m_py_bad_prefix_error.get();
    }

    PyTypeManager::ObjectPtr PyTypeManager::getClassNotFoundError() const {
        return m_py_class_not_found_error.get();
    }
    
    std::shared_ptr<const db0::object_model::Class> PyTypeManager::extractConstClass(ObjectPtr py_class) const
    {
        if (!PyClassObject_Check(py_class)) {
            THROWF(db0::InputException) << "Expected a Class object" << THROWF_END;
        }
        return reinterpret_cast<ClassObject*>(py_class)->getSharedPtr();
    }
    
    void PyTypeManager::forAllMemoTypes(std::function<void(TypeObjectPtr)> f) const
    {
        for (auto &memo_type: m_type_cache) {
            f(memo_type.second.get());
        }
    }
    
    PyTypeManager::TypeObjectSharedPtr PyTypeManager::tryGetMemoBaseType() const noexcept {
        return m_memo_base_type;
    }
    
    PyTypeManager::TypeObjectSharedPtr PyTypeManager::getMemoBaseType() const
    {
        if (!m_memo_base_type) {
            THROWF(db0::InternalException) << "MemoBase type not found" << THROWF_END;
        }
        return m_memo_base_type;
    }
    
    bool PyTypeManager::isMemoBase(TypeObjectPtr py_type) const
    {
        assert(m_memo_base_type);
        return py_type == m_memo_base_type;
    }
    
}