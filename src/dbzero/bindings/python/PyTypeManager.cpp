// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyTypeManager.hpp"
#include <Python.h>
#include <datetime.h>
#include <chrono>
#include "Memo.hpp"
#include "PyWeakProxy.hpp"
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/PySet.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/collections/PyDict.hpp>
#include <dbzero/bindings/python/collections/PyIndex.hpp>
#include <dbzero/bindings/python/collections/PyByteArray.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterable.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterator.hpp>
#include <dbzero/bindings/python/PyTagSet.hpp>
#include <dbzero/bindings/python/PyWeakProxy.hpp>
#include <dbzero/bindings/python/MemoExpiredRef.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/bytes/ByteArray.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
#include <dbzero/bindings/python/types/PyObjectId.hpp>
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/bindings/python/types/PyClassFields.hpp>
#include <dbzero/bindings/python/types/PyClass.hpp>
#include <dbzero/bindings/python/types/PyTag.hpp>
#include <dbzero/bindings/python/types/PyDecimal.hpp>
#include <dbzero/core/utils/ProcessTimer.hpp>

namespace db0::python

{
    
    std::vector<std::unique_ptr<std::string> > PyTypeManager::m_string_pool;
    
    PyTypeManager::PyTypeManager()
    {
        if (!Py_IsInitialized()) {
            Py_InitializeEx(0);
        }

        // date time initialize
        PyDateTime_IMPORT;
        
        // register well known static types, including DB0 extension types
        addStaticSimpleType(&PyLong_Type, TypeId::INTEGER);
        addStaticSimpleType(&PyFloat_Type, TypeId::FLOAT);
        addStaticSimpleType(&PyBool_Type, TypeId::BOOLEAN);
        addStaticSimpleType(Py_TYPE(Py_None), TypeId::NONE);
        addStaticSimpleType(&PyUnicode_Type, TypeId::STRING);
        // add python list type
        addStaticType(&PyList_Type, TypeId::LIST);
        addStaticType(&PySet_Type, TypeId::SET);
        addStaticType(&PyDict_Type, TypeId::DICT);
        addStaticType(&PyTuple_Type, TypeId::TUPLE);
        addStaticType(&PyBytes_Type, TypeId::BYTES);
        // Python date types
        addStaticSimpleType(PyDateTimeAPI->DateType, TypeId::DATE);

        // special cases for types with Timezone
        m_simple_py_type_ids.insert(TypeId::DATETIME);
        m_simple_py_type_ids.insert(TypeId::DATETIME_TZ);
        m_simple_py_type_ids.insert(TypeId::TIME);
        m_simple_py_type_ids.insert(TypeId::TIME_TZ);

        PyObject *decimal_type = getDecimalClass();
 
        addStaticSimpleType(decimal_type, TypeId::DECIMAL);
        // dbzero extension types
        addStaticdbzeroType(&PyTagType, TypeId::DB0_TAG);
        addStaticdbzeroType(&TagSetType, TypeId::DB0_TAG_SET);
        addStaticdbzeroType(&IndexObjectType, TypeId::DB0_INDEX);
        addStaticdbzeroType(&ListObjectType, TypeId::DB0_LIST);
        addStaticdbzeroType(&SetObjectType, TypeId::DB0_SET);
        addStaticdbzeroType(&DictObjectType, TypeId::DB0_DICT);
        addStaticdbzeroType(&TupleObjectType, TypeId::DB0_TUPLE);
        addStaticdbzeroType(&ClassObjectType, TypeId::DB0_CLASS);
        addStaticdbzeroType(&PyObjectIterableType, TypeId::OBJECT_ITERABLE);
        addStaticdbzeroType(&PyObjectIteratorType, TypeId::OBJECT_ITERATOR);
        addStaticdbzeroType(&ByteArrayObjectType, TypeId::DB0_BYTES_ARRAY);
        addStaticdbzeroType(&PyEnumType, TypeId::DB0_ENUM);
        addStaticdbzeroType(&PyEnumValueType, TypeId::DB0_ENUM_VALUE);
        addStaticdbzeroType(&PyEnumValueReprType, TypeId::DB0_ENUM_VALUE_REPR);
        addStaticdbzeroType(&PyFieldDefType, TypeId::DB0_FIELD_DEF);
        addStaticdbzeroType(&PyWeakProxyType, TypeId::DB0_WEAK_PROXY);
        addStaticdbzeroType(&MemoExpiredRefType, TypeId::MEMO_EXPIRED_REF);

        m_py_bad_prefix_error = PyErr_NewException("dbzero.BadPrefixError", NULL, NULL);
        m_py_class_not_found_error = PyErr_NewException("dbzero.ClassNotFoundError", NULL, NULL);
        m_py_reference_error = PyErr_NewException("dbzero.ReferenceError", NULL, NULL);
    }
    
    PyTypeManager::~PyTypeManager()
    {
        if (!Py_IsInitialized()) {
            for (auto &pair: m_type_cache) {
                pair.second.steal();
            }
            for (auto &pair: m_enum_cache) {
                pair.second.steal(); 
            }        
            m_py_bad_prefix_error.steal();        
            m_py_class_not_found_error.steal();        
            m_py_reference_error.steal();            
        }
    }

    std::optional<PyTypeManager::TypeId> PyTypeManager::tryGetTypeId(TypeObjectPtr py_type) const
    {
        if (!py_type) {
            return std::nullopt;
        }

        // check if a memo class first
        if (PyMemoType_Check<MemoObject>(py_type)) {
            return TypeId::MEMO_OBJECT;
        }
        if (PyMemoType_Check<MemoImmutableObject>(py_type)) {
            return TypeId::MEMO_IMMUTABLE_OBJECT;
        }
        
        // check with the static types next
        auto it = m_id_map.find(reinterpret_cast<PyObject*>(py_type));
        if (it == m_id_map.end()) {
            return std::nullopt;
        }

        // return a known registered type
        return it->second;
    }

    PyTypeManager::TypeId PyTypeManager::getTypeId(TypeObjectPtr py_type) const
    {
        auto type_id = tryGetTypeId(py_type);
        if (!type_id) {
            THROWF(db0::InputException) << "Type unsupported by dbzero: " << py_type->tp_name;
        }         
        return *type_id;
    }
    
    std::optional<PyTypeManager::TypeId> PyTypeManager::tryGetTypeId(ObjectPtr ptr) const
    {
        if (!ptr) {
            return TypeId::UNKNOWN;
        }
        
        if (PyType_Check(ptr)) {
            auto py_type = reinterpret_cast<PyTypeObject*>(ptr);
            if (PyAnyMemoType_Check(py_type)) {
                return TypeId::MEMO_TYPE;
            }
        }
        
        auto py_type = Py_TYPE(ptr);
        // case for datetime
        if (py_type == PyDateTimeAPI->DateTimeType) {
            if (isDatatimeWithTZ(ptr)) {
                return TypeId::DATETIME_TZ;
            } else {
                return TypeId::DATETIME;
            }
        }

        if (py_type == PyDateTimeAPI->TimeType) {
            if (isDatatimeWithTZ(ptr)) {
                return TypeId::TIME_TZ;
            } else {
                return TypeId::TIME;
            }
        }

        return tryGetTypeId(py_type);
    }

    PyTypeManager::TypeId PyTypeManager::getTypeId(ObjectPtr ptr) const
    {
        auto type_id = tryGetTypeId(ptr);
        if (!type_id) {
            THROWF(db0::InputException) << "Type unsupported by dbzero: " << Py_TYPE(ptr)->tp_name;
        }
        
        return *type_id;
    }

    const PyTypeManager::ObjectAnyImpl &PyTypeManager::extractAnyObject(ObjectPtr obj_ptr) const
    {
        if (PyAnyMemo_Check(obj_ptr)) {
            return reinterpret_cast<const MemoAnyObject*>(obj_ptr)->ext();
        } else if (PyWeakProxy_Check(obj_ptr)) {
            return reinterpret_cast<const PyWeakProxy*>(obj_ptr)->get()->ext();
        }
        THROWF(db0::InputException) << "Expected a memo object" << THROWF_END;            
    }

    PyTypeManager::ObjectAnyImpl &PyTypeManager::extractMutableAnyObject(ObjectPtr obj_ptr) const
    {
        if (!PyAnyMemo_Check(obj_ptr)) {
            THROWF(db0::InputException) << "Expected a memo object" << THROWF_END;
        }
        return reinterpret_cast<MemoAnyObject*>(obj_ptr)->modifyExt();
    }
    
    template <typename MemoImplT> typename MemoImplT::ExtT &
    PyTypeManager::extractMutableObject(ObjectPtr obj_ptr) const
    {
        if (!PyMemo_Check<MemoImplT>(obj_ptr)) {
            THROWF(db0::InputException) << "Expected a memo object" << THROWF_END;
        }
        return reinterpret_cast<MemoImplT*>(obj_ptr)->modifyExt();
    }

    template <typename MemoImplT> const typename MemoImplT::ExtT &
    PyTypeManager::extractObject(ObjectPtr memo_ptr) const
    {
        if (PyMemo_Check<MemoImplT>(memo_ptr)) {
            return reinterpret_cast<const MemoImplT*>(memo_ptr)->ext();
        } else if (PyWeakProxy_Check(memo_ptr)) {
            return reinterpret_cast<const MemoImplT*>(reinterpret_cast<const PyWeakProxy*>(memo_ptr)->get())->ext();
        }
        THROWF(db0::InputException) << "Expected a memo object" << THROWF_END;            
    }    
    
    const PyTypeManager::ObjectAnyImpl *PyTypeManager::tryExtractObject(ObjectPtr memo_ptr) const
    {
        if (!PyAnyMemo_Check(memo_ptr)) {
            return nullptr;
        }
        return &reinterpret_cast<const MemoAnyObject*>(memo_ptr)->ext();
    }    
    
    template <typename MemoImplT>
    typename MemoImplT::ExtT *PyTypeManager::tryExtractMutableObject(ObjectPtr memo_ptr) const
    {
        if (!PyMemo_Check<MemoImplT>(memo_ptr)) {
            return nullptr;
        }
        return &reinterpret_cast<MemoImplT*>(memo_ptr)->modifyExt();
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
    
    db0::object_model::ByteArray &PyTypeManager::extractMutableByteArray(ObjectPtr py_obj) const
    {
        if (!ByteArrayObject_Check(py_obj)) {
            THROWF(db0::InputException) << "Expected a db0.BytesArray object" << THROWF_END;
        }
        return reinterpret_cast<db0::python::ByteArrayObject*>(py_obj)->modifyExt();
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
        m_string_pool.push_back(std::make_unique<std::string>(std::move(str)));
        return m_string_pool.back()->c_str();
    }
    
    const char *PyTypeManager::getPooledString(const char *str)
    {
        if (!str) {
            return nullptr;
        }
        return getPooledString(std::string(str));
    }
    
    void PyTypeManager::addMemoType(TypeObjectPtr type, const char *type_id, MemoTypeDecoration &&decoration)
    {
        // register the type with the registry first (this allows retrieving type decoration)
        m_type_registry[type] = std::move(decoration);
        // register type with up to 4 key variants
        for (unsigned int i = 0; i < 4; ++i) {
            auto variant_name = db0::object_model::getNameVariant(type, type_id, i);
            if (variant_name) {
                /* FIXME: blocked due to bug in kv-editor import mechanism (double import)
                if (m_type_cache.find(*variant_name) != m_type_cache.end()) {
                    THROWF(db0::InputException) << "Memo type: '" << *variant_name << "' already exists" << THROWF_END;
                }
                */
                m_type_cache[*variant_name] = TypeObjectSharedPtr(type);
            }
        }
        
        // identify the MemoBase Type
        if (type_id && std::string(type_id) == "Division By Zero/dbzero/MemoBase") {
            m_memo_base_type = type;
            // register the type with mappings
            addStaticdbzeroType(m_memo_base_type, TypeId::MEMO_OBJECT);
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
            case TypeId::DATETIME_TZ:
                return pyDateTimeWithTzToUint64(obj_ptr);
            case TypeId::DATE:
                return pyDateToUint64(obj_ptr);
            case TypeId::TIME:
                return pyTimeToUint64(obj_ptr);
            case TypeId::TIME_TZ:
                return pyTimeWithTzToUint64(obj_ptr);
            case TypeId::INTEGER:
                return PyLong_AsUnsignedLongLong(obj_ptr);
            case TypeId::DECIMAL:
                return pyDecimalToUint64(obj_ptr);
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

    PyTypeManager::ObjectPtr PyTypeManager::getLangObject(TypeObjectPtr py_type) const {
        return reinterpret_cast<ObjectPtr>(py_type);
    }
    
    PyTypeManager::ObjectIterable &PyTypeManager::extractObjectIterable(ObjectPtr obj_ptr) const
    {
        if (!PyObjectIterable_Check(obj_ptr)) {
            THROWF(db0::InputException) << "Expected an ObjectIterable object" << THROWF_END;
        }
        return reinterpret_cast<PyObjectIterable*>(obj_ptr)->modifyExt();
    }
    
    bool PyTypeManager::isNull(ObjectPtr obj_ptr) const {
        return !obj_ptr || obj_ptr == Py_None;
    }
    
    const db0::object_model::EnumValue &PyTypeManager::extractEnumValue(ObjectPtr enum_value_ptr) const
    {
        if (!PyEnumValue_Check(enum_value_ptr)) {
            THROWF(db0::InputException) << "Expected an EnumValue object" << THROWF_END;
        }
        return reinterpret_cast<PyEnumValue*>(enum_value_ptr)->ext();
    }
    
    const db0::object_model::EnumValueRepr &PyTypeManager::extractEnumValueRepr(ObjectPtr enum_value_repr_ptr) const
    {
        if (!PyEnumValueRepr_Check(enum_value_repr_ptr)) {
            THROWF(db0::InputException) << "Expected an EnumValueRepr object" << THROWF_END;
        }
        return reinterpret_cast<PyEnumValueRepr*>(enum_value_repr_ptr)->ext();
    }

    db0::object_model::FieldDef &PyTypeManager::extractFieldDef(ObjectPtr py_object) const
    {
        if (!PyFieldDef_Check(py_object)) {
            THROWF(db0::InputException) << "Expected a FieldDef object" << THROWF_END;
        }
        return reinterpret_cast<PyFieldDef*>(py_object)->modifyExt();
    }
    
    void PyTypeManager::addEnum(PyEnum *py_enum)
    {
        auto &enum_data = py_enum->ext();
        // register type with up to 4 key variants
        for (unsigned int i = 0; i < 4; ++i) {
            auto variant_name = getEnumKeyVariant(enum_data, i);
            if (variant_name) {
                auto it = m_enum_cache.find(*variant_name);
                if (it != m_enum_cache.end()) {                    
                    THROWF(db0::InputException) << "Enum with name '" << *variant_name << "' already exists" << THROWF_END;
                }
                m_enum_cache[*variant_name] = ObjectSharedPtr(py_enum);
            }
        }
    }
    
    void PyTypeManager::close(db0::ProcessTimer *timer_ptr)
    {
        std::unique_ptr<db0::ProcessTimer> timer;
        if (timer_ptr) {
            timer = std::make_unique<db0::ProcessTimer>("PyTypeManager::close", *timer_ptr);
        }
        // close Enum's but don't remove from cache
        // this is to allow future creation / retrieval of dbzero enums while keeping python objects alive
        for (auto &item : m_enum_cache) {
            PyEnum *py_enum = reinterpret_cast<PyEnum*>(item.second.get());
            py_enum->modifyExt().close();
        }
        
        for (auto &item: m_type_registry) {
            item.second.close();
        }
    }
    
    PyTypeManager::ObjectPtr PyTypeManager::getBadPrefixError() const {
        return m_py_bad_prefix_error.get();
    }

    PyTypeManager::ObjectPtr PyTypeManager::getClassNotFoundError() const {
        return m_py_class_not_found_error.get();
    }
    
    PyTypeManager::ObjectPtr PyTypeManager::getReferenceError() const {
        return m_py_reference_error.get();
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

    bool PyTypeManager::isdbzeroTypeId(TypeId type_id) const {
        return m_dbzero_type_ids.find(type_id) != m_dbzero_type_ids.end();
    }

    bool PyTypeManager::isdbzeroType(ObjectPtr obj_ptr) const {
        return isdbzeroTypeId(getTypeId(obj_ptr));
    }

    bool PyTypeManager::isSimplePyType(ObjectPtr obj_ptr) const {
        return isSimplePyTypeId(getTypeId(obj_ptr));
    }
    
    bool PyTypeManager::isSimplePyTypeId(TypeId type_id) const {
        return m_simple_py_type_ids.find(type_id) != m_simple_py_type_ids.end();
    }

    const PyTypeManager::TagDef &PyTypeManager::extractTag(ObjectPtr py_tag) const 
    {
        assert(PyTag_Check(py_tag));
        return reinterpret_cast<PyTag*>(py_tag)->ext();
    }
    
    const MemoTypeDecoration &PyTypeManager::getMemoTypeDecoration(TypeObjectPtr py_type) const
    {
        auto it = m_type_registry.find(py_type);
        if (it == m_type_registry.end()) {
            THROWF(db0::InputException) << "Not a registered memo type: " << PyToolkit::getTypeName(py_type) << THROWF_END;
        }
        return it->second;
    }

    MemoTypeDecoration &PyTypeManager::getMemoTypeDecoration(TypeObjectPtr py_type)
    {
        auto it = m_type_registry.find(py_type);
        if (it == m_type_registry.end()) {
            THROWF(db0::InputException) << "Not a registered memo type: " << PyToolkit::getTypeName(py_type) << THROWF_END;
        }
        return it->second;
    }

    PyTypeManager::ObjectSharedPtr PyTypeManager::tryFindEnum(const std::string &variant_name) const
    {
        auto it = m_enum_cache.find(variant_name);
        if (it != m_enum_cache.end()) {
            return it->second;
        }
        return nullptr;
    }

    PyTypeManager::ObjectSharedPtr PyTypeManager::tryFindEnum(const EnumDef &enum_def) const
    {
        // try finding by all key variants
        for (unsigned int i = 0; i < 4; ++i) {
            auto variant_name = getEnumKeyVariant(enum_def, i);
            if (variant_name) {
                auto result = tryFindEnum(*variant_name);
                if (result.get()) {
                    return result;
                }
            }
        }
        return nullptr;
    }
    
    std::shared_ptr<EnumTypeDef> PyTypeManager::tryFindEnumTypeDef(const std::string &variant_name) const
    {   
        auto result = tryFindEnum(variant_name);
        if (!result) {
            return nullptr;
        }

        return reinterpret_cast<PyEnum*>(*result)->ext().m_enum_type_def;
    }
    
    std::shared_ptr<EnumTypeDef> PyTypeManager::tryFindEnumTypeDef(const EnumDef &enum_def) const
    {
        auto result = tryFindEnum(enum_def);
        if (!result) {
            return nullptr;
        }

        return reinterpret_cast<PyEnum*>(*result)->ext().m_enum_type_def;
    }
    
    std::shared_ptr<EnumTypeDef> PyTypeManager::findEnumTypeDef(const EnumDef &enum_def) const
    {
        auto result = tryFindEnumTypeDef(enum_def);
        if (!result) {
            THROWF(db0::InputException) << "Enum type not found: " << enum_def << THROWF_END;
        }
        return result;
    }
    
    PyTypeManager::ObjectSharedPtr PyTypeManager::getTypeObject(TypeId type_id) const
    {
        auto it = m_py_type_map.find(type_id);
        if (it == m_py_type_map.end()) {
            THROWF(db0::InputException) 
                << "Type with id: " << (int)type_id << " could not be resolved" << THROWF_END;
        }
        return it->second;
    }
    
    PyTypeManager::ObjectSharedPtr PyTypeManager::tryGetTypeObject(TypeId type_id) const
    {
        auto it = m_py_type_map.find(type_id);
        if (it != m_py_type_map.end()) {
            return it->second;
        }
        return Py_BORROW(Py_None);
    }
    
    std::string PyTypeManager::getLangTypeName(TypeObjectPtr py_type) const {
        return py_type->tp_name ? py_type->tp_name : "";
    }
    
    PyTypeManager::ObjectSharedPtr PyTypeManager::getLangConstant(unsigned int val_code) const
    {
        using Value = db0::object_model::Value;
        switch (val_code) {
            case Value::NONE:
                return Py_BORROW(Py_None);
            case Value::FALSE:
                return Py_BORROW(Py_False);
            case Value::TRUE:
                return Py_BORROW(Py_True);
            default:
                THROWF(db0::InputException) << "Invalid value code: " << val_code << THROWF_END;
        }
    }

    template db0::object_model::Object &
    PyTypeManager::extractMutableObject<MemoObject>(ObjectPtr) const;

    template db0::object_model::ObjectImmutableImpl &
    PyTypeManager::extractMutableObject<MemoImmutableObject>(ObjectPtr) const;

    template const db0::object_model::Object &
    PyTypeManager::extractObject<MemoObject>(ObjectPtr) const;

    template const db0::object_model::ObjectImmutableImpl &
    PyTypeManager::extractObject<MemoImmutableObject>(ObjectPtr) const;
 
    template db0::object_model::ObjectImmutableImpl *
    PyTypeManager::tryExtractMutableObject<MemoImmutableObject>(ObjectPtr) const;

    template db0::object_model::Object *
    PyTypeManager::tryExtractMutableObject<MemoObject>(ObjectPtr) const;

}