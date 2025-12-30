// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyToolkit.hpp"
#include "Memo.hpp"
#include "MemoExpiredRef.hpp"
#include "PyInternalAPI.hpp"
#include "Types.hpp"
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/collections/PyIndex.hpp>
#include <dbzero/bindings/python/collections/PyByteArray.hpp>
#include <dbzero/bindings/python/collections/PySet.hpp>
#include <dbzero/bindings/python/collections/PyDict.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/mptr.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterable.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterator.hpp>
#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/value/long_weak_ref.hpp>
#include <dbzero/bindings/python/types/PyObjectId.hpp>
#include <dbzero/bindings/python/types/PyClassFields.hpp>
#include <dbzero/bindings/python/types/PyClass.hpp>
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/bindings/python/types/PyTag.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>

namespace db0::python

{
    

    PyToolkit::PyWorkspace PyToolkit::m_py_workspace;
    SafeRMutex PyToolkit::m_api_mutex;
    
    std::string PyToolkit::getTypeName(ObjectPtr py_object) {
        return getTypeName(Py_TYPE(py_object));
    }
    
    std::string PyToolkit::getTypeName(TypeObjectPtr py_type) {
        return std::string(py_type->tp_name);
    }
    
    std::string getModuleNameFromFileName(const std::string &file_name)
    {
        // remove extensions and path
        auto pos = file_name.find_last_of('/');
        if (pos == std::string::npos) {
            pos = file_name.find_last_of('\\');
        }
        auto file_name_no_path = file_name.substr(pos + 1);
        pos = file_name_no_path.find_last_of('.');
        if (pos == std::string::npos) {
            return file_name_no_path;
        }
        return file_name_no_path.substr(0, pos);
    }
    
    std::optional<std::string> PyToolkit::tryGetModuleName(TypeObjectPtr py_type)
    {
        auto py_module_name = Py_OWN(PyObject_GetAttrString(reinterpret_cast<ObjectPtr>(py_type), "__module__"));
        if (!py_module_name) {
            return std::nullopt;
        }
        auto result = std::string(PyUnicode_AsUTF8(*py_module_name));
        if (result == "__main__") {
            // for Memo types we can determine the actual module name from the file name
            // (if stored with the type decoration)
            if (PyAnyMemoType_Check(py_type)) {
                // file name may not be available in the type decoration
                auto file_name = MemoTypeDecoration::get(py_type).tryGetFileName();
                if (file_name) {
                    return getModuleNameFromFileName(file_name);
                }
            }
            return std::nullopt;
        }
        return result;
    }

    std::string PyToolkit::getModuleName(TypeObjectPtr py_type)
    {
        auto result = tryGetModuleName(py_type);
        if (!result) {
            THROWF(db0::InputException) << "Could not get module name for class " << getTypeName(py_type);
        }
        return *result;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadObject(db0::swine_ptr<Fixture> &fixture, Address address,
        TypeObjectPtr lang_class, std::uint16_t instance_id, AccessFlags access_mode)
    {
        auto &class_factory = fixture->get<ClassFactory>();
        return unloadObject(fixture, address, class_factory, lang_class, instance_id, access_mode);
    }
    
    bool PyToolkit::isExistingObject(db0::swine_ptr<Fixture> &fixture, Address address, std::uint16_t instance_id)
    {
        // try unloading from cache first
        auto &lang_cache = fixture->getLangCache();
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, address, nullptr);
        
        if (obj_ptr.get()) {
            // only validate instance ID if provided
            auto &memo = reinterpret_cast<MemoAnyObject*>(obj_ptr.get())->ext();
            if (instance_id) {
                // NOTE: we first must check if this is really a memo object
                if (!isAnyMemoObject(obj_ptr.get())) {
                    return false;
                }
                
                if (memo.getInstanceId() != instance_id) {
                    return false;
                }
            }
            // NOTE: objects with no references (either from dbzero or other lang types) are considered deleted            
            return PyToolkit::hasLangRefs(*obj_ptr) || memo.hasRefs();
        }
        
        // Check if object's stem can be unloaded (and has refs)
        return db0::object_model::Object::checkUnload(fixture, address, instance_id, true);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::tryUnloadObject(
        db0::swine_ptr<Fixture> &fixture, Address address, const ClassFactory &class_factory, 
        TypeObjectPtr lang_type_ptr, std::uint16_t instance_id, AccessFlags access_mode)
    {
        // try unloading from cache first
        auto &lang_cache = fixture->getLangCache();
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, address, nullptr);
        
        if (obj_ptr.get()) {
            // only validate instance ID if provided
            if (instance_id) {
                // NOTE: we first must check if this is really a memo object
                if (!isAnyMemoObject(obj_ptr.get())) {
                    return {};
                }
                if (reinterpret_cast<MemoAnyObject*>(obj_ptr.get())->ext().getInstanceId() != instance_id) {                
                    return {};
                }
            }
            
            return obj_ptr;
        }
        
        // Unload from backend otherwise
        auto stem = db0::object_model::Object::tryUnloadStem(
            fixture, address, instance_id, access_mode
        );
        if (!stem) {
            // object not found
            return {};
        }
        auto [type, lang_type] = class_factory.getTypeByClassRef(stem->getClassRef());
        
        if (!lang_type_ptr) {
            if (!lang_type) {
                lang_type = class_factory.getLangType(*type);
            }
            lang_type_ptr = lang_type.get();
        }
        
        if (!lang_type_ptr) {
            THROWF(db0::ClassNotFoundException) << "Could not find type: " << type->getName();
        }
        
        // construct Python's memo object (placeholder for actual dbzero instance)
        // the associated lang class must be available
        auto *memo_ptr = MemoObjectStub_new(lang_type_ptr);
        // unload from stem (with type hint)
        memo_ptr->unload(fixture, std::move(stem), type, Object::with_type_hint{});
        // NOTE: Py_OWN only possible with a proper object
        obj_ptr = Py_OWN((PyObject*)memo_ptr);
        if (!memo_ptr->ext().isNoCache()) {
            lang_cache.add(address, obj_ptr.get());
        }
        return obj_ptr;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadObject(db0::swine_ptr<Fixture> &fixture, Address address,
        const ClassFactory &class_factory, TypeObjectPtr lang_type_ptr, std::uint16_t instance_id, AccessFlags access_mode)
    {
        auto result = tryUnloadObject(
            fixture, address, class_factory, lang_type_ptr, instance_id, access_mode
        );
        if (!result) {
            THROWF(db0::InputException) << "Invalid UUID or object has been deleted";            
        }
        return result;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadObject(db0::swine_ptr<Fixture> &fixture, Address address,
        std::shared_ptr<Class> type, TypeObjectPtr lang_class, AccessFlags access_mode)
    {
        assert(lang_class);
        // try unloading from cache first
        auto &lang_cache = fixture->getLangCache();
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, address);
        
        if (obj_ptr.get()) {
            return obj_ptr;
        }
        
        // NOTE: lang_class may be of a base type (e.g. MemoBase)
        auto *memo_ptr = MemoObjectStub_new(lang_class);
        // unload with type hint
        memo_ptr->unload(fixture, address, type, Object::with_type_hint{}, access_mode);
        // NOTE: Py_OWN only possible with a proper object
        obj_ptr = Py_OWN((PyObject*)memo_ptr);
        if (!memo_ptr->ext().isNoCache()) {
            lang_cache.add(address, obj_ptr.get());
        }        
        return obj_ptr;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadExpiredRef(db0::swine_ptr<Fixture> &fixture, Address addr,
        std::uint64_t obj_fixture_uuid, UniqueAddress obj_address)
    {
        // try unloading from cache first
        auto &lang_cache = fixture->getLangCache();
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, addr);
        
        if (obj_ptr.get()) {
            return obj_ptr;
        }
        
        obj_ptr = MemoExpiredRef_new(obj_fixture_uuid, obj_address);
        lang_cache.add(addr, obj_ptr.get());
        return obj_ptr;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadExpiredRef(db0::swine_ptr<Fixture> &fixture, const LongWeakRef &weak_ref) {
        return unloadExpiredRef(fixture, weak_ref.getAddress(), weak_ref->m_fixture_uuid, weak_ref->m_address);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadList(db0::swine_ptr<Fixture> fixture, Address address, 
        std::uint16_t, AccessFlags access_mode)
    {
        using List = db0::object_model::List;

        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();        
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }
        
        auto list_object = ListDefaultObject_new();
        // retrieve actual dbzero instance
        list_object->unload(fixture, address, access_mode);
        // add list object to cache
        if (!list_object->ext().isNoCache()) {
            lang_cache.add(address, list_object.get());
        }
        return shared_py_cast<PyObject*>(std::move(list_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadByteArray(db0::swine_ptr<Fixture> fixture, 
        Address address, AccessFlags access_mode)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }
        
        auto byte_array_object = ByteArrayDefaultObject_new();
        // retrieve actual dbzero instance
        byte_array_object->unload(fixture, address, access_mode);
        // add byte_array object to cache
        if (!byte_array_object->ext().isNoCache()) {
            lang_cache.add(address, byte_array_object.get());
        }
        return shared_py_cast<PyObject*>(std::move(byte_array_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadIndex(db0::swine_ptr<Fixture> fixture,
        Address address, std::uint16_t, AccessFlags access_mode)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }
        
        auto py_index = Py_OWN(IndexDefaultObject_new());
        // retrieve actual dbzero instance
        py_index->unload(fixture, address, access_mode);

        // add list object to cache
        // NOTE: in case of Index (which requires a flush on update) we need to cache instance
        // even if accessed as no-cache to prevent premature deletion        
        lang_cache.add(address, py_index.get());

        auto py_index_ptr = py_index.get();
        py_index->ext().setDirtyCallback([py_index_ptr](bool incRef) {
            if (incRef) {
                Py_INCREF(py_index_ptr);
            } else {
                Py_DECREF(py_index_ptr);
            }
        });
        
        return shared_py_cast<PyObject*>(std::move(py_index));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadSet(db0::swine_ptr<Fixture> fixture, 
        Address address, std::uint16_t, AccessFlags access_mode)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }
        
        auto set_object = SetDefaultObject_new();
        // retrieve actual dbzero instance
        set_object->unload(fixture, address, access_mode);
        
        // add list object to cache
        if (!set_object->ext().isNoCache()) {
            lang_cache.add(address, set_object.get());
        }
        return shared_py_cast<PyObject*>(std::move(set_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadDict(db0::swine_ptr<Fixture> fixture, 
        Address address, std::uint16_t, AccessFlags access_mode)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }
        
        auto dict_object = DictDefaultObject_new();
        // retrieve actual dbzero instance
        dict_object->unload(fixture, address, access_mode);
        
        // add list object to cache
        if (!dict_object->ext().isNoCache()) {
            lang_cache.add(address, *dict_object);
        }
        return shared_py_cast<PyObject*>(std::move(dict_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadTuple(db0::swine_ptr<Fixture> fixture, 
        Address address, std::uint16_t, AccessFlags access_mode)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }
        
        auto tuple_object = TupleDefaultObject_new();
        // retrieve actual dbzero instance        
        tuple_object->unload(fixture, address, access_mode);
        
        // add list object to cache
        if (!tuple_object->ext().isNoCache()) {
            lang_cache.add(address, *tuple_object);
        }
        return shared_py_cast<PyObject*>(std::move(tuple_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::deserializeObjectIterable(db0::swine_ptr<Fixture> fixture,
        std::vector<std::byte>::const_iterator &iter, 
        std::vector<std::byte>::const_iterator end)
    {
        auto obj_iter = db0::object_model::ObjectIterator::deserialize(fixture, iter, end);
        auto py_iter = PyObjectIterableDefault_new();
        py_iter->makeNew(std::move(*obj_iter));
        return shared_py_cast<PyObject*>(std::move(py_iter));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::deserializeEnumValue(db0::swine_ptr<Fixture> fixture,
        std::vector<std::byte>::const_iterator &iter, 
        std::vector<std::byte>::const_iterator end)
    {
        auto &snapshot = fixture->getWorkspace();
        return db0::object_model::EnumValue::deserialize(snapshot, iter, end);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::deserializeEnumValueRepr(db0::swine_ptr<Fixture> fixture,
        std::vector<std::byte>::const_iterator &iter, 
        std::vector<std::byte>::const_iterator end)
    {
        return db0::object_model::EnumValueRepr::deserialize(fixture, iter, end);
    }
    
    std::uint64_t PyToolkit::getTagFromString(ObjectPtr py_object, db0::pools::RC_LimitedStringPool &string_pool)
    {
        if (!PyUnicode_Check(py_object)) {
            // unable to resolve as tag
            THROWF(db0::InputException) << "Unable to resolve object as tag";
        }
                
        return string_pool.toAddress(string_pool.get(PyUnicode_AsUTF8(py_object)));
    }
    
    std::uint64_t PyToolkit::addTagFromString(ObjectPtr py_object, db0::pools::RC_LimitedStringPool &string_pool, bool &inc_ref)
    {
        if (!PyUnicode_Check(py_object)) {
            // unable to resolve as tag
            THROWF(db0::InputException) << "Unable to resolve object as tag";
        }
        return string_pool.toAddress(string_pool.add(inc_ref, PyUnicode_AsUTF8(py_object)));
    }

    bool PyToolkit::isIterable(ObjectPtr py_object) {
        return Py_TYPE(py_object)->tp_iter != nullptr;
    }

    bool PyToolkit::isString(ObjectPtr py_object) {
        return PyUnicode_Check(py_object);
    }

    bool PyToolkit::isSequence(ObjectPtr py_object) {
        return PySequence_Check(py_object);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::getIterator(ObjectPtr py_object)
    {
        auto py_iterator = Py_OWN(PyObject_GetIter(py_object));
        if (!py_iterator) {
            THROWF(db0::InputException) << "Unable to get iterator" << THROWF_END;
        }
        return py_iterator;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::next(ObjectPtr py_object)
    {
        auto py_next = Py_OWN(PyIter_Next(py_object));
        if (!py_next) {
            // StopIteration exception raised
            PyErr_Clear();
        }

        return py_next;
    }

    std::size_t PyToolkit::length(ObjectPtr py_object)
    {
        Py_ssize_t size = PySequence_Length(py_object);
        if (size < 0) {
            THROWF(db0::InputException) << "Unable to get sequence length" << THROWF_END;
        }
        return size;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::getItem(ObjectPtr py_object, std::size_t i)
    {
        auto item = Py_OWN(PySequence_GetItem(py_object, i));
        if (!item) {
            THROWF(db0::InputException) << "Unable to get sequence item at index ";
        }
        return item;
    }
    
    bool PyToolkit::isSingleton(TypeObjectPtr py_type) {
        return PyMemoType_IsSingleton(py_type);
    }
    
    bool PyToolkit::isType(ObjectPtr py_object) {
        return PyType_Check(py_object);
    }

    bool PyToolkit::isAnyMemoObject(ObjectPtr py_object) {
        return PyAnyMemo_Check(py_object);
    }

    bool PyToolkit::isMemoObject(ObjectPtr py_object) {
        return PyMemo_Check<MemoObject>(py_object);
    }

    bool PyToolkit::isMemoImmutableObject(ObjectPtr py_object) {
        return PyMemo_Check<MemoImmutableObject>(py_object);
    }
    
    PyToolkit::ObjectPtr PyToolkit::getUUID(ObjectPtr py_object) {
        return db0::python::tryGetUUID(py_object);
    }
    
    bool PyToolkit::isEnumValue(ObjectPtr py_object) {
        return PyEnumValue_Check(py_object);
    }

    bool PyToolkit::isFieldDef(ObjectPtr py_object) {
        return PyFieldDef_Check(py_object);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::makeEnumValue(const EnumValue &value) {
        return shared_py_cast<PyObject*>(makePyEnumValue(value));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::makeEnumValueRepr(std::shared_ptr<EnumTypeDef> type_def,
        const char *str_value) 
    {
        return shared_py_cast<PyObject*>(makePyEnumValueRepr(type_def, str_value));
    }
    
    std::string PyToolkit::getLastError()
    {
        PyObject *ptype, *pvalue, *ptraceback;
        PyErr_Fetch(&ptype, &pvalue, &ptraceback);
        PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
        auto pstr = Py_OWN(PyObject_Str(pvalue));
        Py_XDECREF(ptype);
        Py_XDECREF(pvalue);
        Py_XDECREF(ptraceback);

        return PyUnicode_AsUTF8(*pstr);
    }
    
    std::uint64_t PyToolkit::getFixtureUUID(ObjectPtr py_object)
    {
        if (PyType_Check(py_object)) {
            return getFixtureUUID(reinterpret_cast<TypeObjectPtr>(py_object));
        } else if (PyEnumValue_Check(py_object)) {
            return reinterpret_cast<PyEnumValue*>(py_object)->ext().m_fixture.safe_lock()->getUUID();
        } else if (PyAnyMemo_Check(py_object)) {
            return reinterpret_cast<MemoAnyObject*>(py_object)->ext().getFixture()->getUUID();
        } else if (PyObjectIterable_Check(py_object)) {
            return reinterpret_cast<PyObjectIterable*>(py_object)->ext().getFixture()->getUUID();
        } else if (PyObjectIterator_Check(py_object)) {
            return reinterpret_cast<PyObjectIterator*>(py_object)->ext().getFixture()->getUUID();
        } else if (PyTag_Check(py_object)) {
            return reinterpret_cast<PyTag*>(py_object)->ext().tryGetFixtureUUID();
        } else {
            return 0;
        }
    }
    
    std::uint64_t PyToolkit::getFixtureUUID(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFixtureUUID(AccessType::READ_ONLY);
        } else {
            return 0;
        }
    }
    
    bool PyToolkit::isNoDefaultTags(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFlags()[MemoOptions::NO_DEFAULT_TAGS];
        } else {
            return false;
        }
    }
    
    bool PyToolkit::isNoCache(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFlags()[MemoOptions::NO_CACHE];
        } else {
            return false;
        }
    }
    
    bool PyToolkit::isImmutable(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFlags()[MemoOptions::IMMUTABLE];
        } else {
            return false;
        }
    }

    FlagSet<MemoOptions> PyToolkit::getMemoFlags(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFlags();
        } else {
            return {};
        }
    }

    const char *PyToolkit::getPrefixName(TypeObjectPtr memo_type)
    {
        assert(isAnyMemoType(memo_type));
        return MemoTypeDecoration::get(memo_type).tryGetPrefixName();
    }
    
    const char *PyToolkit::getMemoTypeID(TypeObjectPtr memo_type)
    {
        assert(isAnyMemoType(memo_type));
        return MemoTypeDecoration::get(memo_type).tryGetTypeId();        
    }
    
    const std::vector<std::string> &PyToolkit::getInitVars(TypeObjectPtr memo_type)
    {
        assert(isAnyMemoType(memo_type));
        return MemoTypeDecoration::get(memo_type).getInitVars();
    }
    
    bool PyToolkit::isAnyMemoType(TypeObjectPtr py_type) {
        return PyAnyMemoType_Check(py_type);
    }
    
    void PyToolkit::setError(ObjectPtr err_obj, std::uint64_t err_value) {
        PyErr_SetObject(err_obj, *Py_OWN(PyLong_FromUnsignedLongLong(err_value)));
    }
    
    bool PyToolkit::hasLangRefs(ObjectPtr obj) {
        // NOTE: total number of references must be greater than the extended (inner) reference count
        // NOTE: for regular objects (we use defult = 1 to account for the reference held by the LangCache)
        return Py_REFCNT(obj) > PyEXT_REFCOUNT(obj, 1);
    }
    
    bool PyToolkit::hasAnyLangRefs(ObjectPtr obj, unsigned int ext_ref_count) {
        return Py_REFCNT(obj) > ext_ref_count;
    }
    
    PyObject *getValue(PyObject *py_dict, const std::string &key)
    {
        if (!PyDict_Check(py_dict)) {
            THROWF(db0::InputException) << "Invalid type of object. Dictionary expected" << THROWF_END;
        }
        auto result = PyDict_GetItemString(py_dict, key.c_str());
        if (!result) {
            // key not found
            return nullptr;
        }
        return Py_NEW(result);
    }
    
    std::optional<long> PyToolkit::getLong(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        if (!py_value) {
            return std::nullopt;
        }        

        if (!PyLong_Check(*py_value)) {
            THROWF(db0::InputException) << "Invalid type of: " << key << ". Integer expected but got: " 
                << Py_TYPE(*py_value)->tp_name << THROWF_END;
        }
        return PyLong_AsLong(*py_value);
    }

    std::optional<unsigned long long> PyToolkit::getUnsignedLongLong(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        if (!py_value) {
            return std::nullopt;
        }        

        if (!PyLong_Check(*py_value)) {
            THROWF(db0::InputException) << "Invalid type of: " << key << ". Integer expected but got: " 
                << Py_TYPE(*py_value)->tp_name << THROWF_END;
        }
        return PyLong_AsUnsignedLongLong(*py_value);
    }
    
    std::optional<unsigned int> PyToolkit::getUnsignedInt(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        if (!py_value) {
            return std::nullopt;
        }        
        
        if (!PyLong_Check(*py_value)) {
            THROWF(db0::InputException) << "Invalid type of: " << key << ". Integer expected but got: " 
                << Py_TYPE(*py_value)->tp_name << THROWF_END;
        }
        return PyLong_AsUnsignedLong(*py_value);
    }

    std::optional<bool> PyToolkit::getBool(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        if (!py_value) {
            return std::nullopt;
        }        
        if (!PyBool_Check(*py_value)) {
            THROWF(db0::InputException) << "Invalid type of: " << key << ". Boolean expected" << THROWF_END;
        }
        return PyObject_IsTrue(*py_value);
    }
    
    std::optional<std::string> PyToolkit::getString(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        if (!py_value) {
            return std::nullopt;
        }        
        if (!PyUnicode_Check(*py_value)) {
            THROWF(db0::InputException) << "Invalid type of: " << key << ". String expected" << THROWF_END;
        }
        return std::string(PyUnicode_AsUTF8(*py_value));
    }
    
    bool PyToolkit::hasKey(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        return py_value.get() != nullptr;
    }
    
    bool PyToolkit::compare(ObjectPtr py_object1, ObjectPtr py_object2)
    {
        auto result = PyObject_RichCompareBool(py_object1, py_object2, Py_EQ);
        if (result < 0) {
            // comparison failed
            THROWF(db0::InputException) << "Comparison failed" << THROWF_END;
        }
        return result == 1;
    }
    
    bool PyToolkit::isClassObject(ObjectPtr py_object) {
        return PyClassObject_Check(py_object);
    }
    
    SafeRLock PyToolkit::lockApi() {
        return { m_api_mutex };        
    }
    
    SafeRLock PyToolkit::lockPyApi()
    {
        if (m_api_mutex.isOwnedByThisThread()) {            
            // already locked by this thread
            return {};            
        } 

        if (!Py_IsInitialized()) {
            // Simply return the lock after python instance was finalized
            // This is safe because fixture threads should be stopped at this point
            return SafeRLock(m_api_mutex);
        }

        // unlock GIL while waiting for the API mutex
        PyThreadState *__save = PyEval_SaveThread();
        auto result = SafeRLock(m_api_mutex);
        // restore GIL
        PyEval_RestoreThread(__save);
        return result;
    }

    PyToolkit::TypeObjectPtr PyToolkit::getBaseType(TypeObjectPtr py_object) {
        return py_object->tp_base;
    }
    
    PyToolkit::TypeObjectPtr PyToolkit::getBaseMemoType(TypeObjectPtr py_memo_type)
    {
        assert(isAnyMemoType(py_memo_type));
        // first base type is python base. From there we can get the actual base type
        auto base_py_type = getBaseType(py_memo_type);
        if (!base_py_type) {
            return nullptr;
        }
        auto memo_base_type = getBaseType(base_py_type);
        if (memo_base_type && isAnyMemoType(memo_base_type)) {
            return memo_base_type;
        }
        return nullptr;
    }

    bool PyToolkit::isTag(ObjectPtr py_object) {
        return PyTag_Check(py_object);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::makeTuple(const std::vector<ObjectSharedPtr> &values)
    {
        auto result = Py_OWN(PyTuple_New(values.size()));
        for (std::size_t i = 0; i < values.size(); ++i) {
            PySafeTuple_SetItem(*result, i, values[i]);
        }
        return result;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::makeTuple(std::vector<ObjectSharedPtr> &&values)
    {
        auto result = Py_OWN(PyTuple_New(values.size()));
        for (std::size_t i = 0; i < values.size(); ++i) {
            PySafeTuple_SetItem(*result, i, values[i]);
        }
        return result;
    }
    
    PyToolkit::ObjectPtr *PyToolkit::unpackTuple(ObjectPtr py_tuple)
    {
        if (!PyTuple_Check(py_tuple)) {
            THROWF(db0::InputException) << "Invalid type in unpackTuple";
        }
        return reinterpret_cast<PyTupleObject *>(py_tuple)->ob_item;
    }

    bool PyToolkit::isValid() {
        return Py_IsInitialized();
    }
        
    bool PyToolkit::hasTagRefs(ObjectPtr obj_ptr)
    {
        assert(PyAnyMemo_Check(obj_ptr));
        return reinterpret_cast<MemoAnyObject*>(obj_ptr)->ext().hasTagRefs();
    }
    
    std::unique_ptr<GIL_Lock> PyToolkit::ensureLocked()
    {
        if (!Py_IsInitialized()) {
            return {};
        }
        return std::make_unique<GIL_Lock>();
    }
    
    bool PyToolkit::isValid(ObjectPtr py_object) {
        return py_object != nullptr;
    }
    
    template <typename MemoImplT>
    bool decRefMemoImpl(bool is_tag, MemoImplT *memo_obj)
    {
        auto &memo = memo_obj->modifyExt();        
        memo.decRef(is_tag);
        return !memo.hasRefs();
    }

    bool PyToolkit::decRefMemo(bool is_tag, ObjectPtr py_object)
    {
        if (PyMemo_Check<MemoObject>(py_object)) {
            return decRefMemoImpl<MemoObject>(is_tag, reinterpret_cast<MemoObject*>(py_object));
        } else if (PyMemo_Check<MemoImmutableObject>(py_object)) {
            return decRefMemoImpl<MemoImmutableObject>(is_tag, reinterpret_cast<MemoImmutableObject*>(py_object));
        } else {
            assert(false);
            THROWF(db0::InputException) << "Invalid memo object type for decRefMemo" << THROWF_END;
        }
    }

}