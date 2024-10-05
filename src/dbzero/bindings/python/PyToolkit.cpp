#include "PyToolkit.hpp"
#include "Memo.hpp"
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/collections/PyIndex.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/mptr.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/pandas/Block.hpp>
#include <dbzero/bindings/python/Pandas/PandasBlock.hpp>
#include <dbzero/bindings/python/collections/PySet.hpp>
#include <dbzero/bindings/python/collections/PyDict.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include "PyObjectId.hpp"
#include "PyObjectIterator.hpp"
#include "PyEnum.hpp"
#include "PyClassFields.hpp"
#include "PyClass.hpp"

namespace db0::python

{
    
    PyToolkit::TypeManager PyToolkit::m_type_manager;
    PyToolkit::PyWorkspace PyToolkit::m_py_workspace;
    
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
        auto py_module_name = PyObject_GetAttrString(reinterpret_cast<ObjectPtr>(py_type), "__module__");
        if (!py_module_name) {
            return std::nullopt;
        }
        auto result = std::string(PyUnicode_AsUTF8(py_module_name));
        Py_DECREF(py_module_name);
        if (result == "__main__") {
            // for Memo types we can determine the actual module name from the file name
            // (if stored with the type decoration)
            if (PyMemoType_Check(py_type)) {
                // file name may not be available in the type decoration
                auto file_name = MemoTypeDecoration::get(py_type).m_file_name;
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

    PyToolkit::ObjectSharedPtr tryUnloadObjectFromCache(LangCacheView &lang_cache, std::uint64_t address,
        std::shared_ptr<db0::object_model::Class> expected_type = nullptr)
    {
        auto obj_ptr = lang_cache.get(address);
        if (obj_ptr) {
            if (!PyMemo_Check(obj_ptr.get())) {
                THROWF(db0::InputException) << "Invalid object type: " << PyToolkit::getTypeName(obj_ptr.get()) << " (Memo expected)";
            }
            auto &memo = reinterpret_cast<MemoObject*>(obj_ptr.get())->ext();
            // validate type
            if (expected_type && memo.getType() != *expected_type) {
                THROWF(db0::InputException) << "Memo type mismatch";
            }
        }
        return obj_ptr;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadObject(db0::swine_ptr<Fixture> &fixture, std::uint64_t address,
        const ClassFactory &class_factory)
    {
        // try unloading from cache first
        auto &lang_cache = fixture->getLangCache();        
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, address);
        
        if (obj_ptr) {
            return obj_ptr;
        }

        // Unload from backend otherwise
        auto stem = db0::object_model::Object::unloadStem(fixture, address);
        auto type = db0::object_model::getCachedClass(stem->m_class_ref, class_factory);
        
        // construct Python's memo object (placeholder for actual DBZero instance)
        // the associated lang class must be available
        auto memo_object = MemoObjectStub_new(type->getLangClass().get());
        // unload from stem
        db0::object_model::Object::unload(&(memo_object.get())->modifyExt(), std::move(stem), type);
        obj_ptr = shared_py_cast<PyObject*>(std::move(memo_object));
        lang_cache.add(address, obj_ptr.get());
        return obj_ptr;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadObject(db0::swine_ptr<Fixture> &fixture, std::uint64_t address,
        std::shared_ptr<Class> type, TypeObjectPtr lang_class)
    {
        // try unloading from cache first
        auto &lang_cache = fixture->getLangCache();
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, address);
        
        if (obj_ptr) {
            return obj_ptr;
        }

        // NOTE: lang_class may be of a base type (e.g. MemoBase)    
        auto memo_object = MemoObjectStub_new(lang_class ? lang_class : type->getLangClass().get());
        db0::object_model::Object::unload(&(memo_object.get())->modifyExt(), address, type);
        obj_ptr = shared_py_cast<PyObject*>(std::move(memo_object));
        lang_cache.add(address, obj_ptr.get());
        return obj_ptr;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadBlock(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }

        auto block_object = BlockDefaultObject_new();
        // retrieve actual DBZero instance        
        db0::object_model::pandas::Block::unload(&(block_object.get())->modifyExt(), fixture, address);
        lang_cache.add(address, block_object.get());
        return shared_py_cast<PyObject*>(std::move(block_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadList(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }
        
        auto list_object = ListDefaultObject_new();
        // retrieve actual DBZero instance
        db0::object_model::List::unload(&(list_object.get())->modifyExt(), fixture, address);

        // add list object to cache
        lang_cache.add(address, list_object.get());
        return shared_py_cast<PyObject*>(std::move(list_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadIndex(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {        
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }
        
        auto index_object = IndexDefaultObject_new();
        // retrieve actual DBZero instance
        db0::object_model::Index::unload(&(index_object.get())->modifyExt(), fixture, address);

        // add list object to cache
        lang_cache.add(address, index_object.get());
        return shared_py_cast<PyObject*>(std::move(index_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadSet(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }
        
        auto set_object = SetDefaultObject_new();
        // retrieve actual DBZero instance
        db0::object_model::Set::unload(&(set_object.get())->modifyExt(), fixture, address);
        
        // add list object to cache
        lang_cache.add(address, set_object.get());
        return shared_py_cast<PyObject*>(std::move(set_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadDict(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }
        
        auto dict_object = DictDefaultObject_new();
        // retrieve actual DBZero instance        
        db0::object_model::Dict::unload(&(dict_object.get())->modifyExt(), fixture, address);
    
        // add list object to cache
        lang_cache.add(address, dict_object.get());
        return shared_py_cast<PyObject*>(std::move(dict_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadTuple(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }
        
        auto tuple_object = TupleDefaultObject_new();
        // retrieve actual DBZero instance        
        db0::object_model::Tuple::unload(&(tuple_object.get())->modifyExt(), fixture, address);

        // add list object to cache
        lang_cache.add(address, tuple_object.get());
        return shared_py_cast<PyObject*>(std::move(tuple_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadObjectIterator(db0::swine_ptr<Fixture> &fixture,
        std::vector<std::byte>::const_iterator &iter, 
        std::vector<std::byte>::const_iterator end)
    {
        auto obj_iter = db0::object_model::ObjectIterator::deserialize(fixture, iter, end);
        auto py_iter = PyObjectIteratorDefault_new();
        Iterator::makeNew(&(py_iter.get())->modifyExt(), std::move(obj_iter));
        return shared_py_cast<PyObject*>(std::move(py_iter));
    }
    
    std::uint64_t PyToolkit::getTag(ObjectPtr py_object, db0::pools::RC_LimitedStringPool &string_pool)
    {
        if (!PyUnicode_Check(py_object)) {
            // unable to resolve as tag
            THROWF(db0::InputException) << "Unable to resolve object as tag";
        }
                
        return string_pool.toAddress(string_pool.get(PyUnicode_AsUTF8(py_object)));
    }
    
    std::uint64_t PyToolkit::addTag(ObjectPtr py_object, db0::pools::RC_LimitedStringPool &string_pool, bool &inc_ref)
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

    PyToolkit::ObjectSharedPtr PyToolkit::getIterator(ObjectPtr py_object)
    {
        auto py_iterator = PyObject_GetIter(py_object);
        if (!py_iterator) {
            THROWF(db0::InputException) << "Unable to get iterator for object" << THROWF_END;
        }
        return { py_iterator, false };
    }

    PyToolkit::ObjectSharedPtr PyToolkit::next(ObjectPtr py_object)
    {
        auto py_next = PyIter_Next(py_object);
        if (!py_next) {
            // StopIteration exception raised
            PyErr_Clear();
        }

        return { py_next, false };
    }
    
    bool PyToolkit::isSingleton(TypeObjectPtr py_type) {
        return PyMemoType_IsSingleton(py_type);
    }
    
    bool PyToolkit::isType(ObjectPtr py_object) {
        return PyType_Check(py_object);
    }

    bool PyToolkit::isMemoObject(ObjectPtr py_object) {
        return PyMemo_Check(py_object);
    }

    PyToolkit::ObjectPtr PyToolkit::getUUID(ObjectPtr py_object) {
        return db0::python::getUUIDInternal(nullptr, &py_object, 1);
    }
    
    bool PyToolkit::isEnumValue(ObjectPtr py_object) {
        return PyEnumValue_Check(py_object);
    }

    bool PyToolkit::isFieldDef(ObjectPtr py_object) {
        return PyFieldDef_Check(py_object);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadEnumValue(const EnumValue &value) {
        return shared_py_cast<PyObject*>(makePyEnumValue(value));
    }

    std::string PyToolkit::getLastError() 
    {
        PyObject *ptype, *pvalue, *ptraceback;
        PyErr_Fetch(&ptype, &pvalue, &ptraceback);
        PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
        PyObject *pstr = PyObject_Str(pvalue);
        std::string result = PyUnicode_AsUTF8(pstr);
        Py_DECREF(pstr);
        Py_XDECREF(ptype);
        Py_XDECREF(pvalue);
        Py_XDECREF(ptraceback);
        return result;
    }
    
    std::uint64_t PyToolkit::getFixtureUUID(ObjectPtr py_object)
    {
        if (PyType_Check(py_object)) {
            return getFixtureUUID(reinterpret_cast<TypeObjectPtr>(py_object));
        } else if (PyEnumValue_Check(py_object)) {
            return reinterpret_cast<PyEnumValue*>(py_object)->ext().m_fixture_uuid;
        } else if (PyMemo_Check(py_object)) {
            return reinterpret_cast<MemoObject*>(py_object)->ext().getFixture()->getUUID();
        } else {
            return 0;
        }
    }
    
    std::uint64_t PyToolkit::getFixtureUUID(TypeObjectPtr py_type)
    {
        if (isMemoType(py_type)) {
            auto &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)py_type + sizeof(PyHeapTypeObject));
            return decor.getFixtureUUID();
        } else {
            return 0;
        }
    }

    const char *PyToolkit::getPrefixName(TypeObjectPtr memo_type)
    {
        assert(isMemoType(memo_type));
        auto &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)memo_type + sizeof(PyHeapTypeObject));
        return decor.m_prefix_name_ptr;
    }
    
    const char *PyToolkit::getMemoTypeID(TypeObjectPtr memo_type)
    {
        assert(isMemoType(memo_type));
        auto &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)memo_type + sizeof(PyHeapTypeObject));
        return decor.m_type_id;
    }

    bool PyToolkit::isMemoType(TypeObjectPtr py_type) {
        return PyMemoType_Check(py_type);
    }

    void PyToolkit::setError(ObjectPtr err_obj, std::uint64_t err_value) {
        PyErr_SetObject(err_obj, PyLong_FromUnsignedLongLong(err_value));
    }

    unsigned int PyToolkit::getRefCount(ObjectPtr obj) {
        return Py_REFCNT(obj);
    }

    PyObject *getValue(PyObject *py_dict, const std::string &key)
    {
        if (!PyDict_Check(py_dict)) {
            THROWF(db0::InputException) << "Invalid type of object. Dictionary expected" << THROWF_END;
        }        
        return PyDict_GetItemString(py_dict, key.c_str());
    }

    std::optional<long> PyToolkit::getLong(ObjectPtr py_object, const std::string &key) 
    {
        auto py_value = getValue(py_object, key);
        if (!py_value) {
            return std::nullopt;
        }
        if (!PyLong_Check(py_value)) {
            Py_DECREF(py_value);
            THROWF(db0::InputException) << "Invalid type of: " << key << ". Integer expected" << THROWF_END;
        }
        Py_DECREF(py_value);
        return PyLong_AsLong(py_value);
    }

    std::optional<bool> PyToolkit::getBool(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = getValue(py_object, key);
        if (!py_value) {
            return std::nullopt;
        }
        if (!PyBool_Check(py_value)) {
            Py_DECREF(py_value);
            THROWF(db0::InputException) << "Invalid type of: " << key << ". Boolean expected" << THROWF_END;
        }
        Py_DECREF(py_value);
        return PyObject_IsTrue(py_value);
    }

    std::optional<std::string> PyToolkit::getString(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = getValue(py_object, key);
        if (!py_value) {
            return std::nullopt;
        }
        if (!PyUnicode_Check(py_value)) {
            Py_DECREF(py_value);
            THROWF(db0::InputException) << "Invalid type of: " << key << ". String expected" << THROWF_END;
        }
        auto result = std::string(PyUnicode_AsUTF8(py_value));
        Py_DECREF(py_value);
        return result;
    }
    
    bool PyToolkit::compare(ObjectPtr py_object1, ObjectPtr py_object2) {
        return PyObject_RichCompareBool(py_object1, py_object2, Py_EQ);
    }
    
    bool PyToolkit::isClassObject(ObjectPtr py_object) {
        return PyClassObject_Check(py_object);
    }

}