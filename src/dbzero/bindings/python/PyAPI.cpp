#include "PyAPI.hpp"
#include "PyInternalAPI.hpp"
#include "PyToolkit.hpp"
#include "PyEnum.hpp"
#include "PyObjectId.hpp"
#include "PyTypeManager.hpp"
#include "PyWorkspace.hpp"
#include "Memo.hpp"
#include "PySnapshot.hpp"
#include "PyInternalAPI.hpp"
#include "PyObjectIterator.hpp"
#include "Memo.hpp"
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/tags/QueryObserver.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/MetaAllocator.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include "Types.hpp"
#include "PyAtomic.hpp"
#include "PyReflectionAPI.hpp"
#include <datetime.h>

namespace db0::python

{

    PyObject *getCacheStats(PyObject *, PyObject *)
    {
        PY_API_FUNC
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        auto &cache_recycler = workspace.getCacheRecycler();
        std::size_t deferred_free_count = 0;
        workspace.forEachFixture([&deferred_free_count](auto &fixture) {
            deferred_free_count += fixture.getMetaAllocator().getDeferredFreeCount();
            return true;
        });
        auto lang_cache_size = workspace.getLangCache()->size();
#ifndef NDEBUG
        auto dram_prefix_size = DRAM_Prefix::getTotalMemoryUsage().first;
#endif        
        
        PyObject* dict = PyDict_New();
        if (dict == NULL) {
            PyErr_SetString(PyExc_MemoryError, "Failed to create a dictionary.");
            return NULL;
        }
        
        PyDict_SetItemString(dict, "size", PyLong_FromLong(cache_recycler.size()));
        PyDict_SetItemString(dict, "capacity", PyLong_FromLong(cache_recycler.getCapacity()));
        PyDict_SetItemString(dict, "deferred_free_count", PyLong_FromLong(deferred_free_count));
        PyDict_SetItemString(dict, "lang_cache_size", PyLong_FromLong(lang_cache_size));
#ifndef NDEBUG
        PyDict_SetItemString(dict, "dram_prefix_size", PyLong_FromLong(dram_prefix_size));
#endif        
        return dict;
    }
    
    PyObject *getLangCacheStats(PyObject *, PyObject *)
    {
        PY_API_FUNC
        auto lang_cache = PyToolkit::getPyWorkspace().getWorkspace().getLangCache();
        
        PyObject* dict = PyDict_New();
        if (dict == NULL) {
            PyErr_SetString(PyExc_MemoryError, "Failed to create a dictionary.");
            return NULL;
        }
        
        PyDict_SetItemString(dict, "size", PyLong_FromLong(lang_cache->size()));
        PyDict_SetItemString(dict, "capacity", PyLong_FromLong(lang_cache->getCapacity()));
        return dict;
    }
    
    PyObject *PyAPI_clearCache(PyObject *, PyObject *)
    {
        PY_API_FUNC
        PyToolkit::getPyWorkspace().getWorkspace().clearCache();
        Py_RETURN_NONE;
    }
    
    shared_py_object<PyObject*> tryFetch(PyObject *const *args, Py_ssize_t nargs) {
        return tryFetchFrom(PyToolkit::getPyWorkspace().getWorkspace(), args, nargs);
    }

    PyObject *fetch(PyObject *, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_API_FUNC
        return runSafe(tryFetch, args, nargs).steal();
    }

    PyObject *tryOpen(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        // prefix_name, open_mode, autocommit (bool)
        static const char *kwlist[] = {"prefix_name", "open_mode", "autocommit", "slab_size", "lock_flags", NULL};
        const char *prefix_name = nullptr;
        const char *open_mode = nullptr;
        PyObject *py_autocommit = nullptr;
        PyObject *py_slab_size = nullptr;
        PyObject *py_lock_flags = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|sOOO", const_cast<char**>(kwlist),
            &prefix_name, &open_mode, &py_autocommit, &py_slab_size, &py_lock_flags))
        {
            PyErr_SetString(PyExc_TypeError, "Invalid arguments");
            return NULL;
        }
        
        int autocommit_interval = 0;
        if (py_autocommit && PyLong_Check(py_autocommit)) {
            autocommit_interval = PyLong_AsLong(py_autocommit);
        }

        std::optional<std::size_t> slab_size;
        if (py_slab_size && !PyLong_Check(py_slab_size)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type: slab_size");
            return NULL;
        }

        if (py_slab_size) {
            slab_size = PyLong_AsUnsignedLong(py_slab_size);
        }
        
        std::optional<bool> autocommit;
        if (autocommit_interval > 0) {
            autocommit = true;
        } else if (py_autocommit) {
            autocommit = PyObject_IsTrue(py_autocommit);
        }
        
        // py_config must be a dict
        if (py_lock_flags && !PyDict_Check(py_lock_flags)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type: lock_flags");
            return NULL;
        }
        auto access_type = open_mode ? parseAccessType(open_mode) : db0::AccessType::READ_WRITE;
        PyToolkit::getPyWorkspace().open(prefix_name, access_type, autocommit, slab_size, py_lock_flags);
        Py_RETURN_NONE;
    }

    PyObject *open(PyObject *self, PyObject *args, PyObject *kwargs) 
    {
        PY_API_FUNC
        return runSafe(tryOpen, self, args, kwargs);
    }
    
    PyObject *tryInit(PyObject *self, PyObject *args, PyObject *kwargs)
    {        
        PyObject *py_path = nullptr;
        PyObject *py_config = nullptr;
        PyObject *py_flags= nullptr;
        // extract optional "path" string argument and "autcommit_interval" keyword argument
        static const char *kwlist[] = {"path", "config", "lock_flags", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOO", const_cast<char**>(kwlist), &py_path, &py_config, &py_flags)) {
            PyErr_SetString(PyExc_TypeError, "Invalid arguments");
            return NULL;
        }

        const char *str_path = "";
        if (py_path && py_path != Py_None) {
            if (!PyUnicode_Check(py_path)) {
                PyErr_SetString(PyExc_TypeError, "Invalid argument type: path");
                return NULL;
            }
            str_path = PyUnicode_AsUTF8(py_path);
        }

        // py_config must be a dict
        if (py_config && !PyDict_Check(py_config)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type: config");
            return NULL;
        }
        
        // py_config must be a dict
        if (py_flags && !PyDict_Check(py_flags)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type: flags");
            return NULL;
        }

        PyToolkit::getPyWorkspace().initWorkspace(str_path, py_config, py_flags);
        Py_RETURN_NONE;
    }
    
    PyObject *init(PyObject *self, PyObject *args, PyObject *kwargs) 
    {
        PY_API_FUNC        
        return runSafe(tryInit, self, args, kwargs);
    }
    
    PyObject *tryDrop(PyObject *self, PyObject *args)
    {
        // extract prefix_name & optional if_exists
        const char *prefix_name = nullptr;
        int if_exists = true;
        if (!PyArg_ParseTuple(args, "s|p", &prefix_name, &if_exists)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        PyToolkit::getPyWorkspace().getWorkspace().drop(prefix_name, if_exists);
        Py_RETURN_NONE;
    }
    
    PyObject *drop(PyObject *self, PyObject *args) 
    {
        PY_API_FUNC
        return runSafe(tryDrop, self, args);
    }
    
    PyObject *tryCommit(PyObject *, PyObject *args)
    {
        // extract optional prefix name
        const char *prefix_name = nullptr;
        if (!PyArg_ParseTuple(args, "|s", &prefix_name)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        if (prefix_name) {
            PyToolkit::getPyWorkspace().getWorkspace().commit(prefix_name);
        } else {
            PyToolkit::getPyWorkspace().getWorkspace().commit();
        }
        Py_RETURN_NONE;
    }
    
    PyObject *commit(PyObject *self, PyObject *args)
    {
        PY_API_FUNC        
        return runSafe(tryCommit, self, args);
    }

    PyObject *tryClose(PyObject *, PyObject *args)
    {
        // extract optional prefix name
        const char *prefix_name = nullptr;
        if (!PyArg_ParseTuple(args, "|s", &prefix_name)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        if (prefix_name) {
            PyToolkit::getPyWorkspace().getWorkspace().close(prefix_name);
        } else {
            PyToolkit::getPyWorkspace().close();
        }
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_close(PyObject *self, PyObject *args)
    {
        PY_API_FUNC        
        return runSafe(tryClose, self, args);
    }
    
    PyObject *getPrefixOf(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        db0::swine_ptr<Fixture> fixture;
        if (PyType_Check(py_object)) {
            if (PyMemoType_Check(reinterpret_cast<PyTypeObject*>(py_object))) {
                PyTypeObject *py_type = reinterpret_cast<PyTypeObject*>(py_object);
                auto &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)py_type + sizeof(PyHeapTypeObject));
                if (decor.m_prefix_name_ptr) {
                    return PyUnicode_FromString(decor.m_prefix_name_ptr);
                }
            }
            return Py_None;            
        } else if (PyObjectIterator_Check(py_object)) {
            auto &iter = reinterpret_cast<PyObjectIterator*>(py_object)->ext();
            fixture = iter->getFixture();
        } else {
            fixture = getFixtureOf(py_object);
        }
                
        if (!fixture) {
            return Py_None;
        }
        
        // name & UUID as tuple
        return Py_BuildValue("sK", fixture->getPrefix().getName().c_str(), fixture->getUUID());        
    }
    
    PyObject *getCurrentPrefix(PyObject *, PyObject *)
    {
        PY_API_FUNC
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        // name & UUID as tuple
        return Py_BuildValue("sK", fixture->getPrefix().getName().c_str(), fixture->getUUID());        
    }
    
    PyObject *tryDel(PyObject *self, PyObject *args)
    {
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto type_id = PyToolkit::getTypeManager().getTypeId(py_object);
        dropInstance(type_id, py_object);      
        Py_RETURN_NONE;
    }
    
    PyObject *del(PyObject *self, PyObject *args) 
    {
        PY_API_FUNC        
        return runSafe(tryDel, self, args);
    }
    
    PyObject *tryRefresh(PyObject *self, PyObject *args)
    {
        PyToolkit::getPyWorkspace().refresh();
        Py_RETURN_NONE;
    }
    
    PyObject *refresh(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        return runSafe(tryRefresh, self, args);
    }

    PyObject *tryGetStateNum(PyObject *args, PyObject *kwargs)
    {
        auto fixture = getPrefixFromArgs(args, kwargs, "prefix");
        fixture->refreshIfUpdated();
        return PyLong_FromLong(fixture->getPrefix().getStateNum());
    }

    PyObject *getStateNum(PyObject *, PyObject *args, PyObject *kwargs) 
    {
        PY_API_FUNC        
        return runSafe(tryGetStateNum, args, kwargs);
    }
    
    PyObject *getPrefixStats(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return runSafe(tryGetPrefixStats, args, kwargs);
    }
        
    PyObject *getSnapshot(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        // requested state number of the default fixture
        std::optional<std::uint64_t> state_num;
        // state numbers by prefix name
        std::unordered_map<std::string, std::uint64_t> prefix_state_nums;
        for (unsigned int i = 0; i < nargs; ++i) {
            // can be either a number of a dict
            if (PyLong_Check(args[i])) {
                if (state_num) {
                    PyErr_SetString(PyExc_TypeError, "Duplicate state_num argument");
                    return NULL;
                }
                state_num = PyLong_AsUnsignedLong(args[i]);                
            } else if (PyDict_Check(args[i])) {
                PyObject *py_dict = args[i];
                PyObject *py_key, *py_value;
                Py_ssize_t pos = 0;
                while (PyDict_Next(py_dict, &pos, &py_key, &py_value)) {
                    if (!PyUnicode_Check(py_key) || !PyLong_Check(py_value)) {
                        PyErr_SetString(PyExc_TypeError, "Invalid argument type");
                        return NULL;
                    }
                    const char *prefix_name = PyUnicode_AsUTF8(py_key);
                    std::uint64_t state_num = PyLong_AsUnsignedLong(py_value);
                    // validate conflicting state numbers
                    auto it = prefix_state_nums.find(prefix_name);
                    if (it != prefix_state_nums.end() && it->second != state_num) {
                        std::stringstream _str;
                        _str << "Conflicting state numbers requested for the same prefix: " << prefix_name;
                        PyErr_SetString(PyExc_TypeError, _str.str().c_str());
                        return NULL;
                    }
                    prefix_state_nums[prefix_name] = state_num;
                }
            }
        }
        
        return runSafe(tryGetSnapshot, state_num, prefix_state_nums);
    }

    PyObject *tryDescribeObject(PyObject *self, PyObject *args)
    {
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (!PyMemo_Check(py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        return MemoObject_DescribeObject(reinterpret_cast<MemoObject*>(py_object));
    }
    
    PyObject *describeObject(PyObject *self, PyObject *args) 
    {
        PY_API_FUNC        
        return runSafe(tryDescribeObject, self, args);
    }
    
    PyObject *tryRenameField(PyObject *args)
    {
        // extract 3 required arguments: class, from name, to name
        PyTypeObject *py_type;        
        const char *from_name = nullptr;
        const char *to_name = nullptr;
        if (!PyArg_ParseTuple(args, "Oss", &py_type, &from_name, &to_name)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        // check if py type
        if (!PyType_Check(py_type)) {
            PyErr_SetString(PyExc_TypeError, "First argument must be a type");
            return nullptr;
        }

        renameField(py_type, from_name, to_name);
        Py_RETURN_NONE;       
    }
    
    PyObject *renameField(PyObject *, PyObject *args) 
    {
        PY_API_FUNC        
        return runSafe(tryRenameField, args);
    }
    
    PyObject *isSingleton(PyObject *, PyObject *args)
    {
        PY_API_FUNC
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (!PyMemo_Check(py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        return (reinterpret_cast<MemoObject*>(py_object)->ext().isSingleton()) ? Py_True : Py_False;
    }

    PyObject *getRefCount(PyObject *, PyObject *args)
    {
        PY_API_FUNC        
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        return runSafe(tryGetRefCount, py_object);
    }
    
    PyObject *getTypeInfo(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (!PyType_Check(py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        PyTypeObject *py_type = reinterpret_cast<PyTypeObject*>(py_object);
        PyObject *py_dict = PyDict_New();
        if (PyMemoType_Check(py_type)) {
            MemoType_get_info(py_type, py_dict);
            return py_dict;
        }

        Py_DECREF(py_dict);
        PyErr_SetString(PyExc_TypeError, "Invalid argument type");
        return NULL;
    }
    
    PyObject *negTags(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
        return NULL;
    }
    
    PyObject *toDict(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "toDict requires exactly 1 argument");
            return NULL;
        }

        if (!PyMemo_Check(args[0])) {
            PyErr_SetString(PyExc_TypeError, "Invalid object type");
            return NULL;
        }
        
        auto &memo_obj = *reinterpret_cast<MemoObject*>(args[0]);
        PyObject *py_result = PyDict_New();
        memo_obj.ext().forAll([py_result](const std::string &key, ObjectSharedPtr py_value) {
            PyDict_SetItemString(py_result, key.c_str(), py_value.steal());
        });
        return py_result;
    }
    
    PyObject *getBuildFlags(PyObject *, PyObject *)
    {
        PY_API_FUNC
        std::stringstream str_flags;
#ifndef NDEBUG
        str_flags << "D";
#endif  
        return PyUnicode_FromString(str_flags.str().c_str());
    }
    
    PyObject *pySerialize(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "serialize requires exactly 1 argument");
            return NULL;
        }
        return runSafe(trySerialize, args[0]);
    }
    
    PyObject *pyDeserialize(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "deserialize requires exactly 1 argument");
            return NULL;
        }        
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        return runSafe(tryDeserialize, &workspace, args[0]);
    }
    
    template <> db0::object_model::StorageClass getStorageClass<MemoObject>() {
        return db0::object_model::StorageClass::OBJECT_REF;
    }

    template <> db0::object_model::StorageClass getStorageClass<ListObject>() {
        return db0::object_model::StorageClass::DB0_LIST;
    }
    
    template <> db0::object_model::StorageClass getStorageClass<IndexObject>() {
        return db0::object_model::StorageClass::DB0_INDEX;
    }
    
    PyObject *makeEnum(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        // extract string and list
        PyObject* py_first_arg = nullptr;
        PyObject *py_enum_values = nullptr;
        PyObject *py_enum_type_id = nullptr;
        PyObject *py_prefix_name = nullptr;
        // pull values / type_id / prefix_name from kwargs
        static const char *kwlist[] = {"input", "values", "type_id", "prefix", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOO", const_cast<char**>(kwlist), &py_first_arg,
            &py_enum_values, &py_enum_type_id, &py_prefix_name))
        {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (!py_enum_values || !PyList_Check(py_enum_values)) {
            PyErr_SetString(PyExc_TypeError, "Invalid enum values");
            return NULL;
        }
        
        // if first argument is a string - use it as enum name
        const char *enum_name = nullptr;
        PyTypeObject *py_type = nullptr;
        if (PyUnicode_Check(py_first_arg)) {
            enum_name = PyUnicode_AsUTF8(py_first_arg);
            if (!enum_name) {
                PyErr_SetString(PyExc_TypeError, "Unable to extract enum name");
                return NULL;
            }
        } else if (PyType_Check(py_first_arg)) {
            // or extract a python type
            py_type = reinterpret_cast<PyTypeObject*>(py_first_arg);
        } else {
            PyErr_SetString(PyExc_TypeError, "Invalid first argument type");
            return NULL;
        }

        std::vector<std::string> enum_values;
        for (Py_ssize_t i = 0; i < PyList_Size(py_enum_values); ++i) {
            PyObject *py_item = PyList_GetItem(py_enum_values, i);
            if (!PyUnicode_Check(py_item)) {
                PyErr_SetString(PyExc_TypeError, "Only string type is allowed for enum values");
                return NULL;
            }
            enum_values.push_back(PyUnicode_AsUTF8(py_item));
        }
        
        const char *type_id = (py_enum_type_id && py_enum_type_id != Py_None ) ? PyUnicode_AsUTF8(py_enum_type_id) : nullptr;
        // check if none
        const char *prefix_name = (py_prefix_name && py_prefix_name != Py_None) ? PyUnicode_AsUTF8(py_prefix_name) : nullptr;
        if (enum_name) {
            return runSafe(tryMakeEnum, self, enum_name, enum_values, type_id, prefix_name);
        } else {
            return runSafe(tryMakeEnumFromType, self, py_type, enum_values, type_id, prefix_name);
        }
    }

    using TagIndex = db0::object_model::TagIndex;
    using ObjectIterator = db0::object_model::ObjectIterator;
    using TypedObjectIterator = db0::object_model::TypedObjectIterator;
    using QueryObserver = db0::object_model::QueryObserver;
    
    std::pair<std::unique_ptr<TagIndex::QueryIterator>, std::vector<std::unique_ptr<QueryObserver> > >
    splitBy(PyObject *py_tag_list, ObjectIterator &iterator)
    {        
        std::vector<std::unique_ptr<QueryObserver> > query_observers;
        auto query = iterator.releaseQuery(query_observers);
        auto &tag_index = iterator.getFixture()->get<db0::object_model::TagIndex>();
        auto result = tag_index.splitBy(py_tag_list, std::move(query));
        query_observers.push_back(std::move(result.second));
        return { std::move(result.first), std::move(query_observers) };
    }
    
    PyObject *trySplitBy(PyObject *args, PyObject *kwargs)
    {
        // extract 2 object arguments
        PyObject *py_tag_list = nullptr;
        PyObject *py_query = nullptr;
        if (!PyArg_ParseTuple(args, "OO", &py_tag_list, &py_query)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        if (!PyObjectIterator_Check(py_query)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        auto &iter = reinterpret_cast<PyObjectIterator*>(py_query)->modifyExt();
        auto split_query = splitBy(py_tag_list, *iter);
        auto py_iter = PyObjectIteratorDefault_new();
        // create decorated iterator (either plain or typed)
        if (iter.isTyped()) {
            auto typed_iter = iter.m_typed_iterator_ptr->makeTypedIter(std::move(split_query.first), 
                std::move(split_query.second), iter->getFilters());
            Iterator::makeNew(&(py_iter.get())->modifyExt(), std::move(typed_iter));
        } else {
            auto _iter = std::make_unique<ObjectIterator>(iter->getFixture(), std::move(split_query.first), 
                iter->getLangType(), std::move(split_query.second), iter->getFilters());
            Iterator::makeNew(&(py_iter.get())->modifyExt(), std::move(_iter));
        }
        return py_iter.steal();
    }
    
    PyObject *splitBy(PyObject *, PyObject *args, PyObject *kwargs) 
    {
        PY_API_FUNC
        return runSafe(trySplitBy, args, kwargs);
    }
    
    PyObject *isEnumValue(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "isEnumValue requires exactly 1 argument");
            return NULL;
        }

        return PyEnumValue_Check(args[0]) ? Py_True : Py_False;
    }

    PyObject *tryFilterBy(PyObject *args, PyObject *kwargs)
    {
        using ObjectIterator = db0::object_model::ObjectIterator;
        using ObjectSharedPtr = PyTypes::ObjectSharedPtr;

        // extract filter callable and query objects
        PyObject *py_filter = nullptr;
        PyObject *py_query = nullptr;
        if (!PyArg_ParseTuple(args, "OO", &py_filter, &py_query)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }

        // py_filter must be a python callable
        if (!PyCallable_Check(py_filter)) {
            THROWF(db0::InputException) << "Invalid filter object";
        }
        
        if (!PyObjectIterator_Check(py_query)) {
            THROWF(db0::InputException) << "Invalid query object";
        }

        std::vector<ObjectIterator::FilterFunc> filters;
        ObjectSharedPtr py_filter_ptr = ObjectSharedPtr(py_filter);
        filters.push_back([py_filter_ptr](PyObject *py_item) {
            PyObject *py_result = PyObject_CallFunctionObjArgs(py_filter_ptr.get(), py_item, NULL);                    
            if (!py_result) {
                THROWF(db0::InputException) << PyToolkit::getLastError();
            }
            return PyObject_IsTrue(py_result);
        });

        auto &iter = reinterpret_cast<PyObjectIterator*>(py_query)->ext();
        auto py_iter = PyObjectIteratorDefault_new();
        // create decorated iterator (either plain or typed)
        if (iter.isTyped()) {
            auto typed_iter = iter.m_typed_iterator_ptr->iterTyped(filters);
            Iterator::makeNew(&(py_iter.get())->modifyExt(), std::move(typed_iter));
        } else {
            auto _iter = iter->iter(filters);
            Iterator::makeNew(&(py_iter.get())->modifyExt(), std::move(_iter));
        }
        return py_iter.steal();
    }
    
    PyObject *filter(PyObject *, PyObject *args, PyObject *kwargs) 
    {
        PY_API_FUNC
        return runSafe(tryFilterBy, args, kwargs);
    }
    
    PyObject *setPrefix(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        // extract object / prefix name (can be None)
        PyObject *py_object = nullptr;
        const char *prefix_name = nullptr;
        static const char *kwlist[] = {"object", "prefix", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|z", const_cast<char**>(kwlist), &py_object, &prefix_name)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (!PyMemo_Check(py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid object type");
            return NULL;
        }
        return runSafe(MemoObject_set_prefix, reinterpret_cast<MemoObject*>(py_object), prefix_name);
    }

    PyObject *getSlabMetrics(PyObject *, PyObject *)
    {
        PY_API_FUNC
        return runSafe(tryGetSlabMetrics, &PyToolkit::getPyWorkspace().getWorkspace());
    }
    
    PyObject *setCacheSize(PyObject *, PyObject *args)
    {
        PY_API_FUNC
        Py_ssize_t cache_size;
        if (!PyArg_ParseTuple(args, "n", &cache_size)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        return runSafe(trySetCacheSize, &PyToolkit::getPyWorkspace().getWorkspace(), cache_size);
    }
    
    PyObject *getPrefixes(PyObject *, PyObject *) 
    {
        PY_API_FUNC
        return runSafe(tryGetPrefixes);
    }
    
    PyObject *getMemoClasses(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        // extract optional prefix_name or prefix_uuid
        const char *prefix_name = nullptr;
        std::uint64_t prefix_uuid = 0;
        static const char *kwlist[] = {"prefix_name", "prefix_uuid", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|sK", const_cast<char**>(kwlist), &prefix_name, &prefix_uuid)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        return runSafe(tryGetMemoClasses, prefix_name, prefix_uuid);
    }
    
    PyObject *getStorageStats(PyObject *, PyObject *args, PyObject *kwargs) 
    {
        PY_API_FUNC
        return runSafe(tryGetStorageStats, args, kwargs);
    }
    
    PyObject *getAttributes(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        PyTypeObject *py_type;
        if (!PyArg_ParseTuple(args, "O", &py_type)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (!PyMemoType_Check(py_type)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        return runSafe(tryGetAttributes, py_type);
    }
    
    PyObject *getAttrAs(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        // memo object, attribute name, type
        if (nargs != 3) {
            PyErr_SetString(PyExc_TypeError, "getattr_as requires exactly 2 arguments");
            return NULL;
        }

        if (!PyMemo_Check(args[0])) {
            // fall back to regular getattr if not a memo object
            return PyObject_GetAttr(args[0], args[1]);
        }
                
        if (!PyType_Check(args[2])) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        PyTypeObject *py_type = reinterpret_cast<PyTypeObject*>(args[2]);
        return runSafe(tryGetAttrAs, reinterpret_cast<MemoObject*>(args[0]), args[1], py_type);
    }
    
    PyObject *PyAPI_getAddress(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "getAddress requires exactly 1 argument");
            return NULL;
        }
        
        return runSafe(tryGetAddress, args[0]);
    }
    
#ifndef NDEBUG
    PyObject *getResourceLockUsage(PyObject *, PyObject *)
    {
        PY_API_FUNC
        std::pair<std::size_t, std::size_t> rl_usage = db0::ResourceLock::getTotalMemoryUsage();        
        return Py_BuildValue("KK", rl_usage.first, rl_usage.second);
    }
#endif
    
#ifndef NDEBUG
    PyObject *getDRAM_IOMap(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        if (hasKWArg(kwargs, "path")) {
            const char *path = nullptr;            
            static const char *kwlist[] = {"path", NULL};
            if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|s", const_cast<char**>(kwlist), &path)) {
                PyErr_SetString(PyExc_TypeError, "Invalid argument type");
                return NULL;
            }
            return runSafe(tryGetDRAM_IOMapFromFile, path);
        }
        
        auto fixture = getPrefixFromArgs(args, kwargs, "prefix");
        return runSafe(tryGetDRAM_IOMap, *fixture);
    } 
#endif
    
}