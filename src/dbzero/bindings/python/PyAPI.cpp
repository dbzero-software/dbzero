// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyAPI.hpp"
#include "PyInternalAPI.hpp"
#include "PyTagsAPI.hpp"
#include "PyToolkit.hpp"
#include "PyTypeManager.hpp"
#include "PyWorkspace.hpp"
#include "Memo.hpp"
#include "PySnapshot.hpp"
#include "PyInternalAPI.hpp"
#include "Memo.hpp"
#include "Types.hpp"
#include "PyAtomic.hpp"
#include "PyReflectionAPI.hpp"
#include "PyHash.hpp"
#include "PyWeakProxy.hpp"
#include <dbzero/bindings/python/iter/PyObjectIterable.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterator.hpp>
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/PyDict.hpp>
#include <dbzero/bindings/python/collections/PySet.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/bindings/python/types/PyObjectId.hpp>
#include <dbzero/bindings/python/types/PyClass.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/tags/QueryObserver.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/workspace/Config.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/MetaAllocator.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/threading/SafeRMutex.hpp>

namespace db0::python

{

    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;

    PyObject *tryGetCacheStats()
    {
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
        
        auto dict = Py_OWN(PyDict_New());
        if (!dict) {
            return nullptr;
        }
        
        PySafeDict_SetItemString(*dict, "size", Py_OWN(PyLong_FromLong(cache_recycler.size())));
        
        {
            std::vector<std::size_t> detailed_size = cache_recycler.getDetailedSize();
            auto detailed_size_dict = Py_OWN(PyDict_New());
            unsigned int priority_index = 0;
            for (auto size: detailed_size) {
                std::stringstream key_str;
                key_str << "P" << priority_index++;
                PySafeDict_SetItemString(*detailed_size_dict, key_str.str().c_str(), Py_OWN(PyLong_FromLong(size)));
            }
            // cache size with a by-priority breakdown
            PySafeDict_SetItemString(*dict, "P_size", detailed_size_dict);
        }

        PySafeDict_SetItemString(*dict, "capacity", Py_OWN(PyLong_FromLong(cache_recycler.getCapacity())));
        PySafeDict_SetItemString(*dict, "deferred_free_count", Py_OWN(PyLong_FromLong(deferred_free_count)));
        PySafeDict_SetItemString(*dict, "lang_cache_size", Py_OWN(PyLong_FromLong(lang_cache_size)));
#ifndef NDEBUG
        PySafeDict_SetItemString(*dict, "dram_prefix_size", Py_OWN(PyLong_FromLong(dram_prefix_size)));
#endif        
        return dict.steal();
    }

    PyObject *getCacheStats(PyObject *, PyObject *)
    {
        PY_API_FUNC
        return runSafe(tryGetCacheStats);
    }
    
    PyObject *tryGetLangCacheStats()
    {
        auto lang_cache = PyToolkit::getPyWorkspace().getWorkspace().getLangCache();
        
        auto dict = Py_OWN(PyDict_New());
        if (!dict) {        
            return nullptr;
        }
        
        PySafeDict_SetItemString(*dict, "size", Py_OWN(PyLong_FromLong(lang_cache->size())));
        PySafeDict_SetItemString(*dict, "capacity", Py_OWN(PyLong_FromLong(lang_cache->getCapacity())));
        return dict.steal();
    }

    PyObject *getLangCacheStats(PyObject *, PyObject *){
        PY_API_FUNC
        return runSafe(tryGetLangCacheStats);
    }
    
    PyObject *TryPyAPI_clearCache()
    {
        PyToolkit::getPyWorkspace().getWorkspace().clearCache();
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_clearCache(PyObject *, PyObject *){
        PY_API_FUNC
        return runSafe(TryPyAPI_clearCache);
    }

    shared_py_object<PyObject*> tryFetch(PyObject *py_id, PyTypeObject *type, const char *prefix_name) {
        return tryFetchFrom(PyToolkit::getPyWorkspace().getWorkspace(), py_id, type, prefix_name);
    }
    
    PyObject* tryExists(PyObject *py_id, PyTypeObject *type, const char *prefix_name) {
        return PyBool_fromBool(tryExistsIn(PyToolkit::getPyWorkspace().getWorkspace(), py_id, type, prefix_name));
    }
        
    PyObject *PyAPI_fetch(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PyObject *py_id = nullptr;
        PyObject *py_type = nullptr;
        const char *prefix_name = nullptr;
        if (!tryParseFetchArgs(args, kwargs, py_id, py_type, prefix_name)) {
            // error already set in tryParseFetchArgs
            return NULL;
        }

        PY_API_FUNC
        return runSafe(tryFetch, py_id, reinterpret_cast<PyTypeObject*>(py_type), prefix_name).steal();
    }
    
    PyObject *PyAPI_exists(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PyObject *py_id = nullptr;
        PyObject *py_type = nullptr;
        const char *prefix_name = nullptr;
        // takes same arguments as fetch
        if (!tryParseFetchArgs(args, kwargs, py_id, py_type, prefix_name)) {
            // error already set in tryParseFetchArgs
            return NULL;
        }
        
        PY_API_FUNC
        return runSafe(tryExists, py_id, reinterpret_cast<PyTypeObject*>(py_type), prefix_name);
    }

    PyObject *tryOpen(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        // prefix_name, open_mode, autocommit (bool)
        static const char *kwlist[] = {
            "prefix_name", "open_mode", "autocommit", "slab_size", "lock_flags", "meta_io_step_size", 
            "page_io_step_size", NULL
        };
        const char *prefix_name = nullptr;
        const char *open_mode = nullptr;
        PyObject *py_autocommit = nullptr;
        PyObject *py_slab_size = nullptr;
        PyObject *py_lock_flags = nullptr;
        PyObject *py_meta_io_step_size = nullptr;
        PyObject *py_page_io_step_size = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|sOOOOO:open", const_cast<char**>(kwlist),
            &prefix_name, &open_mode, &py_autocommit, &py_slab_size, &py_lock_flags, &py_meta_io_step_size, &py_page_io_step_size))
        {
            return NULL;
        }
        
        int autocommit_interval = 0;
        if (py_autocommit && PyLong_Check(py_autocommit)) {
            autocommit_interval = PyLong_AsLong(py_autocommit);
        }
        
        std::optional<std::size_t> slab_size;
        if (py_slab_size && py_slab_size != Py_None) {
            if (!PyLong_Check(py_slab_size)) {
                PyErr_SetString(PyExc_TypeError, "Invalid argument type: slab_size");
                return NULL;
            }            
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

        std::optional<std::size_t> meta_io_step_size;
        std::optional<std::size_t> page_io_step_size;
        if (py_meta_io_step_size && py_meta_io_step_size != Py_None) {
            if (!PyLong_Check(py_meta_io_step_size))    {
                PyErr_SetString(PyExc_TypeError, "Invalid argument type: meta_io_step_size");
                return NULL;
            }
            meta_io_step_size = PyLong_AsUnsignedLong(py_meta_io_step_size);
        }

        // check for None (default)
        if (py_page_io_step_size && py_page_io_step_size != Py_None) {
            if (!PyLong_Check(py_page_io_step_size)) {
                PyErr_SetString(PyExc_TypeError, "Invalid argument type: page_io_step_size");
                return NULL;
            }
            page_io_step_size = PyLong_AsUnsignedLong(py_page_io_step_size);
        }

        auto access_type = open_mode ? parseAccessType(open_mode) : db0::AccessType::READ_WRITE;
        PyToolkit::getPyWorkspace().open(
            prefix_name, access_type, autocommit, slab_size, py_lock_flags, meta_io_step_size, page_io_step_size
        );
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_open(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return runSafe(tryOpen, self, args, kwargs);
    }
    
    PyObject *tryInit(PyObject *self, PyObject *args, PyObject *kwargs)
    {        
        PyObject *py_path = nullptr;
        PyObject *py_config = nullptr;
        PyObject *py_flags = nullptr;        
        // extract optional "path" string argument and "autcommit_interval" keyword argument
        static const char *kwlist[] = {"path", "config", "lock_flags", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOO:init", const_cast<char**>(kwlist), &py_path, &py_config, &py_flags)) {
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
        
        // py_flags must be a dict
        if (py_flags && !PyDict_Check(py_flags)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type: flags");
            return NULL;
        }

        auto config_obj = Py_OWN(PyDict_New());
        if (!config_obj) {
            return nullptr;
        }
        
        using DefaultValueFunction = PyObject*(*)();
        const std::pair<const char*, const DefaultValueFunction> defaults[] = {
            {"cache_size", []{ return PyLong_FromUnsignedLongLong(BaseWorkspace::DEFAULT_CACHE_SIZE); }},
            {"lang_cache_size", []{ return PyLong_FromUnsignedLongLong(LangCache::DEFAULT_CAPACITY); }},
            {"autocommit", []{ Py_RETURN_TRUE; }},
            {"autocommit_interval", []{ return PyLong_FromUnsignedLongLong(Workspace::DEFAULT_AUTOCOMMIT_INTERVAL_MS); }},
        };
        for (const auto &[key_str, default_fn] : defaults) {
            // Populate default values so then can be easily accessed with get_config
            auto key = Py_OWN(PyUnicode_FromString(key_str));
            if (!key) {
                return nullptr;
            }

            int contains = 0;
            if(py_config) {
                contains = PyDict_Contains(py_config, *key);
                if (contains == -1) {
                    return nullptr;
                }
            }
            PyTypes::ObjectSharedPtr config_value;
            if (contains) {
                config_value = Py_BORROW(PyDict_GetItemWithError(py_config, *key));
            }
            else {
                config_value = Py_OWN(default_fn());
            }
            if(!config_value) {
                return nullptr;
            }

            if(PyDict_SetItem(*config_obj, *key, *config_value)) {
                return nullptr;
            }
        }
        
        PyToolkit::getPyWorkspace().initWorkspace(str_path, *config_obj, py_flags);
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_init(PyObject *self, PyObject *args, PyObject *kwargs)
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
    
    PyObject *PyAPI_drop(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        return runSafe(tryDrop, self, args);
    }
    
    PyObject *tryCommit(PyObject *, PyObject *args)
    {
        // extract optional prefix name
        const char *prefix_name = nullptr;
        if (!PyArg_ParseTuple(args, "|s:commit", &prefix_name)) {
            return NULL;
        }
        
        if (prefix_name) {
            PyToolkit::getPyWorkspace().getWorkspace().commit(prefix_name);
        } else {
            PyToolkit::getPyWorkspace().getWorkspace().commit();
        }
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_commit(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        return runSafe(tryCommit, self, args);
    }
    
    PyObject *tryStopWorkspaceThreads()
    {
        // NOTE: must unlock GIL or otherwise might cause deadlock
        PyThreadState *save_state = PyEval_SaveThread();
        try {
            // stop workspace threads
            PyToolkit::getPyWorkspace().stopThreads();                        
        } catch (...) {
            PyEval_RestoreThread(save_state);
            throw;
        }
        
        PyEval_RestoreThread(save_state);
        Py_RETURN_NONE;
    }
    
    PyObject *tryClose(const char *prefix_name)
    {
        if (prefix_name) {
            PyToolkit::getPyWorkspace().getWorkspace().close(db0::PrefixName(prefix_name));
        } else {
            PyToolkit::getPyWorkspace().close();
        }
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_close(PyObject *self, PyObject *args)
    {
        // extract optional prefix name
        const char *prefix_name = nullptr;
        if (!PyArg_ParseTuple(args, "|s:close", &prefix_name)) {
            return NULL;
        }
        
        if (!prefix_name) {
            // note we need to stop workspace threads before API lock
            // otherwise it may cause deadlock
            runSafe(tryStopWorkspaceThreads);
        }
        
        PY_API_FUNC
        return runSafe(tryClose, prefix_name);        
    }
    
    PyObject *tryGetPrefixOf(PyObject *self, PyObject *args)
    {
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        db0::swine_ptr<Fixture> fixture;
        
        if (PyType_Check(py_object)) {
            // only memo or enum types can be scoped
            if (PyAnyMemoType_Check(reinterpret_cast<PyTypeObject*>(py_object))) {
                PyTypeObject *py_type = reinterpret_cast<PyTypeObject*>(py_object);
                auto prefix_name = MemoTypeDecoration::get(py_type).tryGetPrefixName();
                if (prefix_name) {
                    // try locating an existing prefix to obtain its UUID                    
                    auto fixture = PyToolkit::getPyWorkspace().getWorkspace().tryGetFixture(prefix_name);
                    // name & UUID as tuple
                    // note that UUID may be 0 if prefix was not found
                    return Py_BuildValue("sK", prefix_name, fixture ? fixture->getUUID() : 0);
                }
            }
            Py_RETURN_NONE;
        } else if (PyObjectIterable_Check(py_object)) {
            fixture = reinterpret_cast<PyObjectIterable*>(py_object)->ext().getFixture();            
        } else if (PyObjectIterator_Check(py_object)) {
            fixture = reinterpret_cast<PyObjectIterator*>(py_object)->ext().getFixture();
        } else if (PyEnum_Check(py_object)) {
            auto &enum_ = reinterpret_cast<PyEnum*>(py_object)->ext();
            auto &enum_type_def = *enum_.m_enum_type_def;
            if (enum_type_def.hasPrefix()) {
                // try retrieving an already opened fixture / prefix
                auto prefix_name = enum_type_def.getPrefixName();
                // fixture may not exist of be accessible, in which case UUID will be 0
                auto fixture = PyToolkit::getPyWorkspace().getWorkspace().tryGetFixture(prefix_name.c_str());
                return Py_BuildValue("sK", prefix_name.c_str(), fixture ? fixture->getUUID() : 0);
            }
            Py_RETURN_NONE;
        } else if (PyObjectIterable_Check(py_object)) {
            fixture = reinterpret_cast<PyObjectIterable*>(py_object)->ext().getFixture();            
        } else if (PyObjectIterator_Check(py_object)) {
            fixture = reinterpret_cast<PyObjectIterator*>(py_object)->ext().getFixture();
        } else if (PyClassObject_Check(py_object)) {
            fixture = reinterpret_cast<ClassObject*>(py_object)->ext().getFixture();
        } else {
            fixture = getFixtureOf(py_object);
        }            
        
        if (!fixture) {
            // there's no prefix associated with the object
            Py_RETURN_NONE;
        }
        
        // name & UUID as tuple
        return Py_BuildValue("sK", fixture->getPrefix().getName().c_str(), fixture->getUUID());        
    }

    PyObject *PyAPI_getPrefixOf(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        return runSafe(tryGetPrefixOf, self, args);
    }
    
    PyObject *tryGetCurrentPrefix(PyObject *, PyObject *)
    {
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        // name & UUID as tuple
        return Py_BuildValue("sK", fixture->getPrefix().getName().c_str(), fixture->getUUID());        
    }
    
    PyObject *PyAPI_getCurrentPrefix(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        return runSafe(tryGetCurrentPrefix, self, args);
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
    
    PyObject *PyAPI_del(PyObject *self, PyObject *args) 
    {
        PY_API_FUNC        
        return runSafe(tryDel, self, args);
    }
    
    PyObject *tryRefresh(PyObject *self, PyObject *args)
    {
        if (PyToolkit::getPyWorkspace().refresh()) {
            Py_RETURN_TRUE;
        }
        Py_RETURN_FALSE;
    }
    
    PyObject *refresh(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        return runSafe(tryRefresh, self, args);
    }
    
    PyObject *tryGetStateNum(PyObject *args, PyObject *kwargs)
    {
        const char *prefix_name = nullptr;
        int  finalized = 0;
        const char * const kwlist[] = {"prefix", "finalized", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|sp:get_state_num", const_cast<char**>(kwlist), &prefix_name, &finalized)) {
            return nullptr;
        }

        auto fixture = getOptionalPrefixFromArg(PyToolkit::getPyWorkspace().getWorkspace(), prefix_name);
        fixture->refreshIfUpdated();
        return PyLong_FromLong(fixture->getPrefix().getStateNum(finalized == 1));
    }
    
    PyObject *PyAPI_getStateNum(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC        
        return runSafe(tryGetStateNum, args, kwargs);
    }
    
    PyObject *getPrefixStats(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return runSafe(tryGetPrefixStats, args, kwargs);
    }
    
    PyObject *PyAPI_getSnapshot(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return runSafe(tryPyGetSnapshot, args, kwargs);
    }
    
    PyObject *tryDescribeObject(PyObject *self, PyObject *args)
    {
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        if (PyMemo_Check<MemoObject>(py_object)) {
            return MemoObject_DescribeObject(reinterpret_cast<MemoObject*>(py_object));
        } else if (PyMemo_Check<MemoImmutableObject>(py_object)) {
            return MemoObject_DescribeObject(reinterpret_cast<MemoImmutableObject*>(py_object));
        } else {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }        
    }
    
    PyObject *describeObject(PyObject *self, PyObject *args) 
    {
        PY_API_FUNC        
        return runSafe(tryDescribeObject, self, args);
    }
    
    PyObject *tryRenameField(PyObject *args, PyObject *kwargs)
    {
        // extract 3 required arguments: class, from name, to name
        PyTypeObject *py_type;        
        const char *from_name = nullptr;
        const char *to_name = nullptr;

        const char * const kwlist[] = {"class_obj", "from_name", "to_name", nullptr};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oss:rename_field", const_cast<char**>(kwlist), &py_type, &from_name, &to_name)) {
            return nullptr;
        }

        // check if py type
        if (!PyType_Check(py_type)) {
            PyErr_SetString(PyExc_TypeError, "First argument must be a type");
            return nullptr;
        }

        renameMemoClassField(py_type, from_name, to_name);
        Py_RETURN_NONE;       
    }
    
    PyObject *renameField(PyObject *, PyObject *args, PyObject *kwargs) 
    {
        PY_API_FUNC        
        return runSafe(tryRenameField, args, kwargs);
    }

    PyObject *TryPyAPI_isSingleton(PyObject *py_object)
    {
        assert((PyMemo_Check<MemoObject>(py_object)));
        return PyBool_fromBool(reinterpret_cast<MemoObject*>(py_object)->ext().isSingleton());
    }

    PyObject *PyAPI_isSingleton(PyObject *, PyObject *args){
        PY_API_FUNC
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (PyMemo_Check<MemoObject>(py_object)) {
            return runSafe(TryPyAPI_isSingleton, py_object);
        } else if (PyMemo_Check<MemoImmutableObject>(py_object)) {
            // immutable memos are never singletons
            Py_RETURN_FALSE;
        } else {        
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }        
    }
    
    PyObject *PyAPI_getRefCount(PyObject *, PyObject *args)
    {
        PY_API_FUNC        
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        return runSafe(tryGetRefCount, py_object);
    }
        
    PyObject *PyAPI_getTypeInfo(PyObject *self, PyObject *args)
    {
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        PY_API_FUNC
        return runSafe(tryGetMemoTypeInfo, py_object);
    }
    
    PyObject *negTags(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
        return NULL;
    }
        
    PyObject *PyAPI_getBuildFlags(PyObject *, PyObject *)
    {
        PY_API_FUNC
        std::stringstream str_flags;
#ifndef NDEBUG
        str_flags << "D";
#endif  
        return PyUnicode_FromString(str_flags.str().c_str());
    }
        
    template <> db0::object_model::StorageClass getStorageClass<MemoObject>() {
        return db0::object_model::StorageClass::OBJECT_REF;
    }
    
    template <> db0::object_model::StorageClass getStorageClass<ListObject>() {
        return db0::object_model::StorageClass::DB0_LIST;
    }

    template <> db0::object_model::StorageClass getStorageClass<DictObject>() {
        return db0::object_model::StorageClass::DB0_DICT;
    }

    template <> db0::object_model::StorageClass getStorageClass<SetObject>() {
        return db0::object_model::StorageClass::DB0_SET;
    }

    template <> db0::object_model::StorageClass getStorageClass<TupleObject>() {
        return db0::object_model::StorageClass::DB0_TUPLE;
    }

    template <> db0::object_model::StorageClass getStorageClass<IndexObject>() {
        return db0::object_model::StorageClass::DB0_INDEX;
    }
    
    PyObject *PyAPI_makeEnum(PyObject *self, PyObject *args, PyObject *kwargs)
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
    using ObjectIterable = db0::object_model::ObjectIterable;
    using ObjectIterator = db0::object_model::ObjectIterator;
    using QueryObserver = db0::object_model::QueryObserver;
    
    PyObject *PyAPI_isEnumValue(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "isEnumValue requires exactly 1 argument");
            return NULL;
        }
        
        // NOTE: to Python programs EnumValue / EnuValueRepr should not be differentiable
        return PyBool_fromBool(PyEnumValue_Check(args[0]) || PyEnumValueRepr_Check(args[0]));
    }
    
    PyObject *tryFilterBy(PyObject *args, PyObject *kwargs)
    {
        using ObjectIterator = db0::object_model::ObjectIterator;
        using ObjectSharedPtr = PyTypes::ObjectSharedPtr;

        // extract filter callable and query objects
        PyObject *py_filter = nullptr;
        PyObject *py_query = nullptr;

        const char * const kwlist[] = {"prefix", "finalized", nullptr};
        if(!PyArg_ParseTupleAndKeywords(args, kwargs, "OO:filter", const_cast<char**>(kwlist), &py_filter, &py_query)) {
            return nullptr;
        }

        // py_filter must be a python callable
        if (!PyCallable_Check(py_filter)) {
            THROWF(db0::InputException) << "Invalid filter object";
        }
        
        if (!PyObjectIterable_Check(py_query)) {
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
        
        auto &iter = reinterpret_cast<PyObjectIterable*>(py_query)->modifyExt();
        auto py_iter = PyObjectIterableDefault_new();
        // create with added filters
        py_iter->makeNew(iter, filters);
        return py_iter.steal();
    }
    
    PyObject *filter(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return runSafe(tryFilterBy, args, kwargs);
    }
    
    PyObject *PyAPI_setPrefix(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        
        // extract object / prefix name (can be None)
        PyObject *py_object = nullptr;
        const char *prefix_name = nullptr;
        static const char *kwlist[] = {"object", "prefix", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|z", const_cast<char**>(kwlist), &py_object, &prefix_name)) {            
            return NULL;
        }
        
        // in case of the "None" target just return prefix name
        if (!py_object || py_object == Py_None) {            
            if (prefix_name) {
                return PyUnicode_FromString(prefix_name);
            }
            Py_RETURN_NONE;
        }
        
        if (PyAnyMemo_Check(py_object)) {
            // this operation is handled differently for singletons (as dyn_prefix)
            // check from type, not instance
            if (PyMemoType_IsSingleton(Py_TYPE(py_object))) {
                if (reinterpret_cast<MemoAnyObject*>(py_object)->ext().hasInstance()) {
                    PyErr_SetString(PyExc_TypeError, "Unable to change scope of an existing singleton instance");
                    return NULL;
                }
                if (prefix_name) {
                    return PyUnicode_FromString(prefix_name);
                }
                Py_RETURN_NONE;
            } else {
                if (PyMemo_Check<MemoObject>(py_object)) {
                    return runSafe(MemoObject_set_prefix<MemoObject>, 
                        reinterpret_cast<MemoObject*>(py_object), prefix_name
                    );
                } else {
                    return runSafe(MemoObject_set_prefix<MemoImmutableObject>, 
                        reinterpret_cast<MemoImmutableObject*>(py_object), prefix_name
                    );
                }                
            }            
        }
        
        PyErr_SetString(PyExc_TypeError, "Invalid object type");
        return NULL;        
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

    PyObject *tryGetMutablePrefixes()
    {
        using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
        auto list = Py_OWN(PyList_New(0));
        if (!list) {
            return nullptr;
        }
        PyToolkit::getPyWorkspace().getWorkspace().forEachFixture([&list](const Fixture &fixture) {
            if (fixture.getAccessType() == AccessType::READ_WRITE) {
                auto prefix = Py_OWN(Py_BuildValue("sK", fixture.getPrefix().getName().c_str(), fixture.getUUID()));
                if (!prefix) {
                    list = nullptr;
                    return false;
                }
                if (PySafeList_Append(*list, prefix) == -1) {
                    list = nullptr;
                    return false;
                }
            }
            return true;
        });
        return list.steal();
    }

    PyObject *PyAPI_getMutablePrefixes(PyObject *, PyObject *)
    {
        PY_API_FUNC
        return runSafe(tryGetMutablePrefixes);
    }
    
    PyObject *PyAPI_getMemoClass(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "getMemoClass requires exactly 1 argument");
            return NULL;
        }
        return runSafe(tryGetMemoClass, args[0]);
    }
    
    PyObject *PyAPI_getMemoClasses(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        // extract optional prefix_name or prefix_uuid
        PyObject *py_prefix_name = nullptr;
        const char *prefix_name = nullptr;
        std::uint64_t prefix_uuid = 0;
        static const char *kwlist[] = {"prefix_name", "prefix_uuid", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OK", const_cast<char**>(kwlist), &py_prefix_name, &prefix_uuid)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (py_prefix_name && py_prefix_name != Py_None) {
            if (!PyUnicode_Check(py_prefix_name)) {
                PyErr_SetString(PyExc_TypeError, "Invalid argument type: prefix_name");
                return NULL;
            }
            prefix_name = PyUnicode_AsUTF8(py_prefix_name);
            if (!prefix_name) {
                PyErr_SetString(PyExc_TypeError, "Unable to extract prefix name");
                return NULL;
            }
        }

        return runSafe(tryGetMemoClasses, prefix_name, prefix_uuid);
    }
    
    PyObject *getStorageStats(PyObject *, PyObject *args, PyObject *kwargs) 
    {
        PY_API_FUNC
        return runSafe(tryGetStorageStats, args, kwargs);
    }
    
    PyObject *PyAPI_getAttributes(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        PyTypeObject *py_type;
        if (!PyArg_ParseTuple(args, "O", &py_type)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        if (PyAnyMemoType_Check(py_type)) {
            return runSafe(tryGetAttributes, py_type);
        } else {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }        
    }
    
    PyObject *PyAPI_getAttrAs(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        // memo object, attribute name, type
        if (nargs != 3) {
            PyErr_SetString(PyExc_TypeError, "getattr_as requires exactly 2 arguments");
            return NULL;
        }
        
        if (!PyAnyMemo_Check(args[0])) {
            // fall back to regular getattr if not a memo object
            return PyObject_GetAttr(args[0], args[1]);
        }
                
        if (!PyType_Check(args[2])) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        PyTypeObject *py_type = reinterpret_cast<PyTypeObject*>(args[2]);
        if (PyMemo_Check<MemoObject>(args[0])) {
            return runSafe(tryGetAttrAs<MemoObject>, reinterpret_cast<MemoObject*>(args[0]), args[1], py_type);
        } else {
            return runSafe(tryGetAttrAs<MemoImmutableObject>, reinterpret_cast<MemoImmutableObject*>(args[0]), args[1], py_type);
        }
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
    
    PyTypeObject *PyAPI_getType(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "getType requires exactly 1 argument");
            return NULL;
        }
        PY_API_FUNC
        return runSafe(tryGetType, args[0]);
    }
    
    PyObject *PyAPI_load(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        // extract object / prefix name (can be None)
        PyObject *py_object = nullptr;
        PyObject *py_exclude = nullptr;
        if (!PyArg_ParseTuple(args,  "O", &py_object)) {
            return NULL;
        }
        if (kwargs != nullptr) {
            if (!PyArg_ValidateKeywordArguments(kwargs)) {
                return NULL;
            }
            py_exclude = PyDict_GetItemString(kwargs, "exclude");
        }
        if (py_exclude != nullptr) {
            if (!PySequence_Check(py_exclude) || PyUnicode_Check(py_exclude)) {
                PyErr_SetString(PyExc_TypeError, "Invalid argument type. Exclude shoud be a sequence");
                return NULL;
            }
            if (!PyAnyMemo_Check(py_object)) {
                PyErr_SetString(PyExc_TypeError, "Exclude is only supported for memo objects");
                return NULL;
            }
        }
        
        PY_API_FUNC
        // load stack to detect circular references
        std::unordered_set<const void*> load_stack;
        return runSafe(tryLoad, py_object, kwargs, py_exclude, &load_stack, false);
    }
    
    PyObject *PyAPI_loadAll(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        // extract object / prefix name (can be None)
        PyObject *py_object = nullptr;
        if (!PyArg_ParseTuple(args,  "O", &py_object)) {
            return NULL;
        }
        PY_API_FUNC
        std::unordered_set<const void*> load_stack;
        return runSafe(tryLoad, py_object, kwargs, nullptr, &load_stack, true);
    }
    
    PyObject *tryHash(PyObject *obj_ptr)
    {
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return getPyHashAsPyObject(fixture, obj_ptr);
    }

    PyObject *PyAPI_hash(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "hash requires exactly 1 argument");
            return NULL;
        }
        PY_API_FUNC
        return runSafe(tryHash, args[0]);
    }
    
    PyObject *PyAPI_materialized(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "materialized requires exactly 1 argument");
            return NULL;
        }
        auto py_obj = args[0];
        if (!PyAnyMemo_Check(py_obj)) {
            // simply return self if not a memo object
            Py_INCREF(py_obj);
            return py_obj;
        }
        
        PY_API_FUNC
        if (PyMemo_Check<MemoObject>(py_obj)) {
            return runSafe(getMaterializedMemoObject<MemoObject>, 
                reinterpret_cast<MemoObject*>(py_obj)
            );
        } else {
            return runSafe(getMaterializedMemoObject<MemoImmutableObject>, 
                reinterpret_cast<MemoImmutableObject*>(py_obj)
            );
        }
    }
    
    PyObject *tryWait(const char *prefix, long state, long timeout)
    {
        db0::SafeRLock api_lock;
        {
            // GIL have to be released to safely lock API
            db0::python::WithGIL_Unlocked no_gil;
            api_lock = db0::python::PyToolkit::lockApi();
        }

        db0::swine_ptr<Fixture> fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(prefix, AccessType::READ_ONLY);
        if(fixture->getAccessType() == AccessType::READ_WRITE) {
            PyErr_SetString(PyExc_RuntimeError, "wait() not supported for read-write prefix");
            return nullptr;
        }

        std::optional<std::chrono::milliseconds> optional_timeout;
        if(timeout > 0) {
            optional_timeout.emplace(std::chrono::milliseconds(timeout));
        }

        class StateReachedNotifier
        {
            std::mutex m_mtx;
            std::condition_variable m_cv;
            bool m_state_reached;

        public:
            StateReachedNotifier()
            : m_state_reached(false)
            {}

            void notify_state_reached()
            {
                std::unique_lock lock(m_mtx);
                m_state_reached = true;
                m_cv.notify_all();
            }

            bool wait_state_reached(const std::optional<std::chrono::milliseconds> &timeout)
            {
                std::unique_lock lock(m_mtx);
                auto pred = [this]{ return m_state_reached; };
                if(timeout) {
                    return m_cv.wait_for(lock, *timeout, pred);
                }
                m_cv.wait(lock, pred);
                return true;
            }
        };

        class StateReachedCallback : public StateReachedCallbackBase
        {
            std::shared_ptr<StateReachedNotifier> m_notifier;

        public:
            explicit StateReachedCallback(std::shared_ptr<StateReachedNotifier> notifier)
                : m_notifier(std::move(notifier))
            {                
            }

            virtual void execute() override
            {                
                m_notifier->notify_state_reached();
            }
        };

        auto notifier = std::make_shared<StateReachedNotifier>();
        auto callback = std::make_unique<StateReachedCallback>(notifier);
        fixture->registerPrefixStateReachedCallback(state, std::move(callback));
        fixture = nullptr;
        bool result;
        {
            // We shouldn't lock python entirely on this blocking wait
            db0::python::WithGIL_Unlocked no_gil;
            // We have to unlock api for refresh to happen
            api_lock.unlock();
            result = notifier->wait_state_reached(optional_timeout);
        }
        if(result) {
            Py_RETURN_TRUE;
        }
        Py_RETURN_FALSE;
    }

    PyObject *PyAPI_wait(PyObject*, PyObject *args, PyObject *kwargs)
    {
        const char *prefix = nullptr;
        long state = 0;
        long timeout = 0;
        const char * const kwlist[] = {"prefix", "state", "timeout", nullptr};
        if(!PyArg_ParseTupleAndKeywords(args, kwargs, "sl|l:wait", const_cast<char**>(kwlist), &prefix, &state, &timeout)) {
            return nullptr;
        }
        if(state <= 0) {
            PyErr_SetString(PyExc_ValueError, "state number have to be greater than 0");
            return nullptr;
        }
        if(state > std::numeric_limits<StateNumType>::max()) {
            PyErr_SetString(PyExc_ValueError, "state number exceeds maximum allowed value");
            return nullptr;
        }
        if(timeout < 0) {
            PyErr_SetString(PyExc_ValueError, "timeout have to be a positive integer");
            return nullptr;
        }

        return runSafe(tryWait, prefix, state, timeout);
    }
    
    PyObject *tryFindSingleton(PyObject *args, PyObject *kwargs)
    {
        // singleton type must be the 1st argument
        PyObject *py_type = nullptr;
        const char *prefix_name = nullptr;
        static const char *kwlist[] = {"type", "prefix", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|s", const_cast<char**>(kwlist), &py_type, &prefix_name)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        if (!PyType_Check(py_type)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        if (!PyMemoType_IsSingleton(reinterpret_cast<PyTypeObject*>(py_type))) {
            THROWF(db0::InputException) << "Type is not a singleton: " << Py_TYPE(py_type)->tp_name;
        }
        
        // retrieve static prefix name
        if (!prefix_name) {
            prefix_name = MemoTypeDecoration::get((PyTypeObject*)py_type).tryGetPrefixName();
        }
        
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        db0::swine_ptr<Fixture> fixture = prefix_name ? workspace.getFixture(prefix_name) : workspace.getCurrentFixture();
        fixture->refreshIfUpdated();
        auto py_singleton = tryMemoObject_open_singleton(reinterpret_cast<PyTypeObject*>(py_type), *fixture);
        if (!py_singleton) {
            Py_RETURN_NONE;
        }
        return py_singleton;
    }

    PyObject *PyAPI_findSingleton(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return runSafe(tryFindSingleton, args, kwargs);
    }

    PyObject *PyAPI_weakProxy(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "weakProxy requires exactly 1 argument");
            return NULL;
        }
        PY_API_FUNC
        return runSafe(tryWeakProxy, args[0]);
    }

    PyObject *PyAPI_expired(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "weakProxy requires exactly 1 argument");
            return NULL;
        }
        PY_API_FUNC
        return runSafe(tryExpired, args[0]);
    }

    PyObject *tryAsyncWait(PyObject *future, const char *prefix, int state)
    {
        db0::swine_ptr<Fixture> fixture = PyToolkit::getPyWorkspace().getWorkspace().tryFindFixture(prefix);
        if (!fixture) {
            PyErr_SetString(PyExc_RuntimeError, "async_wait() requires prefix to be opened");
            return nullptr;
        }

        class StateReachedCallback : public StateReachedCallbackBase
        {
            ObjectSharedPtr m_future;

        public:
            explicit StateReachedCallback(PyObject *future)
                : m_future(Py_BORROW(future))
            {                
            }

            virtual ~StateReachedCallback()
            {
                // First we should check if the python is closing
                // We can't use the API after finalization has started
                if (!Py_IsInitialized()) {
                    m_future.steal();
                }
            }

            virtual void execute() override
            {                
                auto lang_lock = LangToolkit::ensureLocked();
                {
                    auto loop = Py_OWN(PyObject_CallMethod(*m_future, "get_loop", nullptr));
                    ObjectSharedPtr future_set_result, handle;
                    // loop can be null when the process is closing
                    if (loop.get()) {
                        future_set_result = Py_OWN(PyObject_GetAttrString(*m_future, "set_result"));
                        if (future_set_result.get()) {
                            handle = Py_OWN(PyObject_CallMethod(
                                *loop, "call_soon_threadsafe", "OO", *future_set_result, Py_None)
                            );
                        }
                    }
                }
                // We want to avoid 'leaking' exceptions into the main thread
                // The kind of errors that can occur here are related to use of python API when runtime finalization has started
                PyErr_Clear();
                // Cleanup here while we hold the GIL
                m_future = nullptr;
            }
        };
        fixture->registerPrefixStateReachedCallback(state, std::make_unique<StateReachedCallback>(future));
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_async_wait(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PyObject *future = nullptr;
        const char *prefix = nullptr;
        int state = 0;
        const char * const kwlist[] = {"future", "prefix", "state", nullptr};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Osi:async_wait", const_cast<char**>(kwlist), &future, &prefix, &state)) {
            return nullptr;
        }
        if (state <= 0) {
            PyErr_SetString(PyExc_ValueError, "state number have to be greater than 0");
            return nullptr;
        }

        PY_API_FUNC
        return runSafe(tryAsyncWait, future, prefix, state);
    }

    PyObject *tryGetConfig()
    {
        auto config = PyToolkit::getPyWorkspace().getConfig()->getRawConfig();
        return PyDict_Copy(config.get());
    }

    PyObject *PyAPI_getConfig(PyObject*, PyObject*)
    {
        PY_API_FUNC
        return runSafe(tryGetConfig);
    }

    PyObject *PyAPI_compare(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PyObject *py_first = nullptr;
        PyObject *py_second = nullptr;
        PyObject *py_tags = nullptr;
        static const char *kwlist[] = {"first", "second", "tags", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|O", const_cast<char**>(kwlist), &py_first, &py_second, &py_tags)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        // FIXME: implement for MemoImmutableObject as well
        if (!PyMemo_Check<MemoObject>(py_first) || !PyMemo_Check<MemoObject>(py_second)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        return runSafe(tryCompareMemo, reinterpret_cast<MemoObject*>(py_first), 
            reinterpret_cast<MemoObject*>(py_second));
    }    
    
#ifndef NDEBUG
    PyObject *PyAPI_startDebugLogs(PyObject *self, PyObject *)
    {
        PY_API_FUNC
        db0::Settings::__dbg_logs = true;
        Py_RETURN_NONE;
    }
        
    PyObject *PyAPI_breakpoint(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "breakpoint requires exactly 1 argument");
            return NULL;
        }

        // put your debug logic here
        // auto memo_obj = reinterpret_cast<MemoObject*>(args[0]);        
        // memo_obj->ext().getType().startDebug();
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_enableStorageValidation(PyObject *, PyObject *args, PyObject *kwargs)
    {
        bool enable = true;
        static const char *kwlist[] = {"enable", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|p", const_cast<char**>(kwlist), &enable)) {            
            return NULL;
        }

        PY_API_FUNC
        db0::Settings::__storage_validation = enable;
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_setTestParams(PyObject *, PyObject *, PyObject *kwargs)
    {
        PY_API_FUNC
        return runSafe(trySetTestParams, kwargs);        
    }
    
    PyObject *PyAPI_resetTestParams(PyObject *, PyObject *) 
    {
        PY_API_FUNC
        return runSafe(tryResetTestParams);        
    }
#endif

    PyObject *PyAPI_assign(PyObject *, PyObject *args, PyObject *kwargs)
    {
        if (!PyDict_Check(kwargs)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        PY_API_FUNC
        return runSafe(tryAssign, args, kwargs);
    }
    
    PyObject *PyAPI_touch(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        for (Py_ssize_t i = 0; i < nargs; ++i) {
            if (!PyAnyMemo_Check(args[i])) {
                PyErr_SetString(PyExc_TypeError, "Invalid argument type");
                return NULL;
            }
        }
        PY_API_FUNC
        return runSafe(tryTouch, args, nargs);
    }
    
    PyObject *PyAPI_copyPrefix(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return reinterpret_cast<PyObject*>(runSafe(tryCopyPrefix, args, kwargs));
    }

}