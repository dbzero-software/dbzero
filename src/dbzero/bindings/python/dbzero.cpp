// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include<iostream>
#include "Memo.hpp"
#include "PyAPI.hpp"
#include "PyInternalAPI.hpp"
#include "PyTagsAPI.hpp"
#include "PyObjectTagManager.hpp"
#include "PySnapshot.hpp"
#include "PyTagSet.hpp"
#include "PyAtomic.hpp"
#include "PyLocked.hpp"
#include "PyWeakProxy.hpp"
#include <dbzero/bindings/python/types/PyObjectId.hpp>
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/PyByteArray.hpp>
#include <dbzero/bindings/python/collections/PyIndex.hpp>
#include <dbzero/bindings/python/collections/PySet.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/collections/PyDict.hpp>
#include <dbzero/bindings/python/PyWorkspace.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterable.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterator.hpp>
#include <dbzero/bindings/python/iter/PyJoinIterable.hpp>
#include <dbzero/bindings/python/iter/PyJoinIterator.hpp>
#include <dbzero/bindings/python/types/PyClassFields.hpp>
#include <dbzero/bindings/python/types/PyClass.hpp>
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/bindings/python/types/PyTag.hpp>
#include <dbzero/bindings/python/PyTagSet.hpp>

namespace py = db0::python;
    
static PyMethodDef dbzero_methods[] = 
{
    {"_init", (PyCFunction)&py::PyAPI_init, METH_VARARGS | METH_KEYWORDS, "Initialize dbzero workspace at a specific root path"},
    {"open", (PyCFunction)&py::PyAPI_open, METH_VARARGS | METH_KEYWORDS, "Open or create a prefix for read or read/write"},
    {"close", &py::PyAPI_close, METH_VARARGS, ""},
    {"drop", &py::PyAPI_drop, METH_VARARGS, "Drop prefix (if exists)"},
    {"commit", &py::PyAPI_commit, METH_VARARGS, "Commit data to disk / persistent storage"},
    {"fetch", (PyCFunction)&py::PyAPI_fetch, METH_VARARGS | METH_KEYWORDS, "Retrieve dbzero object instance by its UUID or type (in case of a singleton)"},
    {"exists", (PyCFunction)&py::PyAPI_exists, METH_VARARGS | METH_KEYWORDS, "Check if a specific UUID points to a valid dbzero object instance or if singleton of a given type exists"},
    {"delete", &py::PyAPI_del, METH_VARARGS, "Delete dbzero object and the corresponding Python instance"},    
    {"get_type_info", &py::PyAPI_getTypeInfo, METH_VARARGS, "Get dbzero type information"},
    {"uuid", (PyCFunction)&py::PyAPI_getUUID, METH_FASTCALL, "Get unique object ID"},
    {"clear_cache", &py::PyAPI_clearCache, METH_NOARGS, "Clear dbzero cache"},
    {"list", (PyCFunction)&py::PyAPI_makeList, METH_FASTCALL, "Create a new dbzero list instance"},
    {"index", (PyCFunction)&py::PyAPI_makeIndex, METH_FASTCALL, "Create a new dbzero index instance"},
    {"tuple", (PyCFunction)&py::PyAPI_makeTuple, METH_FASTCALL, "Create a new dbzero tuple instance"},
    {"set", (PyCFunction)&py::PyAPI_makeSet, METH_FASTCALL, "Create a new dbzero set instance"},
    {"dict", (PyCFunction)&py::PyAPI_makeDict, METH_VARARGS | METH_KEYWORDS, "Create a new dbzero dict instance"},
    {"bytearray", (PyCFunction)&py::PyAPI_makeByteArray, METH_FASTCALL, "Create a new dbzero bytearray instance"},        
    {"tags", (PyCFunction)&py::makeObjectTagManager, METH_FASTCALL, ""},
    {"find", (PyCFunction)&py::PyAPI_find, METH_VARARGS | METH_KEYWORDS, "Find memo instances by tags with optional filtering"},
    {"join", (PyCFunction)&py::PyAPI_join, METH_VARARGS | METH_KEYWORDS, "Join memo collections by common tags with optional filtering"},
    {"refresh", (PyCFunction)&py::refresh, METH_VARARGS, ""},
    {"get_state_num", (PyCFunction)&py::PyAPI_getStateNum, METH_VARARGS | METH_KEYWORDS, ""},
    {"get_prefix_stats", (PyCFunction)&py::getPrefixStats, METH_VARARGS | METH_KEYWORDS, "Retrieve prefix specific statistics"},
    {"snapshot", (PyCFunction)&py::PyAPI_getSnapshot, METH_VARARGS | METH_KEYWORDS, "Get snapshot of dbzero state"},
    {"get_snapshot_of", (PyCFunction)&py::PyAPI_getSnapshotOf, METH_FASTCALL, "Get snapshot associated with a specific object"},
    {"begin_atomic", (PyCFunction)&py::PyAPI_beginAtomic, METH_FASTCALL, "Opens a new atomic operation's context"},
    {"begin_locked", (PyCFunction)&py::PyAPI_beginLocked, METH_FASTCALL, "Enter a new locked section"},
    {"describe", &py::describeObject, METH_VARARGS, "Get dbzero object's description"},
    {"rename_field", (PyCFunction)&py::renameField, METH_VARARGS | METH_KEYWORDS, "Get snapshot of dbzero state"},
    {"is_singleton", &py::PyAPI_isSingleton, METH_VARARGS, "Check if a specific instance is a dbzero singleton"},
    {"getrefcount", &py::PyAPI_getRefCount, METH_VARARGS, "Get dbzero ref counts"},
    {"no", (PyCFunction)&py::negTagSet, METH_FASTCALL, "Tag negation function"},
    {"build_flags", &py::PyAPI_getBuildFlags, METH_NOARGS, "Retrieve dbzero library build flags"},
    {"serialize", (PyCFunction)&py::PyAPI_serialize, METH_FASTCALL, "Serialize dbzero serializable instance"},
    {"deserialize", (PyCFunction)&py::PyAPI_deserialize, METH_FASTCALL, "Serialize dbzero serializable instance"},    
    {"is_enum_value", (PyCFunction)&py::PyAPI_isEnumValue, METH_FASTCALL, "Check if parameter represents a dbzero enum value"},
    {"split_by", (PyCFunction)&py::PyAPI_splitBy, METH_VARARGS | METH_KEYWORDS, "Split query iterator by a given criteria"},    
    {"filter", (PyCFunction)&py::filter, METH_VARARGS | METH_KEYWORDS, "Filter with a Python callable"},
    {"set_prefix", (PyCFunction)&py::PyAPI_setPrefix, METH_VARARGS | METH_KEYWORDS, "Allows dynamically specifying object's prefix during initialization"},
    {"get_slab_metrics", (PyCFunction)&py::getSlabMetrics, METH_NOARGS, "Retrieve slab metrics of the current prefix"},
    {"set_cache_size", (PyCFunction)&py::setCacheSize, METH_VARARGS, "Update dbzero cache size with immediate effect"},
    {"get_cache_stats", &py::getCacheStats, METH_NOARGS, "Retrieve dbzero cache statistics"},
    {"get_lang_cache_stats", &py::getLangCacheStats, METH_NOARGS, "Retrieve dbzero language cache statistics"},
    {"get_storage_stats", (PyCFunction)&py::getStorageStats, METH_VARARGS | METH_KEYWORDS, "Retrieve dbzero storage utilization statistics for a specific prefix"},
    // the Reflection API functions
    {"get_attributes", (PyCFunction)&py::PyAPI_getAttributes, METH_VARARGS, "Get attributes of a memo type"},
    {"getattr_as", (PyCFunction)&py::PyAPI_getAttrAs, METH_FASTCALL, "Get memo member cast to a user defined type - e.g. MemoBase"},
    {"get_address", (PyCFunction)&py::PyAPI_getAddress, METH_FASTCALL, "Get dbzero object's address"},
    {"get_type", (PyCFunction)&py::PyAPI_getType, METH_FASTCALL, "For a given dbzero instance, get associated Python type"},
    {"load", (PyCFunction)&py::PyAPI_load, METH_VARARGS | METH_KEYWORDS, "Load the entire instance graph from dbzero to memory and return as the closest native Python type"},
    {"load_all", (PyCFunction)&py::PyAPI_loadAll, METH_VARARGS | METH_KEYWORDS, "Load the entire instance graph to memory without calling an overloaded __load__ method of the top level object"},
    {"hash", (PyCFunction)&py::PyAPI_hash, METH_FASTCALL, "Returns hash of python or db0 object"},
    {"as_tag", (PyCFunction)&py::PyAPI_as_tag, METH_FASTCALL, "Returns tag of a @db0.memo object"},
    {"materialized", (PyCFunction)&py::PyAPI_materialized, METH_FASTCALL, "Returns a materialized version of a @db0.memo object"},
    {"is_memo", (PyCFunction)&py::PyAPI_PyMemo_Check, METH_FASTCALL, "Checks if passed object is memo type"},
    {"is_enum", (PyCFunction)&py::PyAPI_isEnum, METH_FASTCALL, "Checks if passed object is a db0 enum value"},
    {"wait", (PyCFunction)&py::PyAPI_wait, METH_VARARGS | METH_KEYWORDS, "Wait for desired prefix state number"},
    {"find_singleton", (PyCFunction)&py::PyAPI_findSingleton, METH_VARARGS | METH_KEYWORDS, "Try retrieving an existing singleton, possibly from a given prefix"},
    {"weak_proxy", (PyCFunction)&py::PyAPI_weakProxy, METH_FASTCALL, "Construct weak proxy from a db0 object"},
    {"expired", (PyCFunction)&py::PyAPI_expired, METH_FASTCALL, "Check if the weak reference has expired"},    
    {"get_config", &py::PyAPI_getConfig, METH_NOARGS, "Get dbzero configuration, as passed to 'init' function"},
    {"assign", (PyCFunction)&py::PyAPI_assign, METH_VARARGS | METH_KEYWORDS, "Assign multiple attributes in a single operation (non-atomic)"},
    {"touch", (PyCFunction)&py::PyAPI_touch, METH_FASTCALL, "Mark object to appear as modified in the current transaction (without actually modifying it)"},
    {"get_schema", (PyCFunction)&py::PyAPI_getSchema, METH_FASTCALL, "Get deduced schema of a memo object"},
    {"copy_prefix", (PyCFunction)&py::PyAPI_copyPrefix, METH_VARARGS | METH_KEYWORDS, "Copy entire prefix contents to make a backup or clone"},
    {"_wrap_memo_type", (PyCFunction)&py::PyAPI_wrapPyClass, METH_VARARGS | METH_KEYWORDS, "Wraps a memo type for use with dbzero"},
    {"_get_prefixes", &py::getPrefixes, METH_NOARGS, "Get the list of prefixes accessible from the current context"},
    {"_get_mutable_prefixes", &py::PyAPI_getMutablePrefixes, METH_NOARGS, "Get the list of prefixes opened with write access"},
    {"_get_memo_class", (PyCFunction)&py::PyAPI_getMemoClass, METH_FASTCALL, "Get memo meta-class information for a given instance"},
    {"_get_memo_classes", (PyCFunction)&py::PyAPI_getMemoClasses, METH_VARARGS | METH_KEYWORDS, "Get the list of memo classes from a specific prefix"},
    {"_get_prefix_of", (PyCFunction)&py::PyAPI_getPrefixOf, METH_VARARGS, "Get prefix name of a specific dbzero object instance"},
    {"_get_current_prefix", &py::PyAPI_getCurrentPrefix, METH_VARARGS, "Get current prefix name & UUID as tuple"},
    {"_make_enum", (PyCFunction)&py::PyAPI_makeEnum, METH_VARARGS | METH_KEYWORDS, "Define new or retrieve existing Enum type"},
    {"_compare", (PyCFunction)&py::PyAPI_compare, METH_VARARGS | METH_KEYWORDS, "Binary-compare (shallow) 2 dbzero objects with optional tags' assignments check"},
    {"_async_wait", (PyCFunction)&py::PyAPI_async_wait, METH_VARARGS | METH_KEYWORDS, "Get notified about state number being reached"},    
    {"_select_mod_candidates", (PyCFunction)&py::PyAPI_selectModCandidates, METH_VARARGS | METH_KEYWORDS, "Filter to return only objects which could potentially be modified within a specific scope"},
    {"_split_by_snapshots", (PyCFunction)&py::PyAPI_splitBySnapshots, METH_FASTCALL, "Splits a given query to produce results from the 2 given snapshots (as a tuple)"},
#ifndef NDEBUG
    {"dbg_write_bytes", &py::writeBytes, METH_VARARGS, "Debug function"},
    {"dbg_free_bytes", &py::freeBytes, METH_VARARGS, "Debug function"},
    {"dbg_read_bytes", &py::readBytes, METH_VARARGS, "Debug function"},
    {"dbg_start_logs", &py::PyAPI_startDebugLogs, METH_VARARGS, "Enable dbzeo debug logs"},    
    {"get_base_lock_usage", &py::getResourceLockUsage, METH_VARARGS, "Debug function, retrieves total memory occupied by ResourceLocks"},
    {"get_dram_io_map", (PyCFunction)&py::getDRAM_IOMap, METH_VARARGS | METH_KEYWORDS, "Get page_num -> state_num mapping related with a specific DRAM_Prefix"},    
    {"breakpoint", (PyCFunction)&py::PyAPI_breakpoint, METH_FASTCALL, "Testing & debugging function "},
    {"enable_storage_validation", (PyCFunction)&py::PyAPI_enableStorageValidation, METH_VARARGS | METH_KEYWORDS, "Enable full storage validation for testing"},
    {"set_test_params", (PyCFunction)&py::PyAPI_setTestParams, METH_VARARGS | METH_KEYWORDS, "Test keyword parameters"},
    {"reset_test_params", (PyCFunction)&py::PyAPI_resetTestParams, METH_NOARGS, "Restore default test parameters"},
#endif
    {NULL} // Sentinel
};

static struct PyModuleDef dbzero_module = {
    PyModuleDef_HEAD_INIT,
    "dbzero",
    NULL,
    -1,
    dbzero_methods
};
    
static struct PyModuleDef dbzero_types_module = {
    PyModuleDef_HEAD_INIT,
    "dbzero.types",
    NULL,
    -1,
    NULL,
};

void initPyType(PyObject *mod, PyTypeObject *py_type)
{
    std::stringstream _str;
    _str << "Initialization of type " << py_type->tp_name << " failed";
    if (PyType_Ready(py_type) < 0) {
        Py_DECREF(mod);
        throw std::runtime_error(_str.str());
    }
    
    Py_INCREF(py_type);
    if (PyModule_AddObject(mod, py_type->tp_name, (PyObject *)py_type) < 0) {
        Py_DECREF(py_type);
        Py_DECREF(mod);    
        throw std::runtime_error(_str.str());    
    }
}

void initPyError(PyObject *mod, PyObject *py_error, const char *error_name)
{
    if (PyModule_AddObject(mod, error_name, py_error) < 0) {        
        Py_DECREF(mod);
        std::stringstream _str;
        _str << "Initialization of error " << error_name << " failed";
        throw std::runtime_error(_str.str()); 
    }
}

// Module initialization function for Python 3.x
PyMODINIT_FUNC PyInit_dbzero(void)
{   
    auto mod = PyModule_Create(&dbzero_module);
    auto types_mod = PyModule_Create(&dbzero_types_module);

    if (PyModule_AddObject(mod, "types", types_mod) < 0) {        
        Py_DECREF(types_mod);
        Py_DECREF(mod);
        return NULL;
    }
    
    std::vector<PyTypeObject*> types = {
        &py::ObjectIdType, 
        &py::ListObjectType, 
        &py::IndexObjectType, 
        &py::SetObjectType, 
        &py::TupleObjectType, 
        &py::DictObjectType,
        &py::PyObjectTagManagerType, 
        &py::PySnapshotObjectType, 
        &py::PyObjectIterableType,
        &py::PyObjectIteratorType,
        &py::PyJoinIterableType,
        &py::PyJoinIteratorType,
        &py::ByteArrayObjectType,
        &py::PyEnumType, 
        &py::PyEnumValueType,
        &py::PyEnumValueReprType,
        &py::PyClassFieldsType,
        &py::PyFieldDefType,
        &py::ClassObjectType,
        &py::TagSetType,
        &py::PyAtomicType,        
        &py::PyTagType,
        &py::PyLockedType,
        &py::PyWeakProxyType,
    };
    
    // register all types
    try {
        for (auto py_type: types) {
            initPyType(types_mod, py_type);
        }
        initPyError(mod, py::PyToolkit::getTypeManager().getBadPrefixError(), "BadPrefixError");
        initPyError(mod, py::PyToolkit::getTypeManager().getClassNotFoundError(), "ClassNotFoundError");
        initPyError(mod, py::PyToolkit::getTypeManager().getReferenceError(), "ReferenceError");
    } catch (const std::exception &e) {
        // set python error
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return mod;
}
