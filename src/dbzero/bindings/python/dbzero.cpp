#include<iostream>
#include "Memo.hpp"
#include "PyAPI.hpp"
#include "PyInternalAPI.hpp"
#include "PyObjectTagManager.hpp"
#include "PySnapshot.hpp"
#include "PyTagSet.hpp"
#include "PyAtomic.hpp"
#include "PyLocked.hpp"
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
#include <dbzero/bindings/python/Pandas/PandasBlock.hpp>
#include <dbzero/bindings/python/Pandas/PandasDataFrame.hpp>
#include <dbzero/bindings/python/types/PyClassFields.hpp>
#include <dbzero/bindings/python/types/PyClass.hpp>
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/bindings/python/types/PyTag.hpp>
#include <dbzero/bindings/python/PyTagSet.hpp>

namespace py = db0::python;
    
static PyMethodDef dbzero_methods[] = 
{
    {"init", (PyCFunction)&py::init, METH_VARARGS | METH_KEYWORDS, "Initialize dbzero CE workspace at a specific root path"},
    {"open", (PyCFunction)&py::open, METH_VARARGS | METH_KEYWORDS, "Open or create a prefix for read or read/write"},
    {"close", &py::PyAPI_close, METH_VARARGS, ""},
    {"drop", &py::drop, METH_VARARGS, "Drop prefix (if exists)"},
    {"commit", &py::PyAPI_commit, METH_VARARGS, "Commit data to disk / persistent storage"},
    {"fetch", (PyCFunction)&py::fetch, METH_FASTCALL, "Retrieve dbzero object instance by its UUID or type (in case of a singleton)"},
    {"delete", &py::del, METH_VARARGS, "Delete dbzero object and the corresponding Python instance"},
    {"wrap_memo_type", (PyCFunction)&py::PyAPI_wrapPyClass, METH_VARARGS | METH_KEYWORDS, "Wraps a memo type for use with dbzero"},
    {"get_type_info", &py::getTypeInfo, METH_VARARGS, "Get dbzero type information"},
    {"uuid", (PyCFunction)&py::PyAPI_getUUID, METH_FASTCALL, "Get unique object ID"},
    {"clear_cache", &py::PyAPI_clearCache, METH_NOARGS, "Clear dbzero cache"},
    {"list", (PyCFunction)&py::PyAPI_makeList, METH_FASTCALL, "Create a new dbzero list instance"},
    {"index", (PyCFunction)&py::PyAPI_makeIndex, METH_FASTCALL, "Create a new dbzero index instance"},
    {"tuple", (PyCFunction)&py::PyAPI_makeTuple, METH_FASTCALL, "Create a new dbzero tuple instance"},
    {"set", (PyCFunction)&py::PyAPI_makeSet, METH_FASTCALL, "Create a new dbzero set instance"},
    {"dict", (PyCFunction)&py::PyAPI_makeDict, METH_VARARGS | METH_KEYWORDS, "Create a new dbzero dict instance"},
    {"block", (PyCFunction)&py::makeBlock, METH_VARARGS | METH_KEYWORDS, "Create a new dbzero pandas block instance"},
    {"dataframe", (PyCFunction)&py::makeDataFrame, METH_VARARGS | METH_KEYWORDS, "Create a new dbzero pandas dataframe instance"},
    {"bytearray", (PyCFunction)&py::PyAPI_makeByteArray, METH_FASTCALL, "Create a new dbzero bytearray instance"},
    {"get_raw_prefix_of", (PyCFunction)&py::PyAPI_getPrefixOf, METH_VARARGS, "Get prefix name of a specific dbzero object instance"},
    {"get_raw_current_prefix", &py::PyAPI_getCurrentPrefix, METH_VARARGS, "Get current prefix name & UUID as tuple"},
    {"tags", (PyCFunction)&py::makeObjectTagManager, METH_FASTCALL, ""},
    {"find", (PyCFunction)&py::PyAPI_find, METH_FASTCALL, ""},
    {"refresh", (PyCFunction)&py::refresh, METH_VARARGS, ""},
    {"get_state_num", (PyCFunction)&py::getStateNum, METH_VARARGS | METH_KEYWORDS, ""},
    {"get_prefix_stats", (PyCFunction)&py::getPrefixStats, METH_VARARGS | METH_KEYWORDS, "Retrieve prefix specific statistics"},
    {"snapshot", (PyCFunction)&py::getSnapshot, METH_VARARGS | METH_KEYWORDS, "Get snapshot of dbzero state"},
    {"begin_atomic", (PyCFunction)&py::PyAPI_beginAtomic, METH_FASTCALL, "Opens a new atomic operation's context"},
    {"begin_locked", (PyCFunction)&py::PyAPI_beginLocked, METH_FASTCALL, "Enter a new locked section"},
    {"describe", &py::describeObject, METH_VARARGS, "Get dbzero object's description"},
    {"rename_field", &py::renameField, METH_VARARGS, "Get snapshot of dbzero state"},
    {"is_singleton", &py::isSingleton, METH_VARARGS, "Check if a specific instance is a dbzero singleton"},
    {"getrefcount", &py::getRefCount, METH_VARARGS, "Get dbzero ref counts"},
    {"no", (PyCFunction)&py::negTagSet, METH_FASTCALL, "Tag negation function"},
    {"to_dict", (PyCFunction)&py::toDict, METH_FASTCALL, "Serialize dbzero object as a Python dict"},
    {"build_flags", &py::getBuildFlags, METH_NOARGS, "Retrieve dbzero library build flags"},
    {"serialize", (PyCFunction)&py::pySerialize, METH_FASTCALL, "Serialize dbzero serializable instance"},
    {"deserialize", (PyCFunction)&py::pyDeserialize, METH_FASTCALL, "Serialize dbzero serializable instance"},
    {"make_enum", (PyCFunction)&py::makeEnum, METH_VARARGS | METH_KEYWORDS, "Define new or retrieve existing Enum type"},
    {"is_enum_value", (PyCFunction)&py::isEnumValue, METH_FASTCALL, "Check if parameter represents a dbzero enum value"},
    {"split_by", (PyCFunction)&py::splitBy, METH_VARARGS | METH_KEYWORDS, "Split query iterator by a given criteria"},
    {"filter", (PyCFunction)&py::filter, METH_VARARGS | METH_KEYWORDS, "Filter with a Python callable"},
    {"set_prefix", (PyCFunction)&py::setPrefix, METH_VARARGS | METH_KEYWORDS, "Allows dynamically specifying object's prefix during initialization"},
    {"get_slab_metrics", (PyCFunction)&py::getSlabMetrics, METH_NOARGS, "Retrieve slab metrics of the current prefix"},
    {"set_cache_size", (PyCFunction)&py::setCacheSize, METH_VARARGS, "Update dbzero cache size with immediate effect"},
    {"get_cache_stats", &py::getCacheStats, METH_NOARGS, "Retrieve dbzero cache statistics"},
    {"get_lang_cache_stats", &py::getLangCacheStats, METH_NOARGS, "Retrieve dbzero language cache statistics"},
    {"get_storage_stats", (PyCFunction)&py::getStorageStats, METH_VARARGS | METH_KEYWORDS, "Retrieve dbzero storage utilization statistics for a specific prefix"},
    // the Reflection API functions
    {"get_raw_prefixes", &py::getPrefixes, METH_NOARGS, "Get the list of prefixes accessible from the current context"},
    {"get_raw_mutable_prefixes", &py::PyAPI_getMutablePrefixes, METH_NOARGS, "Get the list of prefixes opened with write access"},
    {"get_raw_memo_classes", (PyCFunction)&py::getMemoClasses, METH_VARARGS | METH_KEYWORDS, "Get the list of memo classes from a specific prefix"},
    {"get_attributes", (PyCFunction)&py::getAttributes, METH_VARARGS, "Get attributes of a memo type"},
    {"getattr_as", (PyCFunction)&py::getAttrAs, METH_FASTCALL, "Get memo member cast to a user defined type - e.g. MemoBase"},
    {"get_address", (PyCFunction)&py::PyAPI_getAddress, METH_FASTCALL, "Get dbzero object's address"},
    {"get_type", (PyCFunction)&py::PyAPI_getType, METH_FASTCALL, "For a given dbzero instance, get associated Python type"},
    {"load", (PyCFunction)&py::PyAPI_load, METH_VARARGS | METH_KEYWORDS, "Load the entire instance from dbzero to memory and return as the closest native Python type"},
    {"hash", (PyCFunction)&py::PyAPI_hash, METH_FASTCALL, "Returns hash of python or db0 object"},
    {"as_tag", (PyCFunction)&py::PyAPI_as_tag, METH_FASTCALL, "Returns tag of a @db0.memo object"},
    {"materialized", (PyCFunction)&py::PyAPI_materialized, METH_FASTCALL, "Returns a materialized version of a @db0.memo object"},
    {"is_memo", (PyCFunction)&py::PyAPI_PyMemo_Check, METH_FASTCALL, "Checks if passed object is memo type"},
    {"wait", (PyCFunction)&py::PyAPI_wait, METH_VARARGS | METH_KEYWORDS, "Wait for desired prefix state number"},
    {"find_singleton", (PyCFunction)&py::PyApi_findSingleton, METH_VARARGS | METH_KEYWORDS, "Try retrieving an existing singleton, possibly from a given prefix"},
#ifndef NDEBUG
    {"dbg_write_bytes", &py::writeBytes, METH_VARARGS, "Debug function"},
    {"dbg_free_bytes", &py::freeBytes, METH_VARARGS, "Debug function"},
    {"dbg_read_bytes", &py::readBytes, METH_VARARGS, "Debug function"},
    {"get_base_lock_usage", &py::getResourceLockUsage, METH_VARARGS, "Debug function, retrieves total memory occupied by ResourceLocks"},
    {"get_dram_io_map", (PyCFunction)&py::getDRAM_IOMap, METH_VARARGS | METH_KEYWORDS, "Get page_num -> state_num mapping related with a specific DRAM_Prefix"},
#endif
    {NULL} // Sentinel
};

static struct PyModuleDef dbzero_ce_module = {
    PyModuleDef_HEAD_INIT,
    "dbzero_ce",
    NULL,
    -1,
    dbzero_methods
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

        // FIXME: log
        // convert as str(mod)
        auto str_module_name = PyUnicode_AsUTF8(PyObject_Str(mod)); 
        std::cout << "module name: " << str_module_name << std::endl;
        std::cout << "type name: " << py_type->tp_name << std::endl;

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
PyMODINIT_FUNC PyInit_dbzero_ce(void)
{   
    auto mod = PyModule_Create(&dbzero_ce_module);
    std::vector<PyTypeObject*> types = {
        &py::ObjectIdType, 
        &py::ListObjectType, 
        &py::IndexObjectType, 
        &py::SetObjectType, 
        &py::TupleObjectType, 
        &py::DictObjectType,
        &py::PyObjectTagManagerType, 
        &py::PySnapshotObjectType, 
        &py::PandasBlockObjectType,
        &py::PandasDataFrameObjectType,
        &py::PyObjectIterableType,
        &py::PyObjectIteratorType,
        &py::ByteArrayObjectType,
        &py::PyEnumType, 
        &py::PyEnumValueType,
        &py::PyEnumValueReprType,
        &py::PyClassFieldsType,
        &py::PyFieldDefType,
        &py::ClassObjectType,
        &py::TagSetType,
        &py::PyAtomicType,        
        &py::PyTagType
    };
    
    // register all types
    try {
        for (auto py_type: types) {
            initPyType(mod, py_type);
        }
        initPyError(mod, py::PyToolkit::getTypeManager().getBadPrefixError(), "BadPrefixError");
        initPyError(mod, py::PyToolkit::getTypeManager().getClassNotFoundError(), "ClassNotFoundError");
    } catch (const std::exception &e) {
        // set python error
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return mod;
}
