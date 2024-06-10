#include<iostream>
#include "Memo.hpp"
#include "PyAPI.hpp"
#include "PyInternalAPI.hpp"
#include "ObjectId.hpp"
#include <dbzero/bindings/python/collections/List.hpp>
#include <dbzero/bindings/python/collections/ByteArray.hpp>
#include "Index.hpp"
#include <dbzero/bindings/python/collections/Set.hpp>
#include <dbzero/bindings/python/collections/Tuple.hpp>
#include <dbzero/bindings/python/collections/Dict.hpp>
#include <dbzero/bindings/python/PyWorkspace.hpp>
#include "PyObjectTagManager.hpp"
#include "PyObjectIterator.hpp"
#include "PySnapshot.hpp"
#include "PyTagSet.hpp"
#include "PyEnum.hpp"
#include <dbzero/bindings/python/Pandas/PandasBlock.hpp>
#include <dbzero/bindings/python/Pandas/PandasDataFrame.hpp>
#include "PyClassFields.hpp"

namespace py = db0::python;
    
static PyMethodDef DBZeroCE_Methods[] = 
{
    {"init", &py::init, METH_VARARGS, "Initialize DBZero CE workspace at a specific root path"},
    {"open", (PyCFunction)&py::open, METH_VARARGS | METH_KEYWORDS, "Open or create a prefix for read or read/write"},
    {"close", &py::close, METH_VARARGS, ""},
    {"drop", &py::drop, METH_VARARGS, "Drop prefix (if exists)"},
    {"commit", &py::commit, METH_VARARGS, ""},
    {"fetch", (PyCFunction)&py::fetch, METH_FASTCALL, "Retrieve DBZero object instance by its UUID or type (in case of a singleton)"},
    {"delete", &py::del, METH_VARARGS, "Delete DBZero object and corresponding Python instance"},
    {"wrap_memo_type", (PyCFunction)&py::wrapPyClass, METH_VARARGS | METH_KEYWORDS, "Wraps a memo type for use with DBZero"},
    {"get_type_info", &py::getTypeInfo, METH_VARARGS, "Get DBZero type information"},
    {"uuid", (PyCFunction)&py::getUUID, METH_FASTCALL, "Get unique object ID"},
    {"get_address", (PyCFunction)&py::getObjectAddress, METH_FASTCALL, "Get object's address"},
    {"cache_stats", &py::cacheStats, METH_NOARGS, "Retrieve DBZero cache statistics"},
    {"clear_cache", &py::clearCache, METH_NOARGS, "Clear DBZero cache"},
    {"list", (PyCFunction)&py::makeList, METH_FASTCALL, "Create a new DBZero list instance"},
    {"index", (PyCFunction)&py::makeIndex, METH_FASTCALL, "Create a new DBZero index instance"},
    {"tuple", (PyCFunction)&py::makeTuple, METH_FASTCALL, "Create a new DBZero tuple instance"},
    {"set", (PyCFunction)&py::makeSet, METH_FASTCALL, "Create a new DBZero set instance"},
    {"dict", (PyCFunction)&py::makeDict, METH_VARARGS | METH_KEYWORDS, "Create a new DBZero dict instance"},
    {"block", (PyCFunction)&py::makeBlock, METH_VARARGS | METH_KEYWORDS, "Create a new DBZero pandas block instance"},
    {"dataframe", (PyCFunction)&py::makeDataFrame, METH_VARARGS | METH_KEYWORDS, "Create a new DBZero pandas dataframe instance"},
    {"bytearray", (PyCFunction)&py::makeByteArray, METH_FASTCALL, "Create a new DBZero bytearray instance"},
    {"get_prefix", (PyCFunction)&py::getPrefixName, METH_VARARGS, ""},
    {"get_current_prefix", &py::getCurrentPrefixName, METH_VARARGS, ""},
    {"tags", (PyCFunction)&py::makeObjectTagManager, METH_FASTCALL, ""},
    {"find", (PyCFunction)&py::find, METH_FASTCALL, ""},
    {"refresh", (PyCFunction)&py::refresh, METH_VARARGS, ""},
    {"get_state_num", &py::getStateNum, METH_VARARGS, ""},
    {"get_metrics", &py::getDBMetrics, METH_VARARGS, ""},
    {"snapshot", (PyCFunction)&py::getSnapshot, METH_FASTCALL, "Get snapshot of DBZero state"},
    {"describe", &py::describeObject, METH_VARARGS, "Get snapshot of DBZero state"},
    {"rename_field", &py::renameField, METH_VARARGS, "Get snapshot of DBZero state"},
    {"is_singleton", &py::isSingleton, METH_VARARGS, "Check if a specific instance is a DBZero singleton"},
    {"getrefcount", &py::getRefCount, METH_VARARGS, "Get DBZero ref counts"},
    {"no", (PyCFunction)&py::negTagSet, METH_FASTCALL, "Tag negation function"},
    {"is_tag", (PyCFunction)&py::MemoObject_IsTag, METH_FASTCALL, "Checks if a specific Memo instance is a tag"},
    {"to_dict", (PyCFunction)&py::toDict, METH_FASTCALL, "Serialize DBZero object as a Python dict"},
    {"build_flags", &py::getBuildFlags, METH_NOARGS, "Retrieve DBZero library build flags"},
    {"serialize", (PyCFunction)&py::pySerialize, METH_FASTCALL, "Serialize DBZero serializable instance"},
    {"deserialize", (PyCFunction)&py::pyDeserialize, METH_FASTCALL, "Serialize DBZero serializable instance"},
    {"make_enum", (PyCFunction)&py::makeEnum, METH_VARARGS | METH_KEYWORDS, "Define new or retrieve existing Enum type"},
    {"is_enum_value", (PyCFunction)&py::isEnumValue, METH_FASTCALL, "Check if parameter represents a DBZero enum value"},
    {"split_by", (PyCFunction)&py::splitBy, METH_VARARGS | METH_KEYWORDS, "Split query iterator by a given criteria"},
    {"filter", (PyCFunction)&py::filter, METH_VARARGS | METH_KEYWORDS, "Filter with a Python callable"},
#ifndef NDEBUG
    {"dbg_write_bytes", &py::writeBytes, METH_VARARGS, "Debug function"},
    {"dbg_free_bytes", &py::freeBytes, METH_VARARGS, "Debug function"},
    {"dbg_read_bytes", &py::readBytes, METH_VARARGS, "Debug function"},
#endif
    {NULL} // Sentinel
};

static struct PyModuleDef dbzero_ce_module = {
    PyModuleDef_HEAD_INIT,
    "dbzero_ce",
    NULL,
    -1,
    DBZeroCE_Methods
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
    if (PyModule_AddObject(mod, /*py_type->tp_name*/ "xxx", (PyObject *)py_type) < 0) {
        Py_DECREF(py_type);
        Py_DECREF(mod);    
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
        &py::PyObjectIteratorType,        
        &py::ByteArrayObjectType,
        &py::PyEnumType, 
        &py::PyEnumValueType,
        &py::PyClassFieldsType,
        &py::PyFieldDefType        
    };
    
    // register all types
    try {
        for (auto py_type: types) {
            initPyType(mod, py_type);
        }
    } catch (const std::exception &e) {
        // set python error
        PyErr_SetString(PyExc_RuntimeError, e.what());        
        return NULL;
    }
  
    return mod;
}
