#include<iostream>
#include "Memo.hpp"
#include "PyAPI.hpp"
#include "PyInternalAPI.hpp"
#include "ObjectId.hpp"
#include "List.hpp"
#include "Index.hpp"
#include "Set.hpp"
#include "Tuple.hpp"
#include "Dict.hpp"
#include <dbzero/bindings/python/PyWorkspace.hpp>
#include "PyObjectTagManager.hpp"
#include "PyObjectIterator.hpp"
#include "PySnapshot.hpp"
#include "PyTagSet.hpp"
#include <dbzero/bindings/python/Pandas/PandasBlock.hpp>
#include <dbzero/bindings/python/Pandas/PandasDataFrame.hpp>

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
    {"uuid", (PyCFunction)&py::getObjectId, METH_FASTCALL, "Get unique object ID"},
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
    {"get_prefix", (PyCFunction)&py::getPrefixName, METH_VARARGS, ""},
    {"get_current_prefix", &py::getCurrentPrefixName, METH_VARARGS, ""},
    {"tags", (PyCFunction)&py::makeObjectTagManager, METH_FASTCALL, ""},
    {"find", (PyCFunction)&py::find, METH_FASTCALL, ""},
    {"refresh", (PyCFunction)&py::refresh, METH_VARARGS, ""},
    {"get_state_num", &py::getStateNum, METH_VARARGS, ""},
    {"get_metrics", &py::getDBMetrics, METH_VARARGS, ""},
    {"snapshot", &py::getSnapshot, METH_VARARGS, "Get snapshot of DBZero state"},
    {"describe", &py::describeObject, METH_VARARGS, "Get snapshot of DBZero state"},
    {"rename_field", &py::renameField, METH_VARARGS, "Get snapshot of DBZero state"},
    {"is_singleton", &py::isSingleton, METH_VARARGS, "Check if a specific instance is a DBZero singleton"},
    {"getrefcount", &py::getRefCount, METH_VARARGS, "Get DBZero ref counts"},
    {"no", (PyCFunction)&py::negTagSet, METH_FASTCALL, "Tag negation function"},
    {"is_tag", (PyCFunction)&py::MemoObject_IsTag, METH_FASTCALL, "Checks if a specific Memo instance is a tag"},
    {"to_dict", (PyCFunction)&py::toDict, METH_FASTCALL, "Serialize DBZero object as a Python dict"},
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

template <typename T> bool initPyType(PyObject *mod, const char *type_name, T &PyType)
{
    if (PyType_Ready(&PyType) < 0) {
        Py_DECREF(mod);
        return false;
    }
    
    Py_INCREF(&PyType);
    if (PyModule_AddObject(mod, type_name, (PyObject *)&PyType) < 0) {
        Py_DECREF(&PyType);
        Py_DECREF(mod);
        return false;
    }
    
    return true;
}

// Module initialization function for Python 3.x
PyMODINIT_FUNC PyInit_dbzero_ce(void)
{   
    auto mod = PyModule_Create(&dbzero_ce_module);
    
    if (!initPyType(mod, "ObjectId", py::ObjectIdType)) {
        return NULL;
    }

    if (!initPyType(mod, "List", py::ListObjectType)) {
        return NULL;
    }

    if (!initPyType(mod, "Index", py::IndexObjectType)) {
        return NULL;
    }

    if (!initPyType(mod, "Set", py::SetObjectType)) {
        return NULL;
    }

    if (!initPyType(mod, "Tuple", py::TupleObjectType)) {
        return NULL;
    }

    if (!initPyType(mod, "Dict", py::DictObjectType)) {
        return NULL;
    }

    if (!initPyType(mod, "Tags", py::PyObjectTagManagerType)) {
        return NULL;
    }

    if (!initPyType(mod, "Snapshot", py::PySnapshotObjectType)) {
        return NULL;
    }

    if (!initPyType(mod, "PandasBlock", py::PandasBlockObjectType)) {
        return NULL;
    }

    if (!initPyType(mod, "PandasDataFrame", py::PandasDataFrameObjectType)) {
        return NULL;
    }


    return mod;
}
