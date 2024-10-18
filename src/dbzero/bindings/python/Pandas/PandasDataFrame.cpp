#include "PandasDataFrame.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/object_model/pandas/Dataframe.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python

{
    
    PyObject *PandasDataFrameObject_GetItem(PandasDataFrameObject *DataFrame_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "get_block() takes exactly one argument");
            return NULL;
        }
        return DataFrame_obj->ext().getBlock(PyLong_AsLong(args[0])).steal();
    }

    int PandasDataFrameObject_SetItem(PandasDataFrameObject *dataframe_obj, Py_ssize_t i, PyObject *value)
    {
        dataframe_obj->modifyExt().setBlock(i, value);
        return 0;
    }

    PyObject *PandasDataFrameObject_append(PandasDataFrameObject *dataframe_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "append() takes exactly one argument");
            return NULL;
        }
        
        dataframe_obj->modifyExt().appendBlock(args[0]);
        Py_RETURN_NONE;
    }

    PyObject *PandasDataFrameObject_append_index(PandasDataFrameObject *dataframe_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "append() takes exactly one argument");
            return NULL;
        }
        
        dataframe_obj->modifyExt().appendIndex(args[0]);
        Py_RETURN_NONE;
    }

    static PyMethodDef PandasDataFrameObject_methods[] = {
        {"append_index", (PyCFunction)PandasDataFrameObject_append_index, METH_FASTCALL, "Append index element"},
        {"append_block", (PyCFunction)PandasDataFrameObject_append, METH_FASTCALL, "Append block"},
        {"get_block", (PyCFunction)PandasDataFrameObject_GetItem, METH_FASTCALL, "Get block"},
        {NULL}
    };

    PyTypeObject PandasDataFrameObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.dataframe",
        .tp_basicsize = PandasDataFrameObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PandasDataFrameObject_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero dataframe object",
        .tp_methods = PandasDataFrameObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PandasDataFrameObject_new,
        .tp_free = PyObject_Free,        
    };
    
    PandasDataFrameObject *PandasDataFrameObject_new(PyTypeObject *type, PyObject *args, PyObject *) {
        return reinterpret_cast<PandasDataFrameObject*>(type->tp_alloc(type, 0));
    }
    
    void PandasDataFrameObject_del(PandasDataFrameObject* dataframe_obj)
    {
        PY_API_FUNC
        // destroy associated DB0 DataFrame instance
        dataframe_obj->destroy();
        Py_TYPE(dataframe_obj)->tp_free((PyObject*)dataframe_obj);
    }
    
    PandasDataFrameObject *makeDataFrame(PyObject *, PyObject *, PyObject *)
    {
        PY_API_FUNC
        // make actual DBZero instance, use default fixture
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        db0::FixtureLock lock(fixture);
        auto dataframe_obj = PandasDataFrameObject_new(&PandasDataFrameObjectType, NULL, NULL);
        db0::object_model::pandas::DataFrame::makeNew(&dataframe_obj->modifyExt(), *lock);
        lock->getLangCache().add(dataframe_obj->ext().getAddress(), dataframe_obj);
        return dataframe_obj;
    }

    bool PandasDataFrameType_Check(PyTypeObject *type) {
        return type->tp_new == reinterpret_cast<newfunc>(PandasDataFrameObject_new);
    }

    bool PandasDataFrame_Check(PyObject *obj) {
        return PandasDataFrameType_Check(Py_TYPE(obj));
    }

}