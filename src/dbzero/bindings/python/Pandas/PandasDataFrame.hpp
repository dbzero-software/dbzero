#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>

namespace db0::object_model::pandas 

{

    class DataFrame;
    
}

namespace db0::python 

{

    // PANDAS DATAFRAME //

    using PandasDataFrameObject = PyWrapper<db0::object_model::pandas::DataFrame>;
    
    PandasDataFrameObject *PandasDataFrameObject_new(PyTypeObject *type, PyObject *, PyObject *);
    void PandasDataFrameObject_del(PandasDataFrameObject* self);

    PyObject *PandasDataFrameObject_GetItem(PandasDataFrameObject *DataFrame_obj, PyObject *const *args, Py_ssize_t nargs);

    int PandasDataFrameObject_SetItem(PandasDataFrameObject *block_obj, Py_ssize_t i, PyObject *value);

    extern PyTypeObject PandasDataFrameObjectType;

    PandasDataFrameObject *makeDataFrame(PyObject *, PyObject *, PyObject *);
    

    bool PandasDataFrame_Check(PyObject *obj);
    bool PandasDataFrameType_Check(PyTypeObject *type);

}   