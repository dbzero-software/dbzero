#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::object_model::pandas 

{
    
    class Block;
    
}

namespace db0::python 

{
    
    // PANDAS BLOCK //

    using PandasBlockObject = PyWrapper<db0::object_model::pandas::Block>;
    
    PandasBlockObject *PandasBlockObject_new(PyTypeObject *type, PyObject *, PyObject *);
    void PandasBlockObject_del(PandasBlockObject* self);

    void PandasBlockObject_dtype(PandasBlockObject* self);

    // Py_ssize_t PandasBlockObject_nbytes(PandasBlockObject* self);

    PyObject *PandasBlockObject_GetItem(PandasBlockObject* self, Py_ssize_t i);

    int PandasBlockObject_SetItem(PandasBlockObject *block_obj, Py_ssize_t i, PyObject *value);

    Py_ssize_t PandasBlockObject_len(PandasBlockObject* self);

    // bool PandasBlockObject_eq(PandasBlockObject* self, PandasBlockObject* other);

    // PandasBlockObject* PandasBlockObject_copy(PandasBlockObject* self);

    PandasBlockObject *makeBlock(PyObject *, PyObject *, PyObject *);

    shared_py_object<PandasBlockObject*> BlockDefaultObject_new();

    PyObject * PandasBlockObject_GetStorageClass(PandasBlockObject *);

    extern PyTypeObject PandasBlockObjectType;

    bool PandasBlock_Check(PyObject *obj);
    bool PandasBlockType_Check(PyTypeObject *type);

}