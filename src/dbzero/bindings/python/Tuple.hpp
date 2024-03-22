#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/object_model/tuple/Tuple.hpp>

namespace db0::python 

{
    
    using TupleObject = PyWrapper<db0::object_model::Tuple>;
    
    TupleObject *TupleObject_new(PyTypeObject *type, PyObject *, PyObject *);
    TupleObject *TupleDefaultObject_new();
    void TupleObject_del(TupleObject* self);
    Py_ssize_t TupleObject_len(TupleObject *);
    // python array methods methods
    PyObject *TupleObject_count(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs);
    PyObject *TupleObject_index(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs);
    PyObject *TupleObject_GetItem(TupleObject *tuple_obj, Py_ssize_t i);
    
    extern PyTypeObject TupleObjectType;
    
    TupleObject *makeTuple(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    bool TupleObject_Check(PyObject *);
    
}