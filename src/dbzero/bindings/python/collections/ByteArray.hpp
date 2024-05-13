#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/bytes/ByteArray.hpp>

namespace db0::python 

{
    using ByteArrayObject = PyWrapper<db0::object_model::ByteArray>;
    
    ByteArrayObject *ByteArrayObject_new(PyTypeObject *type, PyObject *, PyObject *);
    ByteArrayObject *ByteArrayDefaultObject_new();
    void ByteArrayObject_del(ByteArrayObject* self);
    
    extern PyTypeObject ByteArrayObjectType;
    
    ByteArrayObject *makeByteArray(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    bool ByteArrayObject_Check(PyObject *);
    
}