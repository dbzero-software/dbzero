// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <cstdint>
#include <array>
#include <dbzero/bindings/python/WhichType.hpp>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>
#include <dbzero/bindings/python/MemoObject.hpp>

namespace db0::object_model

{
    
    class Object;
    class List;
    class Index;
    
}

namespace db0::python

{
    
    using ListObject = PyWrapper<db0::object_model::List>;
    using IndexObject = PyWrapper<db0::object_model::Index>;
    using ObjectId = db0::object_model::ObjectId;

    struct PyObjectId
    {        
        using StorageClass = db0::object_model::StorageClass;
        PyObject_HEAD
        ObjectId m_object_id;
    };
    
    PyObject *ObjectId_repr(PyObject *);
    
    extern PyTypeObject ObjectIdType;
    
    // retrieve UUID of a dbzero object
    PyObject *PyAPI_getUUID(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_getUUID(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    bool ObjectId_Check(PyObject *obj);
    
    // Method to pickle the object
    PyObject *ObjectId_reduce(PyObject *);
    int ObjectId_init(PyObject* self, PyObject* state);
    PyObject *ObjectId_richcompare(PyObject *self, PyObject *other, int op);    
    
    template <> bool Which_TypeCheck<PyObjectId>(PyObject *py_object);

}
