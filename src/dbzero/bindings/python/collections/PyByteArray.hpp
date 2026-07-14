// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/bytes/ByteArray.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python

{
    
    using ByteArrayObject = PyWrapper<db0::object_model::ByteArray>;
    
    ByteArrayObject *ByteArrayObject_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<ByteArrayObject*> ByteArrayDefaultObject_new();
    void PyAPI_ByteArrayObject_del(ByteArrayObject* self);
    
    extern PyTypeObject ByteArrayObjectType;
    
    ByteArrayObject *PyAPI_makeByteArray(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    bool ByteArrayObject_Check(PyObject *);
    
}