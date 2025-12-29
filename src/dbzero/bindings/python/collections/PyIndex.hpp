// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python

{

    using IndexObject = PyWrapper<db0::object_model::Index>;
    
    IndexObject *IndexObject_new(PyTypeObject *type, PyObject *, PyObject *);
    IndexObject* IndexDefaultObject_new();
    void PyAPI_IndexObject_del(IndexObject* self);
    Py_ssize_t PyAPI_IndexObject_len(IndexObject *);
    
    // Index operations
    PyObject *PyAPI_IndexObject_add(IndexObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_IndexObject_remove(IndexObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_IndexObject_sort(IndexObject *, PyObject *args, PyObject *kwargs);
    PyObject *PyAPI_IndexObject_range(IndexObject *, PyObject *args, PyObject *kwargs);
    PyObject *PyAPI_IndexObject_flush(IndexObject *);
    
    extern PyTypeObject IndexObjectType;
    
    IndexObject *PyAPI_makeIndex(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    bool IndexObject_Check(PyObject *);

}