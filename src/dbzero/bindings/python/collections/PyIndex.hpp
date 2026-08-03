// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>

namespace db0::python

{

    using IndexObject = PySharedWrapper<db0::object_model::Index, false>;
    
    IndexObject *IndexObject_new(PyTypeObject *type, PyObject *, PyObject *);
    IndexObject* IndexDefaultObject_new();
    IndexObject* IndexDefaultObject_new(db0::swine_ptr<Fixture> fixture, bool passive, bool managed = false);
    void PyAPI_IndexObject_del(IndexObject* self);
    Py_ssize_t PyAPI_IndexObject_len(IndexObject *);
    
    // Index operations
    PyObject *PyAPI_IndexObject_add(IndexObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_IndexObject_remove(IndexObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_IndexObject_sort(IndexObject *, PyObject *args, PyObject *kwargs);
    PyObject *PyAPI_IndexObject_range(IndexObject *, PyObject *args, PyObject *kwargs);
    PyObject *PyAPI_IndexObject_flush(IndexObject *);
    PyObject *PyAPI_IndexObject_clear(IndexObject *);
    
    extern PyTypeObject IndexObjectType;
    
    PyObject *PyAPI_makeIndex(PyObject *self, PyObject *args, PyObject *kwargs);
    bool IndexObject_Check(PyObject *);
    bool isValidIndexedFieldName(PyTypeObject *memo_type, const char *field_name);

}
