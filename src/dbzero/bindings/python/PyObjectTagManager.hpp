// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/object_model/tags/ObjectTagManager.hpp>

namespace db0::python

{

    using ObjectTagManager = db0::object_model::ObjectTagManager;
    using PyObjectTagManager = PyWrapper<ObjectTagManager, false>;

    PyObjectTagManager *PyObjectTagManager_new(PyTypeObject *type, PyObject *, PyObject *);
    void PyAPI_PyObjectTagManager_del(PyObjectTagManager* self);
    PyObject *PyAPI_PyObjectTagManager_add(PyObjectTagManager *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_PyObjectTagManager_add_binary(PyObjectTagManager *, PyObject *obj);
    PyObject *PyAPI_PyObjectTagManager_remove(PyObjectTagManager *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_PyObjectTagManager_remove_binary(PyObjectTagManager *, PyObject *obj);
    
    extern PyTypeObject PyObjectTagManagerType;
    
    PyObjectTagManager *makeObjectTagManager(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
}