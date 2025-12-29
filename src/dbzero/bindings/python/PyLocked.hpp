// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/workspace/LockedContext.hpp>

namespace db0::python

{
    
    using PyLocked = PyWrapper<db0::LockedContext, false>;
    
    PyLocked *PyLocked_new(PyTypeObject *type, PyObject *, PyObject *);
    PyLocked *PyLockedDefault_new();
    void PyAPI_PyLocked_del(PyLocked *);
    
    extern PyTypeObject PyLockedType;

    bool PyLocked_Check(PyObject *);
    
    PyObject *PyAPI_PyLocked_close(PyObject *, PyObject *);
    PyObject *PyAPI_PyLocked_get_mutation_log(PyObject *, PyObject *);
    PyObject *PyAPI_beginLocked(PyObject *self, PyObject *const *, Py_ssize_t nargs);    
       
}