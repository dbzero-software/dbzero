// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <Python.h>

namespace db0::python
{

    struct PyFieldNamespace {
        PyObject_HEAD
        PyTypeObject *memo_type = nullptr;
        PyObject *refs = nullptr;
    };

    struct PyFieldRef {
        PyObject_HEAD
        PyTypeObject *memo_type = nullptr;
        PyTypeObject *owner_type = nullptr;
        PyObject *field_name = nullptr;
        bool declared = false;
    };

    extern PyTypeObject PyFieldNamespaceType;
    extern PyTypeObject PyFieldRefType;

    bool PyFieldRef_Check(PyObject *);
    PyTypeObject *PyFieldRef_getMemoType(PyObject *);
    const char *PyFieldRef_getFieldName(PyObject *);

    PyObject *PyAPI_fieldsOf(PyObject *, PyObject *const *args, Py_ssize_t nargs);

}
