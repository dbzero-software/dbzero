// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/workspace/ReadOnlyContext.hpp>

namespace db0::python

{

    class PyReadOnlyContext
    {
    public:
        PyReadOnlyContext();
        ~PyReadOnlyContext();

        void close();

    private:
        bool m_active = true;
        PyObject *m_token = nullptr;
    };

    using PyReadOnly = PyWrapper<PyReadOnlyContext, false>;

    PyReadOnly *PyReadOnly_new(PyTypeObject *type, PyObject *, PyObject *);
    PyReadOnly *PyReadOnlyDefault_new();
    void PyAPI_PyReadOnly_del(PyReadOnly *);

    extern PyTypeObject PyReadOnlyType;

    bool PyReadOnly_Check(PyObject *);

    PyObject *PyAPI_PyReadOnly_close(PyObject *, PyObject *);
    PyObject *PyAPI_beginReadOnly(PyObject *self, PyObject *const *, Py_ssize_t nargs);
    int initReadOnlyContextSupport();

}
