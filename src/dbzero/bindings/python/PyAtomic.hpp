// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/workspace/AtomicContext.hpp>

namespace db0::python

{
    
    using PyAtomic = PyWrapper<db0::AtomicContext, false>;
    
    PyAtomic *PyAtomic_new(PyTypeObject *type, PyObject *, PyObject *);
    PyAtomic *PyAtomicDefault_new();
    void PyAPI_PyAtomic_del(PyAtomic *);
    
    extern PyTypeObject PyAtomicType;
    
    bool PyAtomic_Check(PyObject *);
        
    PyObject *PyAPI_PyAtomic_cancel(PyObject *, PyObject *);
    PyObject *PyAPI_PyAtomic_close(PyObject *, PyObject *);
    PyObject *PyAPI_beginAtomic(PyObject *self, PyObject *const *, Py_ssize_t nargs);    
       
}