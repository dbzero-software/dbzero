// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/tags/ObjectIterable.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python

{

    using ObjectIterable = db0::object_model::ObjectIterable;
    using PyObjectIterable = PySharedWrapper<ObjectIterable, false>;
    
    PyObjectIterable *PyObjectIterable_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<PyObjectIterable*> PyObjectIterableDefault_new();
    void PyObjectIterable_del(PyObjectIterable *);
    PyObject *PyAPI_PyObjectIterable_iter(PyObjectIterable *);
    Py_ssize_t PyAPI_PyObjectIterable_len(PyObjectIterable *);
    
    extern PyTypeObject PyObjectIterableType;
    
    bool PyObjectIterable_Check(PyObject *);
    
    /**
     * db0.find implementation
     * @return PyObjectIterable
    */    
    PyObject *PyAPI_find(PyObject *, PyObject *args, PyObject *kwargs);

}