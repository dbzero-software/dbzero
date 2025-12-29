// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/tags/JoinIterable.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python

{

    using JoinIterable = db0::object_model::JoinIterable;
    using PyJoinIterable = PySharedWrapper<JoinIterable, false>;
    
    PyJoinIterable *PyJoinIterable_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<PyJoinIterable*> PyJoinIterableDefault_new();
    void PyJoinIterable_del(PyJoinIterable *);
    PyObject *PyAPI_PyJoinIterable_iter(PyJoinIterable *);
    Py_ssize_t PyAPI_PyJoinIterable_len(PyJoinIterable *);
    
    extern PyTypeObject PyJoinIterableType;
    
    bool PyJoinIterable_Check(PyObject *);
    
    /**
     * db0.join implementation
     * @return PyJoinIterable
    */    
    PyObject *PyAPI_join(PyObject *, PyObject *args, PyObject *kwargs);

}