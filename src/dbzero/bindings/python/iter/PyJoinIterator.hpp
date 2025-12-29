// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/tags/JoinIterator.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python

{

    using JoinIterator = db0::object_model::JoinIterator;
    using PyJoinIterator = PySharedWrapper<JoinIterator, false>;

    PyJoinIterator *PyJoinIterator_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<PyJoinIterator*> PyJoinIteratorDefault_new();
    void PyJoinIterator_del(PyJoinIterator *);
    
    extern PyTypeObject PyJoinIteratorType;
    
    bool PyJoinIterator_Check(PyObject *);
    
}