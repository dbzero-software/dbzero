// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python

{

    using ObjectIterator = db0::object_model::ObjectIterator;
    using PyObjectIterator = PySharedWrapper<ObjectIterator, false>;
    
    PyObjectIterator *PyObjectIterator_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<PyObjectIterator*> PyObjectIteratorDefault_new();
    void PyObjectIterator_del(PyObjectIterator *);
    
    extern PyTypeObject PyObjectIteratorType;
    
    bool PyObjectIterator_Check(PyObject *);
    
}