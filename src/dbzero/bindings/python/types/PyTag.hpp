// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>
#include <dbzero/object_model/tags/TagDef.hpp>

namespace db0::python

{

    using TagDef = db0::object_model::TagDef;
    using PyTag = PyWrapper<TagDef, false>;
    
    PyTag *PyTag_new(PyTypeObject *type, PyObject *, PyObject *);
    PyTag *PyTagDefault_new();
    
    void PyTag_del(PyTag *);
    extern PyTypeObject PyTagType;
    
    PyObject *PyAPI_as_tag(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
    bool PyTag_Check(PyObject *);

}