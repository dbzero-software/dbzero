// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/class/ClassFields.hpp>

namespace db0::python

{

    using PyClassFields = PyWrapper<db0::object_model::ClassFields, false>;
    using PyFieldDef = PyWrapper<db0::object_model::FieldDef, false>;
    
    PyClassFields *PyClassFields_new(PyTypeObject *type, PyObject *, PyObject *);    
    PyClassFields *PyClassFieldsDefault_new();
    PyClassFields *PyClassFields_create(PyTypeObject *memo_type);
    
    PyFieldDef *PyFieldDef_new(PyTypeObject *type, PyObject *, PyObject *);    
    PyFieldDef *PyFieldDefDefault_new();

    void PyClassFields_del(PyClassFields *);
    void PyFieldDef_del(PyFieldDef *);
    
    PyObject *PyClassFields_getattro(PyClassFields *, PyObject *name);
    
    extern PyTypeObject PyClassFieldsType;
    extern PyTypeObject PyFieldDefType;
    
    bool PyClassFields_Check(PyObject *);

    bool PyFieldDef_Check(PyObject *);

}