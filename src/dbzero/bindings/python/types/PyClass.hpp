// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python

{

    // immutable Class object
    using ClassObject = PySharedWrapper<const db0::object_model::Class>;
    
    ClassObject *ClassObject_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<ClassObject*> ClassDefaultObject_new();
    void ClassObject_del(ClassObject *);
        
    PyObject *PyAPI_PyClass_type(PyObject *, PyObject *);
    PyObject *PyAPI_PyClass_type_exists(PyObject *, PyObject *);
    PyObject *PyAPI_PyClass_get_attributes(PyObject *, PyObject *);
    PyObject *PyAPI_PyClass_type_info(PyObject *, PyObject *);

    extern PyTypeObject ClassObjectType;
    
    ClassObject *makeClass(std::shared_ptr<db0::object_model::Class>);
    bool PyClassObject_Check(PyObject *);
    
    PyObject *tryGetPyClassAttributes(PyObject *);
    PyObject *tryGetClassAttributes(const db0::object_model::Class &);
    PyObject *tryGetTypeInfo(const db0::object_model::Class &);
    
}