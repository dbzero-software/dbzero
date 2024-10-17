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
    PyObject *PyClass_has_type(PyObject *, PyObject *);
    PyObject *PyClass_type(PyObject *, PyObject *);
    PyObject *PyClass_get_attributes(PyObject *, PyObject *);
    PyObject *PyClass_type_info(PyObject *, PyObject *);

    extern PyTypeObject ClassObjectType;

    ClassObject *makeClass(std::shared_ptr<const db0::object_model::Class>);
    bool PyClassObject_Check(PyObject *);
    
    PyObject *tryGetAttributes(PyObject *);
    PyObject *tryGetClassAttributes(const db0::object_model::Class &);
    PyObject *tryGetTypeInfo(const db0::object_model::Class &);
    
}