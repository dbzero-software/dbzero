// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyClassFields.hpp"
#include <dbzero/bindings/python/PyInternalAPI.hpp>

namespace db0::python

{

    PyClassFields *PyClassFields_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyClassFields*>(type->tp_alloc(type, 0));
    }
    
    PyClassFields *PyClassFieldsDefault_new() {
        return PyClassFields_new(&PyClassFieldsType, NULL, NULL);
    }
    
    PyClassFields *PyClassFields_create(PyTypeObject *memo_type)
    {
        auto py_class_fields = Py_OWN(PyClassFieldsDefault_new());
        py_class_fields->modifyExt().init(memo_type);
        return py_class_fields.steal();
    }

    PyFieldDef *PyFieldDef_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyFieldDef*>(type->tp_alloc(type, 0));
    }

    PyFieldDef *PyFieldDefDefault_new() {
        return PyFieldDef_new(&PyFieldDefType, NULL, NULL);
    }

    void PyClassFields_del(PyClassFields* self)
    {        
        // destroy associated DB0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    void PyFieldDef_del(PyFieldDef *self) 
    {
        // destroy associated DB0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    PyObject *tryPyClassFields_getattro(PyClassFields *self, PyObject *field_name)
    {
        auto field_def = self->ext().get(PyUnicode_AsUTF8(field_name));
        PyFieldDef *py_field_def = PyFieldDefDefault_new();
        py_field_def->modifyExt() = field_def;
        return (PyObject *)py_field_def;
    }

    PyObject *PyClassFields_getattro(PyClassFields *self, PyObject *attr) {
        return runSafe(tryPyClassFields_getattro, self, attr);
    }
    
    static PyMethodDef PyClassFields_methods[] = 
    {
        {NULL}
    };
    
    PyTypeObject PyClassFieldsType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.ClassFields",
        .tp_basicsize = PyClassFields::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyClassFields_del,
        .tp_getattro = reinterpret_cast<getattrofunc>(PyClassFields_getattro),
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "ClassFields object",
        .tp_methods = PyClassFields_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyClassFields_new,
        .tp_free = PyObject_Free
    };

    PyTypeObject PyFieldDefType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.FieldDef",
        .tp_basicsize = PyFieldDef::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyFieldDef_del,        
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "FieldDef object",        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyFieldDef_new,
        .tp_free = PyObject_Free
    };

    bool PyClassFields_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyClassFieldsType;
    }
    
    bool PyFieldDef_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyFieldDefType;
    }

}
