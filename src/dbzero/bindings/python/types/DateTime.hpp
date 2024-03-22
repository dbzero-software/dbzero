#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/datetime/DateTime.hpp>

namespace db0::python 

{

    using DateTimeObject = PyWrapper<db0::object_model::DateTime>;
    
    DateTimeObject *DateTimeObject_new(PyTypeObject *type, PyObject *, PyObject *);
    DateTimeObject *DateTimeDefaultObject_new();
    void DateTimeObject_del(DateTimeObject* self);
    extern PyTypeObject DateTimeObjectType;
    
    DateTimeObject *makeDateTime(PyObject *, PyObject* args, PyObject* kwargs);
    bool DateTimeObject_Check(PyObject *);
    
}