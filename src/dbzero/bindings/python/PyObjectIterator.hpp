#pragma once

#include "PyWrapper.hpp"
#include <dbzero/object_model/tags/TypedObjectIterator.hpp>

namespace db0::python

{

    using PyObjectIterator = PyWrapper<db0::object_model::ObjectIterator>;
    using PyTypedObjectIterator = PyWrapper<db0::object_model::TypedObjectIterator>;
    
    PyObjectIterator *PyObjectIterator_new(PyTypeObject *type, PyObject *, PyObject *);
    PyObjectIterator *PyObjectIteratorDefault_new();
    void PyObjectIterator_del(PyObjectIterator* self);

    PyTypedObjectIterator *PyTypedObjectIterator_new(PyTypeObject *type, PyObject *, PyObject *);
    void PyTypedObjectIterator_del(PyTypedObjectIterator *);

    extern PyTypeObject PyObjectIteratorType;
    extern PyTypeObject PyTypedObjectIteratorType;

    /**
     * db0.find implementation
     * returns either PyObjectIterator or PyTypedObjectIterator
    */
    PyObject *find(PyObject *, PyObject* const *args, Py_ssize_t nargs);
    
    bool ObjectIterator_Check(PyObject *);
    bool TypedObjectIterator_Check(PyObject *);
    
}