#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/list/List.hpp>

namespace db0::python 

{
    
    using ListObject = PyWrapper<db0::object_model::List>;
    
    ListObject *ListObject_new(PyTypeObject *type, PyObject *, PyObject *);
    ListObject *ListDefaultObject_new();
    void ListObject_del(ListObject* self);
    // python array methods methods
    PyObject *ListObject_clear(ListObject *list_obj);
    PyObject *ListObject_copy(ListObject *list_obj);
    PyObject *ListObject_count(ListObject *list_obj, PyObject *const *args, Py_ssize_t nargs);

    // python list as number methods
    PyObject *ListObject_multiply(ListObject *list_obj, PyObject *elem);

    PyObject *ListObject_GetItem(ListObject *list_obj, Py_ssize_t i);

    extern PyTypeObject ListObjectType;
    
    ListObject *makeList(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    bool ListObject_Check(PyObject *);
    
}