#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>
#include <dbzero/object_model/list/List.hpp>

namespace db0::python

{
    
    using ListObject = PyWrapper<db0::object_model::List>;
    
    ListObject *ListObject_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<ListObject*> ListDefaultObject_new();

    void PyAPI_ListObject_del(ListObject* self);
    // python array methods methods
    PyObject *PyAPI_ListObject_clear(ListObject *list_obj);
    PyObject *PyAPI_ListObject_copy(ListObject *list_obj);
    PyObject *PyAPI_ListObject_count(ListObject *list_obj, PyObject *const *args, Py_ssize_t nargs);
    // python list as number methods
    PyObject *PyAPI_ListObject_multiply(ListObject *list_obj, PyObject *elem);
    PyObject *PyAPI_ListObject_GetItem(ListObject *list_obj, Py_ssize_t i);
    
    extern PyTypeObject ListObjectType;
    
    // construct from a Python list (or empty)
    shared_py_object<ListObject*> makeDB0List(db0::swine_ptr<Fixture> &, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_makeList(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    
    bool ListObject_Check(PyObject *);
    
    PyObject *tryLoadPyList(PyObject *py_list, PyObject *kwargs);
    PyObject *tryLoadList(ListObject *list, PyObject *kwargs);
}