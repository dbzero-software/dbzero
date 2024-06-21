#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/object_model/tags/ObjectTagManager.hpp>

namespace db0::python

{

    using ObjectTagManager = db0::object_model::ObjectTagManager;
    using PyObjectTagManager = PyWrapper<ObjectTagManager, false>;

    PyObjectTagManager *PyObjectTagManager_new(PyTypeObject *type, PyObject *, PyObject *);
    void PyObjectTagManager_del(PyObjectTagManager* self);
    PyObject *PyObjectTagManager_add(PyObjectTagManager *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyObjectTagManager_add_binary(PyObjectTagManager *, PyObject *obj);
    PyObject *PyObjectTagManager_remove(PyObjectTagManager *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyObjectTagManager_remove_binary(PyObjectTagManager *, PyObject *obj);
    
    extern PyTypeObject PyObjectTagManagerType;
    
    PyObjectTagManager *makeObjectTagManager(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
}