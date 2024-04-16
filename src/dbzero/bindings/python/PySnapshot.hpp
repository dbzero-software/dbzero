#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/workspace/WorkspaceView.hpp>
#include "WhichType.hpp"

namespace db0::python

{
    
    using PySnapshotObject = PyWrapper<db0::WorkspaceView>;
    
    PySnapshotObject *PySnapshot_new(PyTypeObject *type, PyObject *, PyObject *);
    PySnapshotObject *PySnapshotDefault_new();
    void PySnapshot_del(PySnapshotObject* self);
    
    extern PyTypeObject PySnapshotObjectType;
    
    PySnapshotObject *makeSnapshot(PyObject *, PyObject *args);
    bool PySnapshot_Check(PyObject *);
    
    PyObject *PySnapshot_fetch(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PySnapshot_close(PyObject *, PyObject *);

    PyObject *PySnapshot_enter(PyObject *, PyObject *);
    PyObject *PySnapshot_exit(PyObject *, PyObject *);
    
    db0::WorkspaceView *extractWorkspaceViewPtr(PySnapshotObject );

    template <> bool Which_TypeCheck<PySnapshotObject>(PyObject *py_object);
    
}