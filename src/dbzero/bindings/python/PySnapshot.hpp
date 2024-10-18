#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/workspace/WorkspaceView.hpp>
#include "WhichType.hpp"
#include "shared_py_object.hpp"

namespace db0::python

{
    
    using PySnapshotObject = PySharedWrapper<db0::WorkspaceView, false>;
    
    PySnapshotObject *PySnapshot_new(PyTypeObject *type, PyObject *, PyObject *);
    PySnapshotObject *PySnapshotDefault_new();
    void PySnapshot_del(PySnapshotObject* self);
    
    extern PyTypeObject PySnapshotObjectType;
    
    PySnapshotObject *tryGetSnapshot(std::optional<std::uint64_t> state_num,
        const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums);
    bool PySnapshot_Check(PyObject *);
    
    PyObject* PySnapshot_fetch(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PySnapshot_find(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PySnapshot_deserialize(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_PySnapshot_close(PyObject *, PyObject *);
    
    PyObject *PySnapshot_enter(PyObject *, PyObject *);
    PyObject *PyAPI_PySnapshot_exit(PyObject *, PyObject *);
    
    db0::WorkspaceView *extractWorkspaceViewPtr(PySnapshotObject);

    template <> bool Which_TypeCheck<PySnapshotObject>(PyObject *py_object);
    
}