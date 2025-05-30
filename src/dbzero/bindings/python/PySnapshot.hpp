#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/workspace/WorkspaceView.hpp>
#include "WhichType.hpp"
#include "shared_py_object.hpp"

namespace db0::python

{
    
    using PySnapshotObject = PySharedWrapper<db0::Snapshot, false>;
    
    PySnapshotObject *PySnapshot_new(PyTypeObject *type, PyObject *, PyObject *);
    PySnapshotObject *PySnapshotDefault_new();
    void PyAPI_PySnapshot_del(PySnapshotObject* self);
    
    extern PyTypeObject PySnapshotObjectType;
    
    PySnapshotObject *tryGetSnapshot(std::optional<std::uint64_t> state_num,
        const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums, bool frozen);
    bool PySnapshot_Check(PyObject *);
        
    PyObject *PyAPI_getSnapshotOf(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
    template <> bool Which_TypeCheck<PySnapshotObject>(PyObject *py_object);
    
}