// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
    PyObject *tryPyGetSnapshot(PyObject *args, PyObject *kwargs);
    
    bool PySnapshot_Check(PyObject *);
    
    PyObject *PyAPI_getSnapshotOf(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    // Get the number of prefixes in the snapshot
    Py_ssize_t PyAPI_PySnapshot_len(PySnapshotObject *);
    // Check if a specific prefix name belongs to the snapshot
    int PyAPI_PySnapshot_HasItem(PySnapshotObject *, PyObject *prefix);
    
    template <> bool Which_TypeCheck<PySnapshotObject>(PyObject *py_object);
    
}