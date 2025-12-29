// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <cstdint>
#include <dbzero/bindings/python/types/PyObjectId.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include "shared_py_object.hpp"
#include <type_traits>
#include "PyToolkit.hpp"
    
namespace db0

{

    class Snapshot;

}

namespace db0::object_model

{

    class ObjectIterable;
    
}

namespace db0::python

{   
        
    using ObjectId = db0::object_model::ObjectId;
    using ObjectIterable = db0::object_model::ObjectIterable;
    
    /**
     * Universal find implementation (works on Workspace or WorkspaceView)
     * @param context - the optional context / scope to be attached to the result query
     * @param prefix_name - optional scope (i.e. defined with a prefix name)
     * @return PyObjectIterable
    */
    PyObject *findIn(db0::Snapshot &, PyObject* const *args, Py_ssize_t nargs, PyObject *context = nullptr,
        const char *prefix_name = nullptr);
    
    PyObject *PyAPI_splitBy(PyObject *, PyObject *args, PyObject *kwargs);
    
    PyObject *PyAPI_selectModCandidates(PyObject *, PyObject *args, PyObject *kwargs);

    PyObject *PyAPI_splitBySnapshots(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
    // convert a db0::serial::Serializable to bytes
    PyObject *PyAPI_serialize(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
    // convert bytes to instance (e.g. ObjectIterator)
    PyObject *PyAPI_deserialize(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
    PyObject *joinIn(db0::Snapshot &, PyObject* const *args, Py_ssize_t nargs, PyObject *join_on_arg, 
        PyObject *context = nullptr, const char *prefix_name = nullptr);
    
}