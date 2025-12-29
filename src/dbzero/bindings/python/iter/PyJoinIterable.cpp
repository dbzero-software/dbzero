// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyJoinIterable.hpp"
#include "PyJoinIterator.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PyTagsAPI.hpp>
#include <dbzero/bindings/python/ArgParse.hpp>

namespace db0::python

{

    PyJoinIterable *PyJoinIterable_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyJoinIterable*>(type->tp_alloc(type, 0));
    }
    
    void PyJoinIterable_del(PyJoinIterable* self)
    {
        // destroy associated db0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    shared_py_object<PyJoinIterable*> PyJoinIterableDefault_new() {
        return { PyJoinIterable_new(&PyJoinIterableType, NULL, NULL), false };
    }
    
    PyObject *tryPyAPI_PyJoinIterable_iter(PyJoinIterable *py_iterable)
    {
        // getFixture to prevent segfault in case the associated context (e.g. snapshot) has been destroyed
        auto fixture = py_iterable->ext().getFixture();
        auto py_iter = PyJoinIteratorDefault_new();
        py_iter->makeNew(py_iterable->ext().iter());
        return py_iter.steal();
    }
    
    PyObject *PyAPI_PyJoinIterable_iter(PyJoinIterable *py_join)
    {
        PY_API_FUNC
        return runSafe(tryPyAPI_PyJoinIterable_iter, py_join);
    }
    
    Py_ssize_t tryPyJoinIterable_len(PyJoinIterable *py_join)
    {
        // getFixture to prevent segfault in case the associated context (e.g. snapshot) has been destroyed
        auto fixture = py_join->ext().getFixture();
        return py_join->ext().getSize();
    }

    Py_ssize_t PyAPI_PyJoinIterable_len(PyJoinIterable *py_join)
    {
        PY_API_FUNC
        return runSafe(tryPyJoinIterable_len, py_join);
    }
    
    int PyAPI_PyJoinIterable_bool(PyJoinIterable *py_join)
    {
        PY_API_FUNC
        // check if the iterable is empty
        if (py_join->ext().empty()) {
            return 0; // False
        }
        return 1; // True
    }

    static PyMappingMethods PyJoinIterable_as_mapping = {
        .mp_length = (lenfunc)PyAPI_PyJoinIterable_len,        
    };
    
    static PyNumberMethods PyJoinIterable_as_number = {
        .nb_bool = (inquiry)PyAPI_PyJoinIterable_bool
    };

    PyTypeObject PyJoinIterableType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "JoinIterable",
        .tp_basicsize = PyJoinIterable::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyJoinIterable_del,
        .tp_as_number = &PyJoinIterable_as_number,
        .tp_as_mapping = &PyJoinIterable_as_mapping,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero join iterable",
        .tp_iter = (getiterfunc)PyAPI_PyJoinIterable_iter,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyJoinIterable_new,
        .tp_free = PyObject_Free,
    };
    
    bool PyJoinIterable_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyJoinIterableType;
    }
    
    PyObject *PyAPI_join(PyObject *, PyObject *args, PyObject *kwargs)
    {
        Py_ssize_t num_args = PyTuple_Size(args);
        std::vector<PyObject*> args_data(num_args);
        for (Py_ssize_t i = 0; i < num_args; ++i) {
            args_data[i] = PyTuple_GetItem(args, i);
        }

        if (!kwargs) {
            // The "join" function requires "on" as keyword argument
            PyErr_SetString(PyExc_TypeError, "join() missing 1 required keyword argument: 'on'");
            return NULL;
        }
        
        auto on_arg = PyDict_GetItemString(kwargs, "on");
        if (!on_arg) {
            PyErr_SetString(PyExc_TypeError, "join() missing 1 required keyword argument: 'on'");
            return NULL;
        }
        
        const char prefix_arg[] = "prefix";
        const char *prefix_name = nullptr;      
        PyObject *py_prefix_name = PyDict_GetItemString(kwargs, "prefix");
        if (py_prefix_name) {
            prefix_name = parseStringLikeArgument(py_prefix_name, "join", prefix_arg);
            if (!prefix_name) {
                return nullptr;
            }
        }
        
        PY_API_FUNC
        return runSafe(joinIn, PyToolkit::getPyWorkspace().getWorkspace(), (PyObject* const*)args_data.data(),
            num_args, on_arg, nullptr, prefix_name);
    }
    
}