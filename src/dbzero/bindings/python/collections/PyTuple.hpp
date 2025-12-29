// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python 

{
    
    using TupleObject = PyWrapper<db0::object_model::Tuple>;
    
    TupleObject *TupleObject_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<TupleObject*> TupleDefaultObject_new();
    
    void PyAPI_TupleObject_del(TupleObject* self);
    Py_ssize_t PyAPI_TupleObject_len(TupleObject *);
    // python array methods methods
    PyObject *PyAPI_TupleObject_count(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_TupleObject_index(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_TupleObject_GetItem(TupleObject *tuple_obj, Py_ssize_t i);
    
    extern PyTypeObject TupleObjectType;
        
    shared_py_object<TupleObject*> tryMake_DB0Tuple(db0::swine_ptr<Fixture> &, PyObject *const *args,
        Py_ssize_t nargs, AccessFlags
    );
    PyObject *PyAPI_makeTuple(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    
    bool TupleObject_Check(PyObject *);
    
    // Unload db0.Tuple instance to memory
    PyObject *tryLoadTuple(TupleObject *, PyObject *kwargs, std::unordered_set<const void*> *load_stack_ptr = nullptr);
    // Unload Python tuple instance to memory (in case it contains db0 objects)
    PyObject *tryLoadPyTuple(PyObject *, PyObject *kwargs, std::unordered_set<const void*> *load_stack_ptr = nullptr);
    
}