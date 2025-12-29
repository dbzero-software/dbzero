// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/object_model/dict/DictIterator.hpp>

namespace db0::python 

{
    
    using DictObject = PyWrapper<db0::object_model::Dict>;
    using DictIteratorObject = PySharedWrapper<db0::object_model::DictIterator, false>;
    using AccessFlags = db0::AccessFlags;

    DictObject *DictObject_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<DictObject*> DictDefaultObject_new();
    
    void PyAPI_DictObject_del(DictObject* self);
    Py_ssize_t PyAPI_DictObject_len(DictObject *);
    PyObject *PyAPI_DictObject_GetItem(DictObject *dict_obj, PyObject *key);
    PyObject *PyAPI_DictObject_clear(DictObject *set_obj);
    PyObject *PyAPI_DictObject_copy(DictObject *set_obj);
    PyObject *PyAPI_DictObject_fromKeys(DictObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_DictObject_get(DictObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_DictObject_pop(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_DictObject_setDefault(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_DictObject_update(DictObject *, PyObject* args, PyObject* kwargs);
    PyObject *PyAPI_DictObject_keys(DictObject *dict_obj);
    PyObject *PyAPI_DictObject_values(DictObject *dict_obj);
    PyObject *PyAPI_DictObject_items(DictObject *dict_obj);
    void PyAPI_DictObject_del(DictObject* dict_obj);
    extern PyTypeObject DictObjectType;
    
    shared_py_object<DictObject*> tryMake_DB0Dict(db0::swine_ptr<Fixture> &, PyObject *args,
        PyObject *kwargs, AccessFlags
    );
    
    DictObject *PyAPI_makeDict(PyObject *, PyObject*, PyObject*);
    bool DictObject_Check(PyObject *);
        
    extern PyTypeObject DictIteratorObjectType;

    PyObject *tryLoadDict(PyObject *py_dict, PyObject *kwargs, std::unordered_set<const void*> *load_stack_ptr = nullptr);
    
}