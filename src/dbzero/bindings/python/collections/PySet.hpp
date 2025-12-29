// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python

{
    
    using SetObject = PyWrapper<db0::object_model::Set>;
    using AccessFlags = db0::AccessFlags;
    
    SetObject *SetObject_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<SetObject*> SetDefaultObject_new();
    void SetObject_del(SetObject* self);
    
    Py_ssize_t PyAPI_SetObject_len(SetObject *);
    PyObject *PyAPI_SetObject_add(SetObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_SetObject_remove(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_SetObject_discard(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_SetObject_isdisjoint(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_SetObject_issubset(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_SetObject_issuperset(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    int PyAPI_SetObject_SetItem(SetObject *set_obj, Py_ssize_t i, PyObject *);
    PyObject *PyAPI_SetObject_copy(SetObject *set_obj);
    PyObject *PyAPI_SetObject_union(SetObject *set_obj, PyObject *const *args, Py_ssize_t narg);
    PyObject *PyAPI_SetObject_intersection_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_SetObject_difference_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_SetObject_symmetric_difference_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_SetObject_pop(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_SetObject_clear(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs);

    extern PyTypeObject SetObjectType;
    
    // as number
    PyObject *PyAPI_SetObject_intersection_binary(SetObject *self, PyObject * obj);
    PyObject *PyAPI_SetObject_union_binary(SetObject *self, PyObject * obj);
    PyObject *PyAPI_SetObject_difference_binary(SetObject *self, PyObject * obj);
    PyObject *PyAPI_SetObject_symmetric_difference_binary(SetObject *self, PyObject * obj);
    PyObject *PyAPI_SetObject_symmetric_difference_in_place(SetObject *self, PyObject * ob);
    PyObject *PyAPI_SetObject_difference_in_place(SetObject *self, PyObject * ob);
    PyObject *PyAPI_SetObject_update(SetObject *self, PyObject * ob);
    PyObject *PyAPI_SetObject_intersection_in_place(SetObject *self, PyObject * ob);
    
    shared_py_object<SetObject*> tryMake_DB0Set(db0::swine_ptr<Fixture> &, PyObject *const *args,
        Py_ssize_t nargs, AccessFlags);
    SetObject *PyAPI_makeSet(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
    bool SetObject_Check(PyObject *);
    PyObject *tryLoadSet(PyObject *set, PyObject *kwargs, std::unordered_set<const void*> *load_stack_ptr = nullptr);

}