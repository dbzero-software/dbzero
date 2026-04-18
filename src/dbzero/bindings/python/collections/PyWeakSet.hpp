// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/set/WeakSet.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python

{

    using WeakSetObject = PyWrapper<db0::object_model::WeakSet>;
    using AccessFlags = db0::AccessFlags;

    WeakSetObject *WeakSetObject_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<WeakSetObject*> WeakSetDefaultObject_new();
    void WeakSetObject_del(WeakSetObject *self);

    Py_ssize_t PyAPI_WeakSetObject_len(WeakSetObject *);
    PyObject *PyAPI_WeakSetObject_add(WeakSetObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_WeakSetObject_remove(WeakSetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_WeakSetObject_discard(WeakSetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_WeakSetObject_copy(WeakSetObject *self);
    PyObject *PyAPI_WeakSetObject_clear(WeakSetObject *self, PyObject *const *args, Py_ssize_t nargs);
    int PyAPI_WeakSetObject_HasItem(WeakSetObject *self, PyObject *key);

    extern PyTypeObject WeakSetObjectType;
    extern PyTypeObject WeakSetIteratorObjectType;

    shared_py_object<WeakSetObject*> tryMake_DB0WeakSet(db0::swine_ptr<Fixture> &, PyObject *const *args,
        Py_ssize_t nargs, AccessFlags);
    WeakSetObject *PyAPI_makeWeakSet(PyObject *, PyObject *const *args, Py_ssize_t nargs);

    bool WeakSetObject_Check(PyObject *);

}
