// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyWeakSet.hpp"
#include "PyIterator.hpp"
#include <dbzero/bindings/python/Utils.hpp>
#include <dbzero/object_model/set/WeakSet.hpp>
#include <dbzero/object_model/set/WeakSetIterator.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/PyHash.hpp>

namespace db0::python

{

    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
    using WeakSetIteratorObject = PySharedWrapper<db0::object_model::WeakSetIterator, false>;

    PyTypeObject WeakSetIteratorObjectType = GetIteratorType<WeakSetIteratorObject>("dbzero.WeakSetObjectIterator",
        "dbzero weak-set object iterator");

    WeakSetIteratorObject *tryWeakSetObject_iter(WeakSetObject *self)
    {
        return makeIterator<WeakSetIteratorObject, db0::object_model::WeakSetIterator>(
            WeakSetIteratorObjectType, self->ext().begin(), &self->ext(), self
        );
    }

    WeakSetIteratorObject *PyAPI_WeakSetObject_iter(WeakSetObject *self)
    {
        PY_API_FUNC
        return runSafe(tryWeakSetObject_iter, self);
    }

    int tryWeakSetObject_HasItem(WeakSetObject *self, PyObject *key)
    {
        PY_API_FUNC
        auto fixture = self->ext().getFixture();
        auto maybe_hash_pair = getPyHashIfExists(fixture, key);
        if (!maybe_hash_pair) {
            return 0;
        }
        return self->ext().hasItem(maybe_hash_pair->first, *maybe_hash_pair->second);
    }

    int PyAPI_WeakSetObject_HasItem(WeakSetObject *self, PyObject *key)
    {
        PY_API_FUNC
        return runSafe<-1>(tryWeakSetObject_HasItem, self, key);
    }

    Py_ssize_t tryWeakSetObject_len(WeakSetObject *self)
    {
        self->ext().getFixture()->refreshIfUpdated();
        return self->ext().size();
    }

    Py_ssize_t PyAPI_WeakSetObject_len(WeakSetObject *self)
    {
        PY_API_FUNC
        return runSafe(tryWeakSetObject_len, self);
    }

    PyObject *tryWeakSetObject_add(WeakSetObject *self, PyObject *const *args, Py_ssize_t)
    {
        auto fixture = self->ext().getFixture();
        auto hash = getPyHash(fixture, args[0]);
        db0::FixtureLock lock(fixture);
        self->modifyExt().append(lock, hash, ObjectSharedPtr(args[0]));
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_WeakSetObject_add(WeakSetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "add() takes exactly one argument");
            return NULL;
        }
        return runSafe(tryWeakSetObject_add, self, args, nargs);
    }

    PyObject *tryWeakSetObject_remove(WeakSetObject *self, PyObject *const *args, Py_ssize_t nargs, bool throw_ex)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "remove() takes exactly one argument");
            return NULL;
        }

        auto fixture = self->ext().getFixture();
        auto maybe_hash = getPyHashIfExists(fixture, args[0]);
        if (maybe_hash) {
            db0::FixtureLock lock(fixture);
            if (self->modifyExt().remove(lock, maybe_hash->first, args[0])) {
                Py_RETURN_NONE;
            }
        }

        if (throw_ex) {
            PyErr_SetString(PyExc_KeyError, "Element not found");
            return NULL;
        }
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_WeakSetObject_remove(WeakSetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(tryWeakSetObject_remove, self, args, nargs, true);
    }

    PyObject *PyAPI_WeakSetObject_discard(WeakSetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(tryWeakSetObject_remove, self, args, nargs, false);
    }

    PyObject *tryWeakSetObject_clear(WeakSetObject *self, PyObject *const *, Py_ssize_t)
    {
        db0::FixtureLock lock(self->ext().getFixture());
        self->modifyExt().clear(lock);
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_WeakSetObject_clear(WeakSetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(tryWeakSetObject_clear, self, args, nargs);
    }

    WeakSetObject *tryWeakSetObject_copyInternal(WeakSetObject *src)
    {
        db0::FixtureLock lock(src->ext().getFixture());
        auto py_set = WeakSetDefaultObject_new();
        auto &set = py_set->makeNew(*lock);
        set.insert(src->ext());
        lock->getLangCache().add(set.getAddress(), py_set.get());
        return py_set.steal();
    }

    PyObject *PyAPI_WeakSetObject_copy(WeakSetObject *src)
    {
        PY_API_FUNC
        return runSafe(tryWeakSetObject_copyInternal, src);
    }

    PyObject *tryWeakSetObject_str(WeakSetObject *self)
    {
        std::stringstream str;
        str << "weak_set({";
        auto iterator = Py_OWN(PyObject_GetIter(reinterpret_cast<PyObject*>(self)));
        if (!iterator) {
            return nullptr;
        }
        bool first = true;
        ObjectSharedPtr elem;
        Py_FOR(elem, iterator) {
            if (!first) {
                str << ", ";
            } else {
                first = false;
            }
            auto str_value = Py_OWN(PyObject_Repr(*elem));
            if (!str_value) {
                return nullptr;
            }
            str << PyUnicode_AsUTF8(*str_value);
        }
        str << "})";
        return PyUnicode_FromString(str.str().c_str());
    }

    PyObject *PyAPI_WeakSetObject_str(WeakSetObject *self)
    {
        PY_API_FUNC
        return runSafe(tryWeakSetObject_str, self);
    }

    static PySequenceMethods WeakSetObject_sq = {
        .sq_length = (lenfunc)PyAPI_WeakSetObject_len,
        .sq_contains = (objobjproc)PyAPI_WeakSetObject_HasItem
    };

    static PyMethodDef WeakSetObject_methods[] =
    {
        {"add", (PyCFunction)PyAPI_WeakSetObject_add, METH_FASTCALL, "Add an item to the weak set."},
        {"remove", (PyCFunction)PyAPI_WeakSetObject_remove, METH_FASTCALL, "Remove an item from the weak set; raises KeyError if not present."},
        {"discard", (PyCFunction)PyAPI_WeakSetObject_discard, METH_FASTCALL, "Discard an item from the weak set."},
        {"clear", (PyCFunction)PyAPI_WeakSetObject_clear, METH_FASTCALL, "Clear all items from the weak set."},
        {"copy", (PyCFunction)PyAPI_WeakSetObject_copy, METH_NOARGS, "Returns copy of weak set."},
        {NULL}
    };

    PyTypeObject WeakSetObjectType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "WeakSet",
        .tp_basicsize = WeakSetObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)WeakSetObject_del,
        .tp_repr = (reprfunc)PyAPI_WeakSetObject_str,
        .tp_as_sequence = &WeakSetObject_sq,
        .tp_str = (reprfunc)PyAPI_WeakSetObject_str,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero weak-set collection object",
        .tp_iter = (getiterfunc)PyAPI_WeakSetObject_iter,
        .tp_methods = WeakSetObject_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)WeakSetObject_new,
        .tp_free = PyObject_Free,
    };

    WeakSetObject *WeakSetObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<WeakSetObject*>(type->tp_alloc(type, 0));
    }

    shared_py_object<WeakSetObject*> WeakSetDefaultObject_new() {
        return { WeakSetObject_new(&WeakSetObjectType, NULL, NULL), false };
    }

    void WeakSetObject_del(WeakSetObject *self)
    {
        PY_API_FUNC
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    shared_py_object<WeakSetObject*> tryMake_DB0WeakSet(
        db0::swine_ptr<Fixture> &fixture, PyObject *const *args, Py_ssize_t nargs, AccessFlags access_mode)
    {
        auto py_set = WeakSetDefaultObject_new();
        if (!py_set) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to create new weak set");
            return nullptr;
        }

        db0::FixtureLock lock(fixture);
        auto &set = py_set->makeNew(*lock);
        if (nargs == 1) {
            auto iterator = Py_OWN(PyObject_GetIter(args[0]));
            if (!iterator) {
                return nullptr;
            }
            ObjectSharedPtr item;
            Py_FOR(item, iterator) {
                auto hash = getPyHash(fixture, *item);
                set.append(lock, hash, item);
            }
        }

        fixture->getLangCache().add(set.getAddress(), *py_set);
        return py_set;
    }

    WeakSetObject *tryMake_WeakSet(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return tryMake_DB0WeakSet(fixture, args, nargs, {}).steal();
    }

    WeakSetObject *PyAPI_makeWeakSet(PyObject *obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(tryMake_WeakSet, obj, args, nargs);
    }

    bool WeakSetObject_Check(PyObject *object) {
        return Py_TYPE(object) == &WeakSetObjectType;
    }

}
