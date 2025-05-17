#pragma once

#include <Python.h>
#include "shared_py_object.hpp"

namespace db0::python

{

    // The ownership-safe db0 counterparts of Python API functions
    template <typename T = PyObject *> 
    int PyList_SetItem(PyObject *, Py_ssize_t index, shared_py_object<T> item);

    template <typename K = PyObject *, typename V = PyObject *>
    int PyDict_SetItem(PyObject *, shared_py_object<K> key, shared_py_object<V> val);

    template <typename T = PyObject *>
    int PyDict_SetItemString(PyObject *, const char *key, shared_py_object<T> val);

    template <typename T = PyObject *>
    int PySet_Add(PyObject *, shared_py_object<T> key);

    template <typename T = PyObject *> 
    int PyList_SetItem(PyObject *self, Py_ssize_t index, shared_py_object<T> item)
    {
        assert(item.get() != nullptr);
        return PyList_SetItem(self, index, item.steal());
    }

    template <typename K = PyObject *, typename V = PyObject *>
    int PyDict_SetItem(PyObject *self, shared_py_object<K> key, shared_py_object<V> val)
    {
        assert(key.get() != nullptr);
        assert(val.get() != nullptr);
        // NOTE: Python API does NOT steal the value reference
        return PyDict_SetItem(self, key.steal(), *val);
    }

    template <typename T = PyObject *>
    int PyDict_SetItemString(PyObject *self, const char *key, shared_py_object<T> val)
    {
        assert(key);
        assert(val.get() != nullptr);
        // NOTE: Python API does NOT steal the value reference
        return PyDict_SetItemString(self, key, *val);
    }

    template <typename T = PyObject *>
    int PySet_Add(PyObject *self, shared_py_object<T> key)
    {
        assert(key.get() != nullptr);
        return PySet_Add(self, key.steal());
    }

}
