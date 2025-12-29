// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include "shared_py_object.hpp"

namespace db0::python

{

    // The ownership-safe db0 counterparts of Python API functions
    template <typename T = PyObject *> 
    int PySafeList_SetItem(PyObject *, Py_ssize_t index, shared_py_object<T> item);

    template <typename T = PyObject *> 
    int PySafeList_Append(PyObject *, shared_py_object<T> item);

    template <typename T = PyObject *>
    int PySafeTuple_SetItem(PyObject *, Py_ssize_t index, shared_py_object<T> item);

    template <typename K = PyObject *, typename V = PyObject *>
    int PySafeDict_SetItem(PyObject *, shared_py_object<K> key, shared_py_object<V> val);

    template <typename K = PyObject *, typename V = PyObject *>
    PyObject *PySafeDict_SetDefault(PyObject *, shared_py_object<K> key, shared_py_object<V> val);

    template <typename T = PyObject *>
    int PySafeDict_SetItemString(PyObject *, const char *key, shared_py_object<T> val);

    template <typename T = PyObject *>
    int PySafeSet_Add(PyObject *, shared_py_object<T> key);

    template <typename T = PyObject *>
    int PySafeModule_AddObject(PyObject *, const char *name, shared_py_object<T> obj);

    template <typename T = PyObject *>
    PyObject *PySafeTuple_Pack(shared_py_object<T> item);

    template <typename T1 = PyObject *, typename T2 = PyObject *>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2);

    template <typename T1 = PyObject *, typename T2 = PyObject *, typename T3 = PyObject *>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2, shared_py_object<T3> item3);

    template <typename T1 = PyObject *, typename T2 = PyObject *, typename T3 = PyObject *, typename T4 = PyObject *>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2, shared_py_object<T3> item3, 
        shared_py_object<T4> item4);
    
    template <typename T1 = PyObject *, typename T2 = PyObject *, typename T3 = PyObject *, typename T4 = PyObject *, typename T5 = PyObject *>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2, shared_py_object<T3> item3,
        shared_py_object<T4> item4, shared_py_object<T5> item5);
    
    PyObject * PyBool_fromBool(bool);
    
    template <typename T>
    int PySafeList_SetItem(PyObject *self, Py_ssize_t index, shared_py_object<T> item)
    {
        assert(item.get() != nullptr);
        return PyList_SetItem(self, index, item.steal());
    }
    
    template <typename T>
    int PySafeList_Append(PyObject *self, shared_py_object<T> item)
    {
        assert(item.get() != nullptr);
        return PyList_Append(self, item.steal());
    }
    
    template <typename T> 
    int PySafeTuple_SetItem(PyObject *self, Py_ssize_t index, shared_py_object<T> item)
    {
        assert(item.get() != nullptr);
        return PyTuple_SetItem(self, index, item.steal());
    }
    
    template <typename K, typename V>
    int PySafeDict_SetItem(PyObject *self, shared_py_object<K> key, shared_py_object<V> val)
    {
        assert(key.get() != nullptr);        
        // NOTE: Python API does NOT steal the value reference
        return PyDict_SetItem(self, key.steal(), *val);
    }
    
    template <typename K, typename V>
    PyObject *PySafeDict_SetDefault(PyObject *self, shared_py_object<K> key, shared_py_object<V> val)
    {
        assert(key.get() != nullptr);
        // NOTE: Python API does NOT steal the value reference
        return PyDict_SetDefault(self, key.steal(), *val);
    }

    template <typename T>
    int PySafeDict_SetItemString(PyObject *self, const char *key, shared_py_object<T> val)
    {
        assert(key);        
        // NOTE: Python API does NOT steal the value reference
        return PyDict_SetItemString(self, key, *val);
    }
    
    template <typename T>
    int PySafeSet_Add(PyObject *self, shared_py_object<T> key)
    {
        assert(key.get() != nullptr);
        return PySet_Add(self, key.steal());
    }
    
    template <typename T>
    PyObject *PySafeTuple_Pack(shared_py_object<T> item)
    {
        return PyTuple_Pack(1, *item);
    }
    
    template <typename T1, typename T2>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2)
    {
        return PyTuple_Pack(2, *item1, *item2);
    }

    template <typename T1, typename T2, typename T3>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2, shared_py_object<T3> item3)
    {
        return PyTuple_Pack(3, *item1, *item2, *item3);
    }

    template <typename T1, typename T2, typename T3, typename T4>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2, shared_py_object<T3> item3, shared_py_object<T4> item4)
    {
        return PyTuple_Pack(4, *item1, *item2, *item3, *item4);
    }
    
    template <typename T1, typename T2, typename T3, typename T4, typename T5>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2, shared_py_object<T3> item3,
        shared_py_object<T4> item4, shared_py_object<T5> item5)
    {
        return PyTuple_Pack(5, *item1, *item2, *item3, *item4, *item5);
    }

    template <typename T>
    int PySafeModule_AddObject(PyObject *self, const char *name, shared_py_object<T> obj)
    {
        assert(obj.get() != nullptr);
        return PyModule_AddObject(self, name, (PyObject*)obj.steal());
    }

}

// exception-safe iteration
#define Py_FOR(item, iterator) for (auto item = Py_OWN(PyIter_Next(*iterator)); item.get(); item = Py_OWN(PyIter_Next(*iterator)))
