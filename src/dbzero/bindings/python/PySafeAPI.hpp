#pragma once

#include <Python.h>
#include "shared_py_object.hpp"

namespace db0::python

{

    // The ownership-safe db0 counterparts of Python API functions
    template <typename T = PyObject *> 
    int PyList_SetItem(PyObject *, Py_ssize_t index, shared_py_object<T> item);

    template <typename T = PyObject *> 
    int PyList_Append(PyObject *, shared_py_object<T> item);

    template <typename T = PyObject *>
    int PyTuple_SetItem(PyObject *, Py_ssize_t index, shared_py_object<T> item);

    template <typename K = PyObject *, typename V = PyObject *>
    int PyDict_SetItem(PyObject *, shared_py_object<K> key, shared_py_object<V> val);

    template <typename K = PyObject *, typename V = PyObject *>
    PyObject *PyDict_SetDefault(PyObject *, shared_py_object<K> key, shared_py_object<V> val);

    template <typename T = PyObject *>
    int PyDict_SetItemString(PyObject *, const char *key, shared_py_object<T> val);

    template <typename T = PyObject *>
    int PySet_Add(PyObject *, shared_py_object<T> key);

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

    template <typename T = PyObject *>
    int PyList_SetItem(PyObject *self, Py_ssize_t index, shared_py_object<T> item)
    {
        assert(item.get() != nullptr);
        return PyList_SetItem(self, index, item.steal());
    }
    
    template <typename T = PyObject *>
    int PyList_Append(PyObject *self, shared_py_object<T> item)
    {
        assert(item.get() != nullptr);
        return PyList_Append(self, item.steal());
    }

    template <typename T = PyObject *> 
    int PyTuple_SetItem(PyObject *self, Py_ssize_t index, shared_py_object<T> item)
    {
        assert(item.get() != nullptr);
        return PyTuple_SetItem(self, index, item.steal());
    }

    template <typename K = PyObject *, typename V = PyObject *>
    int PyDict_SetItem(PyObject *self, shared_py_object<K> key, shared_py_object<V> val)
    {
        assert(key.get() != nullptr);
        assert(val.get() != nullptr);
        // NOTE: Python API does NOT steal the value reference
        return PyDict_SetItem(self, key.steal(), *val);
    }
    
    template <typename K = PyObject *, typename V = PyObject *>
    PyObject *PyDict_SetDefault(PyObject *self, shared_py_object<K> key, shared_py_object<V> val)
    {
        assert(key.get() != nullptr);
        assert(val.get() != nullptr);
        // NOTE: Python API does NOT steal the value reference
        return PyDict_SetDefault(self, key.steal(), *val);
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
    
    template <typename T = PyObject *>
    PyObject *PySafeTuple_Pack(shared_py_object<T> item)
    {
        return PyTuple_Pack(1, *item);
    }
    
    template <typename T1 = PyObject *, typename T2 = PyObject *>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2)
    {
        return PyTuple_Pack(2, *item1, *item2);
    }

    template <typename T1 = PyObject *, typename T2 = PyObject *, typename T3 = PyObject *>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2, shared_py_object<T2> item3)
    {
        return PyTuple_Pack(3, *item1, *item2, *item3);
    }

    template <typename T1 = PyObject *, typename T2 = PyObject *, typename T3 = PyObject *, typename T4 = PyObject *>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2, shared_py_object<T2> item3, shared_py_object<T2> item4)
    {
        return PyTuple_Pack(4, *item1, *item2, *item3, *item4);
    }
    
    template <typename T1 = PyObject *, typename T2 = PyObject *, typename T3 = PyObject *, typename T4 = PyObject *, typename T5 = PyObject *>
    PyObject *PySafeTuple_Pack(shared_py_object<T1> item1, shared_py_object<T2> item2, shared_py_object<T2> item3,
        shared_py_object<T2> item4, shared_py_object<T2> item5)
    {
        return PyTuple_Pack(5, *item1, *item2, *item3, *item4, *item5);
    }

}
