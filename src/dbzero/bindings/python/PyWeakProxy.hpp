#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include "shared_py_object.hpp"
#include "Memo.hpp"

namespace db0::python

{
    
    struct PyWeakProxy
    {
        PyObject_HEAD
        shared_py_object<PyObject*> m_py_object;   
        
        // get the underlying memo object
        MemoObject *get() const;
    };
    
    extern PyTypeObject PyWeakProxyType;
    
    void PyAPI_PyWeakProxy_del(PyWeakProxy *self);
    bool PyWeakProxy_Check(PyObject *obj);

    PyObject *tryWeakProxy(PyObject *);
    PyObject *tryExpired(PyObject *);
    
}