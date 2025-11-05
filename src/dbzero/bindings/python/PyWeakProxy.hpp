#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include "Memo.hpp"

namespace db0::python

{
        
    struct PyWeakProxy
    {
        PyObject_HEAD
        PyObject* m_py_object;
        
        // get the underlying memo object (as MemoAnyObject)
        MemoAnyObject *get() const;
    };
    
    extern PyTypeObject PyWeakProxyType;
            
    bool PyWeakProxy_Check(PyObject *obj);    
    PyObject *tryWeakProxy(PyObject *);
    PyObject *tryExpired(PyObject *);
    
}