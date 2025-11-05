#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include "Memo.hpp"

namespace db0::python

{
    
    template <typename MemoImplT>
    struct PyWeakProxy
    {
        PyObject_HEAD
        PyObject* m_py_object;
        
        // get the underlying memo object
        MemoImplT *get() const;
    };
    
    extern PyTypeObject PyWeakProxyType;
    // a weak proxy to immutable memo object
    extern PyTypeObject PyWeakProxyImmutableType;
    
    bool PyAnyWeakProxy_Check(PyObject *);
    
    template <typename MemoImplT>
    bool PyWeakProxy_Check(PyObject *obj);

    PyObject *tryWeakProxy(PyObject *);
    PyObject *tryExpired(PyObject *);

    extern template struct PyWeakProxy<MemoObject>;
    extern template struct PyWeakProxy<MemoImmutableObject>;

    extern template bool PyWeakProxy_Check<MemoObject>(PyObject *);
    extern template bool PyWeakProxy_Check<MemoImmutableObject>(PyObject *);
    
}