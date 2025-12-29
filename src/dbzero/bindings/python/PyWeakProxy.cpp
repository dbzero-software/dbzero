// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyWeakProxy.hpp"
#include "Memo.hpp"
#include "MemoExpiredRef.hpp"
#include "PyToolkit.hpp"
#include "PyInternalAPI.hpp"

namespace db0::python

{
    
    void PyAPI_PyWeakProxy_del(PyWeakProxy *py_weak_proxy)
    {
        PY_API_FUNC
        if (py_weak_proxy->m_py_object) {
            Py_DECREF(py_weak_proxy->m_py_object);            
        }
        Py_TYPE(py_weak_proxy)->tp_free((PyObject*)py_weak_proxy);
    }

    PyTypeObject PyWeakProxyType = 
    {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "WeakProxy",
        .tp_basicsize = sizeof(PyWeakProxy),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_PyWeakProxy_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_new = PyType_GenericNew,
    };
    
    MemoAnyObject *PyWeakProxy::get() const {
        return reinterpret_cast<MemoAnyObject*>(m_py_object);
    }
    
    bool PyWeakProxy_Check(PyObject *obj) {
        return PyObject_TypeCheck(obj, &PyWeakProxyType);
    }
    
    PyObject *tryWeakProxy(PyObject *py_obj)
    {
        assert(PyAnyMemo_Check(py_obj));
        auto py_weak_proxy = PyObject_New(PyWeakProxy, &PyWeakProxyType);

        if (!py_weak_proxy) {
            return nullptr;
        }

        Py_INCREF(py_obj);
        py_weak_proxy->m_py_object = py_obj;
        return reinterpret_cast<PyObject *>(py_weak_proxy);
    }
    
    PyObject *tryExpired(PyObject *py_obj)
    {
        if (MemoExpiredRef_Check(py_obj)) {
            Py_RETURN_TRUE;
        } else {
            Py_RETURN_FALSE;
        }
    }
    
}
