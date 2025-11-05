#include "PyWeakProxy.hpp"
#include "Memo.hpp"
#include "MemoExpiredRef.hpp"
#include "PyToolkit.hpp"
#include "PyInternalAPI.hpp"

namespace db0::python

{

    template <typename MemoImplT>
    void PyAPI_PyWeakProxy_del(PyWeakProxy<MemoImplT> *py_weak_proxy)
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
        .tp_basicsize = sizeof(PyWeakProxy<MemoObject>),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_PyWeakProxy_del<MemoObject>,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_new = PyType_GenericNew,
    };
    
    PyTypeObject PyWeakProxyImmutableType = 
    {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "WeakProxy",
        .tp_basicsize = sizeof(PyWeakProxy<MemoImmutableObject>),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_PyWeakProxy_del<MemoImmutableObject>,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_new = PyType_GenericNew,
    };

    template <typename MemoImplT> MemoImplT *PyWeakProxy<MemoImplT>::get() const {
        return reinterpret_cast<MemoImplT*>(m_py_object);
    }
    
    bool PyWeakProxy_Check(PyObject *obj) {
        return PyObject_TypeCheck(obj, &PyWeakProxyType);
    }
    
    // MemoObject specialization
    template <> PyObject *tryWeakProxy<MemoObject>(PyObject *py_obj)
    {
        assert(PyMemo_Check<MemoObject>(py_obj));
        auto py_weak_proxy = PyObject_New(PyWeakProxy<MemoObject>, &PyWeakProxyType);

        if (!py_weak_proxy) {
            return nullptr;
        }

        Py_INCREF(py_obj);
        py_weak_proxy->m_py_object = py_obj;
        return reinterpret_cast<PyObject *>(py_weak_proxy);
    }

    // MemoImmutableObject specialization
    template <> PyObject *tryWeakProxy<MemoImmutableObject>(PyObject *py_obj)
    {
        assert(PyMemo_Check<MemoImmutableObject>(py_obj));
        auto py_weak_proxy = PyObject_New(PyWeakProxy<MemoImmutableObject>, &PyWeakProxyImmutableType);

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
