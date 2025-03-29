#include "PyWeakProxy.hpp"
#include "Memo.hpp"
#include "PyToolkit.hpp"

namespace db0::python

{

    PyTypeObject PyWeakProxyType = 
    {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "WeakProxy",
        .tp_basicsize = sizeof(PyWeakProxy),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyObject_Del,        
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_new = PyType_GenericNew,
    };
    
    MemoObject *PyWeakProxy::get() const {
        return reinterpret_cast<MemoObject*>(m_py_object.get());
    }
    
    bool PyWeakProxy_Check(PyObject *obj) {
        return PyObject_TypeCheck(obj, &PyWeakProxyType);
    }

    PyObject *tryWeakProxy(PyObject *py_obj)
    {
        if (!PyMemo_Check(py_obj)) {
            THROWF(db0::InputException) << "Invalid argument type: " << PyToolkit::getTypeName(py_obj) << " (memo expected)";
        }
        // new PyWeakProxy
        auto py_weak_proxy = PyObject_New(PyWeakProxy, &PyWeakProxyType);
        if (!py_weak_proxy) {
            return nullptr;
        }
        py_weak_proxy->m_py_object = py_obj;
        return reinterpret_cast<PyObject *>(py_weak_proxy);
    }
    
}