#include "PyIteratorObserver.hpp"

namespace db0::python

{
    
    PyIteratorObserver *PyIteratorObserver_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyIteratorObserver*>(type->tp_alloc(type, 0));
    }
    
    PyIteratorObserver *PyIteratorObserverDefault_new() {
        return PyIteratorObserver_new(&PyIteratorObserverType, NULL, NULL);
    }

    void PyIteratorObserver_del(PyIteratorObserver* self)
    {        
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyMethodDef PyIteratorObserver_methods[] = 
    {
        {NULL}
    };
    
    PyTypeObject PyIteratorObserverType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.IteratorObserver",
        .tp_basicsize = PyIteratorObserver::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyIteratorObserver_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Object iterator observer",                
        .tp_methods = PyIteratorObserver_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyIteratorObserver_new,
        .tp_free = PyObject_Free,
    };

    bool PyIteratorObserver_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyIteratorObserverType;
    }

    PyIteratorObserver *makeObserver(std::unique_ptr<db0::object_model::QueryObserver> &&observer) 
    {
        auto py_observer = PyIteratorObserverDefault_new();
        py_observer->ext().m_observer = std::move(observer);
        return py_observer;
    }

}
