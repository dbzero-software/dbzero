#include "PyRunnable.hpp"

namespace db0::python

{
    
    static PyMethodDef PyRunnable_methods[] = {
        {NULL}
    };
    
    PyRunnableObject *PyRunnable_new(PyTypeObject *type, PyObject *, PyObject *) {
        return (PyRunnableObject *)type->tp_alloc(type, 0);
    }

    PyRunnableObject *PyRunnableDefault_new() {
        return PyRunnable_new(&PyRunnableObjectType, nullptr, nullptr);
    }

    void PyRunnable_del(PyRunnableObject* self) {
        self->~PyRunnableObject();
        Py_TYPE(self)->tp_free(self);
    }
    
    bool PyRunnable_Check(PyObject *py_object) {
        return PyObject_TypeCheck(py_object, &PyRunnableObjectType);
    }

    PyTypeObject PyRunnableObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Runnable",
        .tp_basicsize = PyRunnableObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyRunnable_del, 
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero runnable query object",
        .tp_methods = PyRunnable_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyRunnable_new,
        .tp_free = PyObject_Free,
    };

}