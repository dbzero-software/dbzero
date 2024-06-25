#include "PyAtomic.hpp"
#include "PyToolkit.hpp"

namespace db0::python

{

    static PyMethodDef PyAtomic_methods[] = 
    {
        {"__enter__", &PyAtomic_enter, METH_NOARGS, "Enter DBZero atomic context"},
        {"__exit__", &PyAtomic_exit, METH_VARARGS, "Exit DBZero atomic context"},
        {"cancel", (PyCFunction)&PyAtomic_cancel, METH_NOARGS, "Cancel atomic operation"},
        {"commit", (PyCFunction)&PyAtomic_commit, METH_NOARGS, "Commit atomic operation"},
        {NULL}
    };
    
    PyAtomic *PyAtomic_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyAtomic*>(type->tp_alloc(type, 0));
    }

    PyAtomic *PyAtomicDefault_new() {
        return PyAtomic_new(&PyAtomicType, NULL, NULL);
    }
    
    void PyAtomic_del(PyAtomic* self)
    {
        // destroy associated DB0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    PyTypeObject PyAtomicType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.atomic",
        .tp_basicsize = PyAtomic::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAtomic_del, 
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero atomic operation context",
        .tp_methods = PyAtomic_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyAtomic_new,
        .tp_free = PyObject_Free,
    };

    PyAtomic *tryBeginAtomic(PyObject *self)
    {
        auto py_object = PyAtomic_new(&PyAtomicType, NULL, NULL);
        auto workspace_ptr = PyToolkit::getPyWorkspace().getWorkspaceSharedPtr();
        db0::AtomicContext::makeNew(&py_object->modifyExt(), workspace_ptr);
        return py_object;
    }

    bool PyAtomic_Check(PyObject *object) {
        return Py_TYPE(object) == &PyAtomicType;
    }
    
    PyObject *PyAtomic_enter(PyObject *self, PyObject *)
    {
        Py_IncRef(self);
        return self;
    }
    
    PyObject *PyAtomic_exit(PyObject *self, PyObject *) {
        Py_RETURN_NONE;
    }

    PyObject *PyAtomic_cancel(PyObject *self, PyObject *) {
        if (!PyAtomic_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto &atomic = reinterpret_cast<PyAtomic*>(self)->modifyExt();
        atomic.cancel();
        Py_RETURN_NONE;        
    }
    
    PyObject *PyAtomic_commit(PyObject *, PyObject *) {
        Py_RETURN_NONE;
    }

}