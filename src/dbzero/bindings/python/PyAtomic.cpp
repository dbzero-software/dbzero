// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyAtomic.hpp"
#include "PyToolkit.hpp"
#include "PyInternalAPI.hpp"

namespace db0::python

{

    static PyMethodDef PyAtomic_methods[] = 
    {
        {"close", (PyCFunction)&PyAPI_PyAtomic_close, METH_NOARGS, "Close/commit atomic operation"},
        {"cancel", (PyCFunction)&PyAPI_PyAtomic_cancel, METH_NOARGS, "Cancel/rollback atomic operation"},
        {NULL}
    };
    
    PyAtomic *PyAtomic_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyAtomic*>(type->tp_alloc(type, 0));
    }
    
    PyAtomic *PyAtomicDefault_new() {
        return PyAtomic_new(&PyAtomicType, NULL, NULL);
    }
    
    void PyAPI_PyAtomic_del(PyAtomic* self)
    {
        PY_API_FUNC
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    PyTypeObject PyAtomicType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.AtomicContext",
        .tp_basicsize = PyAtomic::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_PyAtomic_del, 
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero atomic operation context",
        .tp_methods = PyAtomic_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyAtomic_new,
        .tp_free = PyObject_Free,
    };
    
    PyAtomic *PyAPI_tryBeginAtomic(PyObject *self, std::unique_lock<std::mutex> &&lock)
    {
        PY_API_FUNC
        auto py_object = Py_OWN(PyAtomic_new(&PyAtomicType, NULL, NULL));        
        auto workspace_ptr = PyToolkit::getPyWorkspace().getWorkspaceSharedPtr();
        py_object->makeNew(workspace_ptr, std::move(lock));
        return py_object.steal();
    }
    
    PyObject *PyAPI_beginAtomic(PyObject *self, PyObject *const *, Py_ssize_t nargs)
    {
        if (nargs != 0) {
            PyErr_SetString(PyExc_TypeError, "beginAtomic requires no arguments");
            return NULL;
        }

        // need to acquire atomic lock before API lock
        std::unique_lock<std::mutex> atomic_lock;
        {
            // this is to prevent GIL-related deadlocks
            WithGIL_Unlocked no_gil;
            atomic_lock = db0::AtomicContext::lock();
        }
        return runSafe(PyAPI_tryBeginAtomic, self, std::move(atomic_lock));
    }
    
    bool PyAtomic_Check(PyObject *object) {
        return Py_TYPE(object) == &PyAtomicType;
    }
    
    PyObject *tryPyAtomic_close(PyAtomic *self)
    {        
        self->modifyExt().close();
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_PyAtomic_close(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        return runSafe(tryPyAtomic_close, reinterpret_cast<PyAtomic*>(self));
    }
    
    PyObject *tryPyAtomic_cancel(PyAtomic *self)
    {    
        self->modifyExt().cancel();
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_PyAtomic_cancel(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        return runSafe(tryPyAtomic_cancel, reinterpret_cast<PyAtomic*>(self));
    }
    
}