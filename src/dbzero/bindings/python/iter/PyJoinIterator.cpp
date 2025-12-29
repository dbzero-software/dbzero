// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyJoinIterator.hpp"
#include <dbzero/object_model/tags/JoinIterator.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>

namespace db0::python

{

    using JoinIterator = db0::object_model::JoinIterator;

    PyJoinIterator *PyJoinIterator_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyJoinIterator*>(type->tp_alloc(type, 0));
    }
    
    void PyJoinIterator_del(PyJoinIterator* self)
    {
        // destroy associated db0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    shared_py_object<PyJoinIterator*> PyJoinIteratorDefault_new() {
        return { PyJoinIterator_new(&PyJoinIteratorType, NULL, NULL), false };
    }
    
    PyJoinIterator *PyAPI_PyJoinIterator_iter(PyJoinIterator *self)
    {
        Py_INCREF(self);
        return self;
    }
        
    PyObject *tryPyJoinIterator_iternext(PyJoinIterator *iter_obj)
    {
        auto &iter = iter_obj->modifyExt();
        auto py_item = iter.next();
        if (py_item.get()) {
            return py_item.steal();            
        }
        
        // raise stop iteration
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }
    
    PyObject *PyAPI_PyJoinIterator_iternext(PyJoinIterator *iter_obj)
    {
        PY_API_FUNC
        return runSafe(tryPyJoinIterator_iternext, iter_obj);
    }
    
    static PyMethodDef PyJoinIterator_methods[] = 
    {
        {NULL}
    };

    PyTypeObject PyJoinIteratorType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "JoinIterator",        
        .tp_basicsize = PyJoinIterator::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyJoinIterator_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero join iterator",
        .tp_iter = (getiterfunc)PyAPI_PyJoinIterator_iter,
        .tp_iternext = (iternextfunc)PyAPI_PyJoinIterator_iternext,
        .tp_methods = PyJoinIterator_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyJoinIterator_new,
        .tp_free = PyObject_Free,
    };
    
    bool PyJoinIterator_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyJoinIteratorType;
    }
    
}