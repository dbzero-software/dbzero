// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyObjectIterator.hpp"
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/core/utils/base32.hpp>
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>

namespace db0::python

{

    using ObjectIterator = db0::object_model::ObjectIterator;
    
    PyObjectIterator *PyObjectIterator_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyObjectIterator*>(type->tp_alloc(type, 0));
    }
    
    void PyObjectIterator_del(PyObjectIterator* self)
    {
        PY_API_FUNC
        // destroy associated instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    shared_py_object<PyObjectIterator*> PyObjectIteratorDefault_new() {
        return { PyObjectIterator_new(&PyObjectIteratorType, NULL, NULL), false };
    }
    
    PyObjectIterator *PyAPI_PyObjectIterator_iter(PyObjectIterator *self)
    {
        Py_INCREF(self);
        return self;
    }
    
    PyObject *decoratedItem(PyObject *py_item, const std::vector<PyObject*> &decorators)
    {
        // return a tuple consisting of an item + decorators
        auto tuple = PyTuple_New(decorators.size() + 1);
        Py_INCREF(py_item);
        PyTuple_SET_ITEM(tuple, 0, py_item);
        for (std::size_t i = 0; i < decorators.size(); ++i) {
            Py_INCREF(decorators[i]);
            PyTuple_SET_ITEM(tuple, i + 1, decorators[i]);
        }
        return tuple;
    }
    
    PyObject *tryPyObjectIterator_iternext(PyObjectIterator *iter_obj)
    {
        auto &iter = iter_obj->modifyExt();
        auto py_item = iter.next();
        if (py_item.get()) {
            if (iter.numDecorators() > 0) {
                return decoratedItem(py_item.steal(), iter.getDecorators());
            } else {
                return py_item.steal();
            }
        }
        
        // raise stop iteration
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    PyObject *PyAPI_PyObjectIterator_iternext(PyObjectIterator *iter_obj)
    {
        PY_API_FUNC
        return runSafe(tryPyObjectIterator_iternext, iter_obj);
    }
        
    static PyMethodDef PyObjectIterator_methods[] = 
    {
        {NULL}
    };
    
    PyTypeObject PyObjectIteratorType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "ObjectIterator",        
        .tp_basicsize = PyObjectIterator::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyObjectIterator_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero object iterator",
        .tp_iter = (getiterfunc)PyAPI_PyObjectIterator_iter,
        .tp_iternext = (iternextfunc)PyAPI_PyObjectIterator_iternext,
        .tp_methods = PyObjectIterator_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyObjectIterator_new,
        .tp_free = PyObject_Free,
    };
    
    bool PyObjectIterator_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyObjectIteratorType;
    }
        
}
