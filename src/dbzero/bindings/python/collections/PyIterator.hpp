// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>

namespace db0::python 

{
    
    template <typename IteratorObjectT>
    IteratorObjectT *IteratorObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<IteratorObjectT*>(type->tp_alloc(type, 0));
    }

    template <typename IteratorObjectT> 
    void IteratorObject_del(IteratorObjectT* self)
    {
        PY_API_FUNC
        // destroy associated DB0 instance
        // calls destructor of ext object
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    } 
    
    template <typename IteratorObjectT, typename IteratorModelT, typename IteratorT, typename ObjectT>
    IteratorObjectT *tryMakeIterator(PyTypeObject& typeObject, IteratorT iterator, const ObjectT *ptr, PyObject *collection_ptr)
    {
        assert(ptr);
        auto iter = (*ptr).getIterator(collection_ptr);
        auto py_iter = IteratorObject_new<IteratorObjectT>(&typeObject, NULL, NULL);
        py_iter->makeNew(iter);
        return py_iter;
    }

    template <typename IteratorObjectT, typename IteratorModelT, typename IteratorT, typename ObjectT>
    IteratorObjectT *makeIterator(PyTypeObject& typeObject, IteratorT iterator, const ObjectT *ptr, PyObject *collection_ptr)
    {
        PY_API_FUNC
        return runSafe(tryMakeIterator<IteratorObjectT, IteratorModelT, IteratorT, ObjectT>, 
            typeObject, iterator, ptr, collection_ptr);
    }
    
    template <typename IteratorObjectT>
    IteratorObjectT *PyAPI_IteratorObject_iter(IteratorObjectT *self)
    {
        Py_INCREF(self);
        return self;        
    }

    template <typename IteratorObjectT>
    PyObject *tryIteratorObject_iternext(IteratorObjectT *iter_obj)
    {       
        if (iter_obj->ext().is_end()) {
            // raise stop iteration
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        }
        return iter_obj->modifyExt().next().steal();
    }

    template <typename IteratorObjectT>
    PyObject *PyAPI_IteratorObject_iternext(IteratorObjectT *iter_obj)
    {
        PY_API_FUNC
        return runSafe(tryIteratorObject_iternext<IteratorObjectT>, iter_obj);
    }
    
    static PyMethodDef IteratorObject_methods[] = {
        {NULL}
    };
    
    template <typename IteratorObjectT>
    PyTypeObject GetIteratorType(const char *name, const char *doc) {
        PyTypeObject object = {
            PYVAROBJECT_HEAD_INIT_DESIGNATED,
            .tp_name = name,
            .tp_basicsize = IteratorObjectT::sizeOf(),
            .tp_itemsize = 0,
            .tp_dealloc = (destructor)IteratorObject_del<IteratorObjectT>,
            .tp_flags = Py_TPFLAGS_DEFAULT,
            .tp_doc = doc,
            .tp_iter = (getiterfunc)PyAPI_IteratorObject_iter<IteratorObjectT>,
            .tp_iternext = (iternextfunc)PyAPI_IteratorObject_iternext<IteratorObjectT>,
            .tp_methods = IteratorObject_methods,
            .tp_alloc = PyType_GenericAlloc,
            .tp_new = (newfunc)IteratorObject_new<IteratorObjectT>,
            .tp_free = PyObject_Free,
        };
        return object;
    }

}
