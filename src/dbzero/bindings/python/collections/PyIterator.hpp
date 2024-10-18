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
    IteratorObjectT *makeIterator(PyTypeObject& typeObject, IteratorT iterator, const ObjectT * ptr)
    {        
        auto iterator_object = IteratorObject_new<IteratorObjectT>(&typeObject, NULL, NULL);
        IteratorModelT::makeNew(&iterator_object->modifyExt(), iterator, ptr);
        return iterator_object;
    }
    
    template <typename IteratorObjectT> 
    IteratorObjectT *IteratorObject_iter(IteratorObjectT *self)
    {
        Py_INCREF(self);
        return self;        
    }
    
    template <typename IteratorObjectT> 
    PyObject *IteratorObject_iternext(IteratorObjectT *iter_obj)
    {
        PY_API_FUNC
        if (iter_obj->ext() != iter_obj->ext().end()) {
            return iter_obj->modifyExt().next().steal();
        } else {
            // raise stop iteration
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } 
    }
    
    static PyMethodDef IteratorObject_methods[] = {
        {NULL}
    };

    template <typename IteratorObjectT>
    PyTypeObject GetIteratorType(const char *name, const char *doc) {
        PyTypeObject object =  {
            PyVarObject_HEAD_INIT(NULL, 0)
            .tp_name = name,
            .tp_basicsize = IteratorObjectT::sizeOf(),
            .tp_itemsize = 0,
            .tp_dealloc = (destructor)IteratorObject_del<IteratorObjectT>,
            .tp_flags = Py_TPFLAGS_DEFAULT,
            .tp_doc = doc,
            .tp_iter = (getiterfunc)IteratorObject_iter<IteratorObjectT>,
            .tp_iternext = (iternextfunc)IteratorObject_iternext<IteratorObjectT>,
            .tp_methods = IteratorObject_methods,
            .tp_alloc = PyType_GenericAlloc,
            .tp_new = (newfunc)IteratorObject_new<IteratorObjectT>,
            .tp_free = PyObject_Free,
        };
        return object;
    }

}