#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/list/ListIterator.hpp>

namespace db0::python 

{
    
    using ListIteratorObject = PyWrapper<db0::object_model::ListIterator>;
        
    ListIteratorObject *ListIteratorObject_new(PyTypeObject *type, PyObject *, PyObject *);
    void ListIteratorObject_del(ListIteratorObject* self);    

    ListIteratorObject *makeIterator(db0::object_model::List::iterator iterator, db0::object_model::List * ptr);

    extern PyTypeObject ListIteratorObjectType;
}