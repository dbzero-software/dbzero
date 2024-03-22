#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/object_model/dict/DictIterator.hpp>

namespace db0::python 

{
    
    using DictIteratorObject = PyWrapper<db0::object_model::DictIterator>;
        
    DictIteratorObject *DictIteratorObject_new(PyTypeObject *type, PyObject *, PyObject *);
    void DictIteratorObject_del(DictIteratorObject* self);    

    DictIteratorObject *makeIterator(db0::object_model::Dict::iterator iterator, db0::object_model::Dict * ptr);

    extern PyTypeObject DictIteratorObjectType;
}