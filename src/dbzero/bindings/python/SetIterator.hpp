#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/set/SetIterator.hpp>

namespace db0::python 

{
    
    using SetIteratorObject = PyWrapper<db0::object_model::SetIterator>;
        
    SetIteratorObject *SetIteratorObject_new(PyTypeObject *type, PyObject *, PyObject *);
    void SetIteratorObject_del(SetIteratorObject* self);    

    SetIteratorObject *makeIterator(db0::object_model::Set::iterator iterator, db0::object_model::Set * ptr);

    extern PyTypeObject SetIteratorObjectType;
}