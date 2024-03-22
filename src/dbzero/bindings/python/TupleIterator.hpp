#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/tuple/TupleIterator.hpp>

namespace db0::python 

{
    
    using TupleIteratorObject = PyWrapper<db0::object_model::TupleIterator>;
        
    TupleIteratorObject *TupleIteratorObject_new(PyTypeObject *type, PyObject *, PyObject *);
    void TupleIteratorObject_del(TupleIteratorObject* self);    

    TupleIteratorObject *makeIterator(db0::object_model::Tuple::iterator iterator, db0::object_model::Tuple * ptr);

    extern PyTypeObject TupleIteratorObjectType;
}