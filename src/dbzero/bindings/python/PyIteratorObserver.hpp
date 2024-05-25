#pragma once

#include "PyWrapper.hpp"
#include <dbzero/object_model/tags/QueryObserver.hpp>

namespace db0::python 

{

    using PyIteratorObserver = PyWrapper<db0::object_model::Observer>;
    
    PyIteratorObserver *PyIteratorObserver_new(PyTypeObject *type, PyObject *, PyObject *);
    PyIteratorObserver *PyIteratorObserverDefault_new();
    PyIteratorObserver *makeObserver(std::unique_ptr<db0::object_model::QueryObserver> &&observer);

    void PyIteratorObserver_del(PyIteratorObserver* self);
    
    extern PyTypeObject PyIteratorObserverType;

    bool PyIteratorObserver_Check(PyObject *);
    

}