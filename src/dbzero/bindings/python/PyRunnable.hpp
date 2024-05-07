#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/core/collections/full_text/FT_Runnable.hpp>

namespace db0::python

{
    
    using PyRunnableObject = PyWrapper<db0::FT_Runnable>;
    
    PyRunnableObject *PyRunnable_new(PyTypeObject *type, PyObject *, PyObject *);
    PyRunnableObject *PyRunnableDefault_new();
    void PyRunnable_del(PyRunnableObject* self);
    
    extern PyTypeObject PyRunnableObjectType;
        
    bool PyRunnable_Check(PyObject *);
        
}