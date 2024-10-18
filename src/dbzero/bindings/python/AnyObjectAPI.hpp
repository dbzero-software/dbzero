#pragma once

#include <Python.h>
#include "PyInternalAPI.hpp"

namespace db0::python

{

    // Wrappers for CPython API functions
    // safe to use with both CPython and db0 objects
    Py_ssize_t AnyObject_Length(PyObject *);
    PyObject *AnyObject_GetIter(PyObject *);
    PyObject *AnyIter_Next(PyObject *);
    Py_hash_t AnyObject_Hash(PyObject *);
    
}
