#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include "shared_py_object.hpp"

namespace db0::python 

{

    class PyTypes
    {
    public:
        // type acting as raw pointer, no ownership passing or sharing
        using ObjectPtr = PyObject*;
        // type acting as a shared pointer to an object instance
        using ObjectSharedPtr = shared_py_object<PyObject*>;
        using TypeObjectPtr = PyTypeObject*;
        using TypeObjectSharedPtr = shared_py_object<PyTypeObject*>;
    };
    
}