// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
        // shared pointer assumed as the "external reference" - i.e. not owned by the language
        using ObjectSharedExtPtr = shared_py_object<PyObject*, true>;
        using TypeObjectPtr = PyTypeObject*;
        using TypeObjectSharedPtr = shared_py_object<PyTypeObject*>;
    };
    
}