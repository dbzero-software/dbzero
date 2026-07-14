// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::python

{

    template <typename T> bool Which_TypeCheck(PyObject *py_object);

    template <> bool Which_TypeCheck<PyTypeObject>(PyObject *py_object);
    
    template <typename... Types> struct WhichInspector;

    template <typename T> struct WhichInspector<T>
    {
        static int whichType(PyObject *arg, int offset) 
        {
            if (Which_TypeCheck<T>(arg)) {
                return offset;
            }

            THROWF(db0::InputException) << "Invalid argument type" << THROWF_END;            
        }
    };

    template <typename T, typename... Types> struct WhichInspector<T, Types...>
    {
        static int whichType(PyObject *arg, int offset) 
        {
            if (Which_TypeCheck<T>(arg)) {
                return offset;
            }

            return WhichInspector<Types...>::whichType(arg, offset + 1);
        }
    };
    
    /**
     * Recognize between possible types
     * @return type index starting from 0
    */
    template <typename T, typename... Types> int whichType(PyObject *py_object, int offset = 0) {
        return WhichInspector<T, Types...>::whichType(py_object, offset);
    }
    
}