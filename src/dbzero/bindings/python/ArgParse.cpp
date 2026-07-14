// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ArgParse.hpp"

namespace db0::python
{

const char* parseStringLikeArgument(PyObject *arg, const char *func_name, const char *arg_name) {
    const char* result = nullptr;

    if (PyUnicode_Check(arg)) {
        result = PyUnicode_AsUTF8(arg);
        if (!result) {
            // Exception already set by PyUnicode_AsUTF8
            return nullptr;
        }
    } else if (PyBytes_Check(arg)) {
        result = PyBytes_AsString(arg);
        if (!result) {
            // Exception already set by PyBytes_AsString
            return nullptr;
        }
    } else {
        PyErr_Format(PyExc_TypeError,
                     "%s() argument '%s' must be str or bytes, not %s",
                     func_name, arg_name, Py_TYPE(arg)->tp_name);
        return nullptr;
    }
    
    return result;
}

}
