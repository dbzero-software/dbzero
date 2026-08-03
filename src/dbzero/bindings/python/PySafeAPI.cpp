// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "PySafeAPI.hpp"    

namespace db0::python

{

    PyObject * PyBool_fromBool(bool value)
    {
        if (value) {
            Py_RETURN_TRUE;
        } else {
            Py_RETURN_FALSE;
        }
    }
    
}