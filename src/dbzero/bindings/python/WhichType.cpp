// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "WhichType.hpp"

namespace db0::python

{
    
    template <> bool Which_TypeCheck<PyTypeObject>(PyObject *py_object)
    {
        return PyType_Check(py_object);
    }

}