// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "WhichType.hpp"

namespace db0::python

{
    
    template <> bool Which_TypeCheck<PyTypeObject>(PyObject *py_object)
    {
        return PyType_Check(py_object);
    }

}