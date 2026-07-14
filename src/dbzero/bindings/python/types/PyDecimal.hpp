// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>

namespace db0::python 

{

    PyObject *getDecimalClass();
    PyObject *uint64ToPyDecimal(std::uint64_t);
    std::uint64_t pyDecimalToUint64(PyObject *);
    
}