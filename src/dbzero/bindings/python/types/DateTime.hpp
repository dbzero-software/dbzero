// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>

namespace db0::python 

{
   

    bool isDatatimeWithTZ(PyObject *py_datetime);

    PyObject * uint64ToPyDatetime(std::uint64_t datetime);
    std::uint64_t pyDateTimeToToUint64(PyObject *py_datetime);
    
    PyObject * uint64ToPyDatetimeWithTZ(std::uint64_t datetime);
    std::uint64_t pyDateTimeWithTzToUint64(PyObject *py_datetime);

    PyObject * uint64ToPyDate(std::uint64_t date);
    std::uint64_t pyDateToUint64(PyObject *py_date);

    PyObject * uint64ToPyTime(std::uint64_t date);
    std::uint64_t pyTimeToUint64(PyObject *py_date);

    PyObject * uint64ToPyTimeWithTz(std::uint64_t date);
    std::uint64_t pyTimeWithTzToUint64(PyObject *py_date);

    
}