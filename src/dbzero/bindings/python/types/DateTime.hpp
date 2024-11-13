#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>

namespace db0::python 

{
   
    PyObject * uint64ToPyDatetime(std::uint64_t datetime);
    std::uint64_t pyDateTimeToToUint64(PyObject *py_datetime);
    
}