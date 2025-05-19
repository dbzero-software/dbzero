#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>

namespace db0::python 

{

    PyObject *getDecimalClass();
    PyObject *uint64ToPyDecimal(std::uint64_t);
    std::uint64_t pyDecimalToUint64(PyObject *);
    
}