#include "WhichType.hpp"

namespace db0::python

{
    
    template <> bool Which_TypeCheck<PyTypeObject>(PyObject *py_object)
    {
        return PyType_Check(py_object);
    }

}