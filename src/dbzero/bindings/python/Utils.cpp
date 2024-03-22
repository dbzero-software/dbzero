
#include "Utils.hpp"

namespace db0::python
{

    PyObject * PyBool_fromBool(bool value){
        return value ? Py_True : Py_False;
    }

}