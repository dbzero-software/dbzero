#include "AnyObjectAPI.hpp"
#include "PyInternalAPI.hpp"

namespace db0::python

{

    Py_ssize_t AnyObject_Length(PyObject *py_obj)
    {
        WITH_PY_API_UNLOCKED
        return PyObject_Length(py_obj);
    }

    PyObject *AnyObject_GetIter(PyObject *py_obj)
    {
        WITH_PY_API_UNLOCKED
        return PyObject_GetIter(py_obj);
    }

    PyObject *AnyIter_Next(PyObject *py_obj)
    {
        WITH_PY_API_UNLOCKED
        return PyIter_Next(py_obj);
    }

    Py_hash_t AnyObject_Hash(PyObject *py_obj)
    {
        WITH_PY_API_UNLOCKED
        return PyObject_Hash(py_obj);
    }
    
}