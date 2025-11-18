#pragma once

#include <Python.h>

namespace db0::python
{

const char* parseStringLikeArgument(PyObject *arg, const char *func_name, const char *arg_name);

}
