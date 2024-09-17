#pragma once

#include <Python.h>
#include <cstdint>

namespace db0::python

{   

    PyObject *tryGetPrefixes();
    
    PyObject *tryGetMemoClasses(const char *prefix_name, std::uint64_t prefix_uuid);
    
    PyObject *tryGetAttributes(const char *memo_uuid);
    
}
