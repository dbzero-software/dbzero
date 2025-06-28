#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/object_model/CommonBase.hpp>

namespace db0::python 

{
   
    // common type for Python counterparts of dbzero object model's objects
    using PyCommonBase = PyWrapper<db0::object_model::CommonBase>;
    
}