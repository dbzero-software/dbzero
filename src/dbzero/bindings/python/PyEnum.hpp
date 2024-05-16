#pragma once
#include "PyWrapper.hpp"
#include <dbzero/object_model/enum/Enum.hpp>

namespace db0::python

{

    using PyEnum = PyWrapper<db0::object_model::Enum>;    
    
    PyEnum *PyEnum_new(PyTypeObject *type, PyObject *, PyObject *);
    PyEnum *PyEnumDefault_new();
    void PyEnum_del(PyEnum* self);
    
    extern PyTypeObject PyEnumType;
        
    bool PyEnum_Check(PyObject *);
    
    // Find existing or create a new enum object (in DB0 it's not a type)
    PyObject *tryMakeEnum(PyObject *, const std::string &enum_name, const std::vector<std::string> &enum_values);
    
}


 