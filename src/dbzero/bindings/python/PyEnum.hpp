#pragma once

#include "PyWrapper.hpp"
#include "PyEnumType.hpp"
#include <dbzero/object_model/enum/EnumDef.hpp>
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/object_model/enum/EnumValue.hpp>

namespace db0::python

{

    using EnumValue = db0::object_model::EnumValue;
    using EnumDef = db0::object_model::EnumDef;
    using Enum = db0::object_model::Enum;
    using PyEnumValue = PyWrapper<EnumValue, false>;
        
    PyEnum *PyEnum_new(PyTypeObject *type, PyObject *, PyObject *);
    PyEnum *PyEnumDefault_new();
    void PyEnum_del(PyEnum* self);
    PyObject *PyEnum_getattro(PyEnum *, PyObject *attr);
    
    PyEnumValue *PyEnumValue_new(PyTypeObject *type, PyObject *, PyObject *);
    PyEnumValue *PyEnumValueDefault_new();
    void PyEnumValue_del(PyEnumValue *);
    PyObject *PyEnumValue_str(PyEnumValue *self);
    PyObject *PyEnumValue_repr(PyEnumValue *self);
    
    extern PyTypeObject PyEnumType;
    extern PyTypeObject PyEnumValueType;
        
    bool PyEnum_Check(PyObject *);
    bool PyEnumValue_Check(PyObject *);
    
    // Find existing or create a new enum object (in DB0 it's not a type)
    PyObject *tryMakeEnum(PyObject *, const std::string &enum_name, const std::vector<std::string> &enum_values, 
        const char *type_id, const char *prefix_name);
    PyObject *tryMakeEnumFromType(PyObject *, PyTypeObject *, const std::vector<std::string> &enum_values,
        const char *type_id, const char *prefix_name);
    
    PyEnumValue *makePyEnumValue(const EnumValue &);
    
}


 