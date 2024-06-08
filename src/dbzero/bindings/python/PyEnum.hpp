#pragma once
#include "PyWrapper.hpp"
#include <dbzero/object_model/enum/EnumDef.hpp>
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/object_model/enum/EnumValue.hpp>

namespace db0::python

{

    using EnumValue = db0::object_model::EnumValue;
    using EnumDef = db0::object_model::EnumDef;
    using Enum = db0::object_model::Enum;
    using PyEnumValue = PyWrapper<EnumValue>;
    
    // must store EnumDef for deferred creation
    struct PyEnumData
    {
        EnumDef m_enum_def;
        std::optional<std::string> m_type_id;
        std::shared_ptr<Enum> m_enum_ptr;

        PyEnumData(const EnumDef &enum_def, const char *type_id);

        // when first accessed, tries pulling existing or creating a new enum in the current fixture
        Enum &operator*();
        Enum *operator->();

        static void makeNew(void *at_ptr, const EnumDef &enum_def, const char *type_id = nullptr);
    };

    using PyEnum = PyWrapper<PyEnumData>;
    
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
        const char *type_id = nullptr);
    PyObject *tryMakeEnumFromType(PyObject *, PyTypeObject *, const std::vector<std::string> &enum_values,
        const char *type_id = nullptr);
    
    PyEnumValue *makePyEnumValue(const EnumValue &);
    
}


 