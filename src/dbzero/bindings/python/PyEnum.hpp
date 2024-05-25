#pragma once
#include "PyWrapper.hpp"
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/object_model/enum/EnumValue.hpp>

namespace db0::python

{

    using EnumValue = db0::object_model::EnumValue;
    using PyEnumValue = PyWrapper<EnumValue>;    
    
    struct EnumCache
    {
        // pull through cache (new item is created if not found)
        PyEnumValue *get(const EnumValue &);
        // find in cache (returns nullptr if not found)
        PyEnumValue *find(const std::string &name) const;

        std::unordered_map<std::string, shared_py_object<PyEnumValue*>> m_py_enum_values_map;
    };
    
    struct EnumData
    {
        EnumCache m_cache;
        db0::object_model::Enum m_enum;

        EnumData(db0::swine_ptr<Fixture> &, std::uint64_t address);
        EnumData(db0::swine_ptr<Fixture> &, const std::vector<std::string> &values);
    };

    using PyEnum = PyWrapper<EnumData>;

    PyEnum *PyEnum_new(PyTypeObject *type, PyObject *, PyObject *);
    PyEnum *PyEnumDefault_new();
    void PyEnum_del(PyEnum* self);
    PyObject *PyEnum_getattro(PyEnum *, PyObject *attr);
    
    PyEnumValue *PyEnumValue_new(PyTypeObject *type, PyObject *, PyObject *);
    PyEnumValue *PyEnumValueDefault_new();
    void PyEnumValue_del(PyEnumValue *);
    
    extern PyTypeObject PyEnumType;
    extern PyTypeObject PyEnumValueType;
        
    bool PyEnum_Check(PyObject *);
    bool PyEnumValue_Check(PyObject *);
    
    // Find existing or create a new enum object (in DB0 it's not a type)
    PyObject *tryMakeEnum(PyObject *, const std::string &enum_name, const std::vector<std::string> &enum_values);
    PyEnumValue *makePyEnumValue(const EnumValue &);
    
}


 