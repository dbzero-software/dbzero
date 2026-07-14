// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/bindings/python/types/PyEnumType.hpp>
#include <dbzero/object_model/enum/EnumDef.hpp>
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/object_model/enum/EnumValue.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python

{

    using EnumValue = db0::object_model::EnumValue;
    using EnumValueRepr = db0::object_model::EnumValueRepr;
    using EnumDef = db0::object_model::EnumDef;
    using EnumFullDef = db0::object_model::EnumFullDef;
    using EnumTypeDef = db0::object_model::EnumTypeDef;
    using Enum = db0::object_model::Enum;
    using PyEnumValue = PyWrapper<EnumValue, false>;
    using PyEnumValueRepr = PyWrapper<EnumValueRepr, false>;
    
    PyEnum *PyEnum_new(PyTypeObject *type, PyObject *, PyObject *);
    PyEnum *PyEnumDefault_new();
    void PyEnum_del(PyEnum* self);
    PyObject *PyEnum_getattro(PyEnum *, PyObject *attr);
    Py_hash_t PyAPI_PyEnumValue_hash(PyObject*);

    PyEnumValue *PyEnumValue_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<PyEnumValue*> PyEnumValueDefault_new();

    void PyEnumValue_del(PyEnumValue *);
    PyObject *PyEnumValue_str(PyEnumValue *);
    PyObject *PyEnumValue_repr(PyEnumValue *);

    PyEnumValueRepr *PyEnumValueRepr_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<PyEnumValueRepr*> PyEnumValueReprDefault_new();
    
    void PyEnumValueRepr_del(PyEnumValueRepr *);
    PyObject *PyEnumValueRepr_str(PyEnumValueRepr *);
    Py_hash_t PyEnumValueRepr_hash(PyEnumValueRepr *);
    PyObject *PyEnumValueRepr_repr(PyEnumValueRepr *);    
    
    extern PyTypeObject PyEnumType;
    extern PyTypeObject PyEnumValueType;
    extern PyTypeObject PyEnumValueReprType;
    
    bool PyEnum_Check(PyObject *);
    bool PyEnumType_Check(PyTypeObject *);
    bool PyEnumValue_Check(PyObject *);
    bool PyEnumValueRepr_Check(PyObject *);
    
    // Find existing or create a new enum object (in DB0 it's not a type)
    PyObject *tryMakeEnum(PyObject *, const std::string &enum_name, const std::vector<std::string> &enum_values, 
        const char *type_id, const char *prefix_name);
    PyObject *tryMakeEnumFromType(PyObject *, PyTypeObject *, const std::vector<std::string> &enum_values,
        const char *type_id, const char *prefix_name);
    
    shared_py_object<PyEnumValue*> makePyEnumValue(const EnumValue &);
    shared_py_object<PyEnumValueRepr*> makePyEnumValueRepr(std::shared_ptr<EnumTypeDef>, const char *value);
    // check if enum value migration / translation is required
    bool isMigrateRequired(db0::swine_ptr<Fixture> &, PyEnumValue *);
    // migrate / translate enum value between prefixes if needed
    shared_py_object<PyObject*> migratedEnumValue(db0::swine_ptr<Fixture> &, PyEnumValue *);
    
    PyObject *tryLoadEnumValue(PyEnumValue *);
    PyObject *PyAPI_isEnum(PyObject *self, PyObject *const * args, Py_ssize_t nargs);
    
}


 