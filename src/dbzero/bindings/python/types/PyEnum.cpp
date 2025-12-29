// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyEnum.hpp"
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/Utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>

namespace db0::python

{
    
    PyEnum *PyEnum_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyEnum*>(type->tp_alloc(type, 0));
    }
    
    PyEnumValue *PyEnumValue_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyEnumValue*>(type->tp_alloc(type, 0));
    }

    PyEnumValueRepr *PyEnumValueRepr_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyEnumValueRepr*>(type->tp_alloc(type, 0));
    }

    PyEnum *PyEnumDefault_new() {
        return PyEnum_new(&PyEnumType, NULL, NULL);
    }

    shared_py_object<PyEnumValue*> PyEnumValueDefault_new() {
        return { PyEnumValue_new(&PyEnumValueType, NULL, NULL), false };
    }

    shared_py_object<PyEnumValueRepr*> PyEnumValueReprDefault_new() {
        return { PyEnumValueRepr_new(&PyEnumValueReprType, NULL, NULL), false };
    }

    void PyEnum_del(PyEnum* self)
    {
        PY_API_FUNC
        // destroy associated DB0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    shared_py_object<PyObject*> tryPyEnum_getenum(PyEnum *self, PyObject *attr)
    {        
        auto &enum_data = self->ext();
        if (enum_data.exists()) {
            return enum_data.get().tryGetLangValue(PyUnicode_AsUTF8(attr));
        } else {
            if (PyToolkit::getPyWorkspace().hasWorkspace()) {
                // note that enum is created on demand
                auto enum_ptr = self->modifyExt().tryCreate();
                if (enum_ptr) {
                    return enum_ptr->tryGetLangValue(PyUnicode_AsUTF8(attr));
                }
            }
            
            // if unable to create (e.g. prefix not accessible for read/write or Worspace unavailable) then
            // return a value placeholder
            if (enum_data.hasValue(PyUnicode_AsUTF8(attr))) {
                return makePyEnumValueRepr(enum_data.m_enum_type_def, PyUnicode_AsUTF8(attr)).steal();
            }
            return nullptr;
        }
    }
    
    PyObject *tryPyEnum_getattro(PyEnum *self, PyObject *attr)
    {   
        auto obj_ptr = tryPyEnum_getenum(self, attr);
        if (obj_ptr.get()) {
            return obj_ptr.steal();
        }
                
        return _PyObject_GenericGetAttrWithDict(reinterpret_cast<PyObject*>(self), attr, NULL, 0);        
    }

    PyObject *PyEnum_getattro(PyEnum *self, PyObject *attr)
    {
        PY_API_FUNC
        return runSafe(tryPyEnum_getattro, self, attr);
    }
    
    void PyEnumValue_del(PyEnumValue* self)
    {
        // destroy associated DB0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    void PyEnumValueRepr_del(PyEnumValueRepr* self)
    {
        // destroy associated DB0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    PyObject *getEnumValueRepr(PyEnum *self, PyObject *py_key)
    {        
        // check if enum value exists in the definition
        auto type_def = self->ext().m_enum_type_def;
        const auto &enum_def = type_def->m_enum_def;
        if (PyUnicode_Check(py_key)) {
            auto key = PyUnicode_AsUTF8(py_key);
            if (std::find(enum_def.m_values.begin(), enum_def.m_values.end(), key) == enum_def.m_values.end()) {
                PyErr_SetString(PyExc_KeyError, "Key not found");
                return NULL;
            }
            return makePyEnumValueRepr(type_def, key).steal();
        } else if (PyLong_Check(py_key)) {
            auto index = PyLong_AsLong(py_key);
            if (index < 0) {
                index += enum_def.m_values.size();
            }
            if (index < 0 || index > (long)enum_def.m_values.size()) {
                PyErr_SetString(PyExc_IndexError, "Index out of range");
                return NULL;
            }
            return makePyEnumValueRepr(type_def, enum_def.m_values[index].c_str()).steal();
        } else {
            PyErr_SetString(PyExc_TypeError, "Key must be a string or an integer");
            return NULL;
        }
    }

    PyObject *getEnumValuesRepr(PyEnum *self)
    {
        auto &values = self->ext().getValueDefs();
        auto py_tuple = Py_OWN(PyTuple_New(values.size()));
        unsigned int index = 0;
        auto enum_type_def = self->ext().m_enum_type_def;
        for (auto &value: values) {
            PySafeTuple_SetItem(*py_tuple, index, makePyEnumValueRepr(enum_type_def, value.c_str()));
            ++index;
        }
        return py_tuple.steal();
    }
    
    PyObject *tryGetEnumValues(PyEnum *self)
    {
        const Enum *enum_ = nullptr;
        if (self->ext().exists()) {
            enum_ = &self->ext().get();
        } else {
            // note that enum is created on demand
            enum_ = self->modifyExt().tryCreate();
            // return value repr instead of materialized enum values
            if (!enum_) {
                return getEnumValuesRepr(self);
            }
        }
                
        auto enum_values = enum_->getValues();
        auto py_tuple = Py_OWN(PyTuple_New(enum_values.size()));
        unsigned int index = 0;
        for (auto &value: enum_values) {
            PySafeTuple_SetItem(*py_tuple, index, enum_->getLangValue(value));
            ++index;
        }
        return py_tuple.steal();
    }
    
    PyObject *PyAPI_getEnumValues(PyEnum *self)
    {
        PY_API_FUNC
        return runSafe(tryGetEnumValues, self);
    }

    Py_ssize_t tryPyEnum_len(PyEnum *py_enum) {
        return py_enum->ext().size();
    }

    Py_ssize_t PyAPI_PyEnum_len(PyEnum *py_enum)
    {
        PY_API_FUNC
        return runSafe(tryPyEnum_len, py_enum);
    }

    PyObject *tryPyEnum_GetItem(PyEnum *self, PyObject *py_key)
    {
        // note that only string and integer keys are supported
        if (!PyUnicode_Check(py_key) && !PyLong_Check(py_key)) {
            PyErr_SetString(PyExc_TypeError, "Key must be a string or an integer");
            return NULL;
        }

        const Enum *enum_ = nullptr;
        if (self->ext().exists()) {
            enum_ = &self->ext().get();
        } else {
            // note that enum is created on demand
            enum_ = self->modifyExt().tryCreate();
            // return value repr instead of materialized enum values
            if (!enum_) {
                return getEnumValueRepr(self, py_key);
            }
        }

        if (PyUnicode_Check(py_key)) {
            auto key = PyUnicode_AsUTF8(py_key);
            return enum_->getLangValue(key).steal();
        } else if (PyLong_Check(py_key)) {
            auto index = PyLong_AsLong(py_key);
            if (index < 0) {
                index += enum_->size();
            }
            if (index < 0 || index > (long)enum_->size()) {
                PyErr_SetString(PyExc_IndexError, "Index out of range");
                return NULL;
            }
            return enum_->getLangValue(index).steal();
        }

        assert(false);
        PyErr_SetString(PyExc_TypeError, "Key must be a string or an integer");
        return NULL;        
    }
    
    PyObject *PyAPI_PyEnum_GetItem(PyEnum *py_enum, PyObject *py_key)
    {
        PY_API_FUNC
        return runSafe(tryPyEnum_GetItem, py_enum, py_key);
    }

    static PyMappingMethods PyEnum_mp = {
        .mp_length = (lenfunc)PyAPI_PyEnum_len,
        .mp_subscript = (binaryfunc)PyAPI_PyEnum_GetItem
    };

    static PyMethodDef PyEnum_methods[] = 
    {
        {"values", (PyCFunction)PyAPI_getEnumValues, METH_NOARGS, "Get enum values"},
        {NULL}
    };
    
    PyTypeObject PyEnumType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.Enum",
        .tp_basicsize = PyEnum::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyEnum_del,
        .tp_as_mapping = &PyEnum_mp,
        .tp_getattro = reinterpret_cast<getattrofunc>(PyEnum_getattro),
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Enum object",
        .tp_methods = PyEnum_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyEnum_new,        
        .tp_free = PyObject_Free,
    };
    
    static PyMethodDef PyEnumValue_methods[] = 
    {
        {NULL}
    };
    
    PyObject *EnumValueRepr_rq(PyEnumValueRepr *enum_value_repr_obj, PyObject *other, int op)
    {        
        // Compare to other enum value repr
        if (PyEnumValueRepr_Check(other)) {
            PyEnumValueRepr *other_enum_value_repr = (PyEnumValueRepr*) other;
            switch (op)
            {
            case Py_EQ:
                return PyBool_fromBool(enum_value_repr_obj->ext() == other_enum_value_repr->ext());
            case Py_NE:
                return PyBool_fromBool(enum_value_repr_obj->ext() != other_enum_value_repr->ext());
            default:
                Py_RETURN_NOTIMPLEMENTED;
            }
        // Compare to enum value
        } else if (PyEnumValue_Check(other)) {
            PyEnumValue *other_enum_value = (PyEnumValue*) other;
            auto &enum_value = other_enum_value->ext();
            assert(enum_value);
            auto &enum_factory = enum_value.m_fixture.safe_lock()->get<db0::object_model::EnumFactory>();
            // try converting value repr to enum value
            auto enum_ = enum_factory.tryGetEnumValue(enum_value_repr_obj->ext());
            
            switch (op)
            {
                case Py_EQ: {
                    if (enum_) {
                        // compare as enum values
                        return PyBool_fromBool(*enum_ == enum_value);
                    } else {
                        Py_RETURN_FALSE;
                    }
                }
                break;
                
                case Py_NE: {
                    if (enum_) {
                        // compare as enum values
                        return PyBool_fromBool(*enum_ != enum_value);
                    } else {
                        Py_RETURN_TRUE;
                    }
                }
                break;

                default: {
                    Py_RETURN_NOTIMPLEMENTED;
                }
            }
        } else {
            Py_RETURN_NOTIMPLEMENTED;            
        }
    }

    PyObject *EnumValue_rq(PyEnumValue *enum_value_obj, PyObject *other, int op)
    {        
        // Compare to other enum value
        if (PyEnumValue_Check(other)) {
            PyEnumValue *other_enum_value = (PyEnumValue*) other;
            switch (op)
            {
            case Py_EQ:
                return PyBool_fromBool(enum_value_obj->ext() == other_enum_value->ext());
            case Py_NE:
                return PyBool_fromBool(enum_value_obj->ext() != other_enum_value->ext());
            default:
                Py_RETURN_NOTIMPLEMENTED;
            }
        // Compare to enum value repr
        } else if (PyEnumValueRepr_Check(other)) {
            return EnumValueRepr_rq((PyEnumValueRepr*) other, enum_value_obj, op);
        } else {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }

    PyObject *PyAPI_EnumValue_rq(PyEnumValue *enum_value_obj, PyObject *other, int op)
    {
        PY_API_FUNC
        return runSafe(EnumValue_rq, enum_value_obj, other, op);
    }
    
    Py_hash_t PyAPI_PyEnumValue_hash(PyObject *enum_value_obj)
    {
        PY_API_FUNC
        return PyToolkit::getTypeManager().extractEnumValue(enum_value_obj).getPermHash();        
    }
    
    PyTypeObject PyEnumValueType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.EnumValue",
        .tp_basicsize = PyEnumValue::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyEnumValue_del,
        .tp_repr = reinterpret_cast<reprfunc>(PyEnumValue_repr),
        .tp_hash = reinterpret_cast<hashfunc>(PyAPI_PyEnumValue_hash),
        .tp_str = reinterpret_cast<reprfunc>(PyEnumValue_str),        
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Enum value object",
        .tp_richcompare = (richcmpfunc)PyAPI_EnumValue_rq,
        .tp_methods = PyEnumValue_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyEnumValue_new,
        .tp_free = PyObject_Free,
    };
    
    PyObject *PyAPI_EnumValueRepr_rq(PyEnumValueRepr *enum_value_repr_obj, PyObject *other, int op)
    {
        PY_API_FUNC
        return runSafe(EnumValueRepr_rq, enum_value_repr_obj, other, op);
    }
    
    PyTypeObject PyEnumValueReprType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.EnumValueRepr",
        .tp_basicsize = PyEnumValueRepr::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyEnumValueRepr_del,
        .tp_repr = reinterpret_cast<reprfunc>(PyEnumValueRepr_repr),
        .tp_hash = reinterpret_cast<hashfunc>(PyEnumValueRepr_hash),
        .tp_str = reinterpret_cast<reprfunc>(PyEnumValueRepr_str),        
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Enum value object",
        .tp_richcompare = (richcmpfunc)PyAPI_EnumValueRepr_rq,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyEnumValueRepr_new,
        .tp_free = PyObject_Free,
    };
    
    bool PyEnum_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyEnumType;        
    }

    bool PyEnumType_Check(PyTypeObject *py_type) {
        return py_type == &PyEnumType;
    }
    
    bool PyEnumValue_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyEnumValueType;        
    }
    
    bool PyEnumValueRepr_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyEnumValueReprType;        
    }
    
    PyObject *tryMakeEnum(PyObject *, const std::string &enum_name,
        const std::vector<std::string> &user_enum_values, const char *type_id, const char *prefix_name)
    {
        auto &type_manager = PyToolkit::getTypeManager();
        // use empty module name since it's unknown
        auto enum_def = EnumFullDef {enum_name, "", user_enum_values, type_id };
        auto enum_ = type_manager.tryFindEnum(enum_def);
        if (enum_.get()) {
            // validate if the definitions are matching
            if (reinterpret_cast<PyEnum*>(*enum_)->ext().m_enum_type_def->m_enum_def != enum_def) {
                std::stringstream _str;
                _str << "Enum type already defined with different values: " << enum_def.m_name
                     << " in module " << enum_def.m_module_name;
                PyErr_SetString(PyExc_RuntimeError, _str.str().c_str());
                return NULL;
            }
            // enum already exists
            return enum_.steal();
        }
        
        // create a new enum instance
        auto py_enum = Py_OWN(PyEnumDefault_new());
        // use empty module name since it's unknown
        py_enum->makeNew(enum_def, prefix_name);
        PyToolkit::getTypeManager().addEnum(*py_enum);
        return py_enum.steal();
    }
    
    PyObject *tryMakeEnumFromType(PyObject *self, PyTypeObject *py_type, const std::vector<std::string> &enum_values,
        const char *type_id, const char *prefix_name)
    {
        return tryMakeEnum(self, py_type->tp_name, enum_values, type_id, prefix_name);
    }
    
    shared_py_object<PyEnumValue*> makePyEnumValue(const EnumValue &enum_value)
    {
        auto py_enum_value = PyEnumValueDefault_new();
        py_enum_value->makeNew(enum_value);
        return py_enum_value;
    }
    
    PyObject *PyEnumValue_str(PyEnumValue *self) {
        return PyUnicode_FromString(self->ext().m_str_repr.c_str());
    }
    
    PyObject *PyEnumValue_repr(PyEnumValue *self)
    {        
        auto &enum_factory = self->ext().m_fixture.safe_lock()->get<db0::object_model::EnumFactory>();
        auto enum_ = enum_factory.getEnumByUID(self->ext().m_enum_uid);
        return PyUnicode_FromFormat("<EnumValue %s.%s>", enum_->getName().c_str(), self->ext().m_str_repr.c_str());
    }

    PyObject *PyEnumValueRepr_str(PyEnumValueRepr *self) {
        return PyUnicode_FromString(self->ext().m_str_repr.c_str());
    }
    
    PyObject *PyEnumValueRepr_repr(PyEnumValueRepr *self) {
        return PyUnicode_FromFormat("<EnumValue ???.%s>", self->ext().m_str_repr.c_str());
    }
    
    shared_py_object<PyEnumValueRepr*> makePyEnumValueRepr(std::shared_ptr<EnumTypeDef> enum_type_def, 
        const char *str_value)
    {
        auto py_enum_value = PyEnumValueReprDefault_new();
        py_enum_value->makeNew(enum_type_def, str_value);
        return py_enum_value;
    }
    
    PyObject *tryLoadEnumValue(PyEnumValue *py_enum_value) {
        // load as string
        return PyEnumValue_str(py_enum_value);        
    }
    
    bool isMigrateRequired(db0::swine_ptr<Fixture> &fixture, PyEnumValue *py_enum_value)
    {
        auto &enum_value = py_enum_value->ext();
        // translation is needed if prefixes differ
        assert(enum_value);
        return !db0::is_same(enum_value.m_fixture, fixture);
    }
    
    shared_py_object<PyObject*> migratedEnumValue(db0::swine_ptr<Fixture> &fixture, PyEnumValue *py_enum_value)
    {
        auto &enum_value = py_enum_value->ext();
        assert(enum_value);
        if (db0::is_same(enum_value.m_fixture, fixture)) {
            // no translation needed
            return py_enum_value;
        }
        
        // migrate enum value to the destination fixture
        return fixture->get<db0::object_model::EnumFactory>().migrateEnumLangValue(enum_value);        
    }
    
    PyObject *tryIsEnum(PyObject *arg) {
        return PyBool_fromBool(PyEnum_Check(arg));
    }

    PyObject *PyAPI_isEnum(PyObject *, PyObject *const * args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "isEnum requires exactly 1 argument");
            return NULL;
        }
        return runSafe(tryIsEnum, args[0]);
    }
    
    Py_hash_t PyEnumValueRepr_hash(PyEnumValueRepr *obj_ptr) {
        return obj_ptr->ext().getPermHash();
    }

}
