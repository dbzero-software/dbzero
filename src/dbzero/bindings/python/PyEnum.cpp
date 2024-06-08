#include "PyEnum.hpp"
#include "PyInternalAPI.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>

namespace db0::python

{
    
    PyEnum *PyEnum_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyEnum*>(type->tp_alloc(type, 0));
    }

    PyEnumValue *PyEnumValue_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyEnumValue*>(type->tp_alloc(type, 0));
    }

    PyEnum *PyEnumDefault_new() {
        return PyEnum_new(&PyEnumType, NULL, NULL);
    }

    PyEnumValue *PyEnumValueDefault_new() {
        return PyEnumValue_new(&PyEnumValueType, NULL, NULL);
    }

    void PyEnum_del(PyEnum* self)
    {
        // destroy associated DB0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    PyObject *tryPyEnum_getattro(PyEnum *self, PyObject *attr) {
        return self->ext()->getLangValue(PyUnicode_AsUTF8(attr)).steal();
    }
    
    PyObject *PyEnum_getattro(PyEnum *self, PyObject *attr) 
    {
        auto res = _PyObject_GetDescrOptional(reinterpret_cast<PyObject*>(self), attr);
        if (res) {
            return res;
        }
        return runSafe(tryPyEnum_getattro, self, attr);
    }
    
    void PyEnumValue_del(PyEnumValue* self)
    {
        // destroy associated DB0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    PyObject *getEnumValues(PyEnum *self)
    {
        auto &enum_ = *self->ext();
        auto enum_values = enum_.getValues();
        // create tuple
        auto py_tuple = PyTuple_New(enum_values.size());
        unsigned int index = 0;
        for (auto &value: enum_values) {
            PyTuple_SET_ITEM(py_tuple, index, enum_.getLangValue(value).steal());
            ++index;
        }
        return py_tuple;
    }

    static PyMethodDef PyEnum_methods[] = 
    {
        {"values", (PyCFunction)getEnumValues, METH_NOARGS, "Get enum values"},
        {NULL}
    };

    PyTypeObject PyEnumType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Enum",
        .tp_basicsize = PyEnum::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyEnum_del,
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

    PyTypeObject PyEnumValueType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.EnumValue",
        .tp_basicsize = PyEnumValue::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyEnumValue_del,
        .tp_repr = reinterpret_cast<reprfunc>(PyEnumValue_repr),
        .tp_str = reinterpret_cast<reprfunc>(PyEnumValue_str),        
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Enum value object",
        .tp_methods = PyEnumValue_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyEnumValue_new,
        .tp_free = PyObject_Free,
    };
    
    bool PyEnum_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyEnumType;        
    }

    bool PyEnumValue_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyEnumValueType;        
    }
    
    PyObject *tryMakeEnum(PyObject *, const std::string &enum_name,
        const std::vector<std::string> &user_enum_values, const char *type_id)
    {
        auto py_enum = PyEnumDefault_new();
        // use empty module name since it's unknown
        PyEnumData::makeNew(&py_enum->ext(), EnumDef {enum_name, "", user_enum_values}, type_id);
        PyToolkit::getTypeManager().addEnum(py_enum);
        return py_enum;
    }

    PyObject *tryMakeEnumFromType(PyObject *self, PyTypeObject *py_type, const std::vector<std::string> &enum_values, const char *type_id) {
        return tryMakeEnum(self, py_type->tp_name, enum_values, type_id);
    }

    PyEnumValue *makePyEnumValue(const EnumValue &enum_value)
    {
        auto py_enum_value = PyEnumValueDefault_new();
        py_enum_value->ext() = enum_value;
        return py_enum_value;
    }
    
    PyObject *PyEnumValue_str(PyEnumValue *self) {
        return PyUnicode_FromString(self->ext().m_str_repr.c_str());
    }

    PyObject *PyEnumValue_repr(PyEnumValue *self)
    {
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(self->ext().m_fixture_uuid);
        auto &enum_factory = fixture->get<db0::object_model::EnumFactory>();
        auto enum_ = enum_factory.getEnumByUID(self->ext().m_enum_uid);
        return PyUnicode_FromFormat("<EnumValue %s.%s>", enum_->getName().c_str(), self->ext().m_str_repr.c_str());
    }

}

