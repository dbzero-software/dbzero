#include "PyEnum.hpp"
#include "PyInternalAPI.hpp"
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python

{
    EnumData::EnumData(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : m_enum(fixture, address)
    {
    }

    EnumData::EnumData(db0::swine_ptr<Fixture> &fixture, const std::vector<std::string> &values)
        : m_enum(fixture, values)
    {
    }

    PyEnumValue *EnumCache::get(const EnumValue &value)
    {
        auto py_enum_value = find(value.m_str_repr);
        if (!py_enum_value) {
            py_enum_value = makePyEnumValue(value);
            m_py_enum_values_map[value.m_str_repr] = py_enum_value;
        }
        return py_enum_value;
    }

    PyEnumValue *EnumCache::find(const std::string &name) const
    {
        auto it = m_py_enum_values_map.find(name);
        if (it == m_py_enum_values_map.end()) {
            return nullptr;
        }
        return it->second.get();
    }

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

    PyObject *tryPyEnum_getattro(PyEnum *self, PyObject *attr)
    {
        auto &enum_data = self->ext();
        auto py_enum_value = enum_data.m_cache.find(PyUnicode_AsUTF8(attr));
        if (!py_enum_value) {
            // get from dbzero (pull through cache)
            py_enum_value = enum_data.m_cache.get(enum_data.m_enum.get(PyUnicode_AsUTF8(attr)));
        }

        Py_INCREF(py_enum_value);
        return py_enum_value;
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
        self->ext().~EnumValue();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    PyObject *getEnumValues(PyEnum *self)
    {
        auto &enum_data = self->ext();
        auto enum_values = enum_data.m_enum.getValues();
        // create tuple
        auto py_tuple = PyTuple_New(enum_values.size());
        unsigned int index = 0;
        for (auto &value: enum_values) {
            auto py_enum_value = enum_data.m_cache.get(value);
            PyTuple_SET_ITEM(py_tuple, index, py_enum_value);
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

    PyObject *tryMakeEnum(PyObject *, const std::string &enum_name, const std::vector<std::string> &user_enum_values)
    {
        auto py_enum = PyEnumDefault_new();
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        new (&py_enum->ext()) EnumData(*fixture, user_enum_values);

        auto &enum_data = py_enum->ext();
        // pull into cache
        for (auto &value: enum_data.m_enum.getValues()) {
            enum_data.m_cache.get(value);
        }        
        return py_enum;
    }

    PyEnumValue *makePyEnumValue(const EnumValue &enum_value)
    {
        auto py_enum_value = PyEnumValueDefault_new();
        py_enum_value->ext() = enum_value;
        return py_enum_value;
    }

}

