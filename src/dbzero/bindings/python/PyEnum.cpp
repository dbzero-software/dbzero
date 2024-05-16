#include "PyEnum.hpp"
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python

{

    PyEnum *PyEnum_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyEnum*>(type->tp_alloc(type, 0));
    }

    PyEnum *PyEnumDefault_new() {
        return PyEnum_new(&PyEnumType, NULL, NULL);
    }

    void PyEnum_del(PyEnum* self) 
    {
        // destroy associated DB0 instance
        self->ext().~Enum();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyMethodDef PyEnum_methods[] = 
    {
        {NULL}
    };

    PyTypeObject PyEnumType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Enum",
        .tp_basicsize = PyEnum::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyEnum_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Enum object",
        .tp_methods = PyEnum_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyEnum_new,
        .tp_free = PyObject_Free,
    };

    bool PyEnum_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyEnumType;        
    }

    PyObject *tryMakeEnum(PyObject *, const std::string &enum_name, const std::vector<std::string> &enum_values)
    {
        auto py_object = PyEnumDefault_new();
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();        
        db0::object_model::Enum::makeNew(&py_object->ext(), *fixture, enum_name, enum_values);
        return py_object;
    }

}

