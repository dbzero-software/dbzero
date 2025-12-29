// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <dbzero/bindings/python/types/PyClass.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PyReflectionAPI.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>

namespace db0::python

{

    ClassObject *ClassObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<ClassObject*>(type->tp_alloc(type, 0));
    }
    
    shared_py_object<ClassObject*> ClassDefaultObject_new() {
        return { ClassObject_new(&ClassObjectType, NULL, NULL), false };
    }
    
    static PyMethodDef ClassObject_methods[] = 
    {
        // deprecated
        {"is_known_type", (PyCFunction)&PyAPI_PyClass_type_exists, METH_NOARGS, "Check if the corresponding Python type exists"},
        {"type", (PyCFunction)&PyAPI_PyClass_type, METH_NOARGS, "Retrieve associated Python type"},
        {"type_exists", (PyCFunction)&PyAPI_PyClass_type_exists, METH_NOARGS, "Check if the associated Python type exists without raising an exception"},
        {"get_attributes", (PyCFunction)&PyAPI_PyClass_get_attributes, METH_NOARGS, "Get memo class attributes"},
        {"type_info", (PyCFunction)&PyAPI_PyClass_type_info, METH_NOARGS, "Get memo class type information"},
        {NULL}
    };
    
    PyObject *tryPyClass_type_exists(PyObject *self)
    {        
        auto fixture = reinterpret_cast<ClassObject*>(self)->ext().getFixture();
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        return PyBool_fromBool(class_factory.hasLangType(reinterpret_cast<ClassObject*>(self)->ext()));
    }

    PyObject *PyAPI_PyClass_type_exists(PyObject *self, PyObject *)
    {
        PY_API_FUNC
        return runSafe(tryPyClass_type_exists, self);
    }
    
    PyTypeObject *tryPyClassType(PyObject *self)
    {
        auto fixture = reinterpret_cast<ClassObject*>(self)->ext().getFixture();
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        auto &model_class = reinterpret_cast<ClassObject*>(self)->ext();
        if (!class_factory.hasLangType(model_class)) {
            THROWF(db0::ClassNotFoundException) << "Class not found: " << model_class.getTypeName() << THROWF_END;
        }
        return class_factory.getLangType(model_class).steal();
    }
    
    PyObject *PyAPI_PyClass_type(PyObject *self, PyObject *)
    {
        PY_API_FUNC
        return reinterpret_cast<PyObject*>(runSafe(tryPyClassType, self));
    }

    void PyAPI_ClassObject_del(ClassObject* class_obj)
    {
        PY_API_FUNC
        // release associated shared_ptr
        class_obj->destroy();
        Py_TYPE(class_obj)->tp_free((PyObject*)class_obj);
    }
    
    PyObject *tryGetClassAttributes(const db0::object_model::Class &type)
    {
        auto members = type.getMembers();
        auto py_list = Py_OWN(PyList_New(0));
        for (auto [name, index]: members) {
            // name, index
            auto py_tuple = Py_OWN(PySafeTuple_Pack(Py_OWN(PyUnicode_FromString(name.c_str())), 
                Py_OWN(PyLong_FromUnsignedLong(index))));
            PySafeList_Append(*py_list, py_tuple);
        }
        return py_list.steal();
    }
    
    PyObject *tryGetPyClassAttributes(PyObject *self) {
        return tryGetClassAttributes(reinterpret_cast<ClassObject*>(self)->ext());
    }
    
    PyObject *PyAPI_PyClass_get_attributes(PyObject *self, PyObject *)
    {
        PY_API_FUNC        
        return runSafe(tryGetPyClassAttributes, self);
    }
    
    PyObject *PyAPI_PyClass_type_info(PyObject *self, PyObject *)
    {
        PY_API_FUNC        
        return runSafe(tryGetTypeInfo, reinterpret_cast<ClassObject*>(self)->ext());
    }
    
    PyTypeObject ClassObjectType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.Class",
        .tp_basicsize = ClassObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_ClassObject_del,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero memo class object",
        .tp_methods = ClassObject_methods,
        .tp_alloc = PyType_GenericAlloc,        
        .tp_free = PyObject_Free,
    };
    
    ClassObject *makeClass(std::shared_ptr<db0::object_model::Class> class_ptr)
    {
        auto class_obj = ClassDefaultObject_new();
        class_obj.get()->makeNew(std::dynamic_pointer_cast<const db0::object_model::Class>(class_ptr));
        return class_obj.steal();
    }
    
    bool PyClassObject_Check(PyObject *self) {
        return Py_TYPE(self) == &ClassObjectType;
    }

    PyObject *tryGetTypeInfo(const db0::object_model::Class &type)
    {        
        if (type.isSingleton()) {
            // name, module, memo_uuid, is_singleton, singleton_uuid
            return PySafeTuple_Pack(
                Py_OWN(PyUnicode_FromString(type.getTypeName().c_str())),
                Py_OWN(PyUnicode_FromString(type.getModuleName().c_str())),
                Py_OWN(PyUnicode_FromString(type.getClassId().toUUIDString().c_str())),
                Py_OWN(PyBool_fromBool(type.isSingleton())),
                Py_OWN(getSingletonUUID(type))
            );
        } else {
            // name, module, memo_uuid
            return PySafeTuple_Pack(
                Py_OWN(PyUnicode_FromString(type.getTypeName().c_str())),
                Py_OWN(PyUnicode_FromString(type.getModuleName().c_str())),
                Py_OWN(PyUnicode_FromString(type.getClassId().toUUIDString().c_str()))
            );
        }        
    }
    
}
