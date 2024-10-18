#include "PyClass.hpp"
#include "PyInternalAPI.hpp"
#include "PyReflectionAPI.hpp"

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
        {"is_known_type", (PyCFunction)&PyClass_has_type, METH_NOARGS, "Check if the corresponding Python type exists"},
        {"type", (PyCFunction)&PyClass_type, METH_NOARGS, "Retrieve associated Python type"},
        {"get_attributes", (PyCFunction)&PyClass_get_attributes, METH_NOARGS, "Get memo class attributes"},
        {"type_info", (PyCFunction)&PyClass_type_info, METH_NOARGS, "Get memo class type information"},
        {NULL}
    };
    
    PyObject *PyClass_has_type(PyObject *self, PyObject *)
    {
        PY_API_FUNC
        auto lang_class = reinterpret_cast<ClassObject*>(self)->ext().tryGetLangClass();
        return lang_class ? Py_True : Py_False;
    }
    
    PyTypeObject *tryPyClassType(PyObject *self) {
        return reinterpret_cast<ClassObject*>(self)->ext().getLangClass().steal();
    }
    
    PyObject *PyClass_type(PyObject *self, PyObject *) 
    {
        PY_API_FUNC
        return reinterpret_cast<PyObject*>(runSafe(tryPyClassType, self));
    }

    void ClassObject_del(ClassObject* class_obj)
    {
        PY_API_FUNC
        // release associated shared_ptr
        class_obj->destroy();
        Py_TYPE(class_obj)->tp_free((PyObject*)class_obj);
    }
    
    PyObject *tryGetClassAttributes(const db0::object_model::Class &type)
    {
        auto members = type.getMembers();
        PyObject *py_list = PyList_New(0);
        for (auto [name, index]: members) {
            // name, index
            PyObject *py_tuple = PyTuple_Pack(2, PyUnicode_FromString(name.c_str()), PyLong_FromUnsignedLong(index));
            PyList_Append(py_list, py_tuple);
        }
        return py_list;
    }

    PyObject *tryGetAttributes(PyObject *self) {
        return tryGetClassAttributes(reinterpret_cast<ClassObject*>(self)->ext());
    }
    
    PyObject *PyClass_get_attributes(PyObject *self, PyObject *)
    {
        PY_API_FUNC        
        return runSafe(tryGetAttributes, self);
    }
    
    PyObject *PyClass_type_info(PyObject *self, PyObject *)
    {
        PY_API_FUNC        
        return runSafe(tryGetTypeInfo, reinterpret_cast<ClassObject*>(self)->ext());
    }
    
    PyTypeObject ClassObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Class",
        .tp_basicsize = ClassObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)ClassObject_del,        
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero memo class object",
        .tp_methods = ClassObject_methods,
        .tp_alloc = PyType_GenericAlloc,        
        .tp_free = PyObject_Free,
    };
    
    ClassObject *makeClass(std::shared_ptr<const db0::object_model::Class> class_ptr)
    {
        auto class_obj = ClassDefaultObject_new();
        class_obj.get()->makeNew(class_ptr);
        return class_obj.steal();
    }

    bool PyClassObject_Check(PyObject *self) {
        return Py_TYPE(self) == &ClassObjectType;
    }

    PyObject *tryGetTypeInfo(const db0::object_model::Class &type)
    {
        PyObject *py_tuple = nullptr;
        if (type.isSingleton()) {
            // name, module, memo_uuid, is_singleton, singleton_uuid
            py_tuple = PyTuple_Pack(5,
                PyUnicode_FromString(type.getTypeName().c_str()),
                PyUnicode_FromString(type.getModuleName().c_str()),
                PyUnicode_FromString(type.getClassId().toUUIDString().c_str()),
                type.isSingleton() ? Py_True : Py_False,
                getSingletonUUID(type)
            );
        } else {
            // name, module, memo_uuid
            py_tuple = PyTuple_Pack(3,
                PyUnicode_FromString(type.getTypeName().c_str()), 
                PyUnicode_FromString(type.getModuleName().c_str()),
                PyUnicode_FromString(type.getClassId().toUUIDString().c_str())
            );
        }
        return py_tuple;
    }
    
}