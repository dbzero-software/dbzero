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
        {"is_known_type", (PyCFunction)&PyClass_has_type, METH_NOARGS, "Check if the corresponding Python type exists"},
        {"type", (PyCFunction)&PyClass_type, METH_NOARGS, "Retrieve associated Python type"},
        {"get_attributes", (PyCFunction)&PyClass_get_attributes, METH_NOARGS, "Get memo class attributes"},
        {"type_info", (PyCFunction)&PyClass_type_info, METH_NOARGS, "Get memo class type information"},
        {NULL}
    };
    
    PyObject *PyClass_has_type(PyObject *self, PyObject *)
    {
        PY_API_FUNC
        auto fixture = reinterpret_cast<ClassObject*>(self)->ext().getFixture();
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        return class_factory.hasLangType(reinterpret_cast<ClassObject*>(self)->ext()) ? Py_True : Py_False;
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
        auto py_list = Py_OWN(PyList_New(0));
        for (auto [name, index]: members) {
            // name, index
            auto py_tuple = Py_OWN(PyTuple_Pack(2, PyUnicode_FromString(name.c_str()), PyLong_FromUnsignedLong(index)));
            PyList_Append(*py_list, py_tuple);
        }
        return py_list.steal();
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