#include "PyClass.hpp"

namespace db0::python

{
    
    ClassObject *ClassObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<ClassObject*>(type->tp_alloc(type, 0));
    }

    shared_py_object<ClassObject*> ClassDefaultObject_new() {
        return { ClassObject_new(&ClassObjectType, NULL, NULL), false };
    }

    static PyMethodDef ClassObject_methods[] = {
        {NULL}
    };

    void ClassObject_del(ClassObject* class_obj)
    {
        // release associated shared_ptr
        class_obj->destroy();
        Py_TYPE(class_obj)->tp_free((PyObject*)class_obj);
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

}