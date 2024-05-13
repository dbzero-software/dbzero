#include "ObjectId.hpp"
#include "Memo.hpp"
#include <dbzero/bindings/python/collections/List.hpp>
#include <dbzero/bindings/python/collections/Set.hpp>
#include <dbzero/bindings/python/collections/Tuple.hpp>
#include "Index.hpp"
#include <dbzero/bindings/python/collections/Dict.hpp>
#include <iostream>
#include <dbzero/object_model/object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include "PyInternalAPI.hpp"
#include "PyAPI.hpp"
#include "PyObjectIterator.hpp"

namespace db0::python

{

    static PyMethodDef ObjectId_methods[] = {
        {"__reduce__", (PyCFunction)ObjectId_reduce, METH_VARARGS, ""},        
        {NULL}
    };

    PyTypeObject ObjectIdType = 
    {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.ObjectId",
        .tp_basicsize = sizeof(PyObjectId),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyObject_Del,
        .tp_repr = (reprfunc)ObjectId_repr,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Type representing generic object ID in DBZero.",
        .tp_richcompare = (richcmpfunc)ObjectId_richcompare,
        .tp_methods = ObjectId_methods,
        .tp_init = reinterpret_cast<initproc>(ObjectId_init),        
        .tp_new = PyType_GenericNew,
    };
    
    // Serializable's UUID implementation
    PyObject *tryGetSerializableUUID(db0::serial::Serializable *self)
    {
        // return as base-32 string
        char buffer[db0::serial::Serializable::UUID_SIZE];
        self->getUUID(buffer);
        return PyUnicode_FromString(buffer);
    }
    
    template <typename T> PyObject *tryGetUUID(T *self)
    {
        auto &instance = self->ext();
        db0::object_model::ObjectId object_id;        
        auto fixture = instance.getFixture();
        assert(fixture);
        object_id.m_fixture_uuid = fixture->getUUID();
        object_id.m_instance_id = instance->m_instance_id;
        object_id.m_typed_addr.setAddress(instance.getAddress());
        object_id.m_typed_addr.setType(getStorageClass<T>());

        // return as base-32 string
        char buffer[ObjectId::encodedSize() + 1];
        object_id.toBase32(buffer);
        return PyUnicode_FromString(buffer);
    }

    PyObject *getUUID(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "Invalid number of arguments");
            return NULL;
        }

        auto obj_ptr = args[0];
        if (PyMemo_Check(obj_ptr)) {
            return runSafe(tryGetUUID<MemoObject>, reinterpret_cast<MemoObject*>(obj_ptr));
        }
        
        if (ListObject_Check(obj_ptr)) {
            return runSafe(tryGetUUID<ListObject>, reinterpret_cast<ListObject*>(obj_ptr));
        }

        if (IndexObject_Check(obj_ptr)) {
            return runSafe(tryGetUUID<IndexObject>, reinterpret_cast<IndexObject*>(obj_ptr));
        }

        if (ObjectIterator_Check(obj_ptr)) {
            // serializable's uuid            
            return runSafe(tryGetSerializableUUID, &reinterpret_cast<PyObjectIterator*>(obj_ptr)->ext());
        }
        
        /* FIXME: implement
        if (DictObject_Check(obj_ptr)) {
            return runSafe(tryGetObjectId<DictObject>, reinterpret_cast<DictObject*>(obj_ptr));
        }
        */

        PyErr_SetString(PyExc_TypeError, "Argument must be a DBZero object");
        return NULL;
    }

    PyObject *getObjectAddress(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "Invalid number of arguments");
            return NULL;
        }

        auto obj_ptr = args[0];
        std::uint64_t address = 0;
        if (PyMemo_Check(obj_ptr)) {
            address = reinterpret_cast<MemoObject*>(obj_ptr)->ext().getAddress();            
        } else if (ListObject_Check(obj_ptr)) {
            address = reinterpret_cast<ListObject*>(obj_ptr)->ext().getAddress();            
        } else if (IndexObject_Check(obj_ptr)) {
            address = reinterpret_cast<IndexObject*>(obj_ptr)->ext().getAddress();            
        } else {
            PyErr_SetString(PyExc_TypeError, "Argument must be a DBZero object");
            return NULL;
        }
        return PyLong_FromUnsignedLongLong(address);        
    }

    PyObject *ObjectId_repr(PyObject *self)
    {
        // Format as base-32 string
        char buffer[ObjectId::encodedSize() + 1];
        auto py_object_id = reinterpret_cast<PyObjectId*>(self);
        py_object_id->m_object_id.toBase32(buffer);
        return PyUnicode_FromString(buffer);
    }
    
    bool ObjectId_Check(PyObject *obj)
    {
        return PyObject_IsInstance(obj, reinterpret_cast<PyObject*>(&ObjectIdType));
    }
    
    template <> bool Which_TypeCheck<PyObjectId>(PyObject *py_object) {
        return ObjectId_Check(py_object);
    }

    PyObject *ObjectId_reduce(PyObject *self)
    {
        if (!ObjectId_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid object type");
            return NULL;
        }

        auto py_object_id = reinterpret_cast<PyObjectId*>(self);
        auto &object_id = py_object_id->m_object_id;
        // Create a tuple containing the arguments needed to reconstruct the object
        PyObject *args = PyTuple_Pack(3,
            PyLong_FromUnsignedLongLong(object_id.m_fixture_uuid), 
            PyLong_FromUnsignedLongLong(object_id.m_typed_addr.m_value),
            PyLong_FromUnsignedLong(object_id.m_instance_id)            
        );
        
        // Return a tuple with the object's constructor and its arguments
        return Py_BuildValue("(OO)", Py_TYPE(self), args);
    }
    
    int ObjectId_init(PyObject* self, PyObject* state)
    {
        if (!PyTuple_Check(state)) {
            PyErr_SetString(PyExc_ValueError, "Invalid state data");
            return -1;
        }

        auto py_object_id = reinterpret_cast<PyObjectId*>(self);
        auto &object_id = py_object_id->m_object_id;
        // Set the object's attributes
        object_id.m_fixture_uuid = PyLong_AsUnsignedLongLong(PyTuple_GetItem(state, 0));
        object_id.m_typed_addr = PyLong_AsUnsignedLongLong(PyTuple_GetItem(state, 1));
        object_id.m_instance_id = PyLong_AsUnsignedLong(PyTuple_GetItem(state, 2));

        return 0;
    }
    
    PyObject *ObjectId_richcompare(PyObject *self, PyObject *other, int op)
    {
        if (!ObjectId_Check(self) || !ObjectId_Check(other)) {
            PyErr_SetString(PyExc_TypeError, "Invalid object type");
            return NULL;        
        }

        Py_RETURN_RICHCOMPARE(reinterpret_cast<PyObjectId*>(self)->m_object_id, reinterpret_cast<PyObjectId*>(other)->m_object_id, op);
    }
    
    template PyObject *tryGetUUID<MemoObject>(MemoObject *);
    template PyObject *tryGetUUID<ListObject>(ListObject *);
    template PyObject *tryGetUUID<IndexObject>(IndexObject *);    

}