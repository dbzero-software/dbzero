#include "ObjectId.hpp"
#include "Memo.hpp"
#include <iostream>
#include <dbzero/object_model/object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include "PyInternalAPI.hpp"
#include "PyAPI.hpp"
#include "PyObjectIterator.hpp"
#include "Types.hpp"
#include "GlobalMutex.hpp"

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
    
    PyObject *getUUIDInternal(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "Invalid number of arguments");
            return NULL;
        }
        PyObject *py_arg = args[0];
        return runSafe(tryGetUUID, py_arg);
    }

    PyObject *getUUID(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        return getUUIDInternal(self, args, nargs);
    }
    
    PyObject *ObjectId_repr(PyObject *self)
    {
        // Format as base-32 string
        char buffer[ObjectId::encodedSize() + 1];
        auto py_object_id = reinterpret_cast<PyObjectId*>(self);
        py_object_id->m_object_id.toBase32(buffer);
        return PyUnicode_FromString(buffer);
    }
    
    bool ObjectId_Check(PyObject *obj) {
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
    
}