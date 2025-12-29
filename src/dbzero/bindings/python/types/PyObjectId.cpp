// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyObjectId.hpp"
#include <iostream>
#include <dbzero/object_model/object.hpp>
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterable.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/bindings/python/PyAPI.hpp>
#include <dbzero/bindings/python/Types.hpp>

namespace db0::python

{

    static PyMethodDef ObjectId_methods[] = {
        {"__reduce__", (PyCFunction)ObjectId_reduce, METH_VARARGS, ""},        
        {NULL}
    };

    PyTypeObject ObjectIdType = 
    {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.ObjectId",
        .tp_basicsize = sizeof(PyObjectId),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyObject_Del,
        .tp_repr = (reprfunc)ObjectId_repr,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Type representing generic object ID in dbzero.",
        .tp_richcompare = (richcmpfunc)ObjectId_richcompare,
        .tp_methods = ObjectId_methods,
        .tp_init = reinterpret_cast<initproc>(ObjectId_init),        
        .tp_new = PyType_GenericNew,
    };
    
    PyObject *PyAPI_getUUID(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "Invalid number of arguments");
            return NULL;
        }
        return runSafe(tryGetUUID, args[0]);
    }
    
    
    PyObject *TryObjectId_repr(PyObject *self)
    {
        // Format as base-32 string
        char buffer[ObjectId::maxEncodedSize() + 1];
        auto py_object_id = reinterpret_cast<PyObjectId*>(self);
        py_object_id->m_object_id.toBase32(buffer);
        return PyUnicode_FromString(buffer);
    }
    
    PyObject *ObjectId_repr(PyObject *self){
        PY_API_FUNC
        return runSafe(TryObjectId_repr, self);
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
        auto args = Py_OWN(PySafeTuple_Pack(
            Py_OWN(PyLong_FromUnsignedLongLong(object_id.m_fixture_uuid)),
            Py_OWN(PyLong_FromUnsignedLongLong(object_id.m_address.getValue())),
            Py_OWN(PyLong_FromUnsignedLong(static_cast<unsigned int>(object_id.m_storage_class))))
        );
        
        // Return a tuple with the object's constructor and its arguments
        return Py_BuildValue("(OO)", Py_TYPE(self), *args);
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
        object_id.m_address = db0::UniqueAddress::fromValue(PyLong_AsUnsignedLongLong(PyTuple_GetItem(state, 1)));
        object_id.m_storage_class = static_cast<db0::StorageClass>(PyLong_AsUnsignedLong(PyTuple_GetItem(state, 2)));

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
