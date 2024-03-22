#include "ObjectId.hpp"
#include "Memo.hpp"
#include "List.hpp"
#include "Set.hpp"
#include "Tuple.hpp"
#include "Index.hpp"
#include <iostream>
#include <dbzero/object_model/object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include "PyInternalAPI.hpp"
#include "PyAPI.hpp"

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
    
    template <typename T> PyObject *tryGetObjectId(T *self, bool durable)
    {
        using UUID_Options = db0::object_model::UUID_Options;

        auto &instance = self->ext();
        // new PyObjectId instance
        auto py_object_id = PyObject_New(PyObjectId, &ObjectIdType);
        auto &object_id = py_object_id->m_object_id;
        auto fixture = instance.getFixture();
        assert(fixture);
        object_id.m_fixture_uuid = fixture->getUUID();
        object_id.m_instance_id = instance->m_instance_id;
        object_id.m_typed_addr.setAddress(instance.getAddress());
        object_id.m_typed_addr.setType(getStorageClass<T>());
        if (durable) {
            object_id.m_flags = { UUID_Options::durable };
            self->ext().incRef();
        }
        
        return reinterpret_cast<PyObject*>(py_object_id);
    }

    PyObject *getObjectId(PyObject *self, PyObject *args, bool durable)
    {
        PyObject* obj_ptr;
        if (!PyArg_ParseTuple(args, "O", &obj_ptr)) {
            PyErr_SetString(PyExc_TypeError, "Invalid input arguments");
            return NULL;
        }

        if (PyMemo_Check(obj_ptr)) {
            return runSafe(tryGetObjectId<MemoObject>, reinterpret_cast<MemoObject*>(obj_ptr), durable);
        }
        
        if (ListObject_Check(obj_ptr)) {
            return runSafe(tryGetObjectId<ListObject>, reinterpret_cast<ListObject*>(obj_ptr), durable);
        }

        if (IndexObject_Check(obj_ptr)) {
            return runSafe(tryGetObjectId<IndexObject>, reinterpret_cast<IndexObject*>(obj_ptr), durable);
        }

        PyErr_SetString(PyExc_TypeError, "Argument must be a DBZero object");
        return NULL;
    }

    PyObject *getObjectId(PyObject *self, PyObject *args)
    {
        return getObjectId(self, args, false);
    }

    PyObject *getDurableObjectId(PyObject *self, PyObject *args)
    {
        return getObjectId(self, args, true);
    }

    PyObject *ObjectId_repr(PyObject *self)
    {        
        // Format as hex string
        char buffer[46];
        auto py_object_id = reinterpret_cast<PyObjectId*>(self);
        py_object_id->m_object_id.formatHex(buffer);
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
        PyObject *args = PyTuple_Pack(4,
            PyLong_FromUnsignedLongLong(object_id.m_fixture_uuid), 
            PyLong_FromUnsignedLongLong(object_id.m_typed_addr.m_value),
            PyLong_FromUnsignedLong(object_id.m_instance_id),
            PyLong_FromUnsignedLong(static_cast<std::uint32_t>(object_id.m_flags.value()))
        );
        
        // Return a tuple with the object's constructor and its arguments
        return Py_BuildValue("(OO)", Py_TYPE(self), args);
    }
    
    int ObjectId_init(PyObject* self, PyObject* state)
    {
        using UUID_Options = db0::object_model::UUID_Options;
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
        object_id.m_flags = db0::FlagSet<UUID_Options>(PyLong_AsUnsignedLong(PyTuple_GetItem(state, 3)));

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

    template PyObject *tryGetObjectId<MemoObject>(MemoObject *, bool);
    template PyObject *tryGetObjectId<ListObject>(ListObject *, bool);
    template PyObject *tryGetObjectId<IndexObject>(IndexObject *, bool);

}