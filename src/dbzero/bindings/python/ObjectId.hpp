#pragma once

#include <Python.h>
#include <cstdint>
#include <array>
#include "WhichType.hpp"
#include "PyWrapper.hpp"
#include <dbzero/object_model/value/ObjectId.hpp>
#include <dbzero/core/serialization/Serializable.hpp>

namespace db0::object_model

{
    
    class Object;
    class List;
    class Index;
    
}

namespace db0::python

{

    using MemoObject = PyWrapper<db0::object_model::Object>;
    using ListObject = PyWrapper<db0::object_model::List>;
    using IndexObject = PyWrapper<db0::object_model::Index>;
    using ObjectId = db0::object_model::ObjectId;
    using Serializable = db0::serial::Serializable;
    
    struct PyObjectId
    {        
        using StorageClass = db0::object_model::StorageClass;
        PyObject_HEAD
        ObjectId m_object_id;
    };
    
    PyObject *ObjectId_repr(PyObject *);
    
    extern PyTypeObject ObjectIdType;
    
    template <typename T> PyObject *tryGetUUID(T *self);
    
    // retrieve UUID of a DBZero object
    PyObject *getUUID(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *getObjectAddress(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    bool ObjectId_Check(PyObject *obj);
    
    // Method to pickle the object
    PyObject *ObjectId_reduce(PyObject *);
    int ObjectId_init(PyObject* self, PyObject* state);
    PyObject *ObjectId_richcompare(PyObject *self, PyObject *other, int op);    
    
    template <> bool Which_TypeCheck<PyObjectId>(PyObject *py_object);
    
    // tryGetObjectId specializations
    extern template PyObject *tryGetUUID<MemoObject>(MemoObject *);
    extern template PyObject *tryGetUUID<ListObject>(ListObject *);
    extern template PyObject *tryGetUUID<IndexObject>(IndexObject *);    

}
