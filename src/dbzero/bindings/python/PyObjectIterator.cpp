#include "PyObjectIterator.hpp"
#include "Memo.hpp"
#include "PyInternalAPI.hpp"
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/core/utils/base32.hpp>

namespace db0::python

{

    using ObjectIterator = db0::object_model::ObjectIterator;

    PyObjectIterator *PyObjectIterator_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyObjectIterator*>(type->tp_alloc(type, 0));
    }
    
    void PyObjectIterator_del(PyObjectIterator* self)
    {
        // destroy associated DB0 instance
        self->ext().~ObjectIterator();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    PyTypedObjectIterator *PyTypedObjectIterator_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyTypedObjectIterator*>(type->tp_alloc(type, 0));
    }

    PyObjectIterator *PyObjectIteratorDefault_new() {
        return PyObjectIterator_new(&PyObjectIteratorType, NULL, NULL);
    }
    
    void PyTypedObjectIterator_del(PyTypedObjectIterator* self)
    {
        // destroy associated DB0 instance
        self->ext().~TypedObjectIterator();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    PyObjectIterator *PyObjectIterator_iter(PyObjectIterator *self)
    {
        Py_INCREF(self);
        return self;    
    }
    
    PyObject *PyObjectIterator_iternext(PyObjectIterator *iter_obj)
    {
        std::uint64_t addr;
        if (iter_obj->ext().next(addr)) {
            // retrieve DBZero instance
            return iter_obj->ext().unload(addr);
        } else {
            // raise stop iteration
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } 
    }
    
    PyObject *PyObjectIterator_compare(PyObject *self, PyObject* const *args, Py_ssize_t nargs) 
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "Expected exactly one argument");
            return NULL;
        }

        if (!ObjectIterator_Check(args[0])) {
            PyErr_SetString(PyExc_TypeError, "Expected an ObjectIterator");
            return NULL;
        }

        const auto &iter = reinterpret_cast<PyObjectIterator*>(self)->ext();
        double diff = iter.compareTo(reinterpret_cast<PyObjectIterator*>(args[0])->ext());
        return PyFloat_FromDouble(diff);
    }
    
    PyObject *PyObjectIterator_signature(PyObject *self, PyObject*)
    {
        const auto &iter = reinterpret_cast<PyObjectIterator*>(self)->ext();
        auto signature = iter.getSignature();
        // encode as base32
        std::vector<char> result_buf(signature.size() * 2);
        auto size = db0::base32_encode(reinterpret_cast<std::uint8_t*>(signature.data()), signature.size(), result_buf.data());
        return PyUnicode_FromStringAndSize(result_buf.data(), size);        
    }
    
    static PyMethodDef PyObjectIterator_methods[] = 
    {
        {"compare", (PyCFunction)PyObjectIterator_compare, METH_FASTCALL, "Compare two iterators"},
        {"signature", (PyCFunction)PyObjectIterator_signature, METH_NOARGS, "Get the signature of the query"},
        {NULL}
    };
    
    PyTypeObject PyObjectIteratorType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.ObjectIterator",
        .tp_basicsize = PyObjectIterator::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyObjectIterator_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero object iterator",
        .tp_iter = (getiterfunc)PyObjectIterator_iter,
        .tp_iternext = (iternextfunc)PyObjectIterator_iternext,
        .tp_methods = PyObjectIterator_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyObjectIterator_new,
        .tp_free = PyObject_Free,
    };
    
    PyTypedObjectIterator *PyTypedObjectIterator_iter(PyTypedObjectIterator *self)
    {
        Py_INCREF(self);
        return self;
    }
    
    PyObject *PyTypedObjectIterator_iternext(PyTypedObjectIterator *iter_obj)
    {
        std::uint64_t addr;
        if (iter_obj->ext().next(addr)) {
            // retrieve DBZero instance
            return iter_obj->ext().unload(addr);
        } else {
            // raise stop iteration
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } 
    }
    
    static PyMethodDef PyTypedObjectIterator_methods[] = {
        {NULL}
    };

    PyTypeObject PyTypedObjectIteratorType = 
    {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.TypedObjectIterator",
        .tp_basicsize = PyTypedObjectIterator::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyTypedObjectIterator_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero typed object iterator",
        .tp_iter = (getiterfunc)PyTypedObjectIterator_iter,
        .tp_iternext = (iternextfunc)PyTypedObjectIterator_iternext,
        .tp_methods = PyTypedObjectIterator_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyTypedObjectIterator_new,
        .tp_free = PyObject_Free,
    };
    
    PyObject *find(PyObject *, PyObject* const *args, Py_ssize_t nargs) {
        return findIn(PyToolkit::getPyWorkspace().getWorkspace(), args, nargs);
    }
    
    bool ObjectIterator_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyObjectIteratorType;
    }

    bool TypedObjectIterator_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyTypedObjectIteratorType;
    }
    
}