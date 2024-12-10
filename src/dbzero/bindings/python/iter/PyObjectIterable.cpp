#include "PyObjectIterable.hpp"
#include "PyObjectIterator.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/core/utils/base32.hpp>

namespace db0::python

{

    PyObjectIterable *PyObjectIterable_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyObjectIterable*>(type->tp_alloc(type, 0));
    }
    
    void PyObjectIterable_del(PyObjectIterable* self)
    {
        PY_API_FUNC
        // destroy associated DB0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    shared_py_object<PyObjectIterable*> PyObjectIterableDefault_new() {
        return { PyObjectIterable_new(&PyObjectIterableType, NULL, NULL), false };
    }

    PyObject *tryPyObjectIterable_compare(PyObject *self, PyObject* const *args, Py_ssize_t nargs) 
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "Expected exactly one argument");
            return NULL;
        }

        if (!PyObjectIterable_Check(args[0])) {
            PyErr_SetString(PyExc_TypeError, "Expected an ObjectIterator");
            return NULL;
        }

        const auto &iter = reinterpret_cast<const PyObjectIterable*>(self)->ext();
        double diff = iter.compareTo(reinterpret_cast<const PyObjectIterable*>(args[0])->ext());
        return PyFloat_FromDouble(diff);
    }

    PyObject *PyAPI_PyObjectIterable_compare(PyObject *self, PyObject* const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(tryPyObjectIterable_compare, self, args, nargs);
    }
    
    PyObject *tryPyObjectIterable_signature(PyObject *self)
    {
        const auto &iter = reinterpret_cast<const PyObjectIterable*>(self)->ext();
        auto signature = iter.getSignature();
        // encode as base32
        std::vector<char> result_buf(signature.size() * 2 + 1);
        auto size = db0::base32_encode(reinterpret_cast<std::uint8_t*>(signature.data()), signature.size(), result_buf.data());
        return PyUnicode_FromStringAndSize(result_buf.data(), size);        
    }

    PyObject *PyAPI_PyObjectIterable_signature(PyObject *self, PyObject*)
    {
        PY_API_FUNC
        return runSafe(tryPyObjectIterable_signature, self);
    }
    
    PyObject *tryPyAPI_PyObjectIterable_iter(PyObjectIterable *py_iterable)
    {
        auto &iterable = py_iterable->ext();
        auto py_iter = PyObjectIteratorDefault_new();
        iterable.makeIter(&py_iter.get()->modifyExt());
        return py_iter.steal();
    }
    
    PyObject *PyAPI_PyObjectIterable_iter(PyObjectIterable *py_iterable)
    {
        PY_API_FUNC
        return runSafe(tryPyAPI_PyObjectIterable_iter, py_iterable);
    }

    static PyMethodDef PyObjectIterable_methods[] = 
    {
        {"compare", (PyCFunction)PyAPI_PyObjectIterable_compare, METH_FASTCALL, "Compare two iterators"},
        {"signature", (PyCFunction)PyAPI_PyObjectIterable_signature, METH_NOARGS, "Get the signature of the query"},
        {NULL}
    };
    
    PyTypeObject PyObjectIterableType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.ObjectIterable",
        .tp_basicsize = PyObjectIterable::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyObjectIterable_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero object iterable",
        .tp_iter = (getiterfunc)PyAPI_PyObjectIterable_iter,        
        .tp_methods = PyObjectIterable_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyObjectIterable_new,
        .tp_free = PyObject_Free,
    };
    
    bool PyObjectIterable_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyObjectIterableType;
    }

    PyObject *PyAPI_find(PyObject *, PyObject* const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(findIn, PyToolkit::getPyWorkspace().getWorkspace(), args, nargs);
    }

}