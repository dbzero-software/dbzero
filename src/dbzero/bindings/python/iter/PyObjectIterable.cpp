#include "PyObjectIterable.hpp"
#include "PyObjectIterator.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PyTagsAPI.hpp>
#include <dbzero/core/utils/base32.hpp>

namespace db0::python

{

    PyObjectIterable *PyObjectIterable_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyObjectIterable*>(type->tp_alloc(type, 0));
    }
    
    void PyObjectIterable_del(PyObjectIterable* self)
    {
        PY_API_FUNC
        // destroy associated db0 instance
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
            PyErr_SetString(PyExc_TypeError, "Expected an ObjectIterable");
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
        // getFixture to prevent segfault in case the associated context (e.g. snapshot) has been destroyed
        auto fixture = py_iterable->ext().getFixture();
        auto py_iter = PyObjectIteratorDefault_new();
        py_iterable->ext().makeIter(&(py_iter.get()->modifyExt()));
        return py_iter.steal();
    }
    
    PyObject *PyAPI_PyObjectIterable_iter(PyObjectIterable *py_iterable)
    {
        PY_API_FUNC
        return runSafe(tryPyAPI_PyObjectIterable_iter, py_iterable);
    }
    
    Py_ssize_t tryPyObjectIterable_len(PyObjectIterable *py_iterable)
    {
        // getFixture to prevent segfault in case the associated context (e.g. snapshot) has been destroyed
        auto fixture = py_iterable->ext().getFixture();
        return py_iterable->ext().getSize();
    }

    Py_ssize_t PyAPI_PyObjectIterable_len(PyObjectIterable *py_iterable)
    {
        PY_API_FUNC
        return runSafe(tryPyObjectIterable_len, py_iterable);
    }

    void PySlice_GetUnboundIndices(PyObject *py_slice, std::function<std::size_t(std::optional<std::size_t> &)> size_func, 
        Py_ssize_t &start, Py_ssize_t &stop, Py_ssize_t &step)
    {
        if (!PySlice_Check(py_slice)) {
            THROWF(db0::InputException) << "Expected a slice object";
        }
        if (PySlice_Unpack(py_slice, &start, &stop, &step) < 0) {
            THROWF(db0::InputException) << "Invalid slice object";
        }
        if (step == 0) {
            THROWF(db0::InputException) << "Slice step cannot be zero";
        }    
        
        // only calculate size if negative indices are present
        std::optional<std::size_t> size;
        if (start < 0) start += size_func(size);
        if (stop < 0) stop += size_func(size);
        
        if (start < 0) start = 0;
        if (stop < 0) stop = 0;

        if (step < 0) {
            // FIXME: implement reversed order iteration
            THROWF(db0::InternalException) << "Reversed order iteration over db0 query results is not supported";
            /*
            if (start < stop) {
                std::swap(start, stop);
            }
            */
        } else {
            if (start > stop) {
                std::swap(start, stop);
            }
        }    
    }
    
    PyObject *tryPyObjectIterable_GetItemSlice(PyObjectIterable *py_iterable, PyObject *py_slice)
    {        
        using SliceDef = db0::object_model::SliceDef;
        Py_ssize_t start, stop, step;    
        auto size_func = [&](std::optional<std::size_t> &size) {
            if (!size) {
                size = py_iterable->ext().getSize();
            }
            return *size;
        };
        
        PySlice_GetUnboundIndices(py_slice, size_func, start, stop, step);
        std::size_t _stop = (stop == PY_SSIZE_T_MAX) ? SliceDef::MAX_STOP() : (std::size_t)stop;        
        auto slice_def = SliceDef { (std::size_t)start, _stop, (int)step };
        // the default slice just returns itself
        if (slice_def.isDefault()) {
            Py_INCREF(py_iterable);
            return py_iterable;
        }

        if (py_iterable->ext().isSliced()) {
            THROWF(db0::InputException) << "Cannot slice an already sliced iterable (Operation not supported)";
        }

        auto py_result = PyObjectIterableDefault_new();
        py_iterable->ext().makeSlice(&(py_result.get()->modifyExt()), slice_def);
        return py_result.steal();
    }
    
    PyObject *PyAPI_PyObjectIterable_GetItemSlice(PyObjectIterable *py_iterable, PyObject *py_elem)
    {
        PY_API_FUNC
        return runSafe(tryPyObjectIterable_GetItemSlice, py_iterable, py_elem);
    }
    
    static PyMappingMethods PyObjectIterable_as_mapping = {
        .mp_length = (lenfunc)PyAPI_PyObjectIterable_len,
        .mp_subscript = (binaryfunc)PyAPI_PyObjectIterable_GetItemSlice
    };

    static PyMethodDef PyObjectIterable_methods[] = 
    {
        {"compare", (PyCFunction)PyAPI_PyObjectIterable_compare, METH_FASTCALL, "Compare two iterables"},
        {"signature", (PyCFunction)PyAPI_PyObjectIterable_signature, METH_NOARGS, "Get the signature of the query"},
        {NULL}
    };
    
    PyTypeObject PyObjectIterableType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "ObjectIterable",
        .tp_basicsize = PyObjectIterable::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyObjectIterable_del,
        .tp_as_mapping = &PyObjectIterable_as_mapping,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero object iterable",
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
        return runSafe(findIn, PyToolkit::getPyWorkspace().getWorkspace(), args, nargs, nullptr);
    }
    
}