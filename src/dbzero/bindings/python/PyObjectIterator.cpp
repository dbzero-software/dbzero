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
    using TypedObjectIterator = db0::object_model::TypedObjectIterator;

    bool Iterator::isTyped() const {
        return m_typed_iterator_ptr != nullptr;
    }
    
    void Iterator::makeNew(void *at_ptr, std::unique_ptr<db0::object_model::ObjectIterator> &&obj_iter) 
    {
        auto &iter = *(new (at_ptr) Iterator());
        iter.m_iterator = std::move(obj_iter);
    }

    void Iterator::makeNew(void *at_ptr, std::unique_ptr<db0::object_model::TypedObjectIterator> &&typed_obj_iter)
    {
        auto &iter = *(new (at_ptr) Iterator());
        iter.m_typed_iterator_ptr = typed_obj_iter.get();
        iter.m_iterator = std::move(typed_obj_iter);       
    }

    PyObjectIterator *PyObjectIterator_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyObjectIterator*>(type->tp_alloc(type, 0));
    }
    
    void PyObjectIterator_del(PyObjectIterator* self)
    {
        // destroy associated DB0 instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    shared_py_object<PyObjectIterator*> PyObjectIteratorDefault_new() {
        return { PyObjectIterator_new(&PyObjectIteratorType, NULL, NULL), false };
    }
    
    PyObjectIterator *PyObjectIterator_iter(PyObjectIterator *self)
    {
        Py_INCREF(self);
        return self;    
    }
    
    PyObject *decoratedItem(PyObject *py_item, const std::vector<PyObject*> &decorators)
    {
        // return a tuple consisting of an item + decorators
        auto tuple = PyTuple_New(decorators.size() + 1);
        Py_INCREF(py_item);
        PyTuple_SET_ITEM(tuple, 0, py_item);
        for (std::size_t i = 0; i < decorators.size(); ++i) {
            Py_INCREF(decorators[i]);
            PyTuple_SET_ITEM(tuple, i + 1, decorators[i]);
        }
        return tuple;
    }
    
    PyObject *PyObjectIterator_iternext(PyObjectIterator *iter_obj)
    {
        auto &iter = *iter_obj->modifyExt();
        auto py_item = iter.next();
        if (py_item) {
            if (iter.numDecorators() > 0) {
                return decoratedItem(py_item.steal(), iter.getDecorators());
            } else {
                return py_item.steal();
            }
        }
        
        // raise stop iteration
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;        
    }
    
    PyObject *PyObjectIterator_compare(PyObject *self, PyObject* const *args, Py_ssize_t nargs) 
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "Expected exactly one argument");
            return NULL;
        }

        if (!PyObjectIterator_Check(args[0])) {
            PyErr_SetString(PyExc_TypeError, "Expected an ObjectIterator");
            return NULL;
        }

        const auto &iter = *reinterpret_cast<const PyObjectIterator*>(self)->ext();
        double diff = iter.compareTo(*reinterpret_cast<const PyObjectIterator*>(args[0])->ext());
        return PyFloat_FromDouble(diff);
    }
    
    PyObject *PyObjectIterator_signature(PyObject *self, PyObject*)
    {
        const auto &iter = *reinterpret_cast<const PyObjectIterator*>(self)->ext();
        auto signature = iter.getSignature();
        // encode as base32
        std::vector<char> result_buf(signature.size() * 2 + 1);
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
    
    PyObject *find(PyObject *, PyObject* const *args, Py_ssize_t nargs)
    {
        std::lock_guard api_lock(py_api_mutex);
        return findIn(PyToolkit::getPyWorkspace().getWorkspace(), args, nargs);
    }
    
    bool PyObjectIterator_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyObjectIteratorType;
    }
        
}