// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyIndex.hpp"
#include <dbzero/bindings/python/iter/PyObjectIterable.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>

namespace db0::python

{

    static PyMethodDef IndexObject_methods[] = {
        {"add", (PyCFunction)PyAPI_IndexObject_add, METH_FASTCALL, "Add item to index."},
        {"remove", (PyCFunction)PyAPI_IndexObject_remove, METH_FASTCALL, "Remove item from index if it exists."},
        {"sort", (PyCFunction)PyAPI_IndexObject_sort, METH_VARARGS | METH_KEYWORDS, "Sort results of other iterator."},
        {"range", (PyCFunction)PyAPI_IndexObject_range, METH_VARARGS | METH_KEYWORDS, "Deprecated"},
        {"select", (PyCFunction)PyAPI_IndexObject_range, METH_VARARGS | METH_KEYWORDS, "Extract unsorted values from a specific range"},
        {"flush", (PyCFunction)PyAPI_IndexObject_flush, METH_NOARGS, "Flush buffered changes"},
        {NULL}
    };

    static PySequenceMethods IndexObject_sq = {
        .sq_length = (lenfunc)PyAPI_IndexObject_len
    };

    PyTypeObject IndexObjectType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "Index",
        .tp_basicsize = IndexObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_IndexObject_del,
        .tp_as_sequence = &IndexObject_sq,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero indexing object",
        .tp_methods = IndexObject_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)IndexObject_new,
        .tp_free = PyObject_Free,
    };
    
    IndexObject *IndexObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<IndexObject*>(type->tp_alloc(type, 0));
    }

    IndexObject *IndexDefaultObject_new() {
        return IndexObject_new(&IndexObjectType, NULL, NULL);
    }
    
    void PyAPI_IndexObject_del(IndexObject* index_obj)
    {
        PY_API_FUNC
        // destroy associated DB0 Index instance
        index_obj->destroy();
        Py_TYPE(index_obj)->tp_free((PyObject*)index_obj);
    }
    
    Py_ssize_t tryIndexObject_len(IndexObject *index_obj)
    {
        index_obj->ext().getFixture()->refreshIfUpdated();
        return index_obj->ext().size();
    }

    Py_ssize_t PyAPI_IndexObject_len(IndexObject *index_obj)
    {
        PY_API_FUNC
        return runSafe(tryIndexObject_len, index_obj);
    }
    
    IndexObject *tryMakeIndex(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        // make actual dbzero instance, use default fixture
        auto py_index = Py_OWN(IndexDefaultObject_new());
        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        auto &index = py_index->makeNew(*lock);
        
        // NOTE: this callback is important for proper lifecycle management
        // we must prevent dirty Index instance from deletion
        auto py_index_ptr = py_index.get();
        index.setDirtyCallback([py_index_ptr](bool incRef) {
            if (incRef) {
                Py_INCREF(py_index_ptr);
            } else {
                Py_DECREF(py_index_ptr);
            }
        });
        
        // register newly created index with py-object cache
        lock->getLangCache().add(index.getAddress(), py_index.get());
        return py_index.steal();
    }
    
    IndexObject *PyAPI_makeIndex(PyObject *self, PyObject *const *args, Py_ssize_t nargs)    
    {
        if (nargs != 0) {
            PyErr_SetString(PyExc_TypeError, "Index object does not accept arguments");
            return NULL;
        }        
        PY_API_FUNC
        return runSafe(tryMakeIndex, self, args, nargs);        
    }
    
    PyObject *tryIndexObject_add(IndexObject *index_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        index_obj->modifyExt().add(args[0], args[1]);
        // NOTE: we don't need to lock the fixture here, because add() is a buffered operation
        index_obj->ext().getFixture()->onUpdated();
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_IndexObject_add(IndexObject *index_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 2) {
            PyErr_SetString(PyExc_TypeError, "add() takes exactly two arguments");
            return NULL;
        }

        PY_API_FUNC
        return runSafe(tryIndexObject_add, index_obj, args, nargs);
    }

    PyObject *tryIndexObject_remove(IndexObject *index_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        index_obj->modifyExt().remove(args[0], args[1]);
        // NOTE: we don't need to lock the fixture here, because remove() is a buffered operation
        index_obj->ext().getFixture()->onUpdated();
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_IndexObject_remove(IndexObject *index_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 2) {
            PyErr_SetString(PyExc_TypeError, "remove() takes exactly two arguments");
            return NULL;
        }    
        PY_API_FUNC
        return runSafe(tryIndexObject_remove, index_obj, args, nargs);
    }
    
    PyObject *tryIndexObject_sort(IndexObject *py_index, PyObject *args, PyObject *kwargs)
    {
        using ObjectIterable = db0::object_model::ObjectIterable;
        using ObjectIterator = db0::object_model::ObjectIterator;

        PyObject *py_desc = nullptr;
        PyObject *py_null_first = nullptr;
        // extract optional keyword arugment "desc" (default sort is ascending)
        if (kwargs != NULL) {
            py_desc = PyDict_GetItemString(kwargs, "desc");
            py_null_first = PyDict_GetItemString(kwargs, "null_first");
        }
        
        bool asc = py_desc ? !PyObject_IsTrue(py_desc) : true;
        bool null_first = py_null_first ? PyObject_IsTrue(py_null_first) : false;
        PyObject *py_iter = PyTuple_GetItem(args, 0);
        // sort results of a full-text iterator        
        if (!PyObjectIterable_Check(py_iter)) {
            PyErr_SetString(PyExc_TypeError, "sort() takes ObjectIterable as an argument");
            return NULL;
        }
        
        auto &index = py_index->ext();
        auto &iter = reinterpret_cast<PyObjectIterable*>(py_iter)->modifyExt();
        auto iter_sorted = index.sort(iter, asc, null_first);
        auto iter_obj = PyObjectIterableDefault_new();
        iter_obj->makeNew(iter, std::move(iter_sorted));
        return iter_obj.steal();
    }
    
    PyObject *PyAPI_IndexObject_sort(IndexObject *py_index, PyObject *args, PyObject *kwargs)
    {
        // extract 1 positional argument
        if (PyTuple_Size(args) != 1) {
            PyErr_SetString(PyExc_TypeError, "sort() takes exactly one positional argument");
            return NULL;
        }

        PY_API_FUNC
        return runSafe(tryIndexObject_sort, py_index, args, kwargs);
    }
    
    PyObject *tryIndexObject_range(IndexObject *py_index, PyObject *args, PyObject *kwargs)
    {
        // optional low, optional high, optional null_first (boolean)
        static const char *kwlist[] = {"low", "high", "null_first", NULL};
        PyObject *low = NULL, *high = NULL;
        int null_first = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOp", const_cast<char**>(kwlist), &low, &high, &null_first)) {
            return NULL;
        }
        
        auto &index = py_index->ext();
        // construct range iterator
        auto iter_factory = index.range(low, high, null_first);        
        auto py_iter_obj = PyObjectIterableDefault_new();
        py_iter_obj->makeNew(index.getFixture(), std::move(iter_factory));
        return py_iter_obj.steal();
    }
    
    PyObject *PyAPI_IndexObject_range(IndexObject *py_index, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return runSafe(tryIndexObject_range, py_index, args, kwargs);
    }
    
    bool IndexObject_Check(PyObject *py_object) {
        return PyObject_TypeCheck(py_object, &IndexObjectType);
    }
    
    PyObject *tryIndexObject_flush(IndexObject *self)
    {
        FixtureLock lock(self->ext().getFixture());
        self->modifyExt().flush(lock);
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_IndexObject_flush(IndexObject *self)
    {
        PY_API_FUNC
        return runSafe(tryIndexObject_flush, self);
    }

}
