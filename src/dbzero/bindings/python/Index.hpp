#pragma once

#include <dbzero/object_model/index/Index.hpp>

namespace db0::python

{

    using IndexObject = PyWrapper<db0::object_model::Index>;
    
    IndexObject *IndexObject_new(PyTypeObject *type, PyObject *, PyObject *);
    IndexObject *IndexDefaultObject_new();
    void IndexObject_del(IndexObject* self);
    Py_ssize_t IndexObject_len(IndexObject *);
    
    // Index operations
    PyObject *IndexObject_add(IndexObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *IndexObject_remove(IndexObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *IndexObject_sort(IndexObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *IndexObject_range(IndexObject *, PyObject *args, PyObject *kwargs);
       
    extern PyTypeObject IndexObjectType;
    
    IndexObject *makeIndex(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    bool IndexObject_Check(PyObject *);

}