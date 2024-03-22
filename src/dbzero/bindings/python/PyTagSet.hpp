#pragma once

#include <Python.h>
#include <dbzero/object_model/tags/TagSet.hpp>

namespace db0::python

{

    using TagSet = db0::object_model::TagSet;
    
    struct PyTagSet
    {
        PyObject_HEAD
        TagSet m_tag_set;
    };
    
    extern PyTypeObject TagSetType;
    
    bool TagSet_Check(PyObject *obj);
    // Construct the negated PyTagSet object
    PyObject *negTagSet(PyObject *self, PyObject *const *args, Py_ssize_t nargs);

}