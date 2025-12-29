// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyTagSet.hpp"
#include "PyInternalAPI.hpp"
#include <mutex>

namespace db0::python

{

    PyTypeObject TagSetType = 
    {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "TagSet",
        .tp_basicsize = sizeof(PyTagSet),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_PyTagSet_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,        
        .tp_new = PyType_GenericNew,
    };
    
    void PyAPI_PyTagSet_del(PyTagSet *py_tag_set)
    {      
        py_tag_set->m_tag_set.~TagSet();
        PyObject_Del(py_tag_set);
    }

    bool TagSet_Check(PyObject *obj) {
        return PyObject_TypeCheck(obj, &TagSetType);
    }

    PyObject *try_NegateTagSet( PyObject *const *args, Py_ssize_t nargs)
    {
        auto py_tag_set = PyObject_New(PyTagSet, &TagSetType);
        // construct actual instance via placement new
        new (&py_tag_set->m_tag_set) TagSet(args, nargs, true);
        return reinterpret_cast<PyObject *>(py_tag_set);
    }

    PyObject *negTagSet(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC;
        return runSafe(try_NegateTagSet, args, nargs);
    }
    
}
