// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "shared_py_object.hpp"
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/object_model/object/Object.hpp>

namespace db0::python

{   
    
    template <typename MemoImplT>
    void incExtRefImpl(PyObject *py_object) {
        // increment reference count for memo objects
        reinterpret_cast<const MemoImplT*>(py_object)->ext().addExtRef();        
    }
    
    template <typename MemoImplT>
    void decExtRefImpl(PyObject *py_object) {
        // decrement reference count for memo objects
        reinterpret_cast<const MemoImplT*>(py_object)->ext().removeExtRef();        
    }

    template <typename MemoImplT>
    unsigned int getExtRefcountImpl(PyObject *py_object, unsigned int default_count) {
        // return reference count for memo objects
        return reinterpret_cast<const MemoImplT*>(py_object)->ext().getExtRefs();
    }

    void incExtRef(PyObject *py_object)
    {
        if (PyMemo_Check<MemoObject>(py_object)) {
            incExtRefImpl<MemoObject>(py_object);
        } else if (PyMemo_Check<MemoImmutableObject>(py_object)) {
            incExtRefImpl<MemoImmutableObject>(py_object);
        }
    }
    
    void decExtRef(PyObject *py_object)
    {
        if (PyMemo_Check<MemoObject>(py_object)) {
            decExtRefImpl<MemoObject>(py_object);
        } else if (PyMemo_Check<MemoImmutableObject>(py_object)) {
            decExtRefImpl<MemoImmutableObject>(py_object);
        }
    }
    
    unsigned int getExtRefcount(PyObject *py_object, unsigned int default_count)
    {   
        if (PyMemo_Check<MemoObject>(py_object)) {
            return getExtRefcountImpl<MemoObject>(py_object, default_count);
        } else if (PyMemo_Check<MemoImmutableObject>(py_object)) {
            return getExtRefcountImpl<MemoImmutableObject>(py_object, default_count);
        }
        
        // for non-memo objects, return the default count
        return default_count;
    }
    
}