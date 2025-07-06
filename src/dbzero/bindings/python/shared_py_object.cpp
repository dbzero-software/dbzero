#include "shared_py_object.hpp"
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/object_model/object/Object.hpp>

namespace db0::python

{

    void incExtRef(PyObject *py_object)
    {        
        if (PyMemo_Check(py_object)) {
            // increment reference count for memo objects
            reinterpret_cast<const MemoObject*>(py_object)->ext().addExtRef();
        }
    }
    
    void decExtRef(PyObject *py_object)
    {        
        if (PyMemo_Check(py_object)) {
            // decrement reference count for memo objects
            reinterpret_cast<const MemoObject*>(py_object)->ext().removeExtRef();
        }        
    }
    
    unsigned int getExtRefcount(PyObject *py_object, unsigned int default_count)
    {        
        if (PyMemo_Check(py_object)) {
            // return reference count for memo objects
            return reinterpret_cast<const MemoObject*>(py_object)->ext().getExtRefs();
        }
        // for non-memo objects, return the default count
        return default_count;
    }
    
}