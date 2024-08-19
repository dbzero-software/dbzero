#include "PyTagSet.hpp"
#include "GlobalMutex.hpp"

namespace db0::python

{

    PyTypeObject TagSetType = 
    {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.TagSet",
        .tp_basicsize = sizeof(PyTagSet),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyObject_Del,        
        .tp_flags = Py_TPFLAGS_DEFAULT,        
        .tp_new = PyType_GenericNew,
    };

    bool TagSet_Check(PyObject *obj)
    {
        return PyObject_TypeCheck(obj, &TagSetType);
    }

    PyObject *negTagSet(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {  
        std::lock_guard pbm_lock(python_bindings_mutex);      
        auto py_tag_set = PyObject_New(PyTagSet, &TagSetType);
        // construct actual instance via placement new
        new (&py_tag_set->m_tag_set) TagSet(args, nargs, true);
        return reinterpret_cast<PyObject *>(py_tag_set);
    }
    
}