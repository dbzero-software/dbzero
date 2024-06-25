#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/workspace/AtomicContext.hpp>

namespace db0::python

{
    
    using PyAtomic = PyWrapper<db0::AtomicContext, false>;
    
    PyAtomic *PyAtomic_new(PyTypeObject *type, PyObject *, PyObject *);
    PyAtomic *PyAtomicDefault_new();
    void PyAtomic_del(PyAtomic *);
    
    extern PyTypeObject PyAtomicType;
    
    bool PyAtomic_Check(PyObject *);
    
    PyAtomic *tryBeginAtomic(PyObject *self);
    
    PyObject *PyAtomic_enter(PyObject *, PyObject *);
    PyObject *PyAtomic_exit(PyObject *, PyObject *);
    PyObject *PyAtomic_cancel(PyObject *, PyObject *);
    PyObject *PyAtomic_commit(PyObject *, PyObject *);

}