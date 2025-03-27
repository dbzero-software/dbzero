#include "PyLocked.hpp"
#include "PyToolkit.hpp"
#include "PyInternalAPI.hpp"

namespace db0::python

{

    static PyMethodDef PyLocked_methods[] = 
    {
        {"close", (PyCFunction)&PyAPI_PyLocked_close, METH_NOARGS, "Close/exit the locked context"},
        {"get_mutation_log", (PyCFunction)&PyAPI_PyLocked_get_mutation_log, METH_NOARGS, "Retrieve state numbers of all modified prefixes"},
        {NULL}
    };
    
    PyLocked *PyLocked_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyLocked*>(type->tp_alloc(type, 0));
    }
    
    PyLocked *PyLockedDefault_new() {
        return PyLocked_new(&PyLockedType, NULL, NULL);
    }
    
    void PyAPI_PyLocked_del(PyLocked* self)
    {
        PY_API_FUNC
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    PyTypeObject PyLockedType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.LockedContext",
        .tp_basicsize = PyLocked::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_PyLocked_del, 
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero locked operation context",
        .tp_methods = PyLocked_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyLocked_new,
        .tp_free = PyObject_Free,
    };
    
    PyLocked *PyAPI_tryBeginLocked(PyObject *self)
    {
        PY_API_FUNC
        auto py_object = PyLocked_new(&PyLockedType, NULL, NULL);
        try {            
            auto workspace_ptr = PyToolkit::getPyWorkspace().getWorkspaceSharedPtr();
            auto shared_lock = db0::LockedContext::lockShared();
            db0::LockedContext::makeNew(&py_object->modifyExt(), workspace_ptr, std::move(shared_lock));
            return py_object;
        } catch (...) {        
            Py_DECREF(py_object);
            throw;
        }
    }
    
    PyObject *PyAPI_beginLocked(PyObject *self, PyObject *const *, Py_ssize_t nargs)
    {
        if (nargs != 0) {
            PyErr_SetString(PyExc_TypeError, "beginLocked allows no arguments");
            return NULL;
        }
        return runSafe(PyAPI_tryBeginLocked, self);
    }

    bool PyLocked_Check(PyObject *object) {
        return Py_TYPE(object) == &PyLockedType;
    }
    
    PyObject *tryPyLocked_close(PyLocked *self)
    {        
        self->modifyExt().close();
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_PyLocked_close(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        return runSafe(tryPyLocked_close, reinterpret_cast<PyLocked*>(self));
    }

    PyObject *tryPyLocked_get_mutation_log(PyLocked *self)
    {   
        // list of tuples: prefix name / state number       
        auto mutation_log = self->ext().getMutationLog();
        auto mutation_log_list = PyList_New(mutation_log.size());
        if (!mutation_log_list) {
            return NULL;
        }

        unsigned int i = 0;
        for (const auto &item: mutation_log) {
            auto tuple = PyTuple_Pack(2, PyUnicode_FromString(item.first.c_str()), PyLong_FromUnsignedLongLong(item.second));
            if (!tuple) {
                Py_DECREF(mutation_log_list);
                return NULL;
            }
            PyList_SET_ITEM(mutation_log_list, i, tuple);
            ++i;
        }
        return mutation_log_list;
    }
    
    PyObject *PyAPI_PyLocked_get_mutation_log(PyObject *self, PyObject *)
    {
        PY_API_FUNC
        return runSafe(tryPyLocked_get_mutation_log, reinterpret_cast<PyLocked*>(self));
    }

}