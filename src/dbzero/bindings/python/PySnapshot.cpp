#include "PySnapshot.hpp"
#include "PyInternalAPI.hpp"

namespace db0::python

{
    
    static PyMethodDef PySnapshot_methods[] = 
    {
        {"fetch", (PyCFunction)&PySnapshot_fetch, METH_FASTCALL, "Fetch DBZero object instance by its ID or type (in case of a singleton)"},
        {"find", (PyCFunction)&PySnapshot_find, METH_FASTCALL, ""},
        {"close", &PySnapshot_close, METH_NOARGS, "Close DBZero snapshot"},
        {"__enter__", &PySnapshot_enter, METH_NOARGS, "Enter DBZero snapshot context"},
        {"__exit__", &PySnapshot_exit, METH_VARARGS, "Exit DBZero snapshot context"},
        {NULL}
    };
    
    PySnapshotObject *PySnapshot_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PySnapshotObject*>(type->tp_alloc(type, 0));
    }

    PySnapshotObject *PySnapshotDefault_new() {   
        return PySnapshot_new(&PySnapshotObjectType, NULL, NULL);
    }
    
    void PySnapshot_del(PySnapshotObject* snapshot_obj)
    {
        // destroy associated DB0 instance
        snapshot_obj->ext().~WorkspaceView();
        Py_TYPE(snapshot_obj)->tp_free((PyObject*)snapshot_obj);
    }

    PyTypeObject PySnapshotObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Snapshot",
        .tp_basicsize = PySnapshotObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PySnapshot_del, 
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero state snapshot object",
        .tp_methods = PySnapshot_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PySnapshot_new,
        .tp_free = PyObject_Free,
    };

    bool PySnapshot_Check(PyObject *object) {
        return Py_TYPE(object) == &PySnapshotObjectType;
    }

    PySnapshotObject *makeSnapshot(std::optional<std::uint64_t> state_num)
    {    
        auto py_object = PySnapshot_new(&PySnapshotObjectType, NULL, NULL);
        auto workspace_ptr = PyToolkit::getPyWorkspace().getWorkspaceSharedPtr();
        // make a workspace view
        db0::WorkspaceView::makeNew(&py_object->ext(), workspace_ptr, state_num);
        return py_object;
    }

    PySnapshotObject *tryGetSnapshot(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs > 1) {
            THROWF(db0::InputException) << "Too many arguments";
        }

        std::optional<std::uint64_t> state_num;
        if (nargs == 1) {
            state_num = PyToolkit::getTypeManager().extractUInt64(args[0]);
        }

        return makeSnapshot(state_num);
    }
    
    PyObject *tryPySnapshot_fetch(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (!PySnapshot_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto &snapshot = reinterpret_cast<PySnapshotObject*>(self)->ext();
        return tryFetchFrom(snapshot, args, nargs);
    }

    PyObject *tryPySnapshot_find(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (!PySnapshot_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto &snapshot = reinterpret_cast<PySnapshotObject*>(self)->ext();
        return findIn(snapshot, args, nargs);
    }

    PyObject *PySnapshot_fetch(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
        return runSafe(tryPySnapshot_fetch, self, args, nargs);
    }

    PyObject *PySnapshot_find(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
        return runSafe(tryPySnapshot_find, self, args, nargs);
    }

    PyObject *PySnapshot_enter(PyObject *self, PyObject *) {
        return self;
    }

    PyObject *PySnapshot_exit(PyObject *self, PyObject *) {
        return PySnapshot_close(self, NULL);
    }

    db0::WorkspaceView *extractWorkspaceViewPtr(PySnapshotObject *snapshot)
    {
        if (snapshot == nullptr) {
            return nullptr;
        }
        return &snapshot->ext();
    }
    
    template <> bool Which_TypeCheck<PySnapshotObject>(PyObject *py_object) {
        return PySnapshot_Check(py_object);
    }

    PyObject *tryPySnapshot_close(PyObject *self, PyObject *)
    {
        if (!PySnapshot_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        reinterpret_cast<PySnapshotObject*>(self)->ext().close();        
        Py_RETURN_NONE;
    }

    PyObject *PySnapshot_close(PyObject *self, PyObject *args) {
        return runSafe(tryPySnapshot_close, self, args);
    }

}