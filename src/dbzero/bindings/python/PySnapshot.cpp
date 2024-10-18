#include "PySnapshot.hpp"
#include "PyInternalAPI.hpp"

namespace db0::python

{
    
    static PyMethodDef PySnapshot_methods[] = 
    {
        {"fetch", (PyCFunction)&PySnapshot_fetch, METH_FASTCALL, "Fetch DBZero object instance by its ID or type (in case of a singleton)"},
        {"find", (PyCFunction)&PySnapshot_find, METH_FASTCALL, ""},
        {"deserialize", (PyCFunction)&PySnapshot_deserialize, METH_FASTCALL, "Deserialize from bytes within the snapshot's context"},
        {"close", &PyAPI_PySnapshot_close, METH_NOARGS, "Close DBZero snapshot"},
        {"__enter__", &PySnapshot_enter, METH_NOARGS, "Enter DBZero snapshot context"},
        {"__exit__", &PyAPI_PySnapshot_exit, METH_VARARGS, "Exit DBZero snapshot context"},
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
        // NOTE: it's safe to destroy without API lock (not a v_object)
        // also API lock here would result in a deadlock
        snapshot_obj->destroy();
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
    
    PySnapshotObject *tryGetSnapshot(std::optional<std::uint64_t> state_num,
        const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums)
    {    
        auto py_snapshot = PySnapshot_new(&PySnapshotObjectType, NULL, NULL);
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        py_snapshot->makeNew(workspace.getWorkspaceView(state_num, prefix_state_nums));
        return py_snapshot;
    }
    
    PyObject* tryPySnapshot_fetch(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (!PySnapshot_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto &snapshot = reinterpret_cast<PySnapshotObject*>(self)->modifyExt();
        return tryFetchFrom(snapshot, args, nargs).steal();
    }

    PyObject *tryPySnapshot_find(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (!PySnapshot_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto &snapshot = reinterpret_cast<PySnapshotObject*>(self)->modifyExt();
        return findIn(snapshot, args, nargs);
    }
    
    PyObject* PySnapshot_fetch(PyObject *self, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_API_FUNC
        return runSafe(tryPySnapshot_fetch, self, args, nargs);
    }

    PyObject *PySnapshot_find(PyObject *self, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_API_FUNC
        return runSafe(tryPySnapshot_find, self, args, nargs);
    }

    PyObject *PySnapshot_enter(PyObject *self, PyObject *)
    {
        Py_IncRef(self);
        return self;
    }

    PyObject *PyAPI_PySnapshot_exit(PyObject *self, PyObject *) {
        return PyAPI_PySnapshot_close(self, NULL);
    }

    db0::WorkspaceView *extractWorkspaceViewPtr(PySnapshotObject *snapshot)
    {
        if (snapshot == nullptr) {
            return nullptr;
        }
        return &snapshot->modifyExt();
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
        
        reinterpret_cast<PySnapshotObject*>(self)->modifyExt().close();
        Py_RETURN_NONE;
    }
    
    PyObject *PySnapshot_deserialize(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "deserialize requires exactly 1 argument");
            return NULL;
        }

        if (!PySnapshot_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        auto &workspace = reinterpret_cast<PySnapshotObject*>(self)->modifyExt();
        return runSafe(tryDeserialize, &workspace, args[0]);
    }
    
    PyObject *PyAPI_PySnapshot_close(PyObject *self, PyObject *args) 
    {
        PY_API_FUNC
        return runSafe(tryPySnapshot_close, self, args);
    }
    
}