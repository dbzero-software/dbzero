#include "PySnapshot.hpp"
#include "PyInternalAPI.hpp"
#include "PyToolkit.hpp"
#include "PyTagsAPI.hpp"
#include "Memo.hpp"
#include <dbzero/object_model/object/Object.hpp>

namespace db0::python

{
        
    PySnapshotObject *PySnapshotDefault_new() {
        return PySnapshot_new(&PySnapshotObjectType, NULL, NULL);
    }
    
    void PyAPI_PySnapshot_del(PySnapshotObject* snapshot_obj)
    {        
        // NOTE: it's safe to destroy without API lock (not a v_object)
        // also API lock here would result in a deadlock        
        snapshot_obj->destroy();
        Py_TYPE(snapshot_obj)->tp_free((PyObject*)snapshot_obj);
    }
        
    bool PySnapshot_Check(PyObject *object) {
        return Py_TYPE(object) == &PySnapshotObjectType;
    }
    
    PySnapshotObject *tryGetSnapshot(std::optional<std::uint64_t> state_num,
        const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums, bool frozen)
    {  
        if (frozen && (state_num || !prefix_state_nums.empty())) {
            THROWF(db0::InputException) << "Frozen snapshot can only be taken for the head state (i.e. no state numbers can be requested)";
        }        
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        std::shared_ptr<db0::Snapshot> workspace_view;
        if (frozen) {
            workspace_view = workspace.getFrozenWorkspaceHeadView();
        } else {
            if (!state_num && prefix_state_nums.empty()) {
                // refresh if updated
                workspace.refresh(true);
            }
            workspace_view = workspace.getWorkspaceView(state_num, prefix_state_nums);
        }
        auto py_snapshot = PySnapshot_new(&PySnapshotObjectType, NULL, NULL);
        py_snapshot->makeNew(workspace_view);
        return py_snapshot;
    }
    
    PyObject* tryPySnapshot_fetch(PyObject *self, PyObject *py_id, PyTypeObject *type, const char *prefix_name)
    {
        if (!PySnapshot_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto &snapshot = reinterpret_cast<PySnapshotObject*>(self)->modifyExt();
        return tryFetchFrom(snapshot, py_id, type, prefix_name).steal();
    }
    
    PyObject *tryPySnapshot_find(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (!PySnapshot_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto &snapshot = reinterpret_cast<PySnapshotObject*>(self)->modifyExt();
        // NOTE: self attached as context
        return findIn(snapshot, args, nargs, self);
    }
    
    PyObject *tryGetStateNum(db0::Snapshot &snapshot, PyObject *args, PyObject *kwargs)
    {
        auto fixture = getPrefixFromArgs(snapshot, args, kwargs, "prefix");
        return PyLong_FromLong(fixture->getPrefix().getStateNum());
    }
    
    PyObject *PyAPI_PySnapshot_GetStateNum(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        if (!PySnapshot_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto &snapshot = reinterpret_cast<PySnapshotObject*>(self)->modifyExt();
        PY_API_FUNC
        return runSafe(tryGetStateNum, snapshot, args, kwargs);
    }

    PyObject *PyAPI_PySnapshot_fetch(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC

        static const char *kwlist[] = { "id", "type", "prefix", NULL };
        PyObject *py_id = nullptr;
        PyObject *py_type = nullptr;
        const char *prefix_name = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Os", const_cast<char**>(kwlist), &py_id, &py_type, &prefix_name)) {
            PyErr_SetString(PyExc_TypeError, "Invalid arguments");
            return NULL;
        }

        // NOTE: for backwards compatibility, swap parameters if one is a type and the other is UUID
        if (py_id && py_type && PyType_Check(py_id) && PyUnicode_Check(py_type)) {
            std::swap(py_id, py_type);
        }

        if (py_type && !PyType_Check(py_type)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type: type");
            return NULL;
        }

        return runSafe(tryPySnapshot_fetch, self, py_id, reinterpret_cast<PyTypeObject*>(py_type), prefix_name);
    }

    PyObject *PyAPI_PySnapshot_find(PyObject *self, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_API_FUNC
        return runSafe(tryPySnapshot_find, self, args, nargs);
    }

    PyObject *PyAPI_PySnapshot_enter(PyObject *self, PyObject *)
    {
        Py_IncRef(self);
        return self;
    }
    
    PyObject *tryPySnapshot_exit(PyObject *self, PyObject *) 
    {
        // release the underlying WorkspaceView shared_ptr (may result in destroying the instance)
        reinterpret_cast<PySnapshotObject*>(self)->reset();
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_PySnapshot_exit(PyObject *self, PyObject *) 
    {
        PY_API_FUNC
        return runSafe(tryPySnapshot_exit, self, nullptr);
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
    
    PyObject *PyAPI_PySnapshot_deserialize(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "deserialize requires exactly 1 argument");
            return NULL;
        }

        if (!PySnapshot_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }    
        PY_API_FUNC
        auto &workspace = reinterpret_cast<PySnapshotObject*>(self)->modifyExt();
        return runSafe(tryDeserialize, &workspace, args[0]);
    }
    
    PyObject *PyAPI_PySnapshot_close(PyObject *self, PyObject *args)
    {
        PY_API_FUNC
        return runSafe(tryPySnapshot_close, self, args);
    }
    
    PyObject *tryGetSnapshotOf(MemoObject *memo)
    {
        auto fixture = memo->ext().getFixture();
        auto workspace_view = fixture->getWorkspace().shared_from_this();
        auto py_snapshot = PySnapshot_new(&PySnapshotObjectType, NULL, NULL);
        py_snapshot->makeNew(workspace_view);
        return py_snapshot;
    }
    
    PyObject *PyAPI_getSnapshotOf(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "get_snapshot_of requires exactly 1 argument");
            return NULL;
        }
        
        PyObject *py_arg = args[0];
        if (!PyMemo_Check(py_arg)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type (must be a memo object)");
            return NULL;
        }
        return runSafe(tryGetSnapshotOf, reinterpret_cast<MemoObject*>(py_arg));
    }
    
    static PyMethodDef PySnapshot_methods[] = 
    {
        {"fetch", (PyCFunction)&PyAPI_PySnapshot_fetch, METH_VARARGS | METH_KEYWORDS, "Fetch dbzero object instance by its ID or type (in case of a singleton)"},
        {"find", (PyCFunction)&PyAPI_PySnapshot_find, METH_FASTCALL, ""},
        {"deserialize", (PyCFunction)&PyAPI_PySnapshot_deserialize, METH_FASTCALL, "Deserialize from bytes within the snapshot's context"},
        {"close", &PyAPI_PySnapshot_close, METH_NOARGS, "Close dbzero snapshot"},
        {"get_state_num", (PyCFunction)&PyAPI_PySnapshot_GetStateNum, METH_VARARGS | METH_KEYWORDS, "Get state number of the snapshot"},
        {"__enter__", &PyAPI_PySnapshot_enter, METH_NOARGS, "Enter dbzero snapshot context"},
        {"__exit__", &PyAPI_PySnapshot_exit, METH_VARARGS, "Exit dbzero snapshot's context"},
        {NULL}
    };

    PyTypeObject PySnapshotObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "Snapshot",
        .tp_basicsize = PySnapshotObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_PySnapshot_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero state snapshot object",
        .tp_methods = PySnapshot_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PySnapshot_new,
        .tp_free = PyObject_Free,
    };

    PySnapshotObject *PySnapshot_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PySnapshotObject*>(type->tp_alloc(type, 0));
    }

}