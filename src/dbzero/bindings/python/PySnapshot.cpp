#include "PySnapshot.hpp"
#include "PyInternalAPI.hpp"

namespace db0::python

{
    
    static PyMethodDef PySnapshot_methods[] = 
    {
        {"fetch", &PySnapshot_fetch, METH_VARARGS, "Fetch DBZero object instance by its ID or type (in case of a singleton)"},
        {"close", &PySnapshot_close, METH_NOARGS, "Close DBZero snapshot"},
        {"__enter__", &PySnapshot_enter, METH_NOARGS, "Enter DBZero snapshot context"},
        {"__exit__", &PySnapshot_exit, METH_VARARGS, "Exit DBZero snapshot context"},
        {NULL}
    };
    
    PySnapshotObject *PySnapshot_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<PySnapshotObject*>(type->tp_alloc(type, 0));
    }

    PySnapshotObject *PySnapshotDefault_new()
    {   
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

    bool PySnapshot_Check(PyObject *object)
    {
        return Py_TYPE(object) == &PySnapshotObjectType;
    }

    PySnapshotObject *makeSnapshot(PyObject *, PyObject *args)
    {
        auto py_object = PySnapshot_new(&PySnapshotObjectType, NULL, NULL);        
        auto workspace_ptr = PyToolkit::getPyWorkspace().getWorkspaceSharedPtr();
        // make a workspace view
        db0::WorkspaceView::makeNew(&py_object->ext(), workspace_ptr);
        return py_object;
    }
    
    PyObject *tryPySnapshot_fetch(PyObject *self, PyObject *args)
    {
        if (!PySnapshot_Check(self)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            THROWF(db0::InputException) << "Invalid open arguments";
        }

        // detect which argument type was used
        auto &snapshot = reinterpret_cast<PySnapshotObject*>(self)->ext();
        // decode ObjectId from string
        if (PyUnicode_Check(py_object)) {            
            auto object_id = ObjectId::fromBase32(PyUnicode_AsUTF8(py_object));                
            auto fixture = snapshot.getFixture(object_id.m_fixture_uuid);
            return fetchObject(fixture, object_id);
        }
        
        if (PyType_Check(py_object)) {
            return fetchSingletonObject(snapshot, reinterpret_cast<PyTypeObject*>(py_object));
        }

        PyErr_SetString(PyExc_TypeError, "Invalid argument type");
        return NULL;
    }
    
    PyObject *PySnapshot_fetch(PyObject *self, PyObject *args)
    {
        return runSafe(tryPySnapshot_fetch, self, args);
    }

    PyObject *PySnapshot_enter(PyObject *self, PyObject *)
    {
        return self;
    }

    PyObject *PySnapshot_exit(PyObject *self, PyObject *)
    {
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

    PyObject *PySnapshot_close(PyObject *self, PyObject *args)
    {
        return runSafe(tryPySnapshot_close, self, args);
    }

}