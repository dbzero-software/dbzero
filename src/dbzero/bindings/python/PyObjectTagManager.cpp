// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyObjectTagManager.hpp"
#include "Memo.hpp"
#include "PyInternalAPI.hpp"

namespace db0::python

{

    using ObjectTagManager = db0::object_model::ObjectTagManager;
    
    static PyNumberMethods PyObjectTagManager_as_num = {
        .nb_add = (binaryfunc)PyAPI_PyObjectTagManager_add_binary,
        .nb_subtract= (binaryfunc)PyAPI_PyObjectTagManager_remove_binary
    };

    static PyMethodDef PyObjectTagManager_methods[] = {
        {"add", (PyCFunction)PyAPI_PyObjectTagManager_add, METH_FASTCALL, "Assign tags to an instance."},
        {"remove", (PyCFunction)PyAPI_PyObjectTagManager_remove, METH_FASTCALL, "Remove tags from an instance."},
        {NULL}
    };

    PyObjectTagManager *PyObjectTagManager_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyObjectTagManager*>(type->tp_alloc(type, 0));
    }

    void PyAPI_PyObjectTagManager_del(PyObjectTagManager* tags_obj)
    {
        PY_API_FUNC
        // destroy associated DB0 instance
        tags_obj->destroy();
        Py_TYPE(tags_obj)->tp_free((PyObject*)tags_obj);
    }
    
    PyObject *tryPyObjectTagManager_add_binary(PyObjectTagManager *tag_manager, PyObject *object)
    {    
        tag_manager->modifyExt().add(&object, 1);
        Py_INCREF(tag_manager);
        return tag_manager;
    }

    PyObject *PyAPI_PyObjectTagManager_add_binary(PyObjectTagManager *tag_manager, PyObject *object) 
    {
        PY_API_FUNC
        return runSafe(tryPyObjectTagManager_add_binary, tag_manager, object);
    }

    PyObject *tryPyObjectTagManager_add(PyObjectTagManager *tag_manager, PyObject *const *args, Py_ssize_t nargs) 
    {        
        tag_manager->modifyExt().add(args, nargs);
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_PyObjectTagManager_add(PyObjectTagManager *tag_manager, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_API_FUNC
        return runSafe(tryPyObjectTagManager_add, tag_manager, args, nargs);
    }
    
    PyObject *tryPyObjectTagManager_remove_binary(PyObjectTagManager *tag_manager, PyObject *object)
    {
        tag_manager->modifyExt().remove(&object, 1);
        Py_INCREF(tag_manager);
        return tag_manager;
    }

    PyObject *PyAPI_PyObjectTagManager_remove_binary(PyObjectTagManager *tag_manager, PyObject *object) 
    {
        PY_API_FUNC
        return runSafe(tryPyObjectTagManager_remove_binary, tag_manager, object);
    }

    PyObject *tryPyObjectTagManager_remove(PyObjectTagManager *tag_manager, PyObject *const *args, Py_ssize_t nargs)
    {
        tag_manager->modifyExt().remove(args, nargs);
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_PyObjectTagManager_remove(PyObjectTagManager *tag_manager, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_API_FUNC
        return runSafe(tryPyObjectTagManager_remove, tag_manager, args, nargs);
    }
    
    PyTypeObject PyObjectTagManagerType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.Tags",
        .tp_basicsize = PyObjectTagManager::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_PyObjectTagManager_del,
        .tp_as_number = &PyObjectTagManager_as_num,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero tag manager object",
        .tp_methods = PyObjectTagManager_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyObjectTagManager_new,
        .tp_free = PyObject_Free,
    };
    
    PyObjectTagManager *tryMakeObjectTagManager(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        // all arguments must be Memo objects
        for (Py_ssize_t i = 0; i < nargs; ++i) {
            if (!PyAnyMemo_Check(args[i])) {
                THROWF(db0::InputException) << "All arguments must be dbzero memo objects";
            }
        }
        
        auto tags_obj = Py_OWN(PyObjectTagManager_new(&PyObjectTagManagerType, NULL, NULL));        
        ObjectTagManager::makeNew(&tags_obj->modifyExt(), args, nargs);
        return tags_obj.steal();
    }
    
    PyObjectTagManager *makeObjectTagManager(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(tryMakeObjectTagManager, nullptr, args, nargs);
    }
    
}
