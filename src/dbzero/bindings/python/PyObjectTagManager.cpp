// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyObjectTagManager.hpp"
#include "Memo.hpp"
#include "PyInternalAPI.hpp"
#include "PyToolkit.hpp"
#include "iter/PyObjectIterable.hpp"
#include <memory>
#include <vector>

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
        PY_DEALLOC_GUARD();
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
        PY_MUTATING_API_FUNC(NULL)
        return runSafe(tryPyObjectTagManager_add_binary, tag_manager, object);
    }

    PyObject *tryPyObjectTagManager_add(PyObjectTagManager *tag_manager, PyObject *const *args, Py_ssize_t nargs) 
    {        
        tag_manager->modifyExt().add(args, nargs);
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_PyObjectTagManager_add(PyObjectTagManager *tag_manager, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_MUTATING_API_FUNC(NULL)
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
        PY_MUTATING_API_FUNC(NULL)
        return runSafe(tryPyObjectTagManager_remove_binary, tag_manager, object);
    }

    PyObject *tryPyObjectTagManager_remove(PyObjectTagManager *tag_manager, PyObject *const *args, Py_ssize_t nargs)
    {
        tag_manager->modifyExt().remove(args, nargs);
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_PyObjectTagManager_remove(PyObjectTagManager *tag_manager, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_MUTATING_API_FUNC(NULL)
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
    
    PyObjectTagManager *tryMakeObjectTagManager(PyObject *, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames)
    {
        bool passive = false;
        if (kwnames) {
            auto nkwargs = PyTuple_GET_SIZE(kwnames);
            for (Py_ssize_t i = 0; i < nkwargs; ++i) {
                auto *kwname = PyTuple_GET_ITEM(kwnames, i);
                if (!PyUnicode_Check(kwname) || PyUnicode_CompareWithASCIIString(kwname, "passive") != 0) {
                    THROWF(db0::InputException) << "Unknown keyword argument for dbzero.tags" << THROWF_END;
                }
                auto is_true = PyObject_IsTrue(args[nargs + i]);
                if (is_true < 0) {
                    THROWF(db0::InputException) << "Unable to interpret passive argument as bool" << THROWF_END;
                }
                passive = is_true != 0;
            }
        }

        std::vector<PyObject*> memo_args;
        std::vector<std::shared_ptr<ObjectIterable> > query_targets;
        memo_args.reserve(nargs);
        query_targets.reserve(nargs);

        for (Py_ssize_t i = 0; i < nargs; ++i) {
            if (PyObjectIterable_Check(args[i])) {
                auto *query = reinterpret_cast<PyObjectIterable*>(args[i]);
                query_targets.push_back(query->getSharedPtr());
                continue;
            }
            if (!PyToolkit::isAnyMemoObject(args[i])) {
                THROWF(db0::InputException) << "All arguments must be dbzero memo objects or object queries";
            }
            if (PyMemo_Check<MemoObject>(args[i])) {
                auto *memoObject = reinterpret_cast<MemoObject *>(args[i]);
                if (!memoObject->ext().hasInstance()) {
                    auto materialized = Py_OWN(getMaterializedMemoObject(memoObject));
                }
            } else if (PyMemo_Check<MemoImmutableObject>(args[i])) {
                auto *memoObject = reinterpret_cast<MemoImmutableObject *>(args[i]);
                if (!memoObject->ext().hasInstance()) {
                    auto materialized = Py_OWN(getMaterializedMemoObject(memoObject));
                }
            }
            memo_args.push_back(args[i]);
        }
        
        auto tags_obj = Py_OWN(PyObjectTagManager_new(&PyObjectTagManagerType, NULL, NULL));        
        ObjectTagManager::makeNew(
            &tags_obj->modifyExt(),
            memo_args.data(),
            memo_args.size(),
            std::move(query_targets),
            passive
        );
        return tags_obj.steal();
    }
    
    PyObjectTagManager *makeObjectTagManager(PyObject *, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames)
    {
        PY_API_FUNC
        return runSafe(tryMakeObjectTagManager, nullptr, args, nargs, kwnames);
    }
    
}
