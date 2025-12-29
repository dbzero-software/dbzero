// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <Python.h>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>

namespace db0::python

{

    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
    
    template<typename ObjectT>
    PyObject *tryObjectT_append(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        db0::FixtureLock lock(py_obj->ext().getFixture());
        py_obj->modifyExt().append(lock, args[0]);
        Py_RETURN_NONE;
    }

    template<typename ObjectT>
    PyObject *PyAPI_ObjectT_append(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "append() takes exactly one argument");
            return NULL;
        }

        return runSafe(tryObjectT_append<ObjectT>, py_obj, args, nargs);
    }
    
    template<typename ObjectT>
    PyObject *tryObjectT_extend(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {   
        auto iterator = Py_OWN(PyObject_GetIter(args[0]));
        if (!iterator) {
            PyErr_SetString(PyExc_TypeError, "extend() argument must be iterable.");
            return nullptr;
        }
        
        ObjectSharedPtr item;
        db0::FixtureLock lock(py_obj->ext().getFixture());
        auto &obj = py_obj->modifyExt();
        Py_FOR(item, iterator) {
            obj.append(lock, item);
        }
        
        Py_RETURN_NONE;
    }

    template<typename ObjectT>
    PyObject *PyAPI_ObjectT_extend(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "extend() takes one argument.");
            return NULL;
        }
        return runSafe(tryObjectT_extend<ObjectT>, py_obj, args, nargs);
    }

    template <typename ObjectT>
    int tryObjectT_SetItem(ObjectT *py_obj, Py_ssize_t i, PyObject *value)
    {        
        db0::FixtureLock lock(py_obj->ext().getFixture());
        py_obj->modifyExt().setItem(lock, i, value);
        return 0;
    }

    template <typename ObjectT>
    int PyAPI_ObjectT_SetItem(ObjectT *py_obj, Py_ssize_t i, PyObject *value)
    {
        PY_API_FUNC
        return runSafe(tryObjectT_SetItem<ObjectT>, py_obj, i, value);
    }

    template <typename ObjectT>
    PyObject* tryObjectT_Insert(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {        
        db0::FixtureLock lock(py_obj->ext().getFixture());
        py_obj->modifyExt().setItem(lock, PyLong_AsLong(args[0]), args[1]);
        Py_RETURN_NONE;
    }

    template <typename ObjectT>
    PyObject* PyAPI_ObjectT_Insert(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {        
        PY_API_FUNC
        if (nargs != 2) {
            PyErr_SetString(PyExc_TypeError, "insert() takes exactly two argument");
            return NULL;
        }
        if (PyLong_Check(args[0]) == 0) {
            PyErr_SetString(PyExc_TypeError, "insert() takes an integer as first argument");
            return NULL;
        }
        return runSafe(tryObjectT_Insert<ObjectT>, py_obj, args, nargs);
    }

    template <typename ObjectT>
    PyObject *tryObjectT_GetItem(ObjectT *py_obj, Py_ssize_t i)
    {        
        py_obj->ext().getFixture()->refreshIfUpdated();
        return py_obj->ext().getItem(i).steal();
    }

    template <typename ObjectT>
    PyObject *PyAPI_ObjectT_GetItem(ObjectT *py_obj, Py_ssize_t i)
    {
        PY_API_FUNC
        return runSafe(tryObjectT_GetItem<ObjectT>, py_obj, i);
    }

    template<typename ObjectT>
    Py_ssize_t tryObjectT_len(ObjectT *py_obj)
    {        
        py_obj->ext().getFixture()->refreshIfUpdated();
        return py_obj->ext().size();
    }

    template<typename ObjectT>
    Py_ssize_t PyAPI_ObjectT_len(ObjectT *py_obj)
    {
        PY_API_FUNC
        return runSafe(tryObjectT_len<ObjectT>, py_obj);
    }

    template<typename ObjectT>
    constexpr PySequenceMethods getPySequenceMehods() {
    return {
        .sq_length = (lenfunc)PyAPI_ObjectT_len<ObjectT>,
        .sq_item = (ssizeargfunc)PyAPI_ObjectT_GetItem<ObjectT>,
        .sq_ass_item = (ssizeobjargproc)PyAPI_ObjectT_SetItem<ObjectT>
        };
    }

    template<typename ObjectT>
    PyObject *tryObjectT_pop(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        std::size_t index;
        if (nargs == 0) {
            index = py_obj->ext().size() -1;
        } else if (nargs == 1) {
            index = PyLong_AsLong(args[0]);
        } else {
            PyErr_SetString(PyExc_TypeError, "pop() takes zero or one argument.");
            return NULL;
        }

        db0::FixtureLock lock(py_obj->ext().getFixture());
        return py_obj->modifyExt().pop(lock, index).steal();
    }
    
    template<typename ObjectT>
    PyObject *PyAPI_ObjectT_pop(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(tryObjectT_pop<ObjectT>, py_obj, args, nargs);
    }

    template <typename ObjectT>
    PyObject *tryObjectT_remove(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        auto index = py_obj->ext().index(args[0]);
        db0::FixtureLock lock(py_obj->ext().getFixture());
        py_obj->modifyExt().swapAndPop(lock, {index});
        Py_RETURN_NONE;
    }

    template <typename ObjectT>
    PyObject *PyAPI_ObjectT_remove(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "remove() takes one argument.");
            return NULL;
        }
        return runSafe(tryObjectT_remove<ObjectT>, py_obj, args, nargs);
    }
    
    template <typename ObjectT>
    PyObject *tryObjectT_index(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        return PyLong_FromLong(py_obj->ext().index(args[0]));        
    }

    template <typename ObjectT>
    PyObject *PyAPI_ObjectT_index(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "index() takes one argument.");
            return NULL;
        }
        return runSafe(tryObjectT_index<ObjectT>, py_obj, args, nargs);
    }
    
    // Checks if the key migration (to a different prefix) is required
    template <typename CollectionT>
    bool isMigrateRrequired(const CollectionT &collection, PyObject *key)
    {
        if (PyEnumValue_Check(key)) {
            auto fixture = collection.getFixture();
            return isMigrateRequired(fixture, reinterpret_cast<PyEnumValue*>(key));
        } else if (PyTuple_Check(key)) {
            // check each element of the tuple
            auto size = PyTuple_Size(key);
            for (int i = 0; i < size; ++i) {
                auto item = PyTuple_GetItem(key, i);
                bool result = isMigrateRrequired(collection, item);
                if (result) {
                    return true;
                }
            }
        }

        // no translation needed
        return false;
    }
    
    // Performs key migration (to a colleciton's prefix) where necessary
    // this is required for translating non-scoped EnumValues between prefixes
    template <typename CollectionT>
    shared_py_object<PyObject*> migratedKey(const CollectionT &collection, PyObject *key)
    {
        if (PyEnumValue_Check(key)) {
            auto fixture = collection.getFixture();
            return migratedEnumValue(fixture, reinterpret_cast<PyEnumValue*>(key));
        } else if (PyTuple_Check(key)) {
            // only perform tuple translation if needed
            if (isMigrateRrequired(collection, key)) {
                auto size = PyTuple_Size(key);
                auto py_tuple = Py_OWN(PyTuple_New(size));
                for (int i = 0; i < size; ++i) {
                    PySafeTuple_SetItem(*py_tuple, i, migratedKey(collection, PyTuple_GetItem(key, i)));
                }
                return py_tuple;
            }
        }
        // no translation needed
        return Py_BORROW(key);
    }
    
}