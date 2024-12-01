#include <Python.h>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyEnum.hpp>

namespace db0::python

{

    template<typename ObjectT>
    PyObject *PyAPI_ObjectT_append(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "append() takes exactly one argument");
            return NULL;
        }
        
        db0::FixtureLock lock(py_obj->ext().getFixture());
        py_obj->modifyExt().append(lock, args[0]);
        Py_RETURN_NONE;
    }
    
    template<typename ObjectT>
    PyObject *ObjectT_extend(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "extend() takes one argument.");
            return NULL;
        }
        PyObject *iterator = PyObject_GetIter(args[0]);
        PyObject *item;
        db0::FixtureLock lock(py_obj->ext().getFixture());
        auto &obj = py_obj->modifyExt();
        while ((item = PyIter_Next(iterator))) {
            obj.append(lock, item);            
            Py_DECREF(item);
        }

        Py_DECREF(iterator);
        Py_RETURN_NONE;
    }
    
    template<typename ObjectT>
    PyObject *PyAPI_ObjectT_extend(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return ObjectT_extend(py_obj, args, nargs);
    }
    
    template <typename ObjectT>
    int PyAPI_ObjectT_SetItem(ObjectT *py_obj, Py_ssize_t i, PyObject *value)
    {
        PY_API_FUNC
        db0::FixtureLock lock(py_obj->ext().getFixture());
        py_obj->modifyExt().setItem(lock, i, value);
        return 0;
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
        db0::FixtureLock lock(py_obj->ext().getFixture());
        py_obj->modifyExt().setItem(lock, PyLong_AsLong(args[0]), args[1]);
        Py_RETURN_NONE;
    }
    
    template <typename ObjectT>
    PyObject *PyAPI_ObjectT_GetItem(ObjectT *py_obj, Py_ssize_t i)
    {
        PY_API_FUNC
        py_obj->ext().getFixture()->refreshIfUpdated();
        return py_obj->ext().getItem(i).steal();
    }
    
    template<typename ObjectT>
    Py_ssize_t PyAPI_ObjectT_len(ObjectT *py_obj)
    {
        PY_API_FUNC
        py_obj->ext().getFixture()->refreshIfUpdated();
        return py_obj->ext().size();
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
    PyObject *PyAPI_ObjectT_pop(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
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

    template <typename ObjectT>
    PyObject *PyAPI_ObjectT_remove(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "remove() takes one argument.");
            return NULL;
        }
        auto index = py_obj->ext().index(args[0]);
        db0::FixtureLock lock(py_obj->ext().getFixture());
        py_obj->modifyExt().swapAndPop(lock, {index});
        Py_RETURN_NONE;
    }
    
    template <typename ObjectT>
    PyObject *PyAPI_ObjectT_index(ObjectT *py_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "index() takes one argument.");
            return NULL;
        }
        auto index = PyLong_FromLong(py_obj->ext().index(args[0]));
        return index;
    }
    
    // Checks if the key conversion is required
    template <typename CollectionT>
    bool hasTranslatedKey(const CollectionT &collection, PyObject *key)
    {
        if (PyEnumValue_Check(key)) {
            auto fixture = collection.getFixture();
            return hasTranslatedEnumValue(fixture, reinterpret_cast<PyEnumValue*>(key));
        } else if (PyTuple_Check(key)) {
            // check each element of the tuple
            auto size = PyTuple_Size(key);
            for (int i = 0; i < size; ++i) {
                auto item = PyTuple_GetItem(key, i);
                bool result = hasTranslatedKey(collection, item);
                if (result) {
                    return true;
                }
            }
        }

        // no translation needed
        return false;
    }
    
    // Performs key translaction where necessary
    // this is required for translating non-scoped EnumValues between prefixes
    template <typename CollectionT>
    shared_py_object<PyObject*> translatedKey(const CollectionT &collection, PyObject *key)
    {
        if (PyEnumValue_Check(key)) {
            auto fixture = collection.getFixture();
            return translatedEnumValue(fixture, reinterpret_cast<PyEnumValue*>(key));
        } else if (PyTuple_Check(key)) {
            // only perform tuple translation if needed
            if (hasTranslatedKey(collection, key)) {
                auto size = PyTuple_Size(key);
                auto py_tuple = PyTuple_New(size);
                for (int i = 0; i < size; ++i) {
                    auto item = PyTuple_GetItem(key, i);
                    auto translated = translatedKey(collection, item);                    
                    PyTuple_SET_ITEM(py_tuple, i, translated.steal());
                }
                return shared_py_object<PyObject*>(py_tuple);
            }
        }
        // no translation needed
        return key;
    }

}