// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <dbzero/bindings/python/collections/PyDict.hpp>
#include "PyDictView.hpp"
#include <dbzero/bindings/python/Utils.hpp>
#include "PyIterator.hpp"
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PyHash.hpp>
#include "CollectionMethods.hpp"
#include <iostream>

namespace db0::python

{
    
    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
    PyTypeObject DictIteratorObjectType = GetIteratorType<DictIteratorObject>("dbzero.DictIterator", "dbzero dict iterator");

    DictIteratorObject *tryDictObject_iter(DictObject *self)
    {        
        self->ext().getFixture()->refreshIfUpdated();
        return makeIterator<DictIteratorObject, db0::object_model::DictIterator>(
            DictIteratorObjectType, self->ext().begin(), &self->ext(), self
        );
    }

    DictIteratorObject *PyAPI_DictObject_iter(DictObject *self)
    {
        PY_API_FUNC
        return runSafe(tryDictObject_iter, self);
    }
    
    PyObject *tryDictObject_GetItem(DictObject *py_dict, PyObject *py_key)
    {
        const auto &dict_obj = py_dict->ext();
        auto fixture = dict_obj.getFixture();
        fixture->refreshIfUpdated();
        auto key = migratedKey(dict_obj, py_key);
        auto maybe_hash = getPyHashIfExists(fixture, *key);        
        if (maybe_hash) {
            auto hash = maybe_hash->first;
            if (hash == -1) {
                auto py_str = Py_OWN(PyObject_Str(py_key));
                auto str_name =  PyUnicode_AsUTF8(*py_str);            
                auto error_message = "Cannot get hash for key: " + std::string(str_name);
                PyErr_SetString(PyExc_KeyError, error_message.c_str());
                return nullptr;
            }

            auto item = dict_obj.getItem(hash, *key);
            if (item.get()) {
                return item.steal();
            }
        }     
        
        auto py_str = Py_OWN(PyObject_Str(py_key));
        auto str_name =  PyUnicode_AsUTF8(*py_str);
        PyErr_SetString(PyExc_KeyError, str_name);
        return nullptr;
    }
    
    PyObject *PyAPI_DictObject_GetItem(DictObject *dict_obj, PyObject *key)
    {
        PY_API_FUNC
        return runSafe(tryDictObject_GetItem, dict_obj, key);
    }
    
    int tryDictObject_SetItem(DictObject *py_dict, PyObject *py_key, PyObject *value)
    {
        auto key = migratedKey(py_dict->ext(), py_key);
        auto fixture = py_dict->ext().getFixture();
        auto hash = getPyHash(fixture, *key);
        if (hash == -1) {
            // set PyError
            std::stringstream _str;
            _str << "Unable to find hash function for key of type: " << Py_TYPE(*key)->tp_name;
            PyErr_SetString(PyExc_TypeError, _str.str().c_str());
            return -1;
        }
        
        db0::FixtureLock lock(fixture);
        py_dict->modifyExt().setItem(lock, hash, *key, value);
        return 0;
    }
    
    int PyAPI_DictObject_SetItem(DictObject *dict_obj, PyObject *key, PyObject *value)
    {
        PY_API_FUNC
        return runSafe<-1>(tryDictObject_SetItem, dict_obj, key, value);
    }

    Py_ssize_t tryDictObject_len(DictObject *dict_obj)
    {        
        dict_obj->ext().getFixture()->refreshIfUpdated();
        return dict_obj->ext().size();
    }

    Py_ssize_t PyAPI_DictObject_len(DictObject *dict_obj)
    {
        PY_API_FUNC
        return runSafe(tryDictObject_len, dict_obj);
    }
    
    int tryDictObject_HasItem(DictObject *py_dict, PyObject *py_key)
    {
        auto key = migratedKey(py_dict->ext(), py_key);
        auto fixture = py_dict->ext().getFixture();
        fixture->refreshIfUpdated();
        auto maybe_hash_pair = getPyHashIfExists(fixture, *key);
        if (!maybe_hash_pair) {
            // NOTE: element does not exist because a key does NOT exist either
            return 0;
        }
        return py_dict->ext().hasItem(maybe_hash_pair->first, *maybe_hash_pair->second);
    }
    
    int PyAPI_DictObject_HasItem(DictObject *dict_obj, PyObject *key)
    {
        PY_API_FUNC
        return runSafe<-1>(tryDictObject_HasItem, dict_obj, key);
    }
    
    void PyAPI_DictObject_del(DictObject* dict_obj)
    {
        PY_API_FUNC
        // destroy associated DB0 Dict instance
        dict_obj->destroy();
        Py_TYPE(dict_obj)->tp_free((PyObject*)dict_obj);
    }

    
    PyObject *tryDictObject_items(DictObject *dict_obj) {
        return makeDictView(dict_obj, &dict_obj->ext(), db0::object_model::IteratorType::ITEMS);        
    }
    
    PyObject *PyAPI_DictObject_items(DictObject *dict_obj)
    {
        PY_API_FUNC
        return runSafe(tryDictObject_items, dict_obj);    
    }
    
    
    PyObject *tryDictObject_str(DictObject *self)
    {
        std::stringstream str;
        str << "{";
        // iterate through dict items (key-value pairs)
        auto items_view = tryDictObject_items(self);
        if (!items_view ) {
            return nullptr;
        }
        
        auto iterator = Py_OWN(PyObject_GetIter(items_view));
        if (!iterator) {
            return nullptr;
        }
        bool first = true;
        ObjectSharedPtr item;
        Py_FOR(item, iterator) {
            if(!first){
                str << ", ";
            } else {
                first = false;
            }
            // item is a tuple of (key, value)
            // Borrowed references. no need to Py_OWN
            auto key = PyTuple_GetItem(*item, 0);
            auto value = PyTuple_GetItem(*item, 1);
            if (!key || !value) {
                return nullptr;
            }
            auto key_repr = Py_OWN(PyObject_Repr(key));
            auto value_repr = Py_OWN(PyObject_Repr(value));
            if (!key_repr || !value_repr) {
                return nullptr;
            }
            str << PyUnicode_AsUTF8(*key_repr) << ": " << PyUnicode_AsUTF8(*value_repr);
        } 
        str << "}";
        return PyUnicode_FromString(str.str().c_str());
    }

    PyObject *tryDictObject_rq(DictObject *dict_obj, PyObject *other, int op)
    {
        switch (op) {
            case Py_EQ: {
                
                // check sizes

                if(PyDict_Check(other)) {
                    if (dict_obj->ext().size() != (size_t)(PyDict_Size(other))) {
                        return PyBool_fromBool(false);
                    }
                } else if (DictObject_Check(other)) {
                    DictObject * other_list = (DictObject*) other;
                    if (dict_obj->ext().size() != other_list->ext().size()) {
                        return PyBool_fromBool(false);
                    }
                } else {
                    // false if types do not match
                    return PyBool_fromBool(false);
                }


                // Check all key-value pairs match
                auto iterator = Py_OWN(PyObject_GetIter(dict_obj));
                if (!iterator) {
                    return nullptr;
                }
                ObjectSharedPtr key;
                Py_FOR(key, iterator) {
                    auto our_value = Py_OWN(tryDictObject_GetItem(dict_obj, *key));
                    if (!our_value) {
                        return nullptr;
                    }
                    auto their_value = Py_OWN(PyDict_GetItem(other, *key));
                    if (!their_value) {
                        return PyBool_fromBool(false);
                    }
                    int cmp_result = PyObject_RichCompareBool(*our_value, *their_value, Py_EQ);
                    if (cmp_result == -1) {
                        return nullptr;
                    }
                    if (cmp_result != 1) {
                        return PyBool_fromBool(false);
                    }
                }
                return PyBool_fromBool(true);
            }
            case Py_NE: {
                auto eq_result = Py_OWN(tryDictObject_rq(dict_obj, other, Py_EQ));
                if (!eq_result) {
                    return nullptr;
                }
                return PyBool_fromBool(PyObject_IsTrue(*eq_result) == 0);
            }
            default:
                Py_RETURN_NOTIMPLEMENTED;
        }
    }

    PyObject *PyAPI_DictObject_rq(DictObject *dict_obj, PyObject *other, int op)
    {
        PY_API_FUNC
        return runSafe(tryDictObject_rq, dict_obj, other, op);
    }

    PyObject *PyAPI_DictObject_str(DictObject *self)
    {
        PY_API_FUNC
        return runSafe(tryDictObject_str, self);
    }

    static PySequenceMethods DictObject_seq = {
        .sq_contains = (objobjproc)PyAPI_DictObject_HasItem
    };
    
    static PyMappingMethods DictObject_mp = {
        .mp_length = (lenfunc)PyAPI_DictObject_len,
        .mp_subscript = (binaryfunc)PyAPI_DictObject_GetItem,
        .mp_ass_subscript = (objobjargproc)PyAPI_DictObject_SetItem
    };
    
    static PyMethodDef DictObject_methods[] = {
        {"clear", (PyCFunction)PyAPI_DictObject_clear, METH_NOARGS, "Clear all items from dict."},
        {"copy", (PyCFunction)PyAPI_DictObject_copy, METH_NOARGS, "Copy dict."},
        {"fromkeys", (PyCFunction)PyAPI_DictObject_fromKeys, METH_FASTCALL, "Make dict from keys."},
        {"get", (PyCFunction)PyAPI_DictObject_get, METH_FASTCALL, "Get element or return default value."},
        {"pop", (PyCFunction)PyAPI_DictObject_pop, METH_FASTCALL, "Pops element from dict."},
        {"setdefault", (PyCFunction)PyAPI_DictObject_setDefault, METH_FASTCALL, "If key is in the dictionary, return its value. If not, insert key with a value of default ."},
        {"update", (PyCFunction)PyAPI_DictObject_update, METH_VARARGS | METH_KEYWORDS, "Update dict values"},
        {"keys", (PyCFunction)PyAPI_DictObject_keys, METH_NOARGS, "Get keys dict view."},
        {"values", (PyCFunction)PyAPI_DictObject_values, METH_NOARGS, "Get values dict view."},
        {"items", (PyCFunction)PyAPI_DictObject_items, METH_NOARGS, "Get items dict view."},
        {NULL}
    };


    
    PyTypeObject DictObjectType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "Dict",
        .tp_basicsize = DictObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_DictObject_del,
        .tp_repr = (reprfunc)PyAPI_DictObject_str,
        .tp_as_sequence = &DictObject_seq,
        .tp_as_mapping = &DictObject_mp,
        .tp_str = (reprfunc)PyAPI_DictObject_str,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero dict collection object",
        .tp_richcompare = (richcmpfunc)PyAPI_DictObject_rq,
        .tp_iter = (getiterfunc)PyAPI_DictObject_iter,
        .tp_methods = DictObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)DictObject_new,
        .tp_free = PyObject_Free,
    };

    DictObject *DictObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<DictObject*>(type->tp_alloc(type, 0));
    }
    
    shared_py_object<DictObject*> DictDefaultObject_new() {
        return { DictObject_new(&DictObjectType, NULL, NULL), false };
    }
    
    PyObject *tryDictObject_update(DictObject *dict_object, PyObject* args, PyObject* kwargs)
    {
        auto arg_len = PyObject_Length(args);
        if (arg_len > 1) {
            PyErr_SetString(PyExc_TypeError, "dict expected at most 1 argument");
            return NULL;
        }
        
        if (PyObject_Length(args) == 1) {
            PyObject * arg1 = PyTuple_GetItem(args, 0);
            auto iterator = Py_OWN(PyObject_GetIter(arg1));
            if (!iterator) {
                PyErr_SetString(PyExc_TypeError, "argument must be a sequence or dict");
                return NULL;
            }

            ObjectSharedPtr elem;
            Py_FOR(elem, iterator) {            
                if (PyDict_Check(arg1)) {
                    tryDictObject_SetItem(dict_object, elem.get(), PyDict_GetItem(arg1, *elem));
                } else if (DictObject_Check(arg1)) {
                    tryDictObject_SetItem(dict_object, elem.get(), tryDictObject_GetItem((DictObject*)arg1, *elem));
                } else {
                    if (PyObject_Length(*elem) != 2) {
                        PyErr_SetString(PyExc_ValueError, "dictionary update sequence element #0 has length 1; 2 is required");
                        return NULL;
                    }
                    tryDictObject_SetItem(dict_object, PyTuple_GetItem(elem.get(), 0), PyTuple_GetItem(*elem, 1));
                }
            }
        }
        if (kwargs != NULL && PyObject_Length(kwargs) > 0) {
            auto iterator = Py_OWN(PyObject_GetIter(kwargs));
            if (!iterator) {
                PyErr_SetString(PyExc_TypeError, "argument must be a sequence or dict");
                return NULL;
            }
            ObjectSharedPtr elem;
            Py_FOR(elem, iterator) {            
                tryDictObject_SetItem(dict_object, *elem, *Py_BORROW(PyDict_GetItem(kwargs, *elem)));
            }
        }
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_DictObject_update(DictObject *dict_object, PyObject* args, PyObject* kwargs)
    {
        PY_API_FUNC
        return runSafe(tryDictObject_update, dict_object, args, kwargs);
    }
    
    shared_py_object<DictObject*> tryMake_DB0Dict(db0::swine_ptr<Fixture> &fixture, PyObject *args,
        PyObject *kwargs, AccessFlags access_mode)
    {        
        auto py_dict = DictDefaultObject_new();
        db0::FixtureLock lock(fixture);
        auto &dict = py_dict->makeNew(*lock, access_mode);
        
        // if args
        if (!tryDictObject_update(py_dict.get(), args, kwargs)) {            
            return nullptr;
        }

        // register newly created dict with py-object cache        
        fixture->getLangCache().add(dict.getAddress(), *py_dict);
        return py_dict;
    }

    shared_py_object<DictObject*> tryMake_DB0DictInternal(PyObject *args, PyObject *kwargs, AccessFlags access_mode)
    {
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return tryMake_DB0Dict(fixture, args, kwargs, access_mode);
    }
    
    DictObject *PyAPI_makeDict(PyObject *, PyObject* args, PyObject* kwargs)
    {
        PY_API_FUNC
        return runSafe(tryMake_DB0DictInternal, args, kwargs, AccessFlags {}).steal();
    }
    
    PyObject *tryDictObject_clear(DictObject *dict_obj)
    {
        dict_obj->modifyExt().clear();
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_DictObject_clear(DictObject *dict_obj)
    {
        PY_API_FUNC
        return runSafe(tryDictObject_clear, dict_obj);
    }

    PyObject *tryDictObject_copy(DictObject *py_src_dict)
    {
        auto py_dict = DictDefaultObject_new();
        auto lock = db0::FixtureLock(py_src_dict->ext().getFixture());
        py_src_dict->ext().copy(&py_dict.get()->modifyExt(), *lock);
        lock->getLangCache().add(py_dict.get()->ext().getAddress(), py_dict.get());
        return py_dict.steal();
    }

    PyObject *PyAPI_DictObject_copy(DictObject *py_src_dict)
    {
        // make actual dbzero instance, use default fixture
        PY_API_FUNC
        return runSafe(tryDictObject_copy, py_src_dict);
    }
    
    PyObject *tryDictObject_fromKeys(PyObject *const *args, Py_ssize_t nargs)
    {
        auto iterator = Py_OWN(PyObject_GetIter(args[0]));
        if (!iterator) {
            PyErr_SetString(PyExc_TypeError, "argument must be a sequence or dict");
            return nullptr;
        }

        // make actual dbzero instance, use default fixture
        auto py_dict = DictDefaultObject_new();
        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        auto &dict = py_dict->makeNew(*lock);
        
        ObjectSharedPtr elem;
        auto value = Py_BORROW(Py_None);
        if (nargs == 2) {
            value = args[1];
        }
        Py_FOR(elem, iterator) {     
            tryDictObject_SetItem(*py_dict, *elem, *value);
        }
        
        lock->getLangCache().add(dict.getAddress(), py_dict.get());
        return py_dict.steal();
    }
    
    PyObject *PyAPI_DictObject_fromKeys(DictObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs < 1) {
            PyErr_SetString(PyExc_TypeError, " fromkeys expected at least 1 argument");
            return NULL;
            
        }
        if (nargs > 2) {
            PyErr_SetString(PyExc_TypeError, "fromkeys expected at most 2 arguments");
            return NULL;            
        }
        return runSafe(tryDictObject_fromKeys, args, nargs);
    }
    
    PyObject *tryDictObject_get(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs)
    {
        PyObject *py_elem = args[0];
        auto elem = migratedKey(dict_object->ext(), py_elem);
        auto fixture = dict_object->ext().getFixture();
        auto maybe_hash = getPyHashIfExists(fixture, elem.get());
        if (maybe_hash) {
            if (dict_object->ext().hasItem(maybe_hash->first, *elem)) {
                return tryDictObject_GetItem(dict_object, *elem);
            }
        }
        auto value = Py_BORROW((PyObject*)Py_None);
        if (nargs == 2) {
            value = Py_BORROW((PyObject*)args[1]);
        }
        return value.steal();
    }
    
    PyObject *PyAPI_DictObject_get(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC        
        if (nargs < 1) {
            PyErr_SetString(PyExc_TypeError, " get expected at least 1 argument");
            return NULL;
            
        }
        if (nargs > 2) {
            PyErr_SetString(PyExc_TypeError, "fromkeys expected at most 2 arguments");
            return NULL;            
        }
        return runSafe(tryDictObject_get, dict_object, args, nargs);
    }
    
    PyObject *tryDictObject_pop(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs)
    {
        PyObject *py_elem = args[0];
        PyObject *value = nullptr;
        if (nargs == 2) {
            value = args[1];
        }
        
        auto elem = migratedKey(dict_object->ext(), py_elem);
        auto fixture = dict_object->ext().getFixture();
        auto maybe_hash = getPyHashIfExists(fixture, *elem);
        if (maybe_hash) {
            if (dict_object->ext().hasItem(maybe_hash->first, *elem)) {
                return dict_object->modifyExt().pop(maybe_hash->first, *elem).steal();
            }
        }
        
        if (value == nullptr) {
            PyErr_SetString(PyExc_KeyError, "not found");
            return NULL;
        }
        
        return value;        
    }
    
    PyObject *PyAPI_DictObject_pop(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_API_FUNC        
        if (nargs < 1) {
            PyErr_SetString(PyExc_TypeError, " get expected at least 1 argument");
            return NULL;            
        }
        if (nargs > 2) {
            PyErr_SetString(PyExc_TypeError, "fromkeys expected at most 2 arguments");
            return NULL;            
        }
        return runSafe(tryDictObject_pop, dict_object, args, nargs);
    }

    PyObject *tryDictObject_setDefault(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs)
    {
        PyObject *py_elem = args[0];
        auto value = Py_BORROW((PyObject*)Py_None);
        if (nargs == 2) {
            value = Py_BORROW((PyObject*)args[1]);
        }
        auto elem = migratedKey(dict_object->ext(), py_elem);
        auto fixture = dict_object->ext().getFixture();
        auto hash = getPyHash(fixture, *elem);
        if (!dict_object->ext().hasItem(hash, *elem)) {
            tryDictObject_SetItem(dict_object, *elem, *value);
        }
        return tryDictObject_GetItem(dict_object, *elem);
    }
    
    PyObject *PyAPI_DictObject_setDefault(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_API_FUNC        
        if (nargs < 1 ) {
            PyErr_SetString(PyExc_TypeError, "setdefault expected at least 1 argument");
            return NULL;            
        }
        if (nargs > 2) {
            PyErr_SetString(PyExc_TypeError, "setdefault expected at most 2 arguments");
            return NULL;            
        }
        return runSafe(tryDictObject_setDefault, dict_object, args, nargs);
    }
    
    PyObject *tryDictObject_keys(DictObject *dict_obj) {
        return makeDictView(dict_obj, &dict_obj->ext(), db0::object_model::IteratorType::KEYS);        
    }

    PyObject *PyAPI_DictObject_keys(DictObject *dict_obj)
    {
        PY_API_FUNC        
        return runSafe(tryDictObject_keys, dict_obj);    
    }

    PyObject *tryDictObject_values(DictObject *dict_obj) {
        return makeDictView(dict_obj, &dict_obj->ext(), db0::object_model::IteratorType::VALUES);        
    }

    PyObject *PyAPI_DictObject_values(DictObject *dict_obj)
    {
        PY_API_FUNC
        return runSafe(tryDictObject_values, dict_obj);    
    }

    bool DictObject_Check(PyObject *object) {
        return Py_TYPE(object) == &DictObjectType;        
    }
    
    PyObject *tryLoadDict(PyObject *py_dict, PyObject *kwargs, std::unordered_set<const void*> *load_stack_ptr)
    {   
        auto iterator = Py_OWN(PyObject_GetIter(py_dict));
        if (!iterator) {
            PyErr_SetString(PyExc_TypeError, "argument must be a sequence or dict");
            return NULL;
        }
        
        ObjectSharedPtr elem;
        auto py_result = Py_OWN(PyDict_New());
        Py_FOR(elem, iterator) {        
            auto key = Py_OWN(tryLoad(*elem, kwargs, nullptr, load_stack_ptr));
            if (!key) {
                return nullptr;
            }
            if (PyDict_Check(py_dict)) {
                auto result = Py_OWN(tryLoad(
                    PyDict_GetItem(py_dict, *elem), kwargs, nullptr, load_stack_ptr)
                );                
                if (!result) {
                    return nullptr;                                    
                }
                
                PySafeDict_SetItem(*py_result, key, result);
            } else if (DictObject_Check(py_dict)) {
                auto result = Py_OWN(tryLoad(
                    tryDictObject_GetItem((DictObject*)py_dict, *elem), kwargs, nullptr, load_stack_ptr)
                );
                if (!result) {
                    return nullptr;
                }
                PySafeDict_SetItem(*py_result, key, result);
            } else {
                THROWF(db0::InputException) << "Invalid argument type";                
            }
        }
        
        return py_result.steal();
    }
    
}