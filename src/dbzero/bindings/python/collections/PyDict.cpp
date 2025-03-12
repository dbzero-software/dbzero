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

namespace db0::python

{

    using DictIteratorObject = PyWrapper<db0::object_model::DictIterator, false>;

    PyTypeObject DictIteratorObjectType = GetIteratorType<DictIteratorObject>("dbzero_ce.DictIterator", "dbzero dict iterator");
    
    DictIteratorObject *PyAPI_DictObject_iter(DictObject *self)
    {
        PY_API_FUNC
        self->ext().getFixture()->refreshIfUpdated();
        return makeIterator<DictIteratorObject, db0::object_model::DictIterator>(
            DictIteratorObjectType, self->ext().begin(), &self->ext(), self
        );
    }
    
    PyObject *DictObject_GetItem(DictObject *py_dict, PyObject *py_key)
    {
        const auto &dict_obj = py_dict->ext();
        dict_obj.getFixture()->refreshIfUpdated();
        auto key = migratedKey(dict_obj, py_key);
        auto hash = get_py_hash(key.get());
        if (hash == -1) {
            auto py_str = PyObject_Str(py_key);
            auto str_name =  PyUnicode_AsUTF8(py_str);
            Py_DECREF(py_str);
            auto error_message = "Cannot get hash for key: " + std::string(str_name);
            PyErr_SetString(PyExc_KeyError, error_message.c_str());
            return NULL;
        }
        auto item_ptr = dict_obj.getItem(hash, key.get());
        if (item_ptr == nullptr) {
            auto py_str = PyObject_Str(py_key);
            auto str_name =  PyUnicode_AsUTF8(py_str);
            Py_DECREF(py_str);
            PyErr_SetString(PyExc_KeyError,str_name);
            return NULL;
        }
        return item_ptr.steal();
    }
    
    PyObject *PyAPI_DictObject_GetItem(DictObject *dict_obj, PyObject *key)
    {
        PY_API_FUNC
        return runSafe(DictObject_GetItem, dict_obj, key);
    }
    
    int DictObject_SetItem(DictObject *py_dict, PyObject *py_key, PyObject *value)
    {
        auto key = migratedKey(py_dict->ext(), py_key);
        auto hash = get_py_hash(key.get());
        if (hash == -1) {            
            // set PyError
            std::stringstream _str;
            _str << "Unable to find hash function for key of type: " << Py_TYPE(key.get())->tp_name;
            PyErr_SetString(PyExc_TypeError, _str.str().c_str());
            return -1;
        }
        
        db0::FixtureLock lock(py_dict->ext().getFixture());
        py_dict->modifyExt().setItem(lock, hash, key.get(), value);
        return 0;
    }
    
    int PyAPI_DictObject_SetItem(DictObject *dict_obj, PyObject *key, PyObject *value)
    {
        PY_API_FUNC
        return runSafe<-1>(DictObject_SetItem, dict_obj, key, value);
    }

    Py_ssize_t DictObject_len(DictObject *dict_obj)
    {        
        dict_obj->ext().getFixture()->refreshIfUpdated();
        return dict_obj->ext().size();
    }

    Py_ssize_t PyAPI_DictObject_len(DictObject *dict_obj)
    {
        PY_API_FUNC
        return runSafe(DictObject_len, dict_obj);
    }
    
    int DictObject_HasItem(DictObject *py_dict, PyObject *py_key)
    {        
        auto key = migratedKey(py_dict->ext(), py_key);
        py_dict->ext().getFixture()->refreshIfUpdated();
        auto hash = get_py_hash(key.get());
        return py_dict->ext().has_item(hash, key.get());
    }

    int PyAPI_DictObject_HasItem(DictObject *dict_obj, PyObject *key)
    {
        PY_API_FUNC
        return runSafe(DictObject_HasItem, dict_obj, key);
    }
    
    void PyAPI_DictObject_del(DictObject* dict_obj)
    {
        PY_API_FUNC
        // destroy associated DB0 Dict instance
        dict_obj->destroy();
        Py_TYPE(dict_obj)->tp_free((PyObject*)dict_obj);
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
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Dict",        
        .tp_basicsize = DictObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_DictObject_del,
        .tp_as_sequence = &DictObject_seq,
        .tp_as_mapping = &DictObject_mp,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero dict collection object",
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
    
    PyObject *DictObject_update(DictObject *dict_object, PyObject* args, PyObject* kwargs)
    {
        auto arg_len = PyObject_Length(args);
        if (arg_len > 1) {
            PyErr_SetString(PyExc_TypeError, "dict expected at most 1 argument");
            return NULL;
        }
        
        if (PyObject_Length(args) == 1) {
            PyObject * arg1 = PyTuple_GetItem(args, 0);
            PyObject *iterator = PyObject_GetIter(arg1);
            PyObject *elem;
            while ((elem = PyIter_Next(iterator))) {
                if (PyDict_Check(arg1)) {
                    DictObject_SetItem(dict_object, elem, PyDict_GetItem(arg1, elem));
                } else if (DictObject_Check(arg1)) {
                    DictObject_SetItem(dict_object, elem, DictObject_GetItem((DictObject*)arg1, elem));
                } else {
                    if (PyObject_Length(elem) != 2) {
                        PyErr_SetString(PyExc_ValueError, "dictionary update sequence element #0 has length 1; 2 is required");
                        return NULL;
                    }
                    DictObject_SetItem(dict_object, PyTuple_GetItem(elem,0), PyTuple_GetItem(elem,1));
                }                
                Py_DECREF(elem);
            }            
            Py_DECREF(iterator);
        }
        if (kwargs != NULL && PyObject_Length(kwargs) > 0) {
            PyObject *iterator = PyObject_GetIter(kwargs);
            PyObject *elem;
            while ((elem = PyIter_Next(iterator))) {
                DictObject_SetItem(dict_object, elem, PyDict_GetItem(kwargs, elem));
                Py_DECREF(elem);
            }
            Py_DECREF(iterator);
        }
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_DictObject_update(DictObject *dict_object, PyObject* args, PyObject* kwargs)
    {
        PY_API_FUNC
        return runSafe(DictObject_update, dict_object, args, kwargs);
    }
    
    shared_py_object<DictObject*> makeDB0Dict(db0::swine_ptr<Fixture> &fixture, PyObject *args, PyObject *kwargs)
    {
        auto py_dict = DictDefaultObject_new();
        db0::FixtureLock lock(fixture);
        auto &dict = py_dict.get()->modifyExt();
        db0::object_model::Dict::makeNew(&dict, *lock);
        
        // if args
        DictObject_update(py_dict.get(), args, kwargs);
        // register newly created dict with py-object cache        
        fixture->getLangCache().add(dict.getAddress(), py_dict.get());
        return py_dict;
    }
    
    DictObject *PyAPI_makeDict(PyObject *, PyObject* args, PyObject* kwargs)
    {
        PY_API_FUNC
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return runSafe(makeDB0Dict, fixture, args, kwargs).steal();
    }
    
    PyObject *DictObject_clear(DictObject *dict_obj)
    {
        dict_obj->modifyExt().clear();
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_DictObject_clear(DictObject *dict_obj)
    {
        PY_API_FUNC
        return runSafe(DictObject_clear, dict_obj);
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
        // make actual dbzero instance, use default fixture
        auto py_dict = DictDefaultObject_new();
        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        db0::object_model::Dict::makeNew(&py_dict.get()->modifyExt(), *lock);
        PyObject *iterator = PyObject_GetIter(args[0]);
        PyObject *elem;
        PyObject *value = Py_None;
        if (nargs == 2) {
            value = args[1];
        }
        while ((elem = PyIter_Next(iterator))) {
            DictObject_SetItem(py_dict.get(), elem, value);
        }

        lock->getLangCache().add(py_dict.get()->ext().getAddress(), py_dict.get());
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
        PyObject *value = Py_None;
        if (nargs == 2) {
            value = args[1];
        }
        auto elem = migratedKey(dict_object->ext(), py_elem);
        auto hash = get_py_hash(elem.get());
        if (dict_object->ext().has_item(hash, elem.get())) {
            return DictObject_GetItem(dict_object, elem.get());
        }
        return value;
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
        auto hash = get_py_hash(elem.get());
        if (dict_object->ext().has_item(hash, elem.get())) {
            auto obj = dict_object->modifyExt().pop(hash, elem.get());
            return obj.steal();
        }
        if (value == nullptr) {
            PyErr_SetString(PyExc_KeyError, "item");
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
        PyObject *value = Py_None;
        if(nargs == 2){
            value = args[1];
        }
        auto elem = migratedKey(dict_object->ext(), py_elem);
        auto hash = get_py_hash(elem.get());
        if (!dict_object->ext().has_item(hash, elem.get())) {
            DictObject_SetItem(dict_object, elem.get(), value);
        }
        return DictObject_GetItem(dict_object, elem.get());
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

    PyObject *tryDictObject_items(DictObject *dict_obj) {
        return makeDictView(dict_obj, &dict_obj->ext(), db0::object_model::IteratorType::ITEMS);        
    }
    
    PyObject *PyAPI_DictObject_items(DictObject *dict_obj)
    {
        PY_API_FUNC
        return runSafe(tryDictObject_items, dict_obj);    
    }
    
    bool DictObject_Check(PyObject *object) {
        return Py_TYPE(object) == &DictObjectType;        
    }
        
    PyObject *tryLoadDict(PyObject *py_dict, PyObject *kwargs)
    {   
        PyObject *iterator = PyObject_GetIter(py_dict);
        PyObject *elem;
        PyObject *py_result = PyDict_New();
        while ((elem = PyIter_Next(iterator))) {
            auto key = runSafe(tryLoad, elem, kwargs, nullptr);
            if(key == nullptr) {
                return nullptr;
            }
            if (PyDict_Check(py_dict)) {
                auto result = runSafe(tryLoad, PyDict_GetItem(py_dict, elem), kwargs, nullptr);
                if(result == nullptr) {
                    return nullptr;
                }
                
                PyDict_SetItem(py_result, key, result);
            } else if (DictObject_Check(py_dict)) {
                auto result = runSafe(tryLoad, PyAPI_DictObject_GetItem((DictObject*)py_dict, elem), kwargs, nullptr);
                if(result == nullptr) {
                    return nullptr;
                }
                PyDict_SetItem(py_result, key, result);
            } else {
                throw std::runtime_error("Unknown type");
            }     
            Py_DECREF(elem);
        }            
        Py_DECREF(iterator);
        return py_result;
    }

}