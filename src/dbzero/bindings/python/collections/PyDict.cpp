#include <dbzero/bindings/python/collections/PyDict.hpp>
#include "DictView.hpp"
#include <dbzero/bindings/python/Utils.hpp>
#include "PyIterator.hpp"
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/GlobalMutex.hpp>

namespace db0::python

{

    using DictIteratorObject = PyWrapper<db0::object_model::DictIterator, false>;

    PyTypeObject DictIteratorObjectType = GetIteratorType<DictIteratorObject>("dbzero_ce.DictIterator",
                                                                              "DBZero dict iterator");
    DictIteratorObject *DictObject_iter(DictObject *self)
    {
        self->ext().getFixture()->refreshIfUpdated();
        return makeIterator<DictIteratorObject,db0::object_model::DictIterator>(DictIteratorObjectType, 
            self->ext().begin(), &self->ext());
    }
    
    PyObject *DictObject_GetItemInternal(DictObject *dict_obj, PyObject *key)
    {
        dict_obj->ext().getFixture()->refreshIfUpdated();
        auto hash = PyObject_Hash(key);
        return dict_obj->ext().getItem(hash, key).steal();
    }

    PyObject *DictObject_GetItem(DictObject *dict_obj, PyObject *key)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        return DictObject_GetItemInternal(dict_obj, key);
    }
    
    int DictObject_SetItemInternal(DictObject *dict_obj, PyObject *key, PyObject *value)
    {
        auto hash = PyObject_Hash(key);
        dict_obj->modifyExt().setItem(hash, key, value);
        return 0;
    }

    int DictObject_SetItem(DictObject *dict_obj, PyObject *key, PyObject *value)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        return DictObject_SetItemInternal(dict_obj, key, value);
    }

    Py_ssize_t DictObject_len(DictObject *dict_obj)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        dict_obj->ext().getFixture()->refreshIfUpdated();
        return dict_obj->ext().size();
    }
    
    int DictObject_HasItem(DictObject *dict_obj, PyObject *key)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        dict_obj->ext().getFixture()->refreshIfUpdated();
        return dict_obj->ext().has_item(key);
    }

    void DictObject_del(DictObject* dict_obj)
    {
        // destroy associated DB0 Dict instance
        dict_obj->ext().~Dict();
        Py_TYPE(dict_obj)->tp_free((PyObject*)dict_obj);
    }

    void DictObject_del_locked(DictObject* dict_obj)
    {
        // destroy associated DB0 Dict instance
                //std::lock_guard pbm_lock(python_bindings_mutex);
        return DictObject_del(dict_obj);
    }

    static PySequenceMethods DictObject_seq = {
        .sq_contains = (objobjproc)DictObject_HasItem
    };

    static PyMappingMethods DictObject_mp = {
        .mp_length = (lenfunc)DictObject_len,
        .mp_subscript = (binaryfunc)DictObject_GetItem,
        .mp_ass_subscript = (objobjargproc)DictObject_SetItem
    };
    
    static PyMethodDef DictObject_methods[] = {
        {"clear", (PyCFunction)DictObject_clear, METH_NOARGS, "Clear all items from dict."},
        {"copy", (PyCFunction)DictObject_copy, METH_NOARGS, "Copy dict."},
        {"fromkeys", (PyCFunction)DictObject_fromKeys, METH_FASTCALL, "Make dict from keys."},
        {"get", (PyCFunction)DictObject_get, METH_FASTCALL, "Get element or return default value."},
        {"pop", (PyCFunction)DictObject_pop, METH_FASTCALL, "Pops element from dict."},
        {"setdefault", (PyCFunction)DictObject_setDefault, METH_FASTCALL, "If key is in the dictionary, return its value. If not, insert key with a value of default ."},
        {"update", (PyCFunction)DictObject_update, METH_VARARGS | METH_KEYWORDS, "Update dict values"},
        {"keys", (PyCFunction)DictObject_keys, METH_NOARGS, "Get keys dict view."},
        {"values", (PyCFunction)DictObject_values, METH_NOARGS, "Get values dict view."},
        {"items", (PyCFunction)DictObject_items, METH_NOARGS, "Get items dict view."},
        {NULL}
    };

    PyTypeObject DictObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Dict",        
        .tp_basicsize = DictObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)DictObject_del_locked,
        .tp_as_sequence = &DictObject_seq,
        .tp_as_mapping = &DictObject_mp,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero dict collection object",
        .tp_iter = (getiterfunc)DictObject_iter,
        .tp_methods = DictObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)DictObject_new,
        .tp_free = PyObject_Free,
    };

    DictObject *DictObject_newInternal(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<DictObject*>(type->tp_alloc(type, 0));        
    }

    DictObject *DictObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        std::lock_guard pbm_lock(python_bindings_mutex);
        return DictObject_newInternal(type, NULL, NULL);     
    }
    
    DictObject *DictDefaultObject_new() {   
        std::lock_guard pbm_lock(python_bindings_mutex);
        return DictObject_newInternal(&DictObjectType, NULL, NULL);
    }
    
    PyObject *DictObject_updateInternal(DictObject *dict_object, PyObject* args, PyObject* kwargs) 
    {
        auto arg_len = PyObject_Length(args);
        if(arg_len > 1){
            PyErr_SetString(PyExc_TypeError, "dict expected at most 1 argument");
            return NULL;
        }
        if(PyObject_Length(args) == 1) {
            PyObject * arg1 = PyTuple_GetItem(args, 0);
            PyObject *iterator = PyObject_GetIter(arg1);
            PyObject *elem;
            while ((elem = PyIter_Next(iterator))) {
                if(PyDict_Check(arg1)) {
                    DictObject_SetItemInternal(dict_object, elem, PyDict_GetItem(arg1, elem));
                } else if(DictObject_Check(arg1)) {
                    DictObject_SetItemInternal(dict_object, elem, DictObject_GetItemInternal((DictObject*)arg1, elem));
                } else {
                    if(PyObject_Length(elem) != 2){
                        PyErr_SetString(PyExc_ValueError, "dictionary update sequence element #0 has length 1; 2 is required");
                        return NULL;
                    }
                    DictObject_SetItemInternal(dict_object, PyTuple_GetItem(elem,0), PyTuple_GetItem(elem,1));
                }
                Py_DECREF(elem);
            }
            Py_DECREF(iterator);
        }
        if(kwargs != NULL && PyObject_Length(kwargs) > 0) {

            PyObject *iterator = PyObject_GetIter(kwargs);
            PyObject *elem;

            while ((elem = PyIter_Next(iterator))) {
                DictObject_SetItemInternal(dict_object, elem, PyDict_GetItem(kwargs, elem));
                Py_DECREF(elem);
            }
            Py_DECREF(iterator);
        }
        Py_RETURN_NONE;
    }

    PyObject *DictObject_update(DictObject *dict_object, PyObject* args, PyObject* kwargs) 
    {
        return DictObject_updateInternal(dict_object, args, kwargs);
    }

    DictObject *makeDB0Dict(db0::swine_ptr<Fixture> &fixture, PyObject *args, PyObject *kwargs)
    {
        auto py_dict = DictObject_newInternal(&DictObjectType, NULL, NULL);
        db0::FixtureLock lock(fixture);
        auto &dict = py_dict->modifyExt();
        db0::object_model::Dict::makeNew(&dict, *lock);
        
        // if args
        DictObject_updateInternal(py_dict, args, kwargs);
        // register newly created dict with py-object cache        
        fixture->getLangCache().add(dict.getAddress(), py_dict);
        return py_dict;
    }

    DictObject *makeDict(PyObject *, PyObject* args, PyObject* kwargs)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return makeDB0Dict(fixture, args, kwargs);
    }
    
    PyObject *DictObject_clear(DictObject *dict_obj)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        dict_obj->modifyExt().clear();
        Py_RETURN_NONE;
    }

    PyObject *DictObject_copy(DictObject *py_src_dict)
    {
        // make actual DBZero instance, use default fixture
        std::lock_guard pbm_lock(python_bindings_mutex);
        auto py_dict = DictObject_newInternal(&DictObjectType, NULL, NULL);
        auto lock = db0::FixtureLock(py_src_dict->ext().getFixture());
        py_src_dict->ext().copy(&py_dict->modifyExt(), *lock);
        lock->getLangCache().add(py_dict->ext().getAddress(), py_dict);
        return py_dict;
    }
    
    PyObject *DictObject_fromKeys(DictObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        if (nargs < 1) {
            PyErr_SetString(PyExc_TypeError, " fromkeys expected at least 1 argument");
            return NULL;
            
        }
        if(nargs > 2){
            PyErr_SetString(PyExc_TypeError, "fromkeys expected at most 2 arguments");
            return NULL;
            
        }
        // make actual DBZero instance, use default fixture
        auto py_dict = DictObject_newInternal(&DictObjectType, NULL, NULL);
        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        db0::object_model::Dict::makeNew(&py_dict->modifyExt(), *lock);        
        PyObject *iterator = PyObject_GetIter(args[0]);
        PyObject *elem;
        PyObject *value = Py_None;
        if (nargs == 2) {
            value = args[1];
        }
        while ((elem = PyIter_Next(iterator))) {
            DictObject_SetItemInternal(py_dict, elem, value);
        }

        lock->getLangCache().add(py_dict->ext().getAddress(), py_dict);
        return py_dict;
    }

    PyObject *DictObject_get(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        if (nargs < 1) {
            PyErr_SetString(PyExc_TypeError, " get expected at least 1 argument");
            return NULL;
            
        }
        if (nargs > 2) {
            PyErr_SetString(PyExc_TypeError, "fromkeys expected at most 2 arguments");
            return NULL;
            
        }
        PyObject *elem = args[0];
        PyObject *value = Py_None;
        if(nargs == 2){
            value = args[1];
        }
        if (dict_object->ext().has_item(elem)) {
            return DictObject_GetItemInternal(dict_object, elem);
        }
        return value;
    }

    PyObject *DictObject_pop(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs) 
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        if(nargs < 1 ){
            PyErr_SetString(PyExc_TypeError, " get expected at least 1 argument");
            return NULL;
            
        }
        if(nargs > 2){
            PyErr_SetString(PyExc_TypeError, "fromkeys expected at most 2 arguments");
            return NULL;
            
        }
        PyObject *elem = args[0];
        PyObject *value = nullptr;
        if(nargs == 2){
            value = args[1];
        }
        if(dict_object->ext().has_item(elem)) {
            auto obj = dict_object->modifyExt().pop(elem);
            return obj.steal();
        }
        if(value == nullptr){
            PyErr_SetString(PyExc_KeyError, "item");
            return NULL;
        }
        
        return value;
    }

    PyObject *DictObject_setDefault(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs) 
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        if(nargs < 1 ){
            PyErr_SetString(PyExc_TypeError, "setdefault expected at least 1 argument");
            return NULL;
            
        }
        if(nargs > 2){
            PyErr_SetString(PyExc_TypeError, "setdefault expected at most 2 arguments");
            return NULL;
            
        }
        PyObject *elem = args[0];
        PyObject *value = Py_None;
        if(nargs == 2){
            value = args[1];
        }
        if(!dict_object->ext().has_item(elem)) {
            DictObject_SetItemInternal(dict_object, elem, value);
        }
        return DictObject_GetItemInternal(dict_object, elem);
    }

    PyObject *DictObject_keys(DictObject *dict_obj)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        // make actual DBZero instance, use default fixture
        auto dict_view_object = makeDictView(&dict_obj->ext(), db0::object_model::IteratorType::KEYS);
        return dict_view_object;
    }

    PyObject *DictObject_values(DictObject *dict_obj)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        // make actual DBZero instance, use default fixture
        auto dict_view_object = makeDictView(&dict_obj->ext(), db0::object_model::IteratorType::VALUES);
        return dict_view_object;
    }

    PyObject *DictObject_items(DictObject *dict_obj)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        // make actual DBZero instance, use default fixture
        auto dict_view_object = makeDictView(&dict_obj->ext(), db0::object_model::IteratorType::ITEMS);
        return dict_view_object;
    }

    bool DictObject_Check(PyObject *object)
    {   
        return Py_TYPE(object) == &DictObjectType;        
    }
    
}