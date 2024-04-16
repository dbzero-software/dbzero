#include "Dict.hpp"
#include "DictIterator.hpp"
#include "DictView.hpp"
#include "Utils.hpp"
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>


namespace db0::python

{

    DictIteratorObject *DictObject_iter(DictObject *self)
    {
        return makeIterator(self->ext().begin(), &self->ext());        
    }
    
    PyObject *DictObject_GetItem(DictObject *dict_obj, PyObject *key)
    {   
        auto hash = PyObject_Hash(key);
        return dict_obj->ext().getItem(hash).steal();
    }
    
    int DictObject_SetItem(DictObject *dict_obj, PyObject *key, PyObject *value)
    {
        auto hash = PyObject_Hash(key);
        dict_obj->ext().setItem(hash, key, value);
        return 0;
    }

    Py_ssize_t DictObject_len(DictObject *dict_obj)
    {
        dict_obj->ext().getFixture()->refreshIfUpdated();
        return dict_obj->ext().size();
    }

    int DictObject_HasItem(DictObject *dict_obj, PyObject *key)
    {
        return dict_obj->ext().has_item(key);
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
        .tp_dealloc = (destructor)DictObject_del,
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

    DictObject *DictObject_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<DictObject*>(type->tp_alloc(type, 0));
    }

    DictObject *DictDefaultObject_new()
    {   
        return DictObject_new(&DictObjectType, NULL, NULL);
    }
    
    void DictObject_del(DictObject* dict_obj)
    {
        // destroy associated DB0 Dict instance
        dict_obj->ext().~Dict();
        Py_TYPE(dict_obj)->tp_free((PyObject*)dict_obj);
    }
    
    PyObject *DictObject_update(DictObject *dict_object, PyObject* args, PyObject* kwargs) {
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
                    DictObject_SetItem(dict_object, elem, PyDict_GetItem(arg1, elem));
                } else if(DictObject_Check(arg1)) {
                    DictObject_SetItem(dict_object, elem, DictObject_GetItem((DictObject*)arg1, elem));
                } else {
                    if(PyObject_Length(elem) != 2){
                        PyErr_SetString(PyExc_ValueError, "dictionary update sequence element #0 has length 1; 2 is required");
                        return NULL;
                    }
                    DictObject_SetItem(dict_object, PyTuple_GetItem(elem,0), PyTuple_GetItem(elem,1));
                }
                Py_DECREF(elem);
            }
            Py_DECREF(iterator);
        }
        if(kwargs != NULL && PyObject_Length(kwargs) > 0) {

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

    DictObject *makeDict(PyObject *, PyObject* args, PyObject* kwargs)
    {
        // make actual DBZero instance, use default fixture
        auto dict_object = DictObject_new(&DictObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        db0::object_model::Dict::makeNew(&dict_object->ext(), *fixture);
        // if args
        DictObject_update(dict_object, args, kwargs);
        // register newly created dict with py-object cache
        (*fixture)->getLangCache().add(dict_object->ext().getAddress(), dict_object, true);
        return dict_object;
    }

    PyObject *DictObject_clear(DictObject *dict_obj){
        dict_obj->ext().clear();
        Py_RETURN_NONE;
    }

    PyObject *DictObject_copy(DictObject *dict_obj)
    {
        // make actual DBZero instance, use default fixture
        auto dict_object = DictObject_new(&DictObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        dict_obj->ext().copy(&dict_object->ext(), *fixture);
        (*fixture)->getLangCache().add(dict_object->ext().getAddress(), dict_object, true);
        return dict_object;
    }

    PyObject *DictObject_fromKeys(DictObject *, PyObject *const *args, Py_ssize_t nargs){
        if(nargs < 1 ){
            PyErr_SetString(PyExc_TypeError, " fromkeys expected at least 1 argument");
            return NULL;
            
        }
        if(nargs > 2){
            PyErr_SetString(PyExc_TypeError, "fromkeys expected at most 2 arguments");
            return NULL;
            
        }
        // make actual DBZero instance, use default fixture
        auto dict_object = DictObject_new(&DictObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        db0::object_model::Dict::makeNew(&dict_object->ext(), *fixture);
        (*fixture)->getLangCache().add(dict_object->ext().getAddress(), dict_object, true);
        PyObject *iterator = PyObject_GetIter(args[0]);
        PyObject *elem;
        PyObject *value = Py_None;
        if(nargs == 2){
            value = args[1];
        }
        while ((elem = PyIter_Next(iterator))) {
            DictObject_SetItem(dict_object, elem, value);
        }

        return dict_object;
    }

    PyObject *DictObject_get(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs){
        if(nargs < 1 ){
            PyErr_SetString(PyExc_TypeError, " get expected at least 1 argument");
            return NULL;
            
        }
        if(nargs > 2){
            PyErr_SetString(PyExc_TypeError, "fromkeys expected at most 2 arguments");
            return NULL;
            
        }
        PyObject *elem = args[0];
        PyObject *value = Py_None;
        if(nargs == 2){
            value = args[1];
        }
        if(dict_object->ext().has_item(elem)) {
            return DictObject_GetItem(dict_object, elem);
        }
        return value;
    }

    PyObject *DictObject_pop(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs) {
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
            auto obj = dict_object->ext().pop(elem);
            return obj.steal();
        }
        if(value == nullptr){
            PyErr_SetString(PyExc_KeyError, "item");
            return NULL;
        }
        
        return value;
    }

    PyObject *DictObject_setDefault(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs) {
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
            DictObject_SetItem(dict_object, elem, value);
        }
        return DictObject_GetItem(dict_object, elem);
    }

    PyObject *DictObject_keys(DictObject *dict_obj)
    {
        // make actual DBZero instance, use default fixture
        auto dict_view_object = makeDictView(&dict_obj->ext(), db0::object_model::IteratorType::KEYS);
        return dict_view_object;
    }

    PyObject *DictObject_values(DictObject *dict_obj)
    {
        // make actual DBZero instance, use default fixture
        auto dict_view_object = makeDictView(&dict_obj->ext(), db0::object_model::IteratorType::VALUES);
        return dict_view_object;
    }

    PyObject *DictObject_items(DictObject *dict_obj)
    {
        // make actual DBZero instance, use default fixture
        auto dict_view_object = makeDictView(&dict_obj->ext(), db0::object_model::IteratorType::ITEMS);
        return dict_view_object;
    }

    bool DictObject_Check(PyObject *object)
    {   
        return Py_TYPE(object) == &DictObjectType;        
    }

}