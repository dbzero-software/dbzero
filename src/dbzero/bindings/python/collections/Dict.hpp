#pragma once
#include <dbzero/object_model/dict/Dict.hpp>
namespace db0::python 
{
    
    using DictObject = PyWrapper<db0::object_model::Dict>;


    DictObject *DictObject_new(PyTypeObject *type, PyObject *, PyObject *);
    DictObject *DictDefaultObject_new();
    void DictObject_del(DictObject* self);
    Py_ssize_t DictObject_len(DictObject *);
    PyObject *DictObject_GetItem(DictObject *dict_obj, PyObject *key);
    int DictObject_SetItem(DictObject *dict_obj, PyObject *key, PyObject *value);
    PyObject *DictObject_clear(DictObject *set_obj);
    PyObject *DictObject_copy(DictObject *set_obj);
    PyObject *DictObject_fromKeys(DictObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *DictObject_get(DictObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *DictObject_pop(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs);
    PyObject *DictObject_setDefault(DictObject *dict_object, PyObject *const *args, Py_ssize_t nargs);
    PyObject *DictObject_update(DictObject *, PyObject* args, PyObject* kwargs);
    PyObject *DictObject_keys(DictObject *dict_obj);
    PyObject *DictObject_values(DictObject *dict_obj);
    PyObject *DictObject_items(DictObject *dict_obj);
    extern PyTypeObject DictObjectType;
    
    DictObject *makeDB0Dict(db0::swine_ptr<Fixture> &, PyObject *args, PyObject *kwargs);
    DictObject *makeDict(PyObject *, PyObject*, PyObject*);
    bool DictObject_Check(PyObject *);
    
    void DictObject_del(DictObject* dict_obj);
    extern PyTypeObject DictIteratorObjectType;
}