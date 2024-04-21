#pragma once
#include <dbzero/object_model/dict/DictIterator.hpp>

namespace db0::python 
{
    

    using DictViewObject = PyWrapper<db0::object_model::DictView>;
    
    DictViewObject *DictViewObject_new(PyTypeObject *type, PyObject *, PyObject *);
    DictViewObject *DictViewDefaultObject_new();
    void DictViewObject_del(DictViewObject* self);
    Py_ssize_t DictViewObject_len(DictViewObject *);
    extern PyTypeObject DictViewObjectType;
    
    bool DictViewObject_Check(PyObject *);

    void DictViewObject_del(DictViewObject* dict_obj);
    
    DictViewObject *makeDictView(db0::object_model::Dict * ptr, db0::object_model::IteratorType iterator_type);

}