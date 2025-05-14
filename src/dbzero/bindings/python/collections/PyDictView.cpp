#include "PyDictView.hpp"
#include "PyDict.hpp"
#include "PyIterator.hpp"
#include <dbzero/bindings/python/Utils.hpp>
#include <dbzero/object_model/dict/DictIterator.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>

namespace db0::python

{
        
    DictIteratorObject *DictViewObject_iter(DictViewObject *self)
    {
        PY_API_FUNC
        auto iter = self->ext().getIterator();
        auto py_iter = IteratorObject_new<DictIteratorObject>(&DictIteratorObjectType, NULL, NULL);
        py_iter->makeNew(iter);
        return py_iter;
    }
    
    Py_ssize_t DictViewObject_len(DictViewObject *dict_obj) 
    {
        PY_API_FUNC        
        return dict_obj->ext().size();
    }

    static PyMappingMethods DictViewObject_mp = {
        .mp_length = (lenfunc)DictViewObject_len,
    };
    
    static PyMethodDef DictViewObject_methods[] = {
        {NULL}
    };

    PyTypeObject DictViewObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "Dict",
        .tp_basicsize = DictViewObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)DictViewObject_del,
        .tp_as_mapping = &DictViewObject_mp,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero dict collection object",
        .tp_iter = (getiterfunc)DictViewObject_iter,
        .tp_methods = DictViewObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)DictViewObject_new,
        .tp_free = PyObject_Free,        
    };

    DictViewObject *DictViewObject_newInternal(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<DictViewObject*>(type->tp_alloc(type, 0));
    }

    DictViewObject *DictViewObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return DictViewObject_newInternal(type, NULL, NULL);
    }
    
    DictViewObject *DictViewDefaultObject_new() {
        return DictViewObject_new(&DictViewObjectType, NULL, NULL);
    }
    
    void DictViewObject_del(DictViewObject* dict_obj)
    {
        PY_API_FUNC
        // destroy associated DB0 Dict instance
        dict_obj->ext().~DictView();
        Py_TYPE(dict_obj)->tp_free((PyObject*)dict_obj);
    }
    
    bool DictViewObject_Check(PyObject *object) {
        return Py_TYPE(object) == &DictViewObjectType;        
    }
    
    DictViewObject *makeDictView(PyObject *py_dict, const db0::object_model::Dict *ptr,
        db0::object_model::IteratorType iterator_type)
    {
        // make actual dbzero instance, use default fixture
        auto dict_view_object = DictViewObject_newInternal(&DictViewObjectType, NULL, NULL);
        db0::object_model::DictView::makeNew(&dict_view_object->modifyExt(), ptr, py_dict, iterator_type);
        return dict_view_object;
    }
    
}