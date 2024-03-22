#include "DictIterator.hpp"
#include <dbzero/object_model/tags/TypedObjectIterator.hpp>
#include "Memo.hpp"
#include "Dict.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python

{

    using TypedObjectIterator = db0::object_model::TypedObjectIterator;

    static PyMethodDef DictIteratorObject_methods[] = {
        {NULL}
    };
    
    DictIteratorObject *DictIteratorObject_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<DictIteratorObject*>(type->tp_alloc(type, 0));
    }
    
    void DictIteratorObject_del(DictIteratorObject* self)
    {
        // destroy associated DB0 instance
        self->ext().~DictIterator();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    DictIteratorObject *DictIteratorObject_iter(DictIteratorObject *self)
    {
        Py_INCREF(self);
        return self;        
    }
    
    PyObject *DictIteratorObject_iternext(DictIteratorObject *iter_obj)
    {
        if (iter_obj->ext() != iter_obj->ext().end()) {
            return iter_obj->ext().next().steal();
        } else {
            // raise stop iteration
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } 
    }
    
    PyTypeObject DictIteratorObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.TypedObjectIterator",
        .tp_basicsize = DictIteratorObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)DictIteratorObject_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero typed query object iterator",
        .tp_iter = (getiterfunc)DictIteratorObject_iter,
        .tp_iternext = (iternextfunc)DictIteratorObject_iternext,
        .tp_methods = DictIteratorObject_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)DictIteratorObject_new,
        .tp_free = PyObject_Free,
    };

    DictIteratorObject * makeIterator(db0::object_model::Dict::iterator iterator, db0::object_model::Dict * ptr){
        // make actual DBZero instance, use default fixture
        auto dict_iterator_object = DictIteratorObject_new(&DictIteratorObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        db0::object_model::DictIterator::makeNew(&dict_iterator_object->ext(), iterator, ptr);
        return dict_iterator_object;
    }

}