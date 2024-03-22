#include "SetIterator.hpp"
#include <dbzero/object_model/tags/TypedObjectIterator.hpp>
#include "Memo.hpp"
#include "Set.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python

{

    using TypedObjectIterator = db0::object_model::TypedObjectIterator;

    static PyMethodDef SetIteratorObject_methods[] = {
        {NULL}
    };
    
    SetIteratorObject *SetIteratorObject_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<SetIteratorObject*>(type->tp_alloc(type, 0));
    }
    
    void SetIteratorObject_del(SetIteratorObject* self)
    {
        // destroy associated DB0 instance
        self->ext().~SetIterator();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    SetIteratorObject *SetIteratorObject_iter(SetIteratorObject *self)
    {
        Py_INCREF(self);
        return self;        
    }
    
    PyObject *SetIteratorObject_iternext(SetIteratorObject *iter_obj)
    {
        if (iter_obj->ext() != iter_obj->ext().end()) {
            return iter_obj->ext().next().steal();
        } else {
            // raise stop iteration
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } 
    }
    
    PyTypeObject SetIteratorObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.TypedObjectIterator",
        .tp_basicsize = SetIteratorObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)SetIteratorObject_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero typed query object iterator",
        .tp_iter = (getiterfunc)SetIteratorObject_iter,
        .tp_iternext = (iternextfunc)SetIteratorObject_iternext,
        .tp_methods = SetIteratorObject_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)SetIteratorObject_new,
        .tp_free = PyObject_Free,
    };

    SetIteratorObject * makeIterator(db0::object_model::Set::iterator iterator, db0::object_model::Set * ptr){
        // make actual DBZero instance, use default fixture
        auto set_iterator_object = SetIteratorObject_new(&SetIteratorObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        db0::object_model::SetIterator::makeNew(&set_iterator_object->ext(), iterator, ptr);
        return set_iterator_object;
    }

}