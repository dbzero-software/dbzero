#include "TupleIterator.hpp"
#include <dbzero/object_model/tags/TypedObjectIterator.hpp>
#include "Memo.hpp"
#include "Tuple.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python

{

    using TypedObjectIterator = db0::object_model::TypedObjectIterator;

    static PyMethodDef TupleIteratorObject_methods[] = {
        {NULL}
    };
    
    TupleIteratorObject *TupleIteratorObject_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<TupleIteratorObject*>(type->tp_alloc(type, 0));
    }
    
    void TupleIteratorObject_del(TupleIteratorObject* self)
    {
        // destroy associated DB0 instance
        self->ext().~TupleIterator();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    TupleIteratorObject *TupleIteratorObject_iter(TupleIteratorObject *self)
    {
        Py_INCREF(self);
        return self;        
    }
    
    PyObject *TupleIteratorObject_iternext(TupleIteratorObject *iter_obj)
    {
        if (!iter_obj->ext().is_end()) {
            return iter_obj->ext().next().steal();
        } else {
            // raise stop iteration
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } 
    }
    
    PyTypeObject TupleIteratorObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.TypedObjectIterator",
        .tp_basicsize = TupleIteratorObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)TupleIteratorObject_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero typed query object iterator",
        .tp_iter = (getiterfunc)TupleIteratorObject_iter,
        .tp_iternext = (iternextfunc)TupleIteratorObject_iternext,
        .tp_methods = TupleIteratorObject_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)TupleIteratorObject_new,
        .tp_free = PyObject_Free,
    };

    TupleIteratorObject * makeIterator(db0::object_model::Tuple::iterator iterator, db0::object_model::Tuple * ptr){
        // make actual DBZero instance, use default fixture
        auto tuple_iterator_object = TupleIteratorObject_new(&TupleIteratorObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        db0::object_model::TupleIterator::makeNew(&tuple_iterator_object->ext(), iterator, ptr);
        return tuple_iterator_object;
    }

}