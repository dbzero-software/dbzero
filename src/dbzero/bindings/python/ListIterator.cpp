#include "ListIterator.hpp"
#include <dbzero/object_model/tags/TypedObjectIterator.hpp>
#include "Memo.hpp"
#include "List.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python

{

    using TypedObjectIterator = db0::object_model::TypedObjectIterator;

    static PyMethodDef ListIteratorObject_methods[] = {
        {NULL}
    };
    
    ListIteratorObject *ListIteratorObject_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<ListIteratorObject*>(type->tp_alloc(type, 0));
    }
    
    void ListIteratorObject_del(ListIteratorObject* self)
    {
        // destroy associated DB0 instance
        self->ext().~ListIterator();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    ListIteratorObject *ListIteratorObject_iter(ListIteratorObject *self)
    {
        Py_INCREF(self);
        return self;        
    }
    
    PyObject *ListIteratorObject_iternext(ListIteratorObject *iter_obj)
    {
        if (!iter_obj->ext().is_end()) {
            return iter_obj->ext().next().steal();
        } else {
            // raise stop iteration
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } 
    }
    
    PyTypeObject ListIteratorObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.TypedObjectIterator",
        .tp_basicsize = ListIteratorObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)ListIteratorObject_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero typed query object iterator",
        .tp_iter = (getiterfunc)ListIteratorObject_iter,
        .tp_iternext = (iternextfunc)ListIteratorObject_iternext,
        .tp_methods = ListIteratorObject_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)ListIteratorObject_new,
        .tp_free = PyObject_Free,
    };

    ListIteratorObject * makeIterator(db0::object_model::List::iterator iterator, db0::object_model::List * ptr){
        // make actual DBZero instance, use default fixture
        auto list_iterator_object = ListIteratorObject_new(&ListIteratorObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        db0::object_model::ListIterator::makeNew(&list_iterator_object->ext(), iterator, ptr);
        return list_iterator_object;
    }

}