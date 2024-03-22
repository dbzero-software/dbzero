#include "PyObjectIterator.hpp"
#include "Memo.hpp"
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>

namespace db0::python

{

    using ObjectIterator = db0::object_model::ObjectIterator;

    PyObjectIterator *PyObjectIterator_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<PyObjectIterator*>(type->tp_alloc(type, 0));
    }
    
    void PyObjectIterator_del(PyObjectIterator* self)
    {
        // destroy associated DB0 instance
        self->ext().~ObjectIterator();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    PyTypedObjectIterator *PyTypedObjectIterator_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<PyTypedObjectIterator*>(type->tp_alloc(type, 0));
    }

    void PyTypedObjectIterator_del(PyTypedObjectIterator* self)
    {
        // destroy associated DB0 instance
        self->ext().~TypedObjectIterator();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }
    
    PyObjectIterator *PyObjectIterator_iter(PyObjectIterator *self)
    {
        Py_INCREF(self);
        return self;        
    }
    
    PyObject *PyObjectIterator_iternext(PyObjectIterator *iter_obj)
    {
        std::uint64_t addr;
        if (iter_obj->ext().next(addr)) {
            // retrieve DBZero instance
            return iter_obj->ext().unload(addr);
        } else {
            // raise stop iteration
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } 
    }
    
    static PyMethodDef PyObjectIterator_methods[] = {
        {NULL}
    };
    
    PyTypeObject PyObjectIteratorType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.ObjectIterator",
        .tp_basicsize = PyObjectIterator::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyObjectIterator_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero object iterator",
        .tp_iter = (getiterfunc)PyObjectIterator_iter,
        .tp_iternext = (iternextfunc)PyObjectIterator_iternext,
        .tp_methods = PyObjectIterator_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyObjectIterator_new,
        .tp_free = PyObject_Free,
    };

    PyTypedObjectIterator *PyTypedObjectIterator_iter(PyTypedObjectIterator *self)
    {
        Py_INCREF(self);
        return self;
    }
    
    PyObject *PyTypedObjectIterator_iternext(PyTypedObjectIterator *iter_obj)
    {
        std::uint64_t addr;
        if (iter_obj->ext().next(addr)) {
            // retrieve DBZero instance
            return iter_obj->ext().unload(addr);
        } else {
            // raise stop iteration
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } 
    }

    static PyMethodDef PyTypedObjectIterator_methods[] = {
        {NULL}
    };

    PyTypeObject PyTypedObjectIteratorType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.TypedObjectIterator",
        .tp_basicsize = PyTypedObjectIterator::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyTypedObjectIterator_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero typed object iterator",
        .tp_iter = (getiterfunc)PyTypedObjectIterator_iter,
        .tp_iternext = (iternextfunc)PyTypedObjectIterator_iternext,
        .tp_methods = PyTypedObjectIterator_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyTypedObjectIterator_new,
        .tp_free = PyObject_Free,
    };

    PyObject *find(PyObject *, PyObject* const *args, Py_ssize_t nargs)
    {   
        using TypedObjectIterator = db0::object_model::TypedObjectIterator;
        using TagIndex = db0::object_model::TagIndex;
        using Class = db0::object_model::Class;

        std::shared_ptr<Class> type;
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        auto &tag_index = fixture->get<TagIndex>();
        auto query_iterator = tag_index.find(args, nargs, type);
        if (type) {
            // construct as typed iterator when a type was specified
            auto iter_obj = PyTypedObjectIterator_new(&PyTypedObjectIteratorType, NULL, NULL);
            TypedObjectIterator::makeNew(&iter_obj->ext(), fixture, std::move(query_iterator), type);
            return iter_obj;
        } else {
            auto iter_obj = PyObjectIterator_new(&PyObjectIteratorType, NULL, NULL);
            ObjectIterator::makeNew(&iter_obj->ext(), fixture, std::move(query_iterator));
            return iter_obj;
        }
    }
    
    bool ObjectIterator_Check(PyObject *py_object)
    {
        return Py_TYPE(py_object) == &PyObjectIteratorType;
    }

}