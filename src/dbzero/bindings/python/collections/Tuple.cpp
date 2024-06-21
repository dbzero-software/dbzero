#include "Tuple.hpp"
#include "Iterator.hpp"
#include <dbzero/object_model/tuple/TupleIterator.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>

namespace db0::python

{
    
    using TupleIteratorObject = PyWrapper<db0::object_model::TupleIterator, false>;

    PyTypeObject TupleIteratorObjectType = GetIteratorType<TupleIteratorObject>("dbzero_ce.TupleIterator",
                                                                              "DBZero tuple iterator");

    TupleIteratorObject *TupleObject_iter(TupleObject *self)
    {
        return makeIterator<TupleIteratorObject,db0::object_model::TupleIterator>(TupleIteratorObjectType, 
            self->ext().begin(), &self->ext());        
    }

    PyObject *TupleObject_GetItem(TupleObject *tuple_obj, Py_ssize_t i)
    {
        if (static_cast<std::size_t>(i) >= tuple_obj->ext().getData()->size()) {
            PyErr_SetString(PyExc_IndexError, "tuple index out of range");
            return NULL;
        }
        tuple_obj->ext().getFixture()->refreshIfUpdated();
        return tuple_obj->ext().getItem(i).steal();
    }

    PyObject *TupleObject_count(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "count() takes one argument.");
            return NULL;
        }
        auto index = PyLong_FromLong(tuple_obj->ext().count(args[0]));
        return index;
    }

    PyObject *TupleObject_index(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "index() takes one argument.");
            return NULL;
        }
        auto index = PyLong_FromLong(tuple_obj->ext().index(args[0]));
        return index;
    }

    static PySequenceMethods TupleObject_sq = {
        .sq_length = (lenfunc)TupleObject_len,
        .sq_item = (ssizeargfunc)TupleObject_GetItem,
    };

    
    static PyMethodDef TupleObject_methods[] = {
        {"count", (PyCFunction)TupleObject_count, METH_FASTCALL, "Returns the number of elements with the specified value."},
        {"index", (PyCFunction)TupleObject_index, METH_FASTCALL, "Returns the index of the first element with the specified value."},
        {NULL}
    };

    PyTypeObject TupleObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Tuple",
        .tp_basicsize = TupleObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)TupleObject_del,
        .tp_as_sequence = &TupleObject_sq,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero tuple",
        .tp_iter = (getiterfunc)TupleObject_iter,
        .tp_methods = TupleObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)TupleObject_new,
        .tp_free = PyObject_Free,        
    };

    TupleObject *TupleObject_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<TupleObject*>(type->tp_alloc(type, 0));
    }

    TupleObject *TupleDefaultObject_new()
    {   
        return TupleObject_new(&TupleObjectType, NULL, NULL);
    }
    
    void TupleObject_del(TupleObject* tuple_obj)
    {
        // destroy associated DB0 Tuple instance
        tuple_obj->ext().~Tuple();
        Py_TYPE(tuple_obj)->tp_free((PyObject*)tuple_obj);
    }

    Py_ssize_t TupleObject_len(TupleObject *tuple_obj)
    {
        tuple_obj->ext().getFixture()->refreshIfUpdated();
        return tuple_obj->ext().getData()->size();
    }
    
    PyObject *makeDB0Tuple(db0::swine_ptr<Fixture> &fixture, PyObject *const *args, Py_ssize_t nargs)
    {        
        if (nargs != 1) {
            THROWF(db0::InputException) << "make_tuple() takes exacly 1 arguments";
        }
        // make actual DBZero instance, use default fixture
        auto py_tuple = TupleObject_new(&TupleObjectType, NULL, NULL);
        db0::FixtureLock lock(fixture);
        auto &tuple = py_tuple->modifyExt();
        db0::object_model::Tuple::makeNew(&tuple, *lock, PyObject_Length(args[0]));

        PyObject *iterator = PyObject_GetIter(args[0]);
        PyObject *item;
        int index = 0;
        while ((item = PyIter_Next(iterator))) {
            tuple.setItem(lock, index, item);
            Py_DECREF(item);
            ++index;
        }

        Py_DECREF(iterator);
        // register newly created tuple with py-object cache
        fixture->getLangCache().add(tuple.getAddress(), py_tuple, true);
        return py_tuple;
    }
    
    PyObject *makeTuple(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return runSafe(makeDB0Tuple, fixture, args, nargs);
    }
    
    bool TupleObject_Check(PyObject *object)
    {
        return Py_TYPE(object) == &TupleObjectType;        
    }

}