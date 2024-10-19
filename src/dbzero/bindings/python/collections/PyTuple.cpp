#include "PyTuple.hpp"
#include "PyIterator.hpp"
#include <dbzero/object_model/tuple/TupleIterator.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/Utils.hpp>
#include <dbzero/bindings/python/AnyObjectAPI.hpp>

namespace db0::python

{
    
    using TupleIteratorObject = PyWrapper<db0::object_model::TupleIterator, false>;

    PyTypeObject TupleIteratorObjectType = GetIteratorType<TupleIteratorObject>("dbzero_ce.TupleIterator",
                                                                              "DBZero tuple iterator");

    TupleIteratorObject *PyAPI_TupleObject_iter(TupleObject *self)
    {
        PY_API_FUNC
        return makeIterator<TupleIteratorObject,db0::object_model::TupleIterator>(TupleIteratorObjectType, 
            self->ext().begin(), &self->ext());        
    }

    PyObject *PyAPI_TupleObject_GetItem(TupleObject *tuple_obj, Py_ssize_t i)
    {
        PY_API_FUNC        
        if (static_cast<std::size_t>(i) >= tuple_obj->ext().getData()->size()) {
            PyErr_SetString(PyExc_IndexError, "tuple index out of range");
            return NULL;
        }
        tuple_obj->ext().getFixture()->refreshIfUpdated();
        return tuple_obj->ext().getItem(i).steal();
    }

    PyObject *PyAPI_TupleObject_count(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC        
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "count() takes one argument.");
            return NULL;
        }
        auto index = PyLong_FromLong(tuple_obj->ext().count(args[0]));
        return index;
    }

    PyObject *PyAPI_TupleObject_index(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC        
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "index() takes one argument.");
            return NULL;
        }
        auto index = PyLong_FromLong(tuple_obj->ext().index(args[0]));
        return index;
    }

    static PySequenceMethods TupleObject_sq = {
        .sq_length = (lenfunc)PyAPI_TupleObject_len,
        .sq_item = (ssizeargfunc)PyAPI_TupleObject_GetItem,
    };

    
    static PyMethodDef TupleObject_methods[] = {
        {"count", (PyCFunction)PyAPI_TupleObject_count, METH_FASTCALL, "Returns the number of elements with the specified value."},
        {"index", (PyCFunction)PyAPI_TupleObject_index, METH_FASTCALL, "Returns the index of the first element with the specified value."},
        {NULL}
    };

    static PyObject *PyAPI_TupleObject_rq(TupleObject *tuple_obj, TupleObject *other, int op) 
    {
        PY_API_FUNC
        if (TupleObject_Check(other)) {
            TupleObject * other_tuple = (TupleObject*) other;
            switch (op)
            {
            case Py_EQ:
                return PyBool_fromBool(tuple_obj->ext() == other_tuple->ext());
            case Py_NE:
                return PyBool_fromBool(tuple_obj->ext() != other_tuple->ext());
            default:
                return Py_NotImplemented;
            }
        } else {
            PyObject *iterator = AnyObject_GetIter(other);
            switch (op)
            {
            case Py_EQ:
                return PyBool_fromBool(has_all_elements_same(tuple_obj, iterator));
            case Py_NE:
                return PyBool_fromBool(!has_all_elements_same(tuple_obj, iterator));
            default:
                return Py_NotImplemented;
            }
            WITH_PY_API_UNLOCKED
            Py_DECREF(iterator);
            return Py_True;
        }
    }
    
    PyTypeObject TupleObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Tuple",
        .tp_basicsize = TupleObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_TupleObject_del,
        .tp_as_sequence = &TupleObject_sq,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero tuple",
        .tp_richcompare = (richcmpfunc)PyAPI_TupleObject_rq,
        .tp_iter = (getiterfunc)PyAPI_TupleObject_iter,
        .tp_methods = TupleObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)TupleObject_new,
        .tp_free = PyObject_Free,        
    };
    
    TupleObject *TupleObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<TupleObject*>(type->tp_alloc(type, 0));
    }
    
    shared_py_object<TupleObject*> TupleDefaultObject_new() {
        // not API method, lock not needed (otherwise may cause deadlock)
        return { TupleObject_new(&TupleObjectType, NULL, NULL), false };
    }
    
    void PyAPI_TupleObject_del(TupleObject* tuple_obj)
    {
        PY_API_FUNC
        // destroy associated DB0 Tuple instance
        tuple_obj->destroy();
        Py_TYPE(tuple_obj)->tp_free((PyObject*)tuple_obj);
    }

    Py_ssize_t PyAPI_TupleObject_len(TupleObject *tuple_obj)
    {
        PY_API_FUNC
        tuple_obj->ext().getFixture()->refreshIfUpdated();
        return tuple_obj->ext().getData()->size();
    }
    
    shared_py_object<TupleObject*> makeDB0Tuple(db0::swine_ptr<Fixture> &fixture, PyObject *const *args,
        Py_ssize_t nargs)
    {
        if (nargs != 1) {
            THROWF(db0::InputException) << "make_tuple() takes exacly 1 arguments";
        }

        // make actual DBZero instance, use default fixture
        auto py_tuple = TupleDefaultObject_new();
        db0::FixtureLock lock(fixture);
        auto &tuple = py_tuple.get()->modifyExt();
        db0::object_model::Tuple::makeNew(&tuple, *lock, AnyObject_Length(args[0]));
        PyObject *iterator = AnyObject_GetIter(args[0]);
        PyObject *item;
        int index = 0;
        while ((item = AnyIter_Next(iterator))) {
            tuple.setItem(lock, index, item);
            WITH_PY_API_UNLOCKED
            Py_DECREF(item);
            ++index;
        }

        // register newly created tuple with py-object cache
        fixture->getLangCache().add(tuple.getAddress(), py_tuple.get());
        WITH_PY_API_UNLOCKED
        Py_DECREF(iterator);
        return py_tuple;
    }
    
    PyObject *PyAPI_makeTuple(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return runSafe(makeDB0Tuple, fixture, args, nargs).steal();
    }
    
    bool TupleObject_Check(PyObject *object) {
        return Py_TYPE(object) == &TupleObjectType;        
    }
    
}