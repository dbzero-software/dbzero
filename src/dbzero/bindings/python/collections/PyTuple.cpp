// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyTuple.hpp"
#include "PyIterator.hpp"
#include <dbzero/object_model/tuple/TupleIterator.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/Utils.hpp>

namespace db0::python

{
    
    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
    using TupleIteratorObject = PySharedWrapper<db0::object_model::TupleIterator, false>;

    PyTypeObject TupleIteratorObjectType = GetIteratorType<TupleIteratorObject>("dbzero.TupleIterator",
                                                                              "dbzero tuple iterator");

    TupleIteratorObject *tryTupleObject_iter(TupleObject *self)
    {        
        return makeIterator<TupleIteratorObject,db0::object_model::TupleIterator>(
            TupleIteratorObjectType, self->ext().begin(), &self->ext(), self
        );
    }

    TupleIteratorObject *PyAPI_TupleObject_iter(TupleObject *self)
    {
        PY_API_FUNC
        return runSafe(tryTupleObject_iter, self);
    }

    PyObject *tryTupleObject_GetItem(TupleObject *tuple_obj, Py_ssize_t i)
    {   
        tuple_obj->ext().getFixture()->refreshIfUpdated();     
        if (static_cast<std::size_t>(i) >= tuple_obj->ext().getData()->size()) {
            PyErr_SetString(PyExc_IndexError, "tuple index out of range");
            return NULL;
        }        
        return tuple_obj->ext().getItem(i).steal();
    }

    PyObject *PyAPI_TupleObject_GetItem(TupleObject *tuple_obj, Py_ssize_t i)
    {
        PY_API_FUNC        
        return runSafe(tryTupleObject_GetItem, tuple_obj, i);
    }

    PyObject *tryTupleObject_count(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs) {
        return PyLong_FromLong(tuple_obj->ext().count(args[0]));        
    }

    PyObject *PyAPI_TupleObject_count(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC        
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "count() takes one argument.");
            return NULL;
        }
        return runSafe(tryTupleObject_count, tuple_obj, args, nargs);
    }

    PyObject *tryTupleObject_index(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs) {
        return PyLong_FromLong(tuple_obj->ext().index(args[0]));        
    }

    PyObject *PyAPI_TupleObject_index(TupleObject *tuple_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC        
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "index() takes one argument.");
            return NULL;
        }
        return runSafe(tryTupleObject_index, tuple_obj, args, nargs);
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
    
    PyObject *tryTupleObject_rq(TupleObject *tuple_obj, TupleObject *other, int op)
    {
        if (TupleObject_Check(other)) {
            TupleObject * other_tuple = (TupleObject*)other;
            switch (op) {
                case Py_EQ:
                    return PyBool_fromBool(tuple_obj->ext() == other_tuple->ext());
                case Py_NE:
                    return PyBool_fromBool(tuple_obj->ext() != other_tuple->ext());
                default:
                    Py_RETURN_NOTIMPLEMENTED;
            }
        } else  if (PyTuple_Check(other)) {
            auto iterator = Py_OWN(PyObject_GetIter(other));
            if (!iterator) {
                PyErr_SetString(PyExc_TypeError, "argument must be an iterable");
                return nullptr;
            }
            switch (op) {
                case Py_EQ: {
                    auto eq_result = has_all_elements_same(tuple_obj, iterator.get());
                    if (!eq_result) {
                        return nullptr;
                    }
                    return PyBool_fromBool(*eq_result);
                }
                case Py_NE: {
                    auto ne_result = has_all_elements_same(tuple_obj, iterator.get());
                    if (!ne_result) {
                        return nullptr;
                    }
                    return PyBool_fromBool(!*ne_result);
                }
                default:
                    Py_RETURN_NOTIMPLEMENTED;
            }            
        } else {
            switch (op) {
                case Py_EQ: {
                    Py_RETURN_FALSE;
                }
                case Py_NE: {
                    Py_RETURN_TRUE;
                }
                default:
                    Py_RETURN_NOTIMPLEMENTED;
            } 
        }
        Py_RETURN_NOTIMPLEMENTED;
    }
    
    PyObject *PyAPI_TupleObject_rq(TupleObject *tuple_obj, TupleObject *other, int op)
    {
        PY_API_FUNC
        return runSafe(tryTupleObject_rq, tuple_obj, other, op);
    }

    PyObject *tryTupleObject_str(TupleObject *self)
    {
        std::stringstream str;
        str << "(";
        // iterate through list elements
        auto iterator = Py_OWN(PyObject_GetIter(reinterpret_cast<PyObject*>(self)));
        if (!iterator) {
            return nullptr;
        }
        bool first = true;
        ObjectSharedPtr elem;
        Py_FOR(elem, iterator) {
            if(!first){
                str << ", ";
            } else {
                first = false;
            }
            auto str_value = Py_OWN(PyObject_Repr(*elem));
            if (!str_value) {
                return nullptr;
            }
            str << PyUnicode_AsUTF8(*str_value);
        } 
        str << ")";
        return PyUnicode_FromString(str.str().c_str());
    }

    PyObject *PyAPI_TupleObject_str(TupleObject *self)
    {
        PY_API_FUNC
        return runSafe(tryTupleObject_str, self);
    }
    
    PyTypeObject TupleObjectType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "Tuple",
        .tp_basicsize = TupleObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_TupleObject_del,
        .tp_repr = (reprfunc)PyAPI_TupleObject_str,
        .tp_as_sequence = &TupleObject_sq,
        .tp_str = (reprfunc)PyAPI_TupleObject_str,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero tuple",
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

    Py_ssize_t tryTupleObject_len(TupleObject *tuple_obj)
    {        
        tuple_obj->ext().getFixture()->refreshIfUpdated();
        return tuple_obj->ext().getData()->size();
    }

    Py_ssize_t PyAPI_TupleObject_len(TupleObject *tuple_obj)
    {
        PY_API_FUNC
        return runSafe(tryTupleObject_len, tuple_obj);
    }
    
    shared_py_object<TupleObject*> tryMake_DB0Tuple(db0::swine_ptr<Fixture> &fixture, PyObject *const *args,
        Py_ssize_t nargs, AccessFlags access_mode)
    {
        using Tuple = db0::object_model::Tuple;

        if (nargs > 1) {
            PyErr_SetString(PyExc_TypeError, "tuple() expected at most 1 argument");
            return nullptr;
        }
        
        // make actual dbzero instance, use default fixture
        auto py_tuple = TupleDefaultObject_new();
        db0::FixtureLock lock(fixture);
        if (nargs == 0) {
            auto &tuple = py_tuple->makeNew(*lock, Tuple::tag_new_tuple(), 0, access_mode);
            fixture->getLangCache().add(tuple.getAddress(), py_tuple.get()); 
            return py_tuple;
        }
        
        auto iterator = Py_OWN(PyObject_GetIter(args[0]));
        if (!iterator) {
            return nullptr;
        }

        Py_ssize_t length = PyObject_Length(args[0]);
        if (length == -1) {
            // We are dealing with generator-like object
            PyErr_Clear();
            std::vector<ObjectSharedPtr> values;
            Py_FOR(item, iterator) {
                values.push_back(item);
            }
            if (PyErr_Occurred()) {
                return nullptr; // Error from PyIter_Next
            }
            auto &tuple = py_tuple->makeNew(*lock, Tuple::tag_new_tuple(), values.size(), access_mode);
            for (std::size_t index = 0; index != values.size(); ++index) {
                tuple.setItem(lock, index, values[index]);
            }
        } else {
            auto &tuple = py_tuple->makeNew(*lock, Tuple::tag_new_tuple(), length, access_mode);
            int index = 0;
            Py_FOR(item, iterator) {
                tuple.setItem(lock, index++, item);                
            }
            if (PyErr_Occurred()) {
                return nullptr; // Error from PyIter_Next
            }
        }
        
        // register newly created tuple with py-object cache
        fixture->getLangCache().add(py_tuple->ext().getAddress(), *py_tuple);
        return py_tuple;
    }
    
    shared_py_object<TupleObject*> tryMake_DB0TupleInternal(PyObject *const *args, 
        Py_ssize_t nargs, AccessFlags access_mode)
    {
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return tryMake_DB0Tuple(fixture, args, nargs, access_mode);
    }
    
    PyObject *PyAPI_makeTuple(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(tryMake_DB0TupleInternal, args, nargs, AccessFlags{}).steal();
    }
    
    bool TupleObject_Check(PyObject *object) {
        return Py_TYPE(object) == &TupleObjectType;        
    }
    
    PyObject *tryLoadTuple(TupleObject *py_tuple, PyObject *kwargs, std::unordered_set<const void*> *load_stack_ptr)
    {
        auto &tuple_obj = py_tuple->ext();
        auto py_result = Py_OWN(PyTuple_New(tuple_obj.size()));
        if (!py_result) {
            return nullptr;
        }

        for (std::size_t i = 0; i < tuple_obj.size(); ++i) {
            auto res = Py_OWN(tryLoad(*tuple_obj.getItem(i), kwargs, nullptr, load_stack_ptr));
            if (!res) {                
                return nullptr;
            }
            PySafeTuple_SetItem(*py_result, i, res);
        }
        return py_result.steal();
    }
    
    PyObject *tryLoadPyTuple(PyObject *py_tuple, PyObject *kwargs, std::unordered_set<const void*> *load_stack_ptr)
    {
        Py_ssize_t size = PyTuple_Size(py_tuple);        
        auto py_result = Py_OWN(PyTuple_New(size));
        for (int i = 0; i < size; ++i) {
            auto res = Py_OWN(tryLoad(PyTuple_GetItem(py_tuple, i), kwargs, nullptr, load_stack_ptr));
            if (!res) {                
                return nullptr;
            }
            PySafeTuple_SetItem(*py_result, i, res);
        }
        return py_result.steal();
    }
    
}
