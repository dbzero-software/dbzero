// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyList.hpp"
#include "PyIterator.hpp"
#include "CollectionMethods.hpp"
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/list/ListIterator.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/Utils.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>

namespace db0::python

{
    
    using ListIteratorObject = PySharedWrapper<db0::object_model::ListIterator, false>;

    PyTypeObject ListIteratorObjectType = GetIteratorType<ListIteratorObject>("dbzero.ListIterator",
                                                                              "dbzero list iterator");

    ListIteratorObject *tryListObject_iter(ListObject *self)
    {
        return makeIterator<ListIteratorObject, db0::object_model::ListIterator>(
            ListIteratorObjectType, self->ext().begin(), &self->ext(), self
        );
    }

    ListIteratorObject *PyAPI_ListObject_iter(ListObject *self)
    {
        PY_API_FUNC
        return runSafe(tryListObject_iter, self);
    }
    
    shared_py_object<ListObject*> tryListObject_copy(ListObject *py_src_list)
    {
        // make actual dbzero instance, use default fixture
        auto py_list = Py_OWN(ListObject_new(&ListObjectType, NULL, NULL));
        db0::FixtureLock lock(py_src_list->ext().getFixture());
        py_src_list->ext().copy(&py_list->modifyExt(), *lock);
        lock->getLangCache().add(py_list->ext().getAddress(), py_list.get());
        return py_list;
    }

    PyObject *PyAPI_ListObject_copy(ListObject *py_src_list)
    {
        PY_API_FUNC
        return runSafe(tryListObject_copy, py_src_list).steal();
    }

    shared_py_object<ListObject*> tryListObject_multiply(ListObject *list_obj, PyObject *elem)
    {        
        auto elems = PyLong_AsLong(elem);
        auto list_obj_copy = tryListObject_copy(list_obj);
        PyObject * obj_list = (PyObject *)list_obj;
        PyObject** args = &obj_list;
        for (int i = 1; i< elems; ++i) {
            if (!tryObjectT_extend<ListObject>(list_obj_copy.get(), args, 1)) {
                return nullptr;
            }
        }

        return list_obj_copy;
    }
    
    PyObject *PyAPI_ListObject_multiply(ListObject *list_obj, PyObject *elem)
    {
        PY_API_FUNC
        return runSafe(tryListObject_multiply, list_obj, elem).steal();
    }
    
    shared_py_object<ListObject*> tryMake_DB0ListInternal(db0::swine_ptr<Fixture> &fixture,
        PyObject *const *args, Py_ssize_t nargs, AccessFlags access_mode)
    {
        auto py_list = Py_OWN(ListObject_new(&ListObjectType, NULL, NULL));
        db0::FixtureLock lock(fixture);
        auto &list = py_list->makeNew(*lock, access_mode);
        if (nargs == 1) {
            if (!tryObjectT_extend<ListObject>(py_list.get(), args, nargs)) {                
                return nullptr;
            }
        }
        // register newly created list with py-object cache
        fixture->getLangCache().add(list.getAddress(), py_list.get());
        return py_list;
    }
    
    ListObject *tryMake_ListInternal(PyObject *self, PyObject *const *args, Py_ssize_t nargs, AccessFlags access_mode)
    {
        if (nargs != 1 && nargs != 0) {
            PyErr_SetString(PyExc_TypeError, "list() takes exactly one or zero argument");
            return NULL;
        }

        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return tryMake_DB0ListInternal(fixture, args, nargs, access_mode).steal();
    }
    
    PyObject *tryListObject_GetItemSlice(ListObject *py_src_list, PyObject *elem)
    {
        // check for index
        if (PyLong_Check(elem)) {
            auto index = PyLong_AsLong(elem);
            if (index < 0) {
                index += py_src_list->ext().size();
            }
            return py_src_list->ext().getItem(index).steal();            
        }

        // check for tuple of indices
        if (PyTuple_Check(elem)) {
            Py_ssize_t num_items = PyTuple_Size(elem);
            std::vector<std::uint64_t> indices;
            indices.reserve(num_items);
            
            // Extract indices from tuple
            for (Py_ssize_t i = 0; i < num_items; ++i) {
                PyObject *py_item = PyTuple_GetItem(elem, i);
                if (!PyLong_Check(py_item)) {
                    THROWF(db0::InputException) << "Expected integer indexes in the tuple";
                }
                auto index = PyLong_AsLongLong(py_item);
                // Handle negative indices
                if (index < 0) {
                    index += py_src_list->ext().size();
                }
                if (index < 0 || static_cast<std::size_t>(index) >= py_src_list->ext().size()) {
                    PyErr_SetString(PyExc_IndexError, "list index out of range");
                    return nullptr;
                }
                indices.push_back(static_cast<std::uint64_t>(index));
            }
            
            // Create result list
            auto py_result = Py_OWN(PyList_New(num_items));
            if (!py_result) {
                return nullptr;
            }
            
            // Fetch items at specified indices
            for (std::size_t i = 0; i < indices.size(); ++i) {
                auto item = py_src_list->ext().getItem(indices[i]);
                if (!item) {
                    return nullptr;
                }
                PySafeList_SetItem(*py_result, i, item);
            }
            
            return py_result.steal();
        }

        // Check if the key is a slice object
        if (PySlice_Check(elem)) {
            // Parse slice object to get start, stop, step
            Py_ssize_t start, stop, step, slice_length;
            Py_ssize_t list_size = py_src_list->ext().size();
            
            if (PySlice_GetIndicesEx(elem, list_size, &start, &stop, &step, &slice_length) < 0) {
                return nullptr;
            }
            
            // Create a new Python list for the slice result
            PyObject* py_result = PyList_New(slice_length);
            if (!py_result) {
                return nullptr;
            }
            
            // Copy elements according to slice parameters
            for (Py_ssize_t i = 0; i < slice_length; ++i) {
                Py_ssize_t src_index = start + i * step;
                PyObject* item = py_src_list->ext().getItem(src_index).steal();
                if (!item) {
                    Py_DECREF(py_result);
                    return nullptr;
                }
                PyList_SET_ITEM(py_result, i, item);  // steals reference to item
            }
            
            return py_result;
        }
        
        THROWF(db0::InputException) 
            << "Invalid index type: " << Py_TYPE(elem)->tp_name << THROWF_END;
    }
    
    PyObject *PyAPI_ListObject_GetItemSlice(ListObject *py_src_list, PyObject *elem)
    {
        PY_API_FUNC
        return runSafe(tryListObject_GetItemSlice, py_src_list, elem);
    }
    
    PyObject *tryListObject_clear(ListObject *py_list)
    {                
        db0::FixtureLock lock(py_list->ext().getFixture());
        py_list->modifyExt().clear(lock);
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_ListObject_clear(ListObject *py_list)
    {
        PY_API_FUNC
        return runSafe(tryListObject_clear, py_list);
    }

    PyObject *tryListObject_count(ListObject *py_list, PyObject *const *args, Py_ssize_t nargs) {
        return PyLong_FromLong(py_list->ext().count(args[0]));      
    }

    PyObject *PyAPI_ListObject_count(ListObject *py_list, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC    
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "count() takes one argument.");
            return NULL;
        }
        return runSafe(tryListObject_count, py_list, args, nargs);
    }
    
    shared_py_object<ListObject*> tryListObject_add(ListObject *list_obj_lh, ListObject *list_obj_rh)
    {        
        //make copy of first list
        PyObject * obj_list = (PyObject *)list_obj_rh;
        PyObject** args = &obj_list;
        auto lh_copy = tryListObject_copy(list_obj_lh);
        if (!tryObjectT_extend<ListObject>(lh_copy.get(), args, 1)) {
            return nullptr;
        }

        return lh_copy;
    }
    
    PyObject *PyAPI_ListObject_add(ListObject *list_obj_lh, ListObject *list_obj_rh)
    {
        PY_API_FUNC
        return runSafe(tryListObject_add, list_obj_lh, list_obj_rh).steal();
    }
    
    static PySequenceMethods ListObject_sq = getPySequenceMehods<ListObject>();
    
    static PyMappingMethods ListObject_mp = {
        .mp_length = (lenfunc)PyAPI_ObjectT_len<ListObject>,
        .mp_subscript = (binaryfunc)PyAPI_ListObject_GetItemSlice
    };
    
    static PyNumberMethods ListObject_as_num = {
        .nb_add = (binaryfunc)PyAPI_ListObject_add,
        .nb_multiply = (binaryfunc)PyAPI_ListObject_multiply
    };
    
    static PyMethodDef ListObject_methods[] = {
        {"append", (PyCFunction)PyAPI_ObjectT_append<ListObject>, METH_FASTCALL, "Append an item to the container."},
        {"pop", (PyCFunction)PyAPI_ObjectT_pop<ListObject>, METH_FASTCALL, "Removes the element at the specified position."},
        {"clear", (PyCFunction)PyAPI_ListObject_clear, METH_FASTCALL, "Clears all items from the container."},
        {"copy", (PyCFunction)PyAPI_ListObject_copy, METH_FASTCALL, "Returns a copy of the list."},
        {"count", (PyCFunction)PyAPI_ListObject_count, METH_FASTCALL, "Returns the number of elements with the specified value."},
        {"extend", (PyCFunction)PyAPI_ObjectT_extend<ListObject>, METH_FASTCALL, "Add the elements of a list (or any iterable), to the end of the current list."},
        {"index", (PyCFunction)PyAPI_ObjectT_index<ListObject>, METH_FASTCALL, "Returns the index of the first element with the specified value."},
        {"remove", (PyCFunction)PyAPI_ObjectT_remove<ListObject>, METH_FASTCALL, "Removes first element with the specified value."},
        {NULL}
    };

    PyObject *tryListObject_rq(ListObject *list_obj, PyObject *other, int op)
    {        
        if (ListObject_Check(other)) {
            ListObject * other_list = (ListObject*) other;
            switch (op) {
                case Py_EQ:
                    return PyBool_fromBool(list_obj->ext() == other_list->ext());
                case Py_NE:
                    return PyBool_fromBool(list_obj->ext() != other_list->ext());
                default:
                    Py_RETURN_NOTIMPLEMENTED;
            }
        } else if(PyList_Check(other)) {
            auto iterator = Py_OWN(PyObject_GetIter(other));
            if (!iterator) {
                PyErr_SetString(PyExc_TypeError, "argument must be a sequence");
                return nullptr;
            }
            switch (op) {
                case Py_EQ: {
                    auto eq_result = has_all_elements_same(list_obj, iterator.get());
                    if (!eq_result) {
                        return nullptr;
                    }
                    return PyBool_fromBool(*eq_result);
                }
                case Py_NE: {
                    auto ne_result = has_all_elements_same(list_obj, iterator.get());
                    if (!ne_result) {
                        return nullptr;
                    }
                    return PyBool_fromBool(!(*ne_result));
                }
                default:
                    Py_RETURN_NOTIMPLEMENTED;
            }
            
            Py_RETURN_FALSE;
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
    
    PyObject *PyAPI_ListObject_rq(ListObject *list_obj, PyObject *other, int op)
    {
        PY_API_FUNC
        return runSafe(tryListObject_rq, list_obj, other, op);
    }

    PyObject *tryListObject_str(ListObject *self)
    {
        std::stringstream str;
        str << "[";
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
        str << "]";
        return PyUnicode_FromString(str.str().c_str());
    }

    PyObject *PyAPI_ListObject_str(ListObject *self)
    {
        PY_API_FUNC
        return runSafe(tryListObject_str, self);
    }

    PyTypeObject ListObjectType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "List",
        .tp_basicsize = ListObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_ListObject_del,
        .tp_repr = (reprfunc)PyAPI_ListObject_str,
        .tp_as_number = &ListObject_as_num,
        .tp_as_sequence = &ListObject_sq,
        .tp_as_mapping = &ListObject_mp,
        .tp_str = (reprfunc)PyAPI_ListObject_str,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero indexed collection object",
        .tp_richcompare = (richcmpfunc)PyAPI_ListObject_rq,
        .tp_iter = (getiterfunc)PyAPI_ListObject_iter,
        .tp_methods = ListObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)ListObject_new,
        .tp_free = PyObject_Free, 
    };
    
    ListObject *ListObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        // not API method, lock not needed (otherwise may cause deadlock)     
        return reinterpret_cast<ListObject*>(type->tp_alloc(type, 0));
    }
    
    shared_py_object<ListObject*> ListDefaultObject_new() {
        // not API method, lock not needed (otherwise may cause deadlock)
        return { ListObject_new(&ListObjectType, NULL, NULL), false };
    }
    
    void PyAPI_ListObject_del(ListObject* list_obj)
    {
        PY_API_FUNC
        // destroy associated DB0 List instance
        list_obj->destroy();
        Py_TYPE(list_obj)->tp_free((PyObject*)list_obj);
    }
    
    bool ListObject_Check(PyObject *object) {
        return Py_TYPE(object) == &ListObjectType;
    }
    
    shared_py_object<ListObject*> tryMake_DB0List(db0::swine_ptr<Fixture> &fixture, PyObject *const *args,
        Py_ssize_t nargs, AccessFlags access_mode)
    {
        return tryMake_DB0ListInternal(fixture, args, nargs, access_mode);
    }
    
    PyObject *PyAPI_makeList(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(tryMake_ListInternal, self, args, nargs, AccessFlags{});
    }
    
    PyObject *tryLoadList(ListObject *list, PyObject *kwargs, std::unordered_set<const void*> *load_stack_ptr)
    {    
        auto &list_obj = list->ext();
        auto py_result = Py_OWN(PyList_New(list_obj.size()));
        for (std::size_t i = 0; i < list_obj.size(); ++i) {
            auto res = Py_OWN(tryLoad(*list_obj.getItem(i), kwargs, nullptr, load_stack_ptr));
            if (!res) {
                return nullptr;
            }
            PySafeList_SetItem(*py_result, i, res);
        }
        return py_result.steal();
    }
    
    PyObject *tryLoadPyList(PyObject *py_list, PyObject *kwargs, std::unordered_set<const void*> *load_stack_ptr)
    {
        Py_ssize_t size = PyList_Size(py_list);
        auto py_result = Py_OWN(PyList_New(size));
        for (int i = 0; i < size; ++i) {
            auto res = Py_OWN(tryLoad(PyList_GetItem(py_list, i), kwargs, nullptr, load_stack_ptr));
            if (!res) {
                return nullptr;
            }
            PySafeList_SetItem(*py_result, i, res);
        }
        return py_result.steal();
    }
    
}
