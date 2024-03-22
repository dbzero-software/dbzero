#include "List.hpp"
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include "ListIterator.hpp"
#include "Utils.hpp"

namespace db0::python
{
    
    PyObject *ListObject_GetItem(ListObject *list_obj, Py_ssize_t i)
    {
        list_obj->ext().getFixture()->refreshIfUpdated();
        return list_obj->ext().getItem(i).steal();
    }
    
    ListIteratorObject *ListObject_iter(ListObject *self)
    {
        return makeIterator(self->ext().begin(), &self->ext());        
    }

    PyObject *ListObject_multiply(ListObject *list_obj, PyObject *elem)
    {
        auto elems = PyLong_AsLong(elem);
        auto list_obj_copy = (ListObject *)ListObject_copy(list_obj);
        PyObject * obj_list = (PyObject *)list_obj;
        PyObject** args = &obj_list;
        for(int i = 1; i< elems; ++i){
            ListObject_extend(list_obj_copy, args, 1);
        }

        return list_obj_copy;
    }

    PyObject *ListObject_GetItemSlice(ListObject *list_obj, PyObject *elem)
    {
        // Check if the key is a slice object
        if (PySlice_Check(elem)) {
            Py_ssize_t start, stop, step;
            PySlice_GetIndices(elem, list_obj->ext().size(), &start, &stop, &step);
            ListObject *list = makeList(nullptr,nullptr, 0);
            auto compare = [step](Py_ssize_t i, Py_ssize_t stop) {
                if(step > 0){
                    return i < stop; 
                } else {
                    return i > stop;
                }
            };
            for (Py_ssize_t i = start; compare(i, stop); i += step) {
                list->ext().append(list_obj->ext().getItem(i).steal());
            }

            return list;
        }
        list_obj->ext().getFixture()->refreshIfUpdated();
        return list_obj->ext().getItem(PyLong_AsLong(elem)).steal();
    }
    
    PyObject *ListObject_pop(ListObject *list_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        list_obj->ext().getFixture()->refreshIfUpdated();
        std::size_t index;
        if (nargs == 0) {
            index = list_obj->ext().size() -1;
        } else if (nargs == 1) {
            index = PyLong_AsLong(args[0]);
        } else {
            PyErr_SetString(PyExc_TypeError, "pop() takes zero or one argument.");
            return NULL;
        }
        return list_obj->ext().pop(index).steal();
    }
    
    PyObject * ListObject_clear(ListObject *list_obj)
    {
        list_obj->ext().getFixture()->refreshIfUpdated();
        list_obj->ext().clear();
        Py_RETURN_NONE;
    }

    PyObject *ListObject_copy(ListObject *list_obj)
    {
        // make actual DBZero instance, use default fixture
        auto list_object = ListObject_new(&ListObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        list_obj->ext().copy(&list_object->ext(), *fixture);
        (*fixture)->getLangCache().add(list_object->ext().getAddress(), list_object, true);
        return list_object;
    }
    
    PyObject *ListObject_count(ListObject *list_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "count() takes one argument.");
            return NULL;
        }
        auto count = PyLong_FromLong(list_obj->ext().count(args[0]));
        return count;
    }

    PyObject *ListObject_extend(ListObject *list_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "extend() takes one argument.");
            return NULL;
        }
        PyObject *iterator = PyObject_GetIter(args[0]);
        PyObject *item;
        while ((item = PyIter_Next(iterator))) {
            list_obj->ext().append(item);
            Py_DECREF(item);
        }

        Py_DECREF(iterator);
        Py_RETURN_NONE;
    }

    PyObject *ListObject_add(ListObject *list_obj_lh, ListObject *list_obj_rh)
    {
        //make copy of first list
        PyObject * obj_list = (PyObject *)list_obj_rh;
        PyObject** args = &obj_list;
        ListObject * lh_copy = (ListObject *)ListObject_copy(list_obj_lh);
        ListObject_extend(lh_copy, args, 1);
        return lh_copy;
    }

    PyObject *ListObject_index(ListObject *list_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "index() takes one argument.");
            return NULL;
        }
        auto index = PyLong_FromLong(list_obj->ext().index(args[0]));
        return index;
    }

    PyObject *ListObject_remove(ListObject *list_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "remove() takes one argument.");
            return NULL;
        }
        auto index = list_obj->ext().index(args[0]);
        list_obj->ext().swapAndPop({index});
        Py_RETURN_NONE;
    }

    int ListObject_SetItem(ListObject *list_obj, Py_ssize_t i, PyObject *value)
    {
        list_obj->ext().setItem(i, value);
        return 0;
    }

    static PySequenceMethods ListObject_sq = {
        .sq_length = (lenfunc)ListObject_len,
        .sq_item = (ssizeargfunc)ListObject_GetItem,
        .sq_ass_item = (ssizeobjargproc)ListObject_SetItem
    };

    static PyMappingMethods ListObject_mp = {
        .mp_length = (lenfunc)ListObject_len,
        .mp_subscript = (binaryfunc)ListObject_GetItemSlice
    };

    static PyNumberMethods ListObject_as_num = {
        .nb_add = (binaryfunc)ListObject_add,
        .nb_multiply = (binaryfunc)ListObject_multiply
    };
    
    static PyMethodDef ListObject_methods[] = {
        {"append", (PyCFunction)ListObject_append, METH_FASTCALL, "Append an item to the container."},
        {"pop", (PyCFunction)ListObject_pop, METH_FASTCALL, "Removes the element at the specified position."},
        {"clear", (PyCFunction)ListObject_clear, METH_FASTCALL, "Clears all items from the container."},
        {"copy", (PyCFunction)ListObject_copy, METH_FASTCALL, "Returns a copy of the list."},
        {"count", (PyCFunction)ListObject_count, METH_FASTCALL, "Returns the number of elements with the specified value."},
        {"extend", (PyCFunction)ListObject_extend, METH_FASTCALL, "Add the elements of a list (or any iterable), to the end of the current list."},
        {"index", (PyCFunction)ListObject_index, METH_FASTCALL, "Returns the index of the first element with the specified value."},
        {"remove", (PyCFunction)ListObject_remove, METH_FASTCALL, "Removes first element with the specified value."},
        {NULL}
    };

    static PyObject *ListObject_rq(ListObject *list_obj, PyObject *other, int op) {
        if (ListObject_Check(other)) {
            ListObject * other_list = (ListObject*) other;
            switch (op)
            {
            case Py_EQ:
                return PyBool_fromBool(list_obj->ext() == other_list->ext());
            case Py_NE:
                return PyBool_fromBool(list_obj->ext() != other_list->ext());
            default:
                return Py_NotImplemented;
            }
        } else {
            PyObject *iterator = PyObject_GetIter(other);
            switch (op)
            {
            case Py_EQ:
                return PyBool_fromBool(has_all_elements_same(list_obj, iterator));
            case Py_NE:
                return PyBool_fromBool(!has_all_elements_same(list_obj, iterator));
            default:
                return Py_NotImplemented;
            }

            Py_DECREF(iterator);
            return Py_True;
        }
    }

    PyTypeObject ListObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.List",
        .tp_basicsize = ListObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)ListObject_del,
        .tp_as_number = &ListObject_as_num,
        .tp_as_sequence = &ListObject_sq,
        .tp_as_mapping = &ListObject_mp,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero indexed collection object",
        .tp_richcompare = (richcmpfunc)ListObject_rq,
        .tp_iter = (getiterfunc)ListObject_iter,
        .tp_methods = ListObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)ListObject_new,
        .tp_free = PyObject_Free,        
    };

    ListObject *ListObject_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<ListObject*>(type->tp_alloc(type, 0));
    }

    ListObject *ListDefaultObject_new()
    {   
        return ListObject_new(&ListObjectType, NULL, NULL);
    }
    
    void ListObject_del(ListObject* list_obj)
    {
        // destroy associated DB0 List instance
        list_obj->ext().~List();
        Py_TYPE(list_obj)->tp_free((PyObject*)list_obj);
    }
    
    Py_ssize_t ListObject_len(ListObject *list_obj)
    {
        list_obj->ext().getFixture()->refreshIfUpdated();
        return list_obj->ext().size();
    }
    
    PyObject *ListObject_append(ListObject *list_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "append() takes exactly one argument");
            return NULL;
        }
        
        list_obj->ext().append(args[0]);
        Py_RETURN_NONE;
    }
    
    ListObject *makeList(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if(nargs != 1 && nargs != 0) {
            PyErr_SetString(PyExc_TypeError, "list() takes exactly one or zero argument");
            return NULL;
        }
        // make actual DBZero instance, use default fixture
        auto list_object = ListObject_new(&ListObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        db0::object_model::List::makeNew(&list_object->ext(), *fixture);
        if (nargs == 1) {
            ListObject_extend(list_object, args, 1);
        }
        // register newly created list with py-object cache
        (*fixture)->getLangCache().add(list_object->ext().getAddress(), list_object, true);
        return list_object;
    }
    
    bool ListObject_Check(PyObject *object)
    {
        return Py_TYPE(object) == &ListObjectType;        
    }

}