#include "List.hpp"
#include "CollectionMethods.hpp"
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/list/ListIterator.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include "Iterator.hpp"
#include <dbzero/bindings/python/Utils.hpp>

namespace db0::python
{
    
    using ListIteratorObject = PyWrapper<db0::object_model::ListIterator>;

    PyTypeObject ListIteratorObjectType = GetIteratorType<ListIteratorObject>("dbzero_ce.ListIterator",
                                                                              "DBZero list iterator");

    ListIteratorObject *ListObject_iter(ListObject *self)
    {
        return makeIterator<ListIteratorObject,db0::object_model::ListIterator>(ListIteratorObjectType, 
            self->ext().begin(), &self->ext());        
    }

    PyObject *ListObject_GetItem(ListObject *list_obj, Py_ssize_t i)
    {
        list_obj->ext().getFixture()->refreshIfUpdated();
        return list_obj->ext().getItem(i).steal();
    }

    PyObject *ListObject_multiply(ListObject *list_obj, PyObject *elem)
    {
        auto elems = PyLong_AsLong(elem);
        auto list_obj_copy = (ListObject *)ListObject_copy(list_obj);
        PyObject * obj_list = (PyObject *)list_obj;
        PyObject** args = &obj_list;
        for(int i = 1; i< elems; ++i){
            ObjectT_extend<ListObject>(list_obj_copy, args, 1);
        }

        return list_obj_copy;
    }

    PyObject *ListObject_GetItemSlice(ListObject *list_obj, PyObject *elem)
    {
        // FIXME: this operation should be immutable
        auto fixture = list_obj->ext().getMutableFixture();
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
                list->ext().append(fixture, list_obj->ext().getItem(i).steal());
            }

            return list;
        }
        return list_obj->ext().getItem(PyLong_AsLong(elem)).steal();
    }
    
    PyObject * ListObject_clear(ListObject *list_obj)
    {
        auto fixture = list_obj->ext().getMutableFixture();
        list_obj->ext().clear(fixture);
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


    PyObject *ListObject_add(ListObject *list_obj_lh, ListObject *list_obj_rh)
    {
        //make copy of first list
        PyObject * obj_list = (PyObject *)list_obj_rh;
        PyObject** args = &obj_list;
        ListObject * lh_copy = (ListObject *)ListObject_copy(list_obj_lh);
        ObjectT_extend<ListObject>(lh_copy, args, 1);
        return lh_copy;
    }




    static PySequenceMethods ListObject_sq = getPySequenceMehods<ListObject>();

    static PyMappingMethods ListObject_mp = {
        .mp_length = (lenfunc)ObjectT_len<ListObject>,
        .mp_subscript = (binaryfunc)ListObject_GetItemSlice
    };

    static PyNumberMethods ListObject_as_num = {
        .nb_add = (binaryfunc)ListObject_add,
        .nb_multiply = (binaryfunc)ListObject_multiply
    };
    
    static PyMethodDef ListObject_methods[] = {
        {"append", (PyCFunction)ObjectT_append<ListObject>, METH_FASTCALL, "Append an item to the container."},
        {"pop", (PyCFunction)ObjectT_pop<ListObject>, METH_FASTCALL, "Removes the element at the specified position."},
        {"clear", (PyCFunction)ListObject_clear, METH_FASTCALL, "Clears all items from the container."},
        {"copy", (PyCFunction)ListObject_copy, METH_FASTCALL, "Returns a copy of the list."},
        {"count", (PyCFunction)ListObject_count, METH_FASTCALL, "Returns the number of elements with the specified value."},
        {"extend", (PyCFunction)ObjectT_extend<ListObject>, METH_FASTCALL, "Add the elements of a list (or any iterable), to the end of the current list."},
        {"index", (PyCFunction)ObjectT_index<ListObject>, METH_FASTCALL, "Returns the index of the first element with the specified value."},
        {"remove", (PyCFunction)ObjectT_remove<ListObject>, METH_FASTCALL, "Removes first element with the specified value."},
        {NULL}
    };

    static PyObject *ListObject_rq(ListObject *list_obj, PyObject *other, int op) 
    {
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

    ListObject *ListObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<ListObject*>(type->tp_alloc(type, 0));
    }

    ListObject *ListDefaultObject_new() {   
        return ListObject_new(&ListObjectType, NULL, NULL);
    }
    
    void ListObject_del(ListObject* list_obj)
    {
        // destroy associated DB0 List instance
        list_obj->ext().~List();
        Py_TYPE(list_obj)->tp_free((PyObject*)list_obj);
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
            ObjectT_extend<ListObject>(list_object, args, 1);
        }
        // register newly created list with py-object cache
        (*fixture)->getLangCache().add(list_object->ext().getAddress(), list_object, true);
        return list_object;
    }
    
    bool ListObject_Check(PyObject *object) {
        return Py_TYPE(object) == &ListObjectType;        
    }

}