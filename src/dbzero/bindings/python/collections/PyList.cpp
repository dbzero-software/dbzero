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
    
    using ListIteratorObject = PyWrapper<db0::object_model::ListIterator, false>;

    PyTypeObject ListIteratorObjectType = GetIteratorType<ListIteratorObject>("dbzero_ce.ListIterator",
                                                                              "DBZero list iterator");
    
    ListIteratorObject *PyAPI_ListObject_iter(ListObject *self)
    {
        PY_API_FUNC
        return makeIterator<ListIteratorObject, db0::object_model::ListIterator>(
            ListIteratorObjectType, self->ext().begin(), &self->ext(), self
        );
    }
    
    PyObject *ListObject_GetItem(ListObject *list_obj, Py_ssize_t i)
    {        
        list_obj->ext().getFixture()->refreshIfUpdated();
        return list_obj->ext().getItem(i).steal();
    }
    
    PyObject *ListObject_copy(ListObject *py_src_list)
    {
        // make actual DBZero instance, use default fixture
        auto py_list = ListObject_new(&ListObjectType, NULL, NULL);
        db0::FixtureLock lock(py_src_list->ext().getFixture());
        py_src_list->ext().copy(&py_list->modifyExt(), *lock);
        lock->getLangCache().add(py_list->ext().getAddress(), py_list);
        return py_list;
    }

    PyObject *PyAPI_ListObject_copy(ListObject *py_src_list)
    {
        PY_API_FUNC
        return ListObject_copy(py_src_list);
    }

    PyObject *PyAPI_ListObject_multiply(ListObject *list_obj, PyObject *elem)
    {
        PY_API_FUNC
        auto elems = PyLong_AsLong(elem);
        auto list_obj_copy = (ListObject *)ListObject_copy(list_obj);
        PyObject * obj_list = (PyObject *)list_obj;
        PyObject** args = &obj_list;
        for(int i = 1; i< elems; ++i){
            ObjectT_extend<ListObject>(list_obj_copy, args, 1);
        }

        return list_obj_copy;
    }
    
    shared_py_object<ListObject*> makeDB0ListInternal(db0::swine_ptr<Fixture> &fixture,
        PyObject *const *args, Py_ssize_t nargs)
    {
        auto py_list = ListObject_new(&ListObjectType, NULL, NULL);
        db0::FixtureLock lock(fixture);
        auto &list = py_list->modifyExt();
        db0::object_model::List::makeNew(&list, *lock);
        if (nargs == 1) {
            ObjectT_extend<ListObject>(py_list, args, nargs);
        }
        // register newly created list with py-object cache
        fixture->getLangCache().add(list.getAddress(), py_list);
        return { py_list, false };
    }

    ListObject *makeListInternal(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {        
        if (nargs != 1 && nargs != 0) {
            PyErr_SetString(PyExc_TypeError, "list() takes exactly one or zero argument");
            return NULL;
        }

        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return makeDB0ListInternal(fixture, args, nargs).steal();
    }
    
    PyObject *PyAPI_ListObject_GetItemSlice(ListObject *py_src_list, PyObject *elem)
    {
        PY_API_FUNC
        // FIXME: this operation should be immutable
        db0::FixtureLock lock(py_src_list->ext().getFixture());
        // Check if the key is a slice object
        if (PySlice_Check(elem)) {
            Py_ssize_t start, stop, step;
            PySlice_GetIndices(elem, py_src_list->ext().size(), &start, &stop, &step);
            auto py_list = makeListInternal(nullptr, nullptr, 0);
            auto compare = [step](Py_ssize_t i, Py_ssize_t stop) {
                if (step > 0) {
                    return i < stop; 
                } else {
                    return i > stop;
                }
            };

            auto &list = py_list->modifyExt();
            for (Py_ssize_t i = start; compare(i, stop); i += step) {
                list.append(lock, py_src_list->ext().getItem(i).steal());
            }

            return py_list;
        }
        auto index = PyLong_AsLong(elem);
        if (index < 0) {
            index += py_src_list->ext().size();
        }
        return py_src_list->ext().getItem(index).steal();
    }
    
    PyObject *PyAPI_ListObject_clear(ListObject *py_list)
    {
        PY_API_FUNC
        db0::FixtureLock lock(py_list->ext().getFixture());
        py_list->modifyExt().clear(lock);
        Py_RETURN_NONE;
    }
    
    PyObject *PyAPI_ListObject_count(ListObject *py_list, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC    
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "count() takes one argument.");
            return NULL;
        }
        return PyLong_FromLong(py_list->ext().count(args[0]));        
    }
    
    PyObject *PyAPI_ListObject_add(ListObject *list_obj_lh, ListObject *list_obj_rh)
    {
        PY_API_FUNC
        //make copy of first list
        PyObject * obj_list = (PyObject *)list_obj_rh;
        PyObject** args = &obj_list;
        ListObject *lh_copy = (ListObject *)ListObject_copy(list_obj_lh);
        ObjectT_extend<ListObject>(lh_copy, args, 1);
        return lh_copy;
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

    static PyObject *PyAPI_ListObject_rq(ListObject *list_obj, PyObject *other, int op) 
    {
        PY_API_FUNC
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
        .tp_dealloc = (destructor)PyAPI_ListObject_del,
        .tp_as_number = &ListObject_as_num,
        .tp_as_sequence = &ListObject_sq,
        .tp_as_mapping = &ListObject_mp,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero indexed collection object",
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

    shared_py_object<ListObject*> makeDB0List(db0::swine_ptr<Fixture> &fixture, PyObject *const *args, Py_ssize_t nargs) {
        return makeDB0ListInternal(fixture, args, nargs);
    }
    
    PyObject *PyAPI_makeList(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return makeListInternal(self, args, nargs);        
    }

}