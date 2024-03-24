#include "Index.hpp"
#include "PyObjectIterator.hpp"
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python

{

    static PyMethodDef IndexObject_methods[] = {
        {"add", (PyCFunction)IndexObject_add, METH_FASTCALL, "Add item to index."},
        {"sort", (PyCFunction)IndexObject_sort, METH_FASTCALL, "Sort results of other iterator."},
        {"range", (PyCFunction)IndexObject_range, METH_FASTCALL, "Construct a range filter"},
        {NULL}
    };

    static PySequenceMethods IndexObject_sq = {
        .sq_length = (lenfunc)IndexObject_len
    };

    PyTypeObject IndexObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Index",
        .tp_basicsize = IndexObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)IndexObject_del,
        .tp_as_sequence = &IndexObject_sq,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero indexing object",
        .tp_methods = IndexObject_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)IndexObject_new,
        .tp_free = PyObject_Free,   
    };
    
    IndexObject *IndexObject_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<IndexObject*>(type->tp_alloc(type, 0));
    }

    IndexObject *IndexDefaultObject_new()
    {
        return IndexObject_new(&IndexObjectType, NULL, NULL);
    }

    void IndexObject_del(IndexObject* index_obj)
    {
        // destroy associated DB0 Index instance
        index_obj->ext().~Index();
        Py_TYPE(index_obj)->tp_free((PyObject*)index_obj);
    }
    
    Py_ssize_t IndexObject_len(IndexObject *index_obj)
    {
        index_obj->ext().getFixture()->refreshIfUpdated();
        return index_obj->ext().size();
    }
    
    IndexObject *makeIndex(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 0) {
            PyErr_SetString(PyExc_TypeError, "Index object does not accept arguments");
            return NULL;
        }

        // make actual DBZero instance, use default fixture
        auto index_object = IndexObject_new(&IndexObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        db0::object_model::Index::makeNew(&index_object->ext(), *fixture);
        // register newly created index with py-object cache
        (*fixture)->getLangCache().add(index_object->ext().getAddress(), index_object, true);
        return index_object;
    }
    
    PyObject *IndexObject_add(IndexObject *index_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 2) {
            PyErr_SetString(PyExc_TypeError, "add() takes exactly two arguments");
            return NULL;
        }

        index_obj->ext().add(args[0], args[1]);
        Py_RETURN_NONE;
    }

    PyObject *IndexObject_sort(IndexObject *py_index, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "sort() takes exactly one argument");
            return NULL;
        }
        
        // sort results of a full-text iterator
        auto iter = args[0];
        if (!ObjectIterator_Check(iter) && !TypedObjectIterator_Check(iter)) {
            PyErr_SetString(PyExc_TypeError, "sort() takes ObjectIterator or TypedObjectIterator as argument");
            return NULL;
        }

        auto &index = py_index->ext(); 
        if (TypedObjectIterator_Check(iter)) {
            auto &typed_iter = reinterpret_cast<PyTypedObjectIterator*>(iter)->ext();
            auto iter_obj = PyTypedObjectIterator_new(&PyTypedObjectIteratorType, NULL, NULL);
            index.sort(typed_iter, &iter_obj->ext());
            return iter_obj;
        } else {
            auto &obj_iter = reinterpret_cast<PyObjectIterator*>(iter)->ext();
            
            // construct sorted result iterator
            auto iter_obj = PyObjectIterator_new(&PyObjectIteratorType, NULL, NULL);
            index.sort(obj_iter, &iter_obj->ext());
            return iter_obj;
        }
    }

    PyObject *IndexObject_range(IndexObject *py_index, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs < 1 || nargs > 2) {
            PyErr_SetString(PyExc_TypeError, "range() takes 1 or 2 arguments");
            return NULL;
        }

        auto &index = py_index->ext();
        // construct range iterator
        auto iter_obj = PyObjectIterator_new(&PyObjectIteratorType, NULL, NULL);
        index.range(&iter_obj->ext(), args[0], nargs > 1 ? args[1] : NULL);
        return iter_obj;
    }
    
    bool IndexObject_Check(PyObject *py_object)
    {
        return PyObject_TypeCheck(py_object, &IndexObjectType);
    }
    
}