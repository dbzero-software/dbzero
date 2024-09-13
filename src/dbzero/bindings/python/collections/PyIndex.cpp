#include "PyIndex.hpp"
#include <dbzero/bindings/python/PyObjectIterator.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>

namespace db0::python

{

    static PyMethodDef IndexObject_methods[] = {
        {"add", (PyCFunction)IndexObject_add, METH_FASTCALL, "Add item to index."},
        {"remove", (PyCFunction)IndexObject_remove, METH_FASTCALL, "Remove item from index if it exists."},
        {"sort", (PyCFunction)IndexObject_sort, METH_VARARGS | METH_KEYWORDS, "Sort results of other iterator."},
        {"range", (PyCFunction)IndexObject_range, METH_VARARGS | METH_KEYWORDS, "Extract values from a specific range"},
        {"flush", (PyCFunction)IndexObject_flush, METH_NOARGS, "Flush buffered changes"},
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
    
    IndexObject *IndexObject_newInternal(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<IndexObject*>(type->tp_alloc(type, 0));
    }

    IndexObject *IndexObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return IndexObject_newInternal(type, NULL, NULL);
    }

    shared_py_object<IndexObject*> IndexDefaultObject_new() {
        return { IndexObject_new(&IndexObjectType, NULL, NULL), false };
    }

    void IndexObject_del(IndexObject* index_obj)
    {
        // destroy associated DB0 Index instance
        index_obj->destroy();
        Py_TYPE(index_obj)->tp_free((PyObject*)index_obj);
    }
    
    Py_ssize_t IndexObject_len(IndexObject *index_obj)
    {
        std::lock_guard api_lock(py_api_mutex);
        index_obj->ext().getFixture()->refreshIfUpdated();
        return index_obj->ext().size();
    }
    
    IndexObject *makeIndex(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        std::lock_guard api_lock(py_api_mutex);
        if (nargs != 0) {
            PyErr_SetString(PyExc_TypeError, "Index object does not accept arguments");
            return NULL;
        }

        // make actual DBZero instance, use default fixture
        auto index_object = IndexObject_newInternal(&IndexObjectType, NULL, NULL);
        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        db0::object_model::Index::makeNew(&index_object->modifyExt(), *lock);
        // register newly created index with py-object cache
        lock->getLangCache().add(index_object->ext().getAddress(), index_object);
        return index_object;
    }
    
    PyObject *IndexObject_add(IndexObject *index_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        std::lock_guard api_lock(py_api_mutex);
        if (nargs != 2) {
            PyErr_SetString(PyExc_TypeError, "add() takes exactly two arguments");
            return NULL;
        }

        index_obj->modifyExt().add(args[0], args[1]);
        Py_RETURN_NONE;
    }

    PyObject *IndexObject_remove(IndexObject *index_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        std::lock_guard api_lock(py_api_mutex);
        if (nargs != 2) {
            PyErr_SetString(PyExc_TypeError, "remove() takes exactly two arguments");
            return NULL;
        }

        index_obj->modifyExt().remove(args[0], args[1]);
        Py_RETURN_NONE;
    }

    PyObject *IndexObject_sort(IndexObject *py_index, PyObject *args, PyObject *kwargs)
    {
        std::lock_guard api_lock(py_api_mutex);
        using ObjectIterator = db0::object_model::ObjectIterator;
        using TypedObjectIterator = db0::object_model::TypedObjectIterator;

        // extract 1 positional argument
        if (PyTuple_Size(args) != 1) {
            PyErr_SetString(PyExc_TypeError, "sort() takes exactly one positional argument");
            return NULL;
        }

        PyObject *py_desc = nullptr;
        PyObject *py_null_first = nullptr;
        // extract optional keyword arugment "desc" (default sort is ascending)
        if (kwargs != NULL) {
            py_desc = PyDict_GetItemString(kwargs, "desc");
            py_null_first = PyDict_GetItemString(kwargs, "null_first");
        }
        
        bool asc = py_desc ? !PyObject_IsTrue(py_desc) : true;
        bool null_first = py_null_first ? PyObject_IsTrue(py_null_first) : false;
        PyObject *py_iter = PyTuple_GetItem(args, 0);
        // sort results of a full-text iterator        
        if (!PyObjectIterator_Check(py_iter)) {
            PyErr_SetString(PyExc_TypeError, "sort() takes ObjectIterator as an argument");
            return NULL;
        }

        auto &index = py_index->ext();
        auto &iter = reinterpret_cast<PyObjectIterator*>(py_iter)->ext();
        auto iter_obj = PyObjectIteratorDefault_new();
        
        auto iter_sorted = index.sort(*iter, asc, null_first);
        if (iter.isTyped()) {
            auto typed_iter = std::unique_ptr<TypedObjectIterator>(new TypedObjectIterator(
                iter->getFixture(), std::move(iter_sorted), iter.m_typed_iterator_ptr->getType(), {}, iter->getFilters())
            );
            Iterator::makeNew(&(iter_obj.get())->modifyExt(), std::move(typed_iter));
        } else {
            auto _iter = std::unique_ptr<ObjectIterator>(new ObjectIterator(
                iter->getFixture(), std::move(iter_sorted), {}, iter->getFilters())
            );
            Iterator::makeNew(&(iter_obj.get())->modifyExt(), std::move(_iter));
        }

        return iter_obj.steal();
    }

    PyObject *IndexObject_range(IndexObject *py_index, PyObject *args, PyObject *kwargs)
    {
        std::lock_guard api_lock(py_api_mutex);
        // optional low, optional high, optional null_first (boolean)
        static const char *kwlist[] = {"low", "high", "null_first", NULL};
        PyObject *low = NULL, *high = NULL;
        int null_first = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOp", const_cast<char**>(kwlist), &low, &high, &null_first)) {
            return NULL;
        }
        
        auto &index = py_index->ext();
        // construct range iterator        
        auto iter_factory = index.range(low, high, null_first);
        auto iter = std::make_unique<db0::object_model::ObjectIterator>(index.getFixture(), std::move(iter_factory));
        auto py_iter_obj = PyObjectIteratorDefault_new();
        Iterator::makeNew(&(py_iter_obj.get())->modifyExt(), std::move(iter));
        return py_iter_obj.steal();
    }
    
    bool IndexObject_Check(PyObject *py_object) {
        return PyObject_TypeCheck(py_object, &IndexObjectType);
    }
    
    PyObject *IndexObject_flush(IndexObject *self)
    {
        std::lock_guard api_lock(py_api_mutex);
        self->modifyExt().flush();
        Py_RETURN_NONE;
    }

}