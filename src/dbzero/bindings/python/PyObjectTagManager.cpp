#include "PyObjectTagManager.hpp"
#include "Memo.hpp"

namespace db0::python

{

    using ObjectTagManager = db0::object_model::ObjectTagManager;
    
    static PyNumberMethods PyObjectTagManager_as_num = {
        .nb_add = (binaryfunc)PyObjectTagManager_add_binary,
        .nb_subtract= (binaryfunc)PyObjectTagManager_remove_binary
    };

    static PyMethodDef PyObjectTagManager_methods[] = {
        {"add", (PyCFunction)PyObjectTagManager_add, METH_FASTCALL, "Assign tags to an instance."},
        {"remove", (PyCFunction)PyObjectTagManager_remove, METH_FASTCALL, "Remove tags from an instance."},
        {NULL}
    };

    PyObjectTagManager *PyObjectTagManager_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<PyObjectTagManager*>(type->tp_alloc(type, 0));
    }

    void PyObjectTagManager_del(PyObjectTagManager* tags_obj)
    {
        // destroy associated DB0 instance
        tags_obj->ext().~ObjectTagManager();
        Py_TYPE(tags_obj)->tp_free((PyObject*)tags_obj);
    }

    PyObject *PyObjectTagManager_add_binary(PyObjectTagManager *tag_manager, PyObject *object)
    {        
        tag_manager->ext().add(&object, 1);
        Py_INCREF(tag_manager);
        return tag_manager;
    }

    PyObject *PyObjectTagManager_add(PyObjectTagManager *tag_manager, PyObject *const *args, Py_ssize_t nargs)
    {        
        tag_manager->ext().add(args, nargs);
        Py_RETURN_NONE;
    }
    
    PyObject *PyObjectTagManager_remove_binary(PyObjectTagManager *tag_manager, PyObject *object)
    {        
        tag_manager->ext().remove(&object, 1);
        Py_INCREF(tag_manager);
        return tag_manager;
    }


    PyObject *PyObjectTagManager_remove(PyObjectTagManager *tag_manager, PyObject *const *args, Py_ssize_t nargs)
    {
        tag_manager->ext().remove(args, nargs);
        Py_RETURN_NONE;
    }

    PyTypeObject PyObjectTagManagerType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Tags",
        .tp_basicsize = PyObjectTagManager::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyObjectTagManager_del,
        .tp_as_number = &PyObjectTagManager_as_num,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero tag manager object",
        .tp_methods = PyObjectTagManager_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyObjectTagManager_new,
        .tp_free = PyObject_Free,
    };
    
    PyObjectTagManager *makeObjectTagManager(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {        
        if (nargs == 0) {
            PyErr_SetString(PyExc_TypeError, "No arguments provided");
            return NULL;
        }

        // all arguments must be Memo objects        
        for (Py_ssize_t i = 0; i < nargs; ++i) {
            if (!PyMemo_Check(args[i])) {
                PyErr_SetString(PyExc_TypeError, "All arguments must be memo objects");
                return NULL;
            }            
        }
        
        auto tags_obj = PyObjectTagManager_new(&PyObjectTagManagerType, NULL, NULL);
        ObjectTagManager::makeNew(&tags_obj->ext(), args, nargs);
        return tags_obj;
    }

}
