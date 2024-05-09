#include <Python.h>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python 
{
    template<typename ObjectT>
    PyObject *ObjectT_append(ObjectT *object_inst, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "append() takes exactly one argument");
            return NULL;
        }
        
        auto fixture = object_inst->ext().getMutableFixture();
        object_inst->ext().append(fixture, args[0]);
        Py_RETURN_NONE;
    }

    template<typename ObjectT>
    PyObject *ObjectT_extend(ObjectT *object_inst, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "extend() takes one argument.");
            return NULL;
        }
        PyObject *iterator = PyObject_GetIter(args[0]);
        PyObject *item;
        auto fixture = object_inst->ext().getMutableFixture();
        while ((item = PyIter_Next(iterator))) {
            object_inst->ext().append(fixture, item);
            Py_DECREF(item);
        }

        Py_DECREF(iterator);
        Py_RETURN_NONE;
    }

    template<typename ObjectT>
    int ObjectT_SetItem(ObjectT *object_inst, Py_ssize_t i, PyObject *value)
    {
        auto fixture = object_inst->ext().getMutableFixture();
        object_inst->ext().setItem(fixture, i, value);
        return 0;
    }

    template<typename ObjectT>
    PyObject* ObjectT_Insert(ObjectT *object_inst, PyObject *const *args, Py_ssize_t nargs)
    {
        
        if (nargs != 2) {
            PyErr_SetString(PyExc_TypeError, "insert() takes exactly two argument");
            return NULL;
        }
        if (PyLong_Check(args[0]) == 0) {
            PyErr_SetString(PyExc_TypeError, "insert() takes an integer as first argument");
            return NULL;
        }
        auto fixture = object_inst->ext().getMutableFixture();
        object_inst->ext().setItem(fixture, PyLong_AsLong(args[0]), args[1]);
        Py_RETURN_NONE;
    }

    template<typename ObjectT>
    PyObject *ObjectT_GetItem(ObjectT *object_inst, Py_ssize_t i)
    {
        object_inst->ext().getFixture()->refreshIfUpdated();
        return object_inst->ext().getItem(i).steal();
    }
    
    template<typename ObjectT>
    Py_ssize_t ObjectT_len(ObjectT *object_inst)
    {
        object_inst->ext().getFixture()->refreshIfUpdated();
        return object_inst->ext().size();
    }

    template<typename ObjectT>
    constexpr PySequenceMethods getPySequenceMehods() {
    return {
            .sq_length = (lenfunc)ObjectT_len<ObjectT>,
            .sq_item = (ssizeargfunc)ObjectT_GetItem<ObjectT>,
            .sq_ass_item = (ssizeobjargproc)ObjectT_SetItem<ObjectT>
        };
    }

    template<typename ObjectT>
    PyObject *ObjectT_pop(ObjectT *object_inst, PyObject *const *args, Py_ssize_t nargs)
    {        
        std::size_t index;
        if (nargs == 0) {
            index = object_inst->ext().size() -1;
        } else if (nargs == 1) {
            index = PyLong_AsLong(args[0]);
        } else {
            PyErr_SetString(PyExc_TypeError, "pop() takes zero or one argument.");
            return NULL;
        }
        auto fixture = object_inst->ext().getMutableFixture();
        return object_inst->ext().pop(fixture, index).steal();
    }

    template<typename ObjectT>
    PyObject *ObjectT_remove(ObjectT *object_inst, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "remove() takes one argument.");
            return NULL;
        }
        auto index = object_inst->ext().index(args[0]);
        auto fixture = object_inst->ext().getMutableFixture();
        object_inst->ext().swapAndPop(fixture, {index});
        Py_RETURN_NONE;
    }

    template<typename ObjectT>
    PyObject *ObjectT_index(ObjectT *object_inst, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "index() takes one argument.");
            return NULL;
        }
        auto index = PyLong_FromLong(object_inst->ext().index(args[0]));
        return index;
    }

}