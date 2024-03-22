#include "DateTime.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <structmember.h>
// 
namespace db0::python
{

    static PyMethodDef DateTimeObject_methods[] = {
        {NULL}
    };


    int DateTimeObject_setattro(DateTimeObject *self, PyObject *attr, PyObject *value)
    {
        // assign value to a DB0 attribute
        Py_XINCREF(value);
        try {
            if (PyUnicode_CompareWithASCIIString(attr, "year") == 0) {
                self->ext().setYear(PyLong_AsLong(value));
            } else if (PyUnicode_CompareWithASCIIString(attr, "month") == 0) {
                self->ext().setMonth(PyLong_AsLong(value));
            } else if (PyUnicode_CompareWithASCIIString(attr, "day") == 0) {
                self->ext().setDay(PyLong_AsLong(value));
            } else if (PyUnicode_CompareWithASCIIString(attr, "hour") == 0) {
                self->ext().setHour(PyLong_AsLong(value));
            } else if (PyUnicode_CompareWithASCIIString(attr, "minute") == 0) {
                self->ext().setMinute(PyLong_AsLong(value));
            } else if (PyUnicode_CompareWithASCIIString(attr, "second") == 0) {
                self->ext().setSecond(PyLong_AsLong(value));
            } else if (PyUnicode_CompareWithASCIIString(attr, "millisecond") == 0) {
                self->ext().setMillisecond(PyLong_AsLong(value));
            } else if (PyUnicode_CompareWithASCIIString(attr, "microsecond") == 0) {
                self->ext().setMicrosecond(PyLong_AsLong(value));
            } else if (PyUnicode_CompareWithASCIIString(attr, "nanosecond") == 0) {
                self->ext().setNanosecond(PyLong_AsLong(value));
            } else if (PyUnicode_CompareWithASCIIString(attr, "timezone") == 0) {
                self->ext().setTimezone(PyLong_AsLong(value));
            } else {
                PyErr_SetString(PyExc_AttributeError, "attribute not found");
                return -1;
            }
        } catch (const std::exception &e) {
            Py_XDECREF(value);
            PyErr_SetString(PyExc_AttributeError, e.what());
            return -1;
        }
        Py_XDECREF(value);
        return 0;
    }

    PyObject* DateTimeObject_getattro(DateTimeObject *self, PyObject *attr)
    {
        // get value of a DB0 attribute
        PyObject* value;
        if (PyUnicode_CompareWithASCIIString(attr, "year") == 0) {
            value = PyLong_FromLong(self->ext().getYear());
        } else if (PyUnicode_CompareWithASCIIString(attr, "month") == 0) {
            value = PyLong_FromLong(self->ext().getMonth());
        } else if (PyUnicode_CompareWithASCIIString(attr, "day") == 0) {
            value = PyLong_FromLong(self->ext().getDay());
        } else if (PyUnicode_CompareWithASCIIString(attr, "hour") == 0) {
            value = PyLong_FromLong(self->ext().getHour());
        } else if (PyUnicode_CompareWithASCIIString(attr, "minute") == 0) {
            value = PyLong_FromLong(self->ext().getMinute());
        } else if (PyUnicode_CompareWithASCIIString(attr, "second") == 0) {
            value = PyLong_FromLong(self->ext().getSecond());
        } else if (PyUnicode_CompareWithASCIIString(attr, "millisecond") == 0) {
            value = PyLong_FromLong(self->ext().getMillisecond());
        } else if (PyUnicode_CompareWithASCIIString(attr, "microsecond") == 0) {
            value = PyLong_FromLong(self->ext().getMicrosecond());
        } else if (PyUnicode_CompareWithASCIIString(attr, "nanosecond") == 0) {
            value = PyLong_FromLong(self->ext().getNanosecond());
        } else if (PyUnicode_CompareWithASCIIString(attr, "timezone") == 0) {
            value = PyLong_FromLong(self->ext().getTimezone());
        } else { 
            PyErr_SetString(PyExc_AttributeError, "attribute not found");
            return nullptr;
        }
        return value;
    }

    PyTypeObject DateTimeObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.DateTime",
        .tp_basicsize = DateTimeObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)DateTimeObject_del,
        .tp_getattro = (getattrofunc)DateTimeObject_getattro,
        .tp_setattro = (setattrofunc)DateTimeObject_setattro,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero datetime",
        .tp_methods = DateTimeObject_methods,       
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)DateTimeObject_new,
        .tp_free = PyObject_Free
    };

    DateTimeObject *DateTimeObject_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<DateTimeObject*>(type->tp_alloc(type, 0));
    }

    DateTimeObject *DateTimeDefaultObject_new()
    {   
        return DateTimeObject_new(&DateTimeObjectType, NULL, NULL);
    }
    
    void DateTimeObject_del(DateTimeObject* datetime_obj)
    {
        // destroy associated DB0 DateTime instance
        datetime_obj->ext().~DateTime();
        Py_TYPE(datetime_obj)->tp_free((PyObject*)datetime_obj);
    }

    DateTimeObject *makeDateTime(PyObject *, PyObject* args, PyObject* kwargs)
    {
        if(PyObject_Length(args)  != 3) {
            PyErr_SetString(PyExc_TypeError, "datetime() takes exacly 3 positional argumetns arguments");
            return NULL;
        }
        // make actual DBZero instance, use default fixture
        auto datetime_object = DateTimeObject_new(&DateTimeObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        // parse args
        auto * year = PyTuple_GetItem(args, 0);
        auto * month = PyTuple_GetItem(args, 1);
        auto * day = PyTuple_GetItem(args, 2);
        // parse kwargs
        if(kwargs != NULL) {
            PyObject *hour = PyDict_GetItemString(kwargs, "hour");
            PyObject *minute = PyDict_GetItemString(kwargs, "minute");
            PyObject *second = PyDict_GetItemString(kwargs, "second");
            PyObject *millisecond = PyDict_GetItemString(kwargs, "millisecond");
            PyObject *microsecond = PyDict_GetItemString(kwargs, "microsecond");
            PyObject *nanosecond = PyDict_GetItemString(kwargs, "nanosecond");
            PyObject *timezone = PyDict_GetItemString(kwargs, "timezone");
            db0::object_model::DateTime::makeNew(&datetime_object->ext(), *fixture, year, month, day, hour, minute, 
                                                 second, millisecond, microsecond, nanosecond, timezone);
        } else {
            db0::object_model::DateTime::makeNew(&datetime_object->ext(), *fixture, year, month, day);
        }
        // register newly created datetime with py-object cache
        (*fixture)->getLangCache().add(datetime_object->ext().getAddress(), datetime_object, true);
        return datetime_object;
    }
    
    bool DateTimeObject_Check(PyObject *object)
    {
        return Py_TYPE(object) == &DateTimeObjectType;        
    }

}