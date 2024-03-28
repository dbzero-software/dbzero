#include "DateTime.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <structmember.h>
#include <dbzero/bindings/python/Utils.hpp>

// 
namespace db0::python
{

    static PyMethodDef DateTimeObject_methods[] = {
        {NULL}
    };

    static PyObject *DateTimeObject_rq(PyObject *first, PyObject *other, int op) {
        PyDateTime_IMPORT;
        if(first == Py_None || other == Py_None){
            bool equal = first == other;
            if(op == Py_EQ){
                return PyBool_fromBool(equal);
            } else if (op == Py_NE){
                return PyBool_fromBool(!equal);
            } else {
                PyErr_SetString(PyExc_NotImplementedError, "Not Implemented");
                return NULL;
            }
        }
        uint64_t lhs_datetime, rhs_datetime;
        if (DateTimeObject_Check(first)){
            lhs_datetime = ((DateTimeObject*) first)->ext().getDatetimeAsUint64();
        } else if (PyDateTime_Check(first)){
            lhs_datetime = db0::object_model::pyDateTimeToToUint64(first);
        } else {
            PyErr_SetString(PyExc_NotImplementedError, "Not Implemented");
            return NULL;
        }
            
        if (DateTimeObject_Check(other)){
            rhs_datetime = ((DateTimeObject*) other)->ext().getDatetimeAsUint64();
        } else if (PyDateTime_Check(other)){
            rhs_datetime = db0::object_model::pyDateTimeToToUint64(other);
        } else {
             PyErr_SetString(PyExc_NotImplementedError, "Not Implemented");
            return NULL;
        }

        switch (op)
        {
        case Py_EQ:
            return PyBool_fromBool(lhs_datetime == rhs_datetime);
        case Py_NE:
            return PyBool_fromBool(lhs_datetime != rhs_datetime);
        case Py_LT:
            return PyBool_fromBool(lhs_datetime < rhs_datetime);
        case Py_LE:
            return PyBool_fromBool(lhs_datetime <= rhs_datetime);
        case Py_GT:
            return PyBool_fromBool(lhs_datetime > rhs_datetime);
        case Py_GE:
            return PyBool_fromBool(lhs_datetime >= rhs_datetime);
        default:
            PyErr_SetString(PyExc_NotImplementedError, "Not Implemented");
            return NULL;
        }

    }

    PyObject *date_year(DateTimeObject *datetime_obj, void *)
    {
        return PyLong_FromLong(datetime_obj->ext().getYear());
    }

    PyObject *date_month(DateTimeObject *datetime_obj, void *)
    {
        return PyLong_FromLong(datetime_obj->ext().getMonth());
    }

    PyObject *date_day(DateTimeObject *datetime_obj, void *)
    {
        return PyLong_FromLong(datetime_obj->ext().getDay());
    }

    PyObject *time_hour(DateTimeObject *datetime_obj, void *)
    {
        return PyLong_FromLong(datetime_obj->ext().getHour());
    }

    PyObject *time_minute(DateTimeObject *datetime_obj, void *)
    {
        return PyLong_FromLong(datetime_obj->ext().getMinute());
    }

    PyObject *time_second(DateTimeObject *datetime_obj, void *)
    {
        return PyLong_FromLong(datetime_obj->ext().getSecond());
    }

    PyObject *time_millisecond(DateTimeObject *datetime_obj, void *)
    {
        return PyLong_FromLong(datetime_obj->ext().getMillisecond());
    }


    static PyGetSetDef datetime_getset[] = {
        {"year",        (getter)date_year},
        {"month",       (getter)date_month},
        {"day",         (getter)date_day},
        {"hour",        (getter)time_hour},
        {"minute",      (getter)time_minute},
        {"second",      (getter)time_second},
        {"millisecond", (getter)time_millisecond},
        {NULL}
    };


    PyTypeObject DateTimeObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.DateTime",
        .tp_basicsize = DateTimeObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)DateTimeObject_del,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero datetime",
        .tp_richcompare = (richcmpfunc)DateTimeObject_rq,
        .tp_methods = DateTimeObject_methods,
        .tp_getset = datetime_getset,       
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

    DateTimeObject *makeDateTimeFromPython(PyObject* py_datetime){;
        PyDateTime_IMPORT;
        if(PyDateTime_Check(py_datetime)){
            auto datetime_object = DateTimeObject_new(&DateTimeObjectType, NULL, NULL);
            auto year = PyDateTime_GET_YEAR(py_datetime);
            auto month = PyDateTime_GET_MONTH(py_datetime);
            auto day = PyDateTime_GET_DAY(py_datetime);
            auto hour = PyDateTime_DATE_GET_HOUR(py_datetime);
            auto minute = PyDateTime_DATE_GET_MINUTE(py_datetime);
            auto second = PyDateTime_DATE_GET_SECOND(py_datetime);
            auto millisecond = (PyDateTime_DATE_GET_MICROSECOND(py_datetime) >> 16) & 0x00FF;
            auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
            db0::object_model::DateTime::makeNew(&datetime_object->ext(), *fixture, year, month, day, hour, minute, second, millisecond);
            (*fixture)->getLangCache().add(datetime_object->ext().getAddress(), datetime_object, true);
            return datetime_object;
        } else {
            PyErr_SetString(PyExc_TypeError, "Expected datetime object");
            return NULL;
        }
    }

    bool DateTimeObject_Check(PyObject *object)
    {
        return Py_TYPE(object) == &DateTimeObjectType;        
    }

}