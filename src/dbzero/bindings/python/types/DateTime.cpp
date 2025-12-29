// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "DateTime.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <structmember.h>
#include <dbzero/bindings/python/Utils.hpp>
#include <datetime.h>
#include <dbzero/bindings/python/types/ByteUtils.hpp>
namespace db0::python

{

    void init_datetime() 
    {
        if (!PyDateTimeAPI) {
            PyDateTime_IMPORT;
        }
    }

    // HELPER METHODS

    std::pair<int, int> get_utc_offset(PyObject* tzinfo, PyObject* datetime_instance) 
    {
        if (!tzinfo || !PyObject_HasAttrString(tzinfo, "utcoffset")) {
            PyErr_Print();
            throw std::runtime_error("tzinfo must have a utcoffset method.");
        }

        // Call tzinfo.utcoffset(datetime_instance)
        auto offset = Py_OWN(PyObject_CallMethod(tzinfo, "utcoffset", "O", datetime_instance));
        if (!offset) {            
            THROWF(db0::InputException) << "Failed to call utcoffset method.";
        }

        // Ensure the result is a timedelta object
        if (!PyDelta_Check(*offset)) {
            THROWF(db0::InputException) << "utcoffset must return a timedelta object.";
        }

        // Extract the total seconds from the timedelta object and return as hours
        auto hours = PyDateTime_DELTA_GET_SECONDS(*offset)/3600;
        auto days = PyDateTime_DELTA_GET_DAYS(*offset);
        return std::make_pair(days, hours);
    }

    PyObject *get_tz_info(PyObject * py_datetime)
    {
        if (((PyDateTime_DateTime *)(py_datetime))->hastzinfo) {
            return ((PyDateTime_DateTime *)(py_datetime))->tzinfo;
        }
        Py_RETURN_NONE;
    }
    
    bool isDatatimeWithTZ(PyObject *py_datetime)
    {
        return ((PyDateTime_DateTime *)(py_datetime))->hastzinfo;
    }

    // TIMEZONE WITHOUT TZ
    
    std::uint64_t pyDateTimeToToUint64(PyObject *py_datetime) 
    {
        init_datetime();
        std::uint64_t datetime = 0;
        set_bytes(datetime, 46, 16, PyDateTime_GET_YEAR(py_datetime));
        set_bytes(datetime, 42, 4, PyDateTime_GET_MONTH(py_datetime));
        set_bytes(datetime, 37, 5, PyDateTime_GET_DAY(py_datetime));
        set_bytes(datetime, 32, 5, PyDateTime_DATE_GET_HOUR(py_datetime));
        set_bytes(datetime, 26, 6, PyDateTime_DATE_GET_MINUTE(py_datetime));
        set_bytes(datetime, 20, 6, PyDateTime_DATE_GET_SECOND(py_datetime));
        set_bytes(datetime, 0, 20, PyDateTime_DATE_GET_MICROSECOND(py_datetime));
        return datetime;
    }
    
    PyObject *uint64ToPyDatetime(std::uint64_t datetime)
    {
        init_datetime();
        auto year = get_bytes(datetime, 46, 16);
        auto month = get_bytes(datetime, 42, 4);
        auto day = get_bytes(datetime, 37, 5);
        auto hour = get_bytes(datetime, 32, 5);
        auto minute = get_bytes(datetime, 26, 6);
        auto second = get_bytes(datetime, 20, 6);
        auto microseconds = get_bytes(datetime, 0, 20);
        return PyDateTime_FromDateAndTime(year, month, day, hour, minute, second, microseconds);
    }

    // TIMEZONE WITH TZ

    std::uint64_t pyDateTimeWithTzToUint64(PyObject *py_datetime) 
    {
        init_datetime();
        auto timezone = get_tz_info(py_datetime);
        std::pair<int,int> offset = {0,0};
        if (timezone == Py_None){
            throw std::runtime_error("Datetime with timezone must have tzinfo");
        }
        offset = get_utc_offset(timezone, py_datetime);

        auto tz_days = offset.first;
        // tz_days can only by -1 or 0 so use 1 to indicate -1 to save bites
        if(tz_days != -1 && tz_days != 0){
            throw std::runtime_error("Invalid timezone days: should be -1 or 0");
        }
        if(tz_days == -1){
            tz_days = 1;
        }
        std::uint64_t datetime = 0;
        set_bytes(datetime, 44, 16, PyDateTime_GET_YEAR(py_datetime));
        set_bytes(datetime, 40, 4, PyDateTime_GET_MONTH(py_datetime));
        set_bytes(datetime, 35, 5, PyDateTime_GET_DAY(py_datetime));
        set_bytes(datetime, 30, 5, PyDateTime_DATE_GET_HOUR(py_datetime));
        set_bytes(datetime, 24, 6, PyDateTime_DATE_GET_MINUTE(py_datetime));
        set_bytes(datetime, 18, 6, PyDateTime_DATE_GET_SECOND(py_datetime));
        set_bytes(datetime, 8, 10, PyDateTime_DATE_GET_MICROSECOND(py_datetime)/1000);
        set_bytes(datetime, 7, 1, tz_days);
        set_bytes(datetime, 0, 7, offset.second);

        return datetime;
    }

    PyObject *uint64ToPyDatetimeWithTZ(std::uint64_t datetime)
    {
        init_datetime();
        auto year = get_bytes(datetime, 44, 16);
        auto month = get_bytes(datetime, 40, 4);
        auto day = get_bytes(datetime, 35, 5);
        auto hour = get_bytes(datetime, 30, 5);
        auto minute = get_bytes(datetime, 24, 6);
        auto second = get_bytes(datetime, 18, 6);
        auto miliseconds = get_bytes(datetime, 8, 10);
        auto tz_days = get_bytes(datetime, 7, 1);
        auto tz_hours = get_bytes(datetime, 0, 7);
        tz_hours= tz_hours & 0x3F;
        auto result = Py_OWN(PyDateTime_FromDateAndTime(year, month, day, hour, minute, second, miliseconds*1000));
        if (!result) {
            return nullptr;
        }

        if (tz_days == 1){
            tz_days = -tz_days;
        }
        auto offset = Py_OWN(PyDelta_FromDSU(tz_days, tz_hours*3600, 0));
        if (!offset) {
            return nullptr;
        }
        auto tzinfo = Py_OWN(PyTimeZone_FromOffset(*offset));
        if (!tzinfo) {
            return nullptr;
        }

        ((PyDateTime_DateTime *)(result.get()))->hastzinfo = 1;
        ((PyDateTime_DateTime *)(result.get()))->tzinfo = tzinfo.steal();
        return result.steal();
    }

    // DATE

    PyObject *uint64ToPyDate(std::uint64_t date)
    {
        init_datetime();
        auto year = get_bytes(date, 9, 16);
        auto month = get_bytes(date, 5, 4);
        auto day = get_bytes(date, 0, 5);
        return PyDate_FromDate(year, month, day);
    }

    std::uint64_t pyDateToUint64(PyObject *py_date)
    {
        init_datetime();
        // can use same method as datetime
        std::uint64_t date = 0;
        set_bytes(date, 9, 16, PyDateTime_GET_YEAR(py_date));
        set_bytes(date, 5, 4, PyDateTime_GET_MONTH(py_date));
        set_bytes(date, 0, 5, PyDateTime_GET_DAY(py_date));
        return date;
    }


    // TIME

    std::uint64_t pyTimeToUint64(PyObject *py_date)
    {
        init_datetime();
        std::uint64_t time = 0;
        set_bytes(time, 32, 5, PyDateTime_TIME_GET_HOUR(py_date));
        set_bytes(time, 26, 6, PyDateTime_TIME_GET_MINUTE(py_date));
        set_bytes(time, 20, 6, PyDateTime_TIME_GET_SECOND(py_date));
        set_bytes(time, 0, 20, PyDateTime_TIME_GET_MICROSECOND(py_date));
        return time;
    }


    PyObject *uint64ToPyTime(std::uint64_t date)
    {
        init_datetime();
        auto hour = get_bytes(date, 32, 5);
        auto minute = get_bytes(date, 26, 6);
        auto second = get_bytes(date, 20, 6);
        auto microseconds = get_bytes(date, 0, 20);
        return PyTime_FromTime(hour, minute, second, microseconds);
    }


    // TIME WITH TZ

    std::uint64_t pyTimeWithTzToUint64(PyObject *py_time)
    {
        init_datetime();
        auto timezone = get_tz_info(py_time);
        std::pair<int,int> offset = {0,0};
        if (timezone == Py_None){
            THROWF(db0::InputException) << "Datetime with timezone must have tzinfo";            
        }

        offset = get_utc_offset(timezone, py_time);
        auto tz_days = offset.first;
        // tz_days can only by -1 or 0 so use 1 to indicate -1 to save bites
        if(tz_days != -1 && tz_days != 0){
            throw std::runtime_error("Invalid timezone days: should be -1 or 0");
        }
        if(tz_days == -1){
            tz_days = 1;
        }

        std::uint64_t time_components = 0;
        set_bytes(time_components, 32, 5, PyDateTime_TIME_GET_HOUR(py_time));
        set_bytes(time_components, 26, 6, PyDateTime_TIME_GET_MINUTE(py_time));
        set_bytes(time_components, 20, 6, PyDateTime_TIME_GET_SECOND(py_time));
        set_bytes(time_components, 0, 20, PyDateTime_TIME_GET_MICROSECOND(py_time));

        set_bytes(time_components, 42, 1, tz_days);
        set_bytes(time_components, 43, 7, offset.second);
        return time_components;
    }

    PyObject *uint64ToPyTimeWithTz(std::uint64_t time)
    {
        init_datetime();
        auto hour = get_bytes(time, 0, 5);
        auto minute = get_bytes(time, 5, 6);
        auto second = get_bytes(time, 11, 6);
        auto microseconds = get_bytes(time, 17, 20);
        auto tz_days = get_bytes(time, 42, 1);
        auto tz_hours = get_bytes(time, 43, 7);
        tz_hours= tz_hours & 0x3F;
        auto result = Py_OWN(PyTime_FromTime(hour, minute, second, microseconds));
        if (tz_days == 1){
            tz_days = -tz_days;
        }
        auto offset = Py_OWN(PyDelta_FromDSU(tz_days, tz_hours*3600, 0));
        if (!offset) {
            return nullptr;
        }
        auto tzinfo = Py_OWN(PyTimeZone_FromOffset(*offset));
        if (!tzinfo) {
            return nullptr;
        }
        ((PyDateTime_DateTime *)(result.get()))->hastzinfo = 1;
        ((PyDateTime_DateTime *)(result.get()))->tzinfo = tzinfo.steal();
        return result.steal();
    }
    
}