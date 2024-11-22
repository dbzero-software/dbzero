#include "DateTime.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <structmember.h>
#include <dbzero/bindings/python/Utils.hpp>
#include <datetime.h>
namespace db0::python

{

    void init_datetime() 
    {
        if (!PyDateTimeAPI) {
            PyDateTime_IMPORT;
        }
    }
    
    std::pair<int, int> get_utc_offset(PyObject* tzinfo, PyObject* datetime_instance) {
        if (!tzinfo || !PyObject_HasAttrString(tzinfo, "utcoffset")) {
            PyErr_Print();
            throw std::runtime_error("tzinfo must have a utcoffset method.");
        }

        // Call tzinfo.utcoffset(datetime_instance)
        PyObject* offset = PyObject_CallMethod(tzinfo, "utcoffset", "O", datetime_instance);
        if (!offset) {
            PyErr_Print();
            throw std::runtime_error("Error occurred while calling utcoffset.");
        }

        // Ensure the result is a timedelta object
        if (!PyDelta_Check(offset)) {
            Py_DECREF(offset);
            throw std::runtime_error("utcoffset must return a timedelta object.");
        }

        // Extract the total seconds from the timedelta object and return as hours
        auto hours = PyDateTime_DELTA_GET_SECONDS(offset)/3600;
        auto days = PyDateTime_DELTA_GET_DAYS(offset);
        return std::make_pair(days, hours);
    }

    std::uint64_t dateTimeComponentsToUInt64(std::uint16_t year, std::uint8_t month, std::uint8_t day, std::uint8_t hour,
        std::uint8_t minute, std::uint8_t second, std::uint8_t hast_tz, std::uint8_t timezone_days, std::uint8_t timezone_hours)
    {
        init_datetime();
        // change highest bit to indicate timezone
        auto timezone = timezone_hours & 0x003F;
        timezone = timezone | (hast_tz << 7);
        timezone = timezone | (timezone_days << 6);

        std::uint64_t datetime = 0;
        datetime = datetime | timezone;
        datetime = datetime | ((uint64_t)(second & 0x00FF) << 8);
        datetime = datetime | ((uint64_t)(minute & 0x00FF) << 16);
        datetime = datetime | ((uint64_t)(hour & 0x00FF) << 24);
        datetime = datetime | ((uint64_t)(day & 0x00FF) << 32);
        datetime = datetime | ((uint64_t)(month & 0x00FF) << 40);
        datetime = datetime | ((uint64_t)(year & 0xFFFF) << 48);
        return datetime;
    }
    PyObject * get_tz_info(PyObject * py_datetime)
    {
        if(((PyDateTime_DateTime *)(py_datetime))->hastzinfo){
            return ((PyDateTime_DateTime *)(py_datetime))->tzinfo;
        }
        return Py_None;
    }
    std::uint64_t pyDateTimeToToUint64(PyObject *py_datetime) 
    {
        init_datetime();
        auto year = PyDateTime_GET_YEAR(py_datetime);
        auto month = PyDateTime_GET_MONTH(py_datetime);
        auto day = PyDateTime_GET_DAY(py_datetime);
        auto hour = PyDateTime_DATE_GET_HOUR(py_datetime);
        auto minute = PyDateTime_DATE_GET_MINUTE(py_datetime);
        auto second = PyDateTime_DATE_GET_SECOND(py_datetime);
        auto timezone = get_tz_info(py_datetime);
        std::uint8_t has_tzinfo = 0;
        std::pair<int,int> offset = {0,0};
        if(timezone != Py_None){
            offset = get_utc_offset(timezone, py_datetime);
            has_tzinfo = 1;
        }

        auto tz_days = offset.first;
        // tz_days can only by -1 or 0 so use 1 to indicate -1 to save bites
        if(tz_days != -1 && tz_days != 0){
            throw std::runtime_error("Invalid timezone days: should be -1 or 0");
        }
        if(tz_days == -1){
            tz_days = 1;
        }
        return dateTimeComponentsToUInt64(year, month, day, hour, minute, second, has_tzinfo, tz_days, offset.second);
    }
    
    PyObject *uint64ToPyDatetime(std::uint64_t datetime)
    {
        init_datetime();
        auto year = (datetime >> 48) & 0xFFFF;
        auto month = (datetime >> 40) & 0x00FF;
        auto day = (datetime >> 32) & 0x00FF;
        auto hour = (datetime >> 24) & 0x00FF;
        auto minute = (datetime >> 16) & 0x00FF;
        auto second = (datetime >> 8) & 0x00FF;
        auto tz_hours = datetime & 0x00FF;
        auto tz_days = (datetime >> 6) & 0x01;
        tz_hours= tz_hours & 0x3F;
        auto has_tz = (datetime >> 7) & 0x01;
        auto result = PyDateTime_FromDateAndTime(year, month, day, hour, minute, second, 0);
        if (has_tz) {  
            if (tz_days == 1){
                tz_days = -tz_days;
            }
            auto offset = PyDelta_FromDSU(tz_days, tz_hours*3600, 0);
            auto tzinfo = PyTimeZone_FromOffset(offset);
            Py_DECREF(offset);
            ((PyDateTime_DateTime *)(result))->hastzinfo = 1;
            ((PyDateTime_DateTime *)(result))->tzinfo = tzinfo;
        }
        return result;
    }

}