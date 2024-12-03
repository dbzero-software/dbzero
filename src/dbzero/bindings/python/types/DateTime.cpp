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
    
    std::uint64_t dateTimeComponentsToUInt64(std::uint16_t year, std::uint8_t month, std::uint8_t day, std::uint8_t hour,
        std::uint8_t minute, std::uint8_t second, std::uint8_t millisecond)
    {
        init_datetime();
        std::uint64_t datetime = 0;
        datetime = datetime | (millisecond & 0x00FF);
        datetime = datetime | ((uint64_t)(second & 0x00FF) << 8);
        datetime = datetime | ((uint64_t)(minute & 0x00FF) << 16);
        datetime = datetime | ((uint64_t)(hour & 0x00FF) << 24);
        datetime = datetime | ((uint64_t)(day & 0x00FF) << 32);
        datetime = datetime | ((uint64_t)(month & 0x00FF) << 40);
        datetime = datetime | ((uint64_t)(year & 0xFFFF) << 48);
        return datetime;
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
        auto millisecond = (PyDateTime_DATE_GET_MICROSECOND(py_datetime) >> 16) & 0x00FF;
        return dateTimeComponentsToUInt64(year, month, day, hour, minute, second, millisecond);
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
        auto millisecond = datetime & 0x00FF;
        return PyDateTime_FromDateAndTime(year, month, day, hour, minute, second, millisecond);
    }

}