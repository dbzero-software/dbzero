#include "DateTime.hpp"

namespace db0::object_model
{

    GC0_Define(DateTime)

    std::uint64_t dateTimeComponentsToUInt64(std::uint16_t year, std::uint8_t month, std::uint8_t day, std::uint8_t hour, 
                                    std::uint8_t minute, std::uint8_t second, std::uint8_t millisecond)
    {
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

    std::uint64_t pyDateTimeToToUint64(PyObject *py_datetime){
        auto year = PyDateTime_GET_YEAR(py_datetime);
        auto month = PyDateTime_GET_MONTH(py_datetime);
        auto day = PyDateTime_GET_DAY(py_datetime);
        auto hour = PyDateTime_DATE_GET_HOUR(py_datetime);
        auto minute = PyDateTime_DATE_GET_MINUTE(py_datetime);
        auto second = PyDateTime_DATE_GET_SECOND(py_datetime);
        auto millisecond = (PyDateTime_DATE_GET_MICROSECOND(py_datetime) >> 16) & 0x00FF;
        return dateTimeComponentsToUInt64(year, month, day, hour, minute, second, millisecond);
    }

    o_datetime::o_datetime(std::uint16_t year, std::uint8_t month, std::uint8_t day, std::uint8_t hour, std::uint8_t minute, 
                           std::uint8_t second, std::uint8_t millisecond, std::uint8_t microsecond, std::uint8_t nanosecond, 
                           std::uint8_t timezone)
    {
        if (year < 0 || year > 9999) {
            THROWF(db0::InputException) << "Invalid date: year must be between 0 and 9999";
        
        } 
        if (month < 1 || month > 12) {
            THROWF(db0::InputException) << "Invalid date: month must be between 1 and 12";
        }
        if (day < 1 || day > 31) {
            THROWF(db0::InputException) << "Invalid date: day must be between 1 and 31";
        }
        if (hour < 0 || hour > 23) {
            THROWF(db0::InputException) << "Invalid date: hour must be between 0 and 23";
        }
        if (minute < 0 || minute > 59) {
            THROWF(db0::InputException) << "Invalid date: minute must be between 0 and 59";
        }
        if (second < 0 || second > 59) {

            THROWF(db0::InputException) << "Invalid date: second must be between 0 and 59";
        }
        if (millisecond < 0 || millisecond > 99) {
            THROWF(db0::InputException) << "Invalid date: millisecond must be between 0 and 99";
        }

        m_datetime = dateTimeComponentsToUInt64(year, month, day, hour, minute, second, millisecond);
    }

    // Generate getters
    uint16_t DateTime::getYear() const {
        return ((*this)->m_datetime >> 48) & 0xFFFF;
    }

    std::uint8_t DateTime::getMonth() const {
        return ((*this)->m_datetime >> 40) & 0x00FF;
    }

    std::uint8_t DateTime::getDay() const {
        return ((*this)->m_datetime >> 32) & 0x00FF;
    }

    std::uint8_t DateTime::getHour() const {
        return ((*this)->m_datetime >> 24) & 0x00FF;
    }

    std::uint8_t DateTime::getMinute() const {
        return ((*this)->m_datetime >> 16) & 0x00FF;
    }

    std::uint8_t DateTime::getSecond() const {
        return ((*this)->m_datetime >> 8) & 0x00FF;
    }

    std::uint8_t DateTime::getMillisecond() const {
        return (*this)->m_datetime & 0x00FF;
    }

    long PyLong_AsLongWithDefault(PyObject *obj, long default_value = 0) {
        if (obj == nullptr) {
            return default_value;
        }
        return PyLong_AsLong(obj);
    }
    
    DateTime* DateTime::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture, ObjectPtr year, ObjectPtr month, ObjectPtr day, 
                         ObjectPtr hour, ObjectPtr minute, ObjectPtr second, ObjectPtr millisecond, 
                         ObjectPtr microsecond, ObjectPtr nanosecond, ObjectPtr timezone)
    {
        return new (at_ptr) DateTime(fixture, PyLong_AsLongWithDefault(year), PyLong_AsLongWithDefault(month), 
                                     PyLong_AsLongWithDefault(day), PyLong_AsLongWithDefault(hour), 
                                     PyLong_AsLongWithDefault(minute), PyLong_AsLongWithDefault(second), 
                                     PyLong_AsLongWithDefault(millisecond), PyLong_AsLongWithDefault(microsecond), 
                                     PyLong_AsLongWithDefault(nanosecond), PyLong_AsLongWithDefault(timezone));
    }

    DateTime* DateTime::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture, int16_t year, int8_t month, int8_t day, 
                         int8_t hour, int8_t minute, int8_t second, 
                         int8_t millisecond, int8_t microsecond, int8_t nanosecond, 
                         int8_t timezone)
    {
        return new (at_ptr) DateTime(fixture, year, month, day, hour, minute, second, millisecond, microsecond, nanosecond, timezone);
    }

    DateTime *DateTime::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {
        return new (at_ptr) DateTime(fixture, address);
    }

    DateTime::DateTime(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
    {
    }

    DateTime::DateTime(db0::swine_ptr<Fixture> &fixture, std::uint16_t year, std::uint8_t month, std::uint8_t day, std::uint8_t hour, std::uint8_t minute, 
                      std::uint8_t second, std::uint8_t millisecond, std::uint8_t microsecond, std::uint8_t nanosecond, std::uint8_t timezone)
        : super_t(fixture, year, month, day, hour, minute, second, millisecond, microsecond, nanosecond, timezone)
    {
    }

    bool DateTime::operator==(const DateTime &other) const
    {
        return (*this)->m_datetime == other->m_datetime;
    }

    bool DateTime::operator!=(const DateTime &other) const
    {
        return (*this)->m_datetime != other->m_datetime;
    }

    bool DateTime::operator<(const DateTime &other) const
    {
        return (*this)->m_datetime < other->m_datetime;
    }

    bool DateTime::operator>(const DateTime &other) const
    {
        return (*this)->m_datetime > other->m_datetime;
    }

    std::uint64_t DateTime::getDatetimeAsUint64() const {
        return (*this)->m_datetime;
    }
}