#include "DateTime.hpp"

namespace db0::object_model
{

    GC0_Define(DateTime)

    DateTime::DateTime(db0::swine_ptr<Fixture> &fixture, size_t timestamp, int8_t timezone)
        : super_t(fixture, timestamp, timezone)
    {
    }

    // Generate getters
    int DateTime::getYear() const {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        return timeinfo_local->tm_year + 1900;
    }

    std::uint8_t DateTime::getMonth() const {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        return timeinfo_local->tm_mon + 1;
    }

    std::uint8_t DateTime::getDay() const {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        return timeinfo_local->tm_mday;
    }

    std::uint8_t DateTime::getHour() const {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        return timeinfo_local->tm_hour;
    }

    std::uint8_t DateTime::getMinute() const {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        return timeinfo_local->tm_min;
    }

    std::uint8_t DateTime::getSecond() const {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        return timeinfo_local->tm_sec;
    }

    std::uint8_t DateTime::getMillisecond() const {
        //FIXME: not implemented
        return 0;
    }

    std::uint8_t DateTime::getMicrosecond() const {
        //FIXME: not implemented
        return 0;
    }

    std::uint8_t DateTime::getNanosecond() const {
        //FIXME: not implemented
        return 0;
    }

    std::uint8_t DateTime::getTimezone() const {
        return (*this)->m_timezone;
    }

    void DateTime::setYear(int year) {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        timeinfo_local->tm_year = year - 1900;
        modify().m_timestamp = std::mktime(timeinfo_local);
    }

    void DateTime::setMonth(std::uint8_t month) {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        timeinfo_local->tm_mon = month - 1;
        modify().m_timestamp = std::mktime(timeinfo_local);
    }

    void DateTime::setDay(std::uint8_t day) {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        timeinfo_local->tm_mday = day;
        modify().m_timestamp = std::mktime(timeinfo_local);
    }

    void DateTime::setHour(std::uint8_t hour) {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        timeinfo_local->tm_hour = hour;
        modify().m_timestamp = std::mktime(timeinfo_local);
    }

    void DateTime::setMinute(std::uint8_t minute) {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        timeinfo_local->tm_min = minute;
        modify().m_timestamp = std::mktime(timeinfo_local);
    }

    void DateTime::setSecond(std::uint8_t second) {
        std::tm* timeinfo_local = std::localtime(&(*this)->m_timestamp);
        timeinfo_local->tm_sec = second;
        modify().m_timestamp = std::mktime(timeinfo_local);
    }

    void DateTime::setMillisecond(std::uint8_t millisecond) {
        //FIXME: not implemented
    }

    void DateTime::setMicrosecond(std::uint8_t microsecond) {
        //FIXME: not implemented
    }

    void DateTime::setNanosecond(std::uint8_t nanosecond) {
        //FIXME: not implemented
    }

    void DateTime::setTimezone(std::uint8_t timezone) {
        modify().m_timezone = timezone;
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
        std::tm tm{};
        tm.tm_year = PyLong_AsLongWithDefault(year) - 1900;
        tm.tm_mon = PyLong_AsLongWithDefault(month) - 1;
        tm.tm_mday = PyLong_AsLongWithDefault(day);
        tm.tm_hour = PyLong_AsLongWithDefault(hour);
        tm.tm_min = PyLong_AsLongWithDefault(minute);
        tm.tm_sec = PyLong_AsLongWithDefault(second);
        std::time_t time_t_value = std::mktime(&tm);

        // Check if mktime was successful
        if (time_t_value == -1) {
            THROWF(db0::InputException) << "Invalid date";
            return nullptr;
        }
        return new (at_ptr) DateTime(fixture, time_t_value, PyLong_AsLongWithDefault(timezone));
    }

    DateTime* DateTime::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture, size_t timestamp, int8_t timezone)
    {
        return new (at_ptr) DateTime(fixture, timestamp, timezone);
    }
}