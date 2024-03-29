
#pragma once
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/object_model/ObjectBase.hpp>

namespace db0::object_model
{

    std::uint64_t pyDateTimeToToUint64(PyObject *datetime);

    struct [[gnu::packed]] o_datetime: public db0::o_fixed<o_datetime>
    {
        
        // common object header
        o_object_header m_header;

        // Datetime stored in bytes:
        // 0: milisecond 0 - 99
        // 1: second 0 - 59
        // 2: minute 0 - 59
        // 3: hour 0 - 23
        // 4: day 1 - 31
        // 5: month 1 - 12
        // 6: year 0 - 9999

        std::uint64_t m_datetime = 0;

        o_datetime() = default;
        o_datetime(std::uint16_t year, std::uint8_t month, std::uint8_t day, std::uint8_t hour, std::uint8_t minute, 
                   std::uint8_t second, std::uint8_t millisecond, std::uint8_t microsecond, std::uint8_t nanosecond, 
                   std::uint8_t timezone);
    };

     class DateTime: public db0::ObjectBase<DateTime, v_object<o_datetime>, StorageClass::DB0_DATETIME>
    {
        GC0_Declare
        
        public:
            using super_t = db0::ObjectBase<DateTime, v_object<o_datetime>, StorageClass::DB0_DATETIME>;
            using LangToolkit = db0::python::PyToolkit;
            using ObjectPtr = typename LangToolkit::ObjectPtr;
            using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;

            DateTime(db0::swine_ptr<Fixture> &, std::uint64_t address);

            uint16_t getYear() const;
            uint8_t getMonth() const;
            uint8_t getDay() const; 
            uint8_t getHour() const;
            uint8_t getMinute() const;
            uint8_t getSecond() const;
            uint8_t getMillisecond() const;
            
            static DateTime* makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture, ObjectPtr year, ObjectPtr month, ObjectPtr day, 
                         ObjectPtr hour = nullptr, ObjectPtr minute = nullptr, ObjectPtr second = nullptr, 
                         ObjectPtr millisecond  = nullptr, ObjectPtr microsecond = nullptr, ObjectPtr nanosecond = nullptr, 
                         ObjectPtr timezone  = nullptr);

            static DateTime* makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture, int16_t year, int8_t month, int8_t day, 
                         int8_t hour = 0, int8_t minute = 0, int8_t second = 0, 
                         int8_t millisecond  = 0, int8_t microsecond = 0, int8_t nanosecond = 0, 
                         int8_t timezone  = 0);

            static DateTime *unload(void *at_ptr, db0::swine_ptr<Fixture> &, std::uint64_t address);

            bool operator==(const DateTime &other) const;
            bool operator!=(const DateTime &other) const;
            bool operator<(const DateTime &other) const;
            bool operator>(const DateTime &other) const;

            std::uint64_t getDatetimeAsUint64() const;

        private:
            // new Tuples can only be created via factory members
            DateTime(db0::swine_ptr<Fixture> &, uint16_t year, uint8_t month, uint8_t day, uint8_t hour, uint8_t minute, 
                     uint8_t second, uint8_t millisecond, uint8_t microsecond, uint8_t nanosecond, uint8_t timezone);

            
    };
    
}
