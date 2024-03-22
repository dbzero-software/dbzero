
#pragma once
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/object_model/ObjectBase.hpp>

namespace db0::object_model
{
    struct [[gnu::packed]] o_datetime: public db0::o_fixed<o_datetime>
    {
        std::time_t m_timestamp;
        u_int8_t m_timezone;

        o_datetime() = default;
        o_datetime(std::time_t timestamp, int8_t timezone)
            : m_timestamp(timestamp)
            , m_timezone(timezone)
    {}
    };

     class DateTime: public db0::ObjectBase<DateTime, v_object<o_datetime>, StorageClass::DB0_DATETIME>
    {
        GC0_Declare
        
        public:
            using super_t = db0::ObjectBase<DateTime, v_object<o_datetime>, StorageClass::DB0_DATETIME>;
            using LangToolkit = db0::python::PyToolkit;
            using ObjectPtr = typename LangToolkit::ObjectPtr;
            using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;

            int getYear() const;
            uint8_t getMonth() const;
            uint8_t getDay() const; 
            uint8_t getHour() const;
            uint8_t getMinute() const;
            uint8_t getSecond() const;
            uint8_t getMillisecond() const;
            uint8_t getMicrosecond() const;
            uint8_t getNanosecond() const;
            uint8_t getTimezone() const;

            void setYear(int year);
            void setMonth(uint8_t month);
            void setDay(uint8_t day);
            void setHour(uint8_t hour);
            void setMinute(uint8_t minute);
            void setSecond(uint8_t second);
            void setMillisecond(uint8_t millisecond);
            void setMicrosecond(uint8_t microsecond);
            void setNanosecond(uint8_t nanosecond);
            void setTimezone(uint8_t timezone);
            
            static DateTime* makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture, ObjectPtr year, ObjectPtr month, ObjectPtr day, 
                         ObjectPtr hour = nullptr, ObjectPtr minute = nullptr, ObjectPtr second = nullptr, 
                         ObjectPtr millisecond  = nullptr, ObjectPtr microsecond = nullptr, ObjectPtr nanosecond = nullptr, 
                         ObjectPtr timezone  = nullptr);

            static DateTime* makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture, size_t timestamp, int8_t timezone);

        private:
            // new Tuples can only be created via factory members
            DateTime(db0::swine_ptr<Fixture> &, size_t timestamp, int8_t timezone);
            DateTime(db0::swine_ptr<Fixture> &, std::uint64_t address);
            
    };
    
}
