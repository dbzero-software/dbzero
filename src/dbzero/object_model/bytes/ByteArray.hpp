#pragma once
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/ObjectBase.hpp>

namespace db0
{

    class Fixture;

}

namespace db0::object_model

{


    struct [[gnu::packed]] o_byte : public db0::o_fixed<o_byte> {
        std::byte m_byte;

        o_byte() = default;

        inline o_byte(std::byte byte)
            : m_byte(byte)
        {
        }
        
        //operators 
        bool operator==(const o_byte &other) const
        {
            return m_byte == other.m_byte;
        }

        bool operator!=(const o_byte &other) const
        {
            return m_byte != other.m_byte;
        }

    };

    using Fixture = db0::Fixture;
    
    class ByteArray: public db0::ObjectBase<ByteArray, v_bvector<o_byte>, StorageClass::DB0_BYTES_ARRAY>
    {
        GC0_Declare
        public:
            using super_t = db0::ObjectBase<ByteArray, v_bvector<o_byte>, StorageClass::DB0_BYTES_ARRAY>;
            using LangToolkit = db0::python::PyToolkit;
            using ObjectPtr = typename LangToolkit::ObjectPtr;
            using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
            
            static ByteArray *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &, std::byte *, std::size_t);
            ObjectSharedPtr getItem(std::size_t i) const;
            std::byte getByte(std::size_t i) const;
            void setItem(FixtureLock &fixture, std::size_t i, ObjectPtr lang_value);
            void append(FixtureLock &, ObjectPtr lang_value);
            size_t count(std::byte value);
            size_t count(std::byte *value, std::size_t size);
            size_t count(ByteArray& value, std::size_t size);

            // operators
            bool operator==(const ByteArray &) const;
            bool operator!=(const ByteArray &) const;

        private:
            ByteArray(db0::swine_ptr<Fixture> &, std::byte *, std::size_t);
    };
}