#pragma once

#include <cstdint>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/serialization/Ext.hpp>

namespace db0

{
    
    class Fixture;
    
    /// Common object header
    struct [[gnu::packed]] o_object_header: public o_fixed<o_object_header>
    {
        std::uint32_t m_ref_count = 0;

        o_object_header() = default;
        inline o_object_header(std::uint32_t ref_count)
            : m_ref_count(ref_count)
        {
        }

        void incRef();

        // return true if object is not referenced by any other object
        // and can be safely deleted
        bool decRef();

        inline bool hasRefs() const {
            return m_ref_count > 0;
        }
    };
    
    // Unique header for objects with unique instance id
    struct [[gnu::packed]] o_unique_header: public o_fixed_ext<o_unique_header, o_object_header>
    {
        // instance ID is decoded from object's address (see. db0::getInstanceId)
        std::uint16_t m_instance_id = 0;
        
        o_unique_header() = default;
        o_unique_header(std::uint32_t ref_count)
            : o_fixed_ext<o_unique_header, o_object_header>(ref_count)
        {
        }
    };
    
}