#pragma once

#include <cstdint>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/serialization/ref_counter.hpp>

namespace db0

{
    
    class Fixture;
    
    /// Common object header
    struct [[gnu::packed]] o_object_header: public o_fixed<o_object_header>
    {
        using RefCounterT = o_ref_counter<std::uint32_t, 6>;
        // ref-counter to hold tags / objects reference counts separately
        RefCounterT m_ref_counter;

        o_object_header() = default;

        inline o_object_header(const RefCounterT &ref_counter)
            : m_ref_counter(ref_counter)
        {
        }

        inline o_object_header(std::pair<std::uint32_t, std::uint32_t> ref_counts)
            : m_ref_counter(ref_counts.first, ref_counts.second)
        {
        }
        
        void incRef(bool is_tag);        
        void decRef(bool is_tag);
        
        // check if any references exist (including auto-assigned type tags)
        bool hasRefs() const;
    };
    
    // Unique header for objects with unique instance id
    struct [[gnu::packed]] o_unique_header: public o_fixed_ext<o_unique_header, o_object_header>
    {
        // instance ID is decoded from object's address (see. db0::getInstanceId)
        std::uint16_t m_instance_id = 0;
        
        o_unique_header() = default;
        o_unique_header(const RefCounterT &ref_counter)
            : o_fixed_ext<o_unique_header, o_object_header>(ref_counter)
        {
        }
        
        o_unique_header(std::pair<std::uint32_t, std::uint32_t> ref_counts)
            : o_fixed_ext<o_unique_header, o_object_header>(ref_counts)
        {
        }
    };
    
}