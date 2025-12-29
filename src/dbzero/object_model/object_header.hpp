// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/serialization/ref_counter.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

    class Fixture;

    /// Common object header
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_object_header: public o_fixed_versioned<o_object_header>
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
        // @return true if reference count was decremented to zero
        bool decRef(bool is_tag);
        
        // check if any references exist (including auto-assigned type tags)
        bool hasRefs() const;
    };
DB0_PACKED_END

DB0_PACKED_BEGIN
    // Unique header for objects with unique instance id
    struct DB0_PACKED_ATTR o_unique_header: public o_fixed_ext<o_unique_header, o_object_header>
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
DB0_PACKED_END
    
}