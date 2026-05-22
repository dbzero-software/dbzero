// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cassert>
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
        static constexpr std::uint16_t INSTANCE_ID_MASK = 0x3fff;
        static constexpr std::uint16_t RESERVED_FLAG = 0x4000;
        static constexpr std::uint16_t IMMUTABLE_FLAG = 0x8000;
        // instance ID is decoded from object's address (see. db0::getInstanceId).
        // The top 2 bits are not part of the instance ID; one stores the immutable-object flag
        // and the other is intentionally left unused for a future object-header flag.
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

        inline std::uint16_t getInstanceId() const
        {
            return m_instance_id & INSTANCE_ID_MASK;
        }

        inline void setInstanceId(std::uint16_t instance_id)
        {
            assert((instance_id & ~INSTANCE_ID_MASK) == 0);
            m_instance_id = (m_instance_id & ~INSTANCE_ID_MASK) | instance_id;
        }

        inline bool isImmutableObject() const
        {
            return (m_instance_id & IMMUTABLE_FLAG) != 0;
        }

        inline void setImmutableObject()
        {
            m_instance_id |= IMMUTABLE_FLAG;
        }
    };
DB0_PACKED_END
    
}
