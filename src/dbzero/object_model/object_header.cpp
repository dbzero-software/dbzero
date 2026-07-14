// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "object_header.hpp"
#include <dbzero/core/vspace/v_object.hpp>
#include <limits>

namespace db0

{
    
    void o_object_header::incRef(bool is_tag)
    {
        if (is_tag) {
            auto old_value = m_ref_counter.getFirst();
            if (old_value == std::numeric_limits<std::uint32_t>::max()) {
                THROWF(db0::InternalException) << "Too many tag references";
            }
            m_ref_counter.setFirst(old_value + 1);
        } else {
            auto old_value = m_ref_counter.getSecond();
            if (old_value == std::numeric_limits<std::uint32_t>::max()) {
                THROWF(db0::InternalException) << "Too many object references";
            }
            m_ref_counter.setSecond(old_value + 1);
        }        
    }
    
    bool o_object_header::decRef(bool is_tag)
    {
        auto values = m_ref_counter.get();
        if (is_tag) {
            assert(values.first > 0 && "Bad tags ref-count (trying to decrement below 0)");
            --values.first;
            m_ref_counter.setFirst(values.first);
        } else {
            assert(values.second > 0 && "Bad objects ref-count (trying to decrement below 0)");
            --values.second;
            m_ref_counter.setSecond(values.second);
        }
        return values.first == 0 && values.second == 0;
    }
    
    bool o_object_header::hasRefs() const
    {        
        auto refs = m_ref_counter.get();
        return refs.first != 0 || refs.second != 0;
    }
    
}