// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "o_object.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/value.hpp>

namespace db0::object_model

{

    o_object_base::o_object_base(std::pair<std::uint32_t, std::uint32_t> ref_counts)
        : m_header(ref_counts)                
    {
    }

    void o_object_base::incRef(bool is_tag) {
        m_header.incRef(is_tag);
    }
    
    bool o_object_base::hasAnyRefs() const {
        return m_header.hasRefs();
    }

    std::size_t o_object_base::measure() {
        return super_t::measureMembers();
    }

    std::size_t o_object_base::measure(std::pair<std::uint32_t, std::uint32_t>) {
        return super_t::measureMembers();
    }
    
    o_object::o_object(std::uint32_t class_ref, std::pair<std::uint32_t, std::uint32_t> ref_counts,
        std::uint8_t num_type_tags, const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset, 
        const XValue *index_vt_begin, const XValue *index_vt_end)
        : super_t(ref_counts)
        , m_num_type_tags(num_type_tags)
    {
        arrangeMembers()
            (PosVT::type(), pos_vt_data, pos_vt_offset)
            (packed_int32::type(), class_ref)
            (IndexVT::type(), index_vt_begin, index_vt_end);
    }
    
    std::size_t o_object::measure(std::uint32_t class_ref, std::pair<std::uint32_t, std::uint32_t> ref_counts, std::uint8_t,
        const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset,
        const XValue *index_vt_begin, const XValue *index_vt_end)
    {
        return super_t::measureMembersFromBase(ref_counts)
            (PosVT::type(), pos_vt_data, pos_vt_offset)
            (packed_int32::type(), class_ref)
            (IndexVT::type(), index_vt_begin, index_vt_end);
    }
    
    const PosVT &o_object::pos_vt() const {
        return getDynFirst(PosVT::type());
    }

    PosVT &o_object::pos_vt() {
        return getDynFirst(PosVT::type());
    }

    const packed_int32 &o_object::classRef() const {
        return getDynAfter(pos_vt(), packed_int32::type());
    }

    std::uint32_t o_object::getClassRef() const {
        return classRef().value();
    }

    const IndexVT &o_object::index_vt() const {
        return getDynAfter(classRef(), IndexVT::type());
    }
    
    IndexVT &o_object::index_vt() {
        return getDynAfter(classRef(), IndexVT::type());
    }
    
    bool o_object::hasRefs() const
    {
        // NOTE: type tags are not counted as "proper" references
        if (m_header.m_ref_counter.getFirst() > this->m_num_type_tags) {
            return true;
        }
        return m_header.m_ref_counter.getSecond() > 0;
    }
    
}