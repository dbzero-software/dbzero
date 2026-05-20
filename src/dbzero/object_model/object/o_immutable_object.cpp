// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "o_immutable_object.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/value.hpp>

namespace db0::object_model

{

    o_immutable_object::o_immutable_object(std::uint32_t class_ref, 
        std::pair<std::uint32_t, std::uint32_t> ref_counts, std::uint8_t num_type_tags,
        const ImmutableObjectInitializer &initializer)
        : m_header(ref_counts)
        , m_num_type_tags(num_type_tags)
    {
        arrangeMembers()
            (o_embedded_object::type(), class_ref, initializer);
    }

    o_immutable_object::o_immutable_object(std::uint32_t class_ref, 
        std::pair<std::uint32_t, std::uint32_t> ref_counts, std::uint8_t num_type_tags, const PosVT::Data &pos_vt_data, 
        unsigned int pos_vt_offset, const XValue *index_vt_begin, const XValue *index_vt_end)
        : m_header(ref_counts)        
        , m_num_type_tags(num_type_tags)
    {
        arrangeMembers()
            (o_embedded_object::type(), class_ref, pos_vt_data, pos_vt_offset, index_vt_begin, index_vt_end);
    }

    std::size_t o_immutable_object::measure(std::uint32_t class_ref,
        std::pair<std::uint32_t, std::uint32_t>, std::uint8_t, const ImmutableObjectInitializer &initializer)
    {
        return super_t::measureMembers()
            (o_embedded_object::type(), class_ref, initializer);
    }
    
    std::size_t o_immutable_object::measure(std::uint32_t class_ref,
        std::pair<std::uint32_t, std::uint32_t>, std::uint8_t, const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset, 
        const XValue *index_vt_begin, const XValue *index_vt_end)
    {
        return super_t::measureMembers()
            (o_embedded_object::type(), class_ref, pos_vt_data, pos_vt_offset, index_vt_begin, index_vt_end);
    }

    o_embedded_object &o_immutable_object::embeddedObject()
    {
        return getDynFirst(o_embedded_object::type());
    }

    const o_embedded_object &o_immutable_object::embeddedObject() const
    {
        return getDynFirst(o_embedded_object::type());
    }

    const PosVT &o_immutable_object::pos_vt() const {
        return embeddedObject().pos_vt();
    }

    PosVT &o_immutable_object::pos_vt() {
        return embeddedObject().pos_vt();
    }

    std::uint32_t o_immutable_object::getClassRef() const {
        return embeddedObject().getClassRef();
    }
    
    const IndexVT &o_immutable_object::index_vt() const {
        return embeddedObject().index_vt();
    }
    
    IndexVT &o_immutable_object::index_vt() {
        return embeddedObject().index_vt();
    }

    const o_dict &o_immutable_object::field_map() const
    {
        return embeddedObject().field_map();
    }

    std::optional<FixedValue> o_immutable_object::fixedValue(std::uint32_t index, unsigned int fidelityOffset) const
    {
        return embeddedObject().fixedValue(index, fidelityOffset);
    }

    const o_tuple_item *o_immutable_object::variableValue(std::uint32_t index) const
    {
        return embeddedObject().variableValue(index);
    }
    
    void o_immutable_object::incRef(bool is_tag) {
        m_header.incRef(is_tag);
    }
    
    bool o_immutable_object::hasRefs() const
    {
        // NOTE: type tags are not counted as "proper" references
        if (m_header.m_ref_counter.getFirst() > this->m_num_type_tags) {
            return true;
        }
        return m_header.m_ref_counter.getSecond() > 0;
    }
    
    bool o_immutable_object::hasAnyRefs() const {
        return m_header.hasRefs();
    }
    
}
