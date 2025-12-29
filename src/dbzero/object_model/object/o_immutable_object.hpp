// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/object_model/object_header.hpp>
#include "ValueTable.hpp"
#include <dbzero/core/vspace/v_object.hpp>

namespace db0::object_model

{

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_immutable_object: public db0::o_base<o_immutable_object, 0, false>
    {
    protected:
        using super_t = db0::o_base<o_immutable_object, 0, false>;

    public:
        static constexpr unsigned char REALM_ID = 1;
        // common object header
        o_unique_header m_header;        
        // number of auto-assigned type tags
        std::uint8_t m_num_type_tags = 0;
        
        PosVT &pos_vt();
        const PosVT &pos_vt() const;

        const packed_int32 &classRef() const;
        std::uint32_t getClassRef() const;
        
        const IndexVT &index_vt() const;

        IndexVT &index_vt();

        // ref_counts - the initial reference counts (tags / objects) inherited from the initializer
        o_immutable_object(std::uint32_t class_ref, std::pair<std::uint32_t, std::uint32_t> ref_counts, std::uint8_t num_type_tags, 
            const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset, const XValue *index_vt_begin = nullptr, 
            const XValue *index_vt_end = nullptr);
        
        static std::size_t measure(std::uint32_t, std::pair<std::uint32_t, std::uint32_t>, std::uint8_t num_type_tags,
            const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset, const XValue *index_vt_begin = nullptr, 
            const XValue *index_vt_end = nullptr);
        
        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            return super_t::sizeOfMembers(buf)
                (PosVT::type())
                (packed_int32::type())            
                (IndexVT::type());
        }
        
        void incRef(bool is_tag);
        bool hasRefs() const;
        bool hasAnyRefs() const;
    };
DB0_PACKED_END
    
}